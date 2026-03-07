from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
from dataclasses import dataclass
import logging
import re
import time
from typing import TypeVar
from urllib.parse import parse_qs, urlsplit, urlunsplit

import httpx
from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from tracker.formatting import extract_llm_summary_why, format_digest_markdown, format_im_text
from tracker.connectors.base import FetchedEntry
from tracker.health_reporting import format_health_markdown
from tracker.feed_discovery import discover_feed_urls_from_html
from tracker.pipeline import (
    CreatedDecision,
    fetch_entries_for_source,
    ingest_entries_for_topic_source,
    is_near_duplicate,
)
from tracker.connectors.rss import RssConnector
from tracker.connectors.searxng import build_searxng_search_url, normalize_searxng_base_url, normalize_searxng_search_url
from tracker.push_dispatch import (
    push_dingtalk_markdown,
    push_email_text,
    push_telegram_report_reader,
    push_telegram_text,
    push_webhook_json,
)
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.timezones import resolve_cron_timezone
from tracker.alert_budget import can_send_alert_under_budget, record_alert_delivery
from tracker.models import Item, ItemTopic, Source, Topic, TopicSource
from tracker.llm import (
    llm_curate_topic_items,
    llm_decide_source_candidates,
    llm_gate_alert_candidate,
    llm_guess_feed_urls,
    llm_localize_item_titles,
    llm_triage_topic_items,
)
from tracker.llm_usage import make_llm_usage_recorder
from tracker.fulltext import fetch_fulltext_for_url
from tracker.http_auth import AuthRequiredError, cookie_header_for_url, host_matches_any, parse_cookie_jar_json, parse_domains_csv
from tracker.normalize import canonicalize_url, normalize_text
from tracker.search_query import normalize_search_query
from tracker.simhash import int_to_signed64, simhash64
from tracker.story import story_dedupe_text

_T = TypeVar("_T")

logger = logging.getLogger(__name__)

_LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1"}
_URL_RE = re.compile(r"https?://[^\s<>\")\]]+", re.IGNORECASE)


def _is_local_url(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return False
    try:
        host = (urlsplit(u).hostname or "").strip().lower()
    except Exception:
        host = ""
    return host in _LOCAL_HOSTS


def _rewrite_local_url_to_source_host(*, url: str, source_url: str) -> str:
    """
    Best-effort fix for placeholder links like https://localhost/stream/<id>.

    If `url` is a local host, rewrite it to use the host of `source_url` so the user can open it.
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    if not _is_local_url(raw):
        return raw
    try:
        src = urlsplit((source_url or "").strip())
        src_host = (src.hostname or "").strip()
        src_scheme = (src.scheme or "").strip().lower() or "https"
        if not src_host or src_host.lower() in _LOCAL_HOSTS:
            return raw
        if src_scheme not in {"http", "https"}:
            src_scheme = "https"
        parts = urlsplit(raw)
        return urlunsplit((src_scheme, src.netloc or src_host, parts.path or "/", parts.query or "", parts.fragment or ""))
    except Exception:
        return raw


def _best_push_url_for_item(*, item: Item, source: Source | None) -> str:
    """
    Produce a user-openable URL for push surfaces.

    In rare cases, some RSS feeds publish placeholder item links under localhost. We still want to
    push the content, but never with an unopenable localhost link.
    """
    url = (item.canonical_url or item.url or "").strip()
    if not url:
        return ""
    if not _is_local_url(url):
        return url
    # Prefer the first real external link found in the snippet/body when the feed link is a
    # localhost placeholder (e.g. https://localhost/stream/<id>).
    try:
        snippet = str(getattr(item, "content_text", "") or "").strip()
    except Exception:
        snippet = ""
    for u in _URL_RE.findall(snippet or ""):
        uu = (u or "").strip().rstrip(").,;]")
        if not uu:
            continue
        if _is_local_url(uu):
            continue
        return uu
    if source:
        return _rewrite_local_url_to_source_host(url=url, source_url=source.url)
    return url

def _tpl(
    repo: Repo,
    settings: Settings,
    slot_id: str,
    context: dict[str, object] | None = None,
    *,
    language: str | None = None,
) -> str:
    from tracker.prompt_templates import resolve_prompt_best_effort

    lang = language if language in {"zh", "en"} else None
    return resolve_prompt_best_effort(
        repo=repo,
        settings=settings,
        slot_id=slot_id,
        context=context,
        language=lang,  # type: ignore[arg-type]
    ).text


def _output_lang(*, repo: Repo, settings: Settings) -> str:
    raw = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
    low = raw.lower()
    if raw in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语"}:
        return "zh"
    if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
        return "zh"
    if low in {"en", "en-us", "english", "英文"} or low.startswith("en"):
        return "en"
    return "en"


def _norm_host(value: str) -> str:
    h = (value or "").strip().lower()
    if not h:
        return ""
    h = h.split("/", 1)[0]
    h = h.split(":", 1)[0]
    h = h.lstrip(".")
    if h.startswith("www."):
        h = h[4:]
    return h


def _url_host(url: str) -> str:
    try:
        host = (urlsplit((url or "").strip()).netloc or "").strip()
    except Exception:
        host = ""
    return _norm_host(host)


def _annotate_candidates_domain_feedback(*, repo: Repo, candidates: list[dict], days: int = 90) -> None:
    """
    Attach lightweight domain context to candidate dicts for LLM triage/curation.

    This lets the model learn from operator feedback (👍/👎) without hard-coding domain blocklists.
    """
    if not candidates:
        return
    domains: list[str] = []
    for c in candidates:
        try:
            u = str(c.get("url", "") or "").strip()
        except Exception:
            u = ""
        d = _url_host(u)
        if d:
            domains.append(d)
    if not domains:
        return
    since = dt.datetime.utcnow() - dt.timedelta(days=max(1, int(days or 1)))
    try:
        stats = repo.summarize_feedback_by_domain(domains=domains, since=since, kinds=["like", "dislike"])
    except Exception:
        stats = {}
    for c in candidates:
        try:
            u = str(c.get("url", "") or "").strip()
        except Exception:
            u = ""
        d = _url_host(u)
        if not d:
            continue
        c.setdefault("domain", d)
        bucket = stats.get(d) or {}
        try:
            c["domain_likes"] = int(bucket.get("like") or 0)
        except Exception:
            c["domain_likes"] = 0
        try:
            c["domain_dislikes"] = int(bucket.get("dislike") or 0)
        except Exception:
            c["domain_dislikes"] = 0


def _login_required_appendix_markdown(*, repo: Repo, settings: Settings, lang: str) -> str:
    """
    Build a short appendix listing sources that currently require re-login.

    Rationale: we already emit an idempotent "auth required" alert (once per host/day),
    but operators can miss it; digests/Curated Info are the reliable surface.
    """
    rows = repo.list_sources_with_auth_required()
    if not rows:
        return ""

    is_zh = (lang or "").strip().lower().startswith("zh") or (lang or "").strip().lower() in {"cn"}

    by_host: dict[str, dict[str, object]] = {}
    for src, health in rows:
        host = _url_host(src.url)
        if not host:
            continue
        bucket = by_host.setdefault(host, {"source_ids": set(), "last_error_at": None})
        try:
            bucket["source_ids"].add(int(src.id))  # type: ignore[union-attr]
        except Exception:
            pass
        ts = getattr(health, "last_error_at", None)
        if ts is not None:
            prev = bucket.get("last_error_at")
            if prev is None or (isinstance(prev, dt.datetime) and ts > prev):
                bucket["last_error_at"] = ts

    if not by_host:
        return ""

    # Render in operator timezone when available.
    tz_name = (getattr(settings, "cron_timezone", "") or "").strip()
    tz, tz_ok = resolve_cron_timezone(tz_name)
    if not tz_ok:
        tz = dt.timezone.utc

    def _fmt_ts(ts: dt.datetime | None) -> str:
        if ts is None:
            return ""
        try:
            local = ts.replace(tzinfo=dt.timezone.utc).astimezone(tz)
            return local.isoformat()
        except Exception:
            return ""

    max_hosts = 12
    hosts_sorted = sorted(
        by_host.items(),
        key=lambda kv: (
            -(int(kv[1].get("last_error_at").timestamp()) if isinstance(kv[1].get("last_error_at"), dt.datetime) else 0),
            kv[0],
        ),
    )[:max_hosts]

    lines: list[str] = []
    lines.append("## Login Required" if not is_zh else "## 需要重新登录")
    lines.append("")
    lines.append(
        (
            "Some sources require authentication to fetch. These hosts recently returned a login/redirect response."
            if not is_zh
            else "以下信息源需要登录才能抓取（最近返回了登录/跳转响应）。"
        )
    )
    lines.append("")
    for host, info in hosts_sorted:
        ids = sorted(int(x) for x in (info.get("source_ids") or set()) if isinstance(x, int))
        ids_txt = ", ".join(f"#{i}" for i in ids[:6])
        if len(ids) > 6:
            ids_txt = f"{ids_txt}, …(+{len(ids) - 6})"
        ts_txt = _fmt_ts(info.get("last_error_at") if isinstance(info.get("last_error_at"), dt.datetime) else None)
        tail = []
        if ids_txt:
            tail.append(f"sources: {ids_txt}")
        if ts_txt:
            tail.append(f"last: {ts_txt}")
        suffix = f" ({'; '.join(tail)})" if tail else ""
        lines.append(f"- {host}{suffix}")
    return "\n".join(lines).strip() + "\n"


def _append_login_required_section(*, markdown: str, repo: Repo, settings: Settings, lang: str) -> str:
    appendix = _login_required_appendix_markdown(repo=repo, settings=settings, lang=lang)
    if not appendix:
        return markdown
    base = (markdown or "").rstrip()
    return (base + "\n\n" + appendix.strip() + "\n").lstrip()


def _local_day_iso(settings: Settings, *, when: dt.datetime | None = None) -> str:
    """
    Stable day boundary for idempotency keys (digest/health) and payloads.

    We intentionally align the day with TRACKER_CRON_TIMEZONE (not UTC) to match
    operator expectations (e.g. Asia/Shanghai).
    """
    name = (getattr(settings, "cron_timezone", None) or "").strip()
    tz, tz_ok = resolve_cron_timezone(name)
    if not tz_ok:
        tz = dt.timezone.utc

    ref = when
    if ref is None:
        ref = dt.datetime.now(tz=tz)
    else:
        if ref.tzinfo is None:
            # Treat naive datetimes as UTC (internal convention).
            ref = ref.replace(tzinfo=dt.timezone.utc)
        ref = ref.astimezone(tz)

    try:
        return ref.date().isoformat()
    except Exception:
        return dt.datetime.utcnow().date().isoformat()


_CJK_TEXT_RE = re.compile(r"[\u4e00-\u9fff]")
_TITLE_PATH_HINT_RE = re.compile(r"(?:^|/)(?:readme|docs?|blob|tree|src|packages?|apps?|examples?)(?:/|$)", re.IGNORECASE)
_TITLE_FILE_HINT_RE = re.compile(r"\.(?:md|rst|txt|json|ya?ml|toml|ini|cfg|py|ts|tsx|js|jsx|go|rs|java|kt|sh|ps1)(?:\b|\s)", re.IGNORECASE)


def _contains_cjk_text(text: str) -> bool:
    return bool(_CJK_TEXT_RE.search(text or ""))


def _title_needs_translation(*, title: str, target_lang: str) -> bool:
    t = (title or "").strip()
    lang = (target_lang or "").strip().lower()
    if not t:
        return False
    if lang.startswith("zh") or lang == "cn":
        return not _contains_cjk_text(t)
    if lang.startswith("en"):
        return _contains_cjk_text(t)
    return False


def _looks_low_signal_title(title: str) -> bool:
    t = " ".join((title or "").split())
    low = t.lower()
    if not t:
        return True
    if "..." in t or "…" in t:
        return True
    if low.startswith(("ask hn:", "show hn:", "tell hn:")):
        return True
    if t.count("/") >= 2 and (_TITLE_PATH_HINT_RE.search(t) or _TITLE_FILE_HINT_RE.search(t)):
        return True
    if low.endswith(" - github") and t.count("/") >= 1:
        return True
    if low.endswith(" · github") and t.count("/") >= 1:
        return True
    return False


def _short_title_fallback(*, text: str, target_lang: str) -> str:
    s = " ".join((text or "").split()).strip()
    if not s:
        return ""
    limit = 54 if (target_lang or "").strip().lower().startswith("zh") or _contains_cjk_text(s) else 120
    if len(s) <= limit:
        return s
    return s[:limit].rstrip() + "…"


async def _localize_item_display_titles(
    *,
    repo: Repo,
    settings: Settings,
    entries: list[dict],
    out_lang: str,
    usage_cb=None,
) -> dict[int, str]:
    target_lang = (out_lang or "").strip().lower()
    if target_lang not in {"zh", "en"}:
        return {}

    cookie_jar = parse_cookie_jar_json(getattr(settings, "cookie_jar_json", "") or "")

    async def _cookie_header_cb(url: str) -> str | None:
        static_cookie = cookie_header_for_url(url=url, cookie_jar=cookie_jar)
        return static_cookie or None

    candidates: list[dict] = []
    fallback_titles: dict[int, str] = {}
    fulltext_fetches = 0
    max_fulltext_fetches = 3

    for entry in entries:
        try:
            item_id = int(entry.get("item_id") or 0)
        except Exception:
            item_id = 0
        if item_id <= 0:
            continue
        title = str(entry.get("title") or "").strip()
        url = str(entry.get("url") or "").strip()
        summary = str(entry.get("summary") or "").strip()
        why = str(entry.get("why") or "").strip()
        content_text = str(entry.get("content_text") or "").strip()
        needs_translation = _title_needs_translation(title=title, target_lang=target_lang)
        low_signal = _looks_low_signal_title(title)
        if not (needs_translation or low_signal):
            continue

        snippet_parts: list[str] = []
        if summary:
            snippet_parts.append(summary)
        if why:
            snippet_parts.append(why)
        if content_text:
            snippet_parts.append(content_text)
        else:
            row = repo.get_item_content(item_id=item_id)
            cached = (row.content_text if row and row.content_text else "").strip() if row else ""
            if cached:
                snippet_parts.append(cached)
            elif low_signal and fulltext_fetches < max_fulltext_fetches and url.startswith(("http://", "https://")):
                try:
                    cookie = await _cookie_header_cb(url)
                    text = await fetch_fulltext_for_url(
                        url=url,
                        timeout_seconds=min(12, int(settings.fulltext_timeout_seconds or settings.http_timeout_seconds or 12)),
                        max_chars=min(6000, int(settings.fulltext_max_chars or 6000)),
                        discourse_cookie=((settings.discourse_cookie or "").strip() or cookie or None),
                        cookie_header=cookie,
                    )
                    text = (text or "").strip()
                    if text:
                        repo.upsert_item_content(item_id=item_id, url=url, content_text=text, error="")
                        snippet_parts.append(text)
                        fulltext_fetches += 1
                except Exception as exc:
                    logger.info("title fulltext fetch failed: item_id=%s url=%s err=%s", item_id, url, exc)

        if summary and ((target_lang == "zh" and _contains_cjk_text(summary)) or (target_lang == "en" and not _contains_cjk_text(summary))):
            fallback_titles[item_id] = _short_title_fallback(text=summary, target_lang=target_lang)

        snippet = "\n\n".join([p.strip() for p in snippet_parts if (p or "").strip()])
        if len(snippet) > 2400:
            snippet = snippet[:2400].rstrip() + "…"
        candidates.append(
            {
                "item_id": item_id,
                "title": title,
                "url": url,
                "summary": summary,
                "snippet": snippet,
                "low_signal": low_signal,
                "needs_translation": needs_translation,
            }
        )

    localized: dict[int, str] = {}
    if candidates:
        try:
            out = await llm_localize_item_titles(
                repo=repo,
                settings=settings,
                target_lang=target_lang,
                items=candidates,
                usage_cb=usage_cb,
            )
            if out:
                localized.update({int(k): str(v).strip() for k, v in out.items() if int(k) > 0 and str(v).strip()})
        except Exception as exc:
            logger.warning("title localization failed: %s", exc)

    for item_id, fallback in fallback_titles.items():
        if item_id not in localized and fallback:
            localized[item_id] = fallback
    return localized


def _format_llm_curation_reason(*, summary: str, why: str, hint: str | None = None) -> str:
    lines: list[str] = []
    s = (summary or "").strip()
    w = (why or "").strip()
    h = (hint or "").strip()
    if s:
        lines.append(f"llm_summary: {s}")
    if w:
        lines.append(f"llm_why: {w}")
    if h:
        lines.append(f"llm_hint: {h}")
    return "\n".join(lines).strip()


def _format_alert_markdown(*, topic_name: str, title: str, url: str, reason: str, lang: str = "en") -> str:
    summary, why = extract_llm_summary_why(reason)
    is_zh = (lang or "").strip().lower().startswith("zh") or (lang or "").strip().lower() in {"cn"}
    head = f"# Alert: {topic_name}" if not is_zh else f"# 提醒: {topic_name}"
    lines = [
        head,
        "",
        f"- [{title}]({url})",
    ]
    if summary:
        lines.append(f"- {summary}")
    elif why:
        lines.append(f"- {why}")
    if not (summary or why):
        r = (reason or "").strip()
        if r:
            label = "Reason" if not is_zh else "原因"
            lines += ["", f"{label}: {r}"]
    return "\n".join(lines).strip() + "\n"


def _format_alert_text(*, title: str, url: str, reason: str, lang: str = "en") -> str:
    summary, why = extract_llm_summary_why(reason)
    is_zh = (lang or "").strip().lower().startswith("zh") or (lang or "").strip().lower() in {"cn"}
    lines = [title, url]
    if summary:
        lines.append(f"\n{summary}")
    elif why:
        lines.append(f"\n{why}")
    if not (summary or why):
        r = (reason or "").strip()
        if r:
            label = "Reason" if not is_zh else "原因"
            lines.append(f"\n{label}: {r}")
    return "\n".join(lines).strip() + "\n"


def _fallback_alert_to_digest(*, session: Session, repo: Repo, item_id: int, topic_id: int, reason: str, note: str) -> None:
    """Keep a suppressed alert in the next digest instead of leaving it as an unsent alert."""
    try:
        it_row = repo.get_item_topic(item_id=int(item_id or 0), topic_id=int(topic_id or 0))
    except Exception:
        it_row = None
    if not it_row:
        return
    base_reason = (reason or getattr(it_row, "reason", "") or "").strip()
    extra = (note or "").strip()
    if extra and extra not in base_reason:
        base_reason = f"{base_reason}\n{extra}".strip() if base_reason else extra
    it_row.decision = "digest"
    it_row.reason = base_reason
    session.commit()


@dataclass(frozen=True)
class TickSourceResult:
    topic_name: str
    source_url: str
    created: int
    pushed_alerts: int
    error: str | None = None

@dataclass(frozen=True)
class SourceCandidatePreview:
    fetch_url: str
    titles: list[str]
    source_content: str
    signature: str


def _preferred_candidate_fetch_url(*, candidate_url: str, discovered_from_url: str = "") -> str:
    fetch_url = str(candidate_url or "").strip()
    try:
        if discovered_from_url:
            src = urlsplit(str(discovered_from_url or "").strip())
            dst = urlsplit(fetch_url)
            src_host = (src.hostname or "").strip().lower()
            dst_host = (dst.hostname or "").strip().lower()
            if src_host.startswith("www.") and dst_host and dst_host == src_host[4:]:
                fetch_url = urlunsplit((dst.scheme or src.scheme, src.netloc, dst.path, dst.query, dst.fragment))
    except Exception:
        fetch_url = str(candidate_url or "").strip()
    return fetch_url


def _build_source_candidate_preview(*, entries: list[FetchedEntry], preview_limit: int) -> SourceCandidatePreview | None:
    lines: list[str] = []
    titles: list[str] = []
    sig_tokens: list[str] = []
    for e in entries[: max(1, preview_limit)]:
        title = str(getattr(e, "title", "") or "").strip()
        entry_url = str(getattr(e, "url", "") or "").strip()
        summary = str(getattr(e, "summary", "") or "").strip()
        if title:
            titles.append(title)
        if title and entry_url:
            lines.append(f"- {title} | {entry_url}")
        elif title:
            lines.append(f"- {title}")
        elif entry_url:
            lines.append(f"- {entry_url}")
        if summary:
            lines.append(f"  summary: {' '.join(summary.split())[:320]}")
        token = entry_url or normalize_text(title)
        token = str(token or "").strip()
        if token:
            sig_tokens.append(token[:500])
    source_content = "\n".join(lines).strip()
    if not source_content:
        return None
    signature = hashlib.sha1("\n".join(sig_tokens or [source_content[:1200]]).encode("utf-8", errors="ignore")).hexdigest()
    return SourceCandidatePreview(fetch_url="", titles=titles, source_content=source_content, signature=signature)


async def _fetch_source_candidate_preview(
    *,
    connector: RssConnector,
    candidate_url: str,
    discovered_from_url: str = "",
    preview_limit: int,
) -> SourceCandidatePreview | None:
    attempts: list[str] = []
    preferred = _preferred_candidate_fetch_url(candidate_url=candidate_url, discovered_from_url=discovered_from_url)
    for fetch_url in [preferred, str(candidate_url or "").strip()]:
        url = str(fetch_url or "").strip()
        if not url or url in attempts:
            continue
        attempts.append(url)
        try:
            entries = await connector.fetch(url=url)
        except Exception:
            continue
        preview = _build_source_candidate_preview(entries=entries, preview_limit=preview_limit)
        if preview is not None:
            return SourceCandidatePreview(
                fetch_url=url,
                titles=list(preview.titles),
                source_content=preview.source_content,
                signature=preview.signature,
            )
    return None



@dataclass(frozen=True)
class TickResult:
    total_created: int
    total_pushed_alerts: int
    per_source: list[TickSourceResult]


@dataclass(frozen=True)
class DigestTopicResult:
    topic_name: str
    pushed: int
    markdown: str
    idempotency_key: str = ""


@dataclass(frozen=True)
class DigestResult:
    since: dt.datetime
    per_topic: list[DigestTopicResult]


@dataclass(frozen=True)
class CuratedInfoResult:
    """
    Cross-topic Curated Info (batch) result.

    This is the "noise reduction" batch surface: de-dupe only, no interpretation.
    """

    since: dt.datetime
    pushed: int
    markdown: str
    idempotency_key: str = ""


@dataclass(frozen=True)
class HealthResult:
    pushed: int
    markdown: str


async def run_tick(*, session: Session, settings: Settings, push: bool) -> TickResult:
    repo = Repo(session)
    # Apply DB-backed dynamic overrides for non-secret Settings fields.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass

    # Best-effort self-heal for `searxng_search` sources when operators change SearxNG base URL/port.
    #
    # Background: search sources are often auto-created by Smart Config using a guessed base URL.
    # If the port later drifts (common in docker-compose), those sources can get repeatedly failing
    # and eventually auto-disabled, which makes installs feel "stuck" with no search recall.
    try:
        searx_base = normalize_searxng_base_url((getattr(settings, "searxng_base_url", "") or "").strip())
    except Exception:
        searx_base = ""
    if searx_base:
        try:
            last_raw = (repo.get_app_config("searxng_search_repair_last_at_utc") or "").strip()
        except Exception:
            last_raw = ""
        last_dt: dt.datetime | None = None
        if last_raw:
            try:
                last_dt = dt.datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
                if last_dt.tzinfo is not None:
                    last_dt = last_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)
            except Exception:
                last_dt = None

        # Avoid doing this too often; it's a tiny maintenance step.
        if last_dt is None or (dt.datetime.utcnow() - last_dt).total_seconds() > 600:
            now2 = dt.datetime.utcnow()
            # Only auto-enable disabled search seeds when SearxNG itself is reachable.
            # This avoids flip-flopping (enable → fail → auto-disable) during outages.
            searx_ready = False
            try:
                probe_url = build_searxng_search_url(
                    base_url=searx_base,
                    query="openinfomate",
                    time_range="day",
                    results=1,
                )
                # SearxNG queries can take a couple seconds even on localhost (it fans out to
                # multiple engines). Use a small-but-not-tiny timeout so we don't treat "slow"
                # as "down" and keep seeds disabled forever.
                probe_timeout = 5.0
                try:
                    probe_timeout = float(getattr(settings, "http_timeout_seconds", 20) or 20)
                except Exception:
                    probe_timeout = 20.0
                probe_timeout = max(1.5, min(5.0, probe_timeout))
                async with httpx.AsyncClient(timeout=probe_timeout, follow_redirects=True) as client:
                    resp0 = await client.get(probe_url, headers={"User-Agent": "tracker/0.1"})
                if int(getattr(resp0, "status_code", 0) or 0) == 200:
                    try:
                        obj0 = resp0.json()
                    except Exception:
                        obj0 = None
                    searx_ready = isinstance(obj0, dict) and isinstance(obj0.get("results"), list)
            except Exception:
                searx_ready = False
            changed_any = False
            try:
                for src, health, meta in repo.list_sources_with_health_and_meta():
                    if not src or (src.type or "").strip() != "searxng_search":
                        continue
                    old_url = str(getattr(src, "url", "") or "").strip()
                    if not old_url:
                        continue

                    old_norm = normalize_searxng_search_url(old_url) or old_url
                    try:
                        parts = urlsplit(old_norm)
                        qs = parse_qs(parts.query or "")
                    except Exception:
                        qs = {}
                    q = (qs.get("q") or [""])[0].strip()
                    if not q:
                        continue

                    categories = (qs.get("categories") or [""])[0].strip() or None
                    time_range = (qs.get("time_range") or [""])[0].strip() or None
                    language = (qs.get("language") or [""])[0].strip() or None
                    try:
                        safesearch_s = (qs.get("safesearch") or [""])[0].strip()
                        safesearch = int(safesearch_s) if safesearch_s else None
                    except Exception:
                        safesearch = None
                    try:
                        results_s = (qs.get("results") or [""])[0].strip()
                        results = int(results_s) if results_s else None
                    except Exception:
                        results = None

                    new_url = build_searxng_search_url(
                        base_url=searx_base,
                        query=q[:400],
                        categories=categories,
                        time_range=time_range,
                        language=language,
                        safesearch=safesearch,
                        results=results,
                    )
                    url_changed = bool(new_url and new_url != old_norm)
                    if url_changed:
                        src.url = new_url
                        changed_any = True

                    if not bool(getattr(src, "enabled", True)):
                        tags = str(getattr(meta, "tags", "") or "")
                        notes = str(getattr(meta, "notes", "") or "")
                        last_err = str(getattr(health, "last_error", "") or "")
                        err_count = int(getattr(health, "error_count", 0) or 0) if health else 0

                        # Detect likely auto-disabled sources (due to errors), and also handle
                        # rare cases where an install ends up with disabled seeds but no health row.
                        looks_auto = ("disabled:" in tags) or ("[auto-disabled]" in notes) or (err_count > 0) or bool(last_err)
                        try:
                            base_old = normalize_searxng_base_url(old_norm)
                        except Exception:
                            base_old = ""
                        base_matches = bool(base_old and base_old == searx_base)
                        never_checked = (
                            (getattr(src, "last_checked_at", None) is None)
                            and (health is None)
                            and (not tags.strip())
                            and (not notes.strip())
                        )

                        enable_note = ""
                        should_enable = False
                        if searx_ready:
                            if url_changed:
                                should_enable = True
                                enable_note = "[auto-enabled:searxng-repair:url_changed]"
                            elif looks_auto:
                                should_enable = True
                                enable_note = "[auto-enabled:searxng-repair:auto_disabled]"
                            elif base_matches and never_checked:
                                should_enable = True
                                enable_note = "[auto-enabled:searxng-repair:seed_disabled]"

                        if should_enable:
                            src.enabled = True
                            changed_any = True

                            # Record a small hint for operators and to avoid repeating this auto-fix.
                            try:
                                meta2 = meta or repo.get_or_create_source_meta(source_id=int(src.id))
                                cur_notes = str(getattr(meta2, "notes", "") or "")
                                if enable_note and enable_note not in cur_notes:
                                    meta2.notes = (cur_notes.rstrip() + "\n" + enable_note).strip() if cur_notes.strip() else enable_note
                            except Exception:
                                pass

                            # Ensure the next tick fetches it soon.
                            try:
                                health2 = health or repo.get_or_create_source_health(source_id=int(src.id))
                                health2.error_count = 0
                                health2.last_error = ""
                                health2.last_error_at = None
                                health2.next_fetch_at = now2
                            except Exception:
                                pass
            except Exception:
                changed_any = False

            if changed_any:
                try:
                    session.commit()
                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass
            try:
                repo.set_app_config("searxng_search_repair_last_at_utc", now2.isoformat() + "Z")
            except Exception:
                pass

    out_lang = _output_lang(repo=repo, settings=settings)
    try:
        settings_out = settings.model_copy(update={"output_language": out_lang})  # type: ignore[attr-defined]
    except Exception:
        settings_out = settings
    llm_usage_cb = make_llm_usage_recorder(session=session)
    per_source: list[TickSourceResult] = []
    total_created = 0
    total_pushed_alerts = 0
    created_item_ids: set[int] = set()
    llm_used_by_topic: dict[int, int] = {}
    pushed_alerts_by_topic: dict[int, int] = {}
    policies_by_topic_id = {p.topic_id: p for p in repo.list_topic_policies()}
    new_candidate_ids_by_topic: dict[int, set[int]] = {}
    topic_by_id: dict[int, Topic] = {}

    # Explicit operator feedback: muted domains should not generate alerts/digests.
    active_mute_domains: set[str] = set()
    try:
        active_mute_domains = {
            (m.key or "").strip().lower()
            for m in repo.list_active_mute_rules()
            if (getattr(m, "scope", "") or "").strip() == "domain" and (m.key or "").strip()
        }
    except Exception:
        active_mute_domains = set()

    # Quality tiering (optional): low-quality domains should not be pushed as alerts.
    try:
        from tracker.domain_quality import build_domain_quality_policy

        domain_policy = build_domain_quality_policy(settings=settings)
        if (
            (not domain_policy.low_patterns)
            and (not domain_policy.medium_patterns)
            and (not domain_policy.high_patterns)
            and int(domain_policy.min_push_rank) == 1
        ):
            domain_policy = None
    except Exception:
        domain_policy = None

    # Source score hard filter (0..100). Falls back to a tier-derived numeric score when missing.
    try:
        min_source_score = int(getattr(settings, "source_quality_min_score", 0) or 0)
    except Exception:
        min_source_score = 0
    min_source_score = max(0, min(100, int(min_source_score)))
    scores_by_source_id: dict[int, int] = {}
    locked_by_source_id: dict[int, bool] = {}
    try:
        for sc in repo.list_source_scores(limit=10_000):
            sid = int(getattr(sc, "source_id", 0) or 0)
            if sid <= 0:
                continue
            scores_by_source_id[sid] = int(getattr(sc, "score", 0) or 0)
            locked_by_source_id[sid] = bool(getattr(sc, "locked", False))
    except Exception:
        scores_by_source_id = {}
        locked_by_source_id = {}

    def _tier_score_for_url(url: str) -> int:
        if not domain_policy:
            return 50
        try:
            tier = str(domain_policy.tier_for_url(url) or "unknown").strip().lower()
        except Exception:
            tier = "unknown"
        if tier == "high":
            return 75
        if tier == "medium":
            return 55
        if tier == "low":
            return 35
        return 45

    def _effective_source_score(*, source_id: int, source_url: str) -> int:
        sid = int(source_id or 0)
        if sid > 0 and sid in scores_by_source_id:
            return max(0, min(100, int(scores_by_source_id.get(sid) or 0)))
        return _tier_score_for_url(source_url)

    has_dingtalk = bool(settings.dingtalk_webhook_url)
    telegram_chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
    has_telegram = bool(settings.telegram_bot_token and telegram_chat_id)
    has_email = bool(settings.smtp_host and settings.email_from and settings.email_to)
    has_webhook = bool(settings.webhook_url)
    has_any_channel = has_dingtalk or has_telegram or has_email or has_webhook

    # --- Auth / Cookie jar (optional)
    cookie_jar = parse_cookie_jar_json(getattr(settings, "cookie_jar_json", "") or "")

    def _url_host(url: str) -> str:
        try:
            return (urlsplit((url or "").strip()).netloc or "").strip()
        except Exception:
            return ""

    async def _cookie_header_cb(url: str) -> str | None:
        # Static cookie jar only.
        static_cookie = cookie_header_for_url(url=url, cookie_jar=cookie_jar)
        return static_cookie or None

    pairs = repo.list_enabled_topic_sources()
    # Keep a stable topic map for tick-time LLM curation, even when a topic
    # has no *new* candidates (we may still need to drain backlog).
    for topic, _source, _ts in pairs:
        if topic and topic.id not in topic_by_id:
            topic_by_id[topic.id] = topic
    by_source: dict[int, tuple[Source, list[tuple[Topic, TopicSource]]]] = {}
    for topic, source, ts in pairs:
        entry = by_source.get(source.id)
        if not entry:
            by_source[source.id] = (source, [(topic, ts)])
        else:
            entry[1].append((topic, ts))

    def _host_key(url: str) -> str:
        parts = urlsplit(url)
        if parts.scheme == "file":
            return "file"
        if parts.scheme == "html-list":
            try:
                from tracker.connectors.html_list import parse_html_list_url

                spec = parse_html_list_url(url)
                inner = urlsplit(spec.page_url)
                if inner.scheme == "file":
                    return "file"
                return inner.netloc or inner.scheme or "unknown"
            except Exception:
                return "html_list"
        return parts.netloc or parts.scheme or "unknown"

    source_jobs: list[
        tuple[
            Source,
            list[tuple[Topic, TopicSource]],
            dt.datetime,
            dt.datetime | None,  # prev_checked_at (before we update it)
        ]
    ] = []
    for source_id in sorted(by_source.keys()):
        source, bindings = by_source[source_id]
        prev_checked_at = source.last_checked_at
        now = dt.datetime.utcnow()

        min_interval = {
            "rss": settings.rss_min_interval_seconds,
            "hn_search": settings.hn_min_interval_seconds,
            "searxng_search": settings.searxng_min_interval_seconds,
            "discourse": settings.discourse_min_interval_seconds,
            "html_list": settings.rss_min_interval_seconds,
        }.get(source.type, settings.rss_min_interval_seconds)

        if source.last_checked_at and (now - source.last_checked_at) < dt.timedelta(seconds=min_interval):
            for topic, _ts in bindings:
                per_source.append(
                    TickSourceResult(
                        topic_name=topic.name,
                        source_url=source.url,
                        created=0,
                        pushed_alerts=0,
                        error=f"skipped: min_interval {min_interval}s",
                    )
                )
            continue

        health = repo.get_source_health(source_id=source.id)
        if health and health.next_fetch_at and now < health.next_fetch_at:
            for topic, _ts in bindings:
                per_source.append(
                    TickSourceResult(
                        topic_name=topic.name,
                        source_url=source.url,
                        created=0,
                        pushed_alerts=0,
                        error=f"skipped: backoff until {health.next_fetch_at.isoformat()}",
                    )
                )
            continue

        # Avoid a "query-triggered autoflush" inside this loop (SQLite can be locked).
        # We'll mark `last_checked_at` for all fetchable sources in a single flush/commit
        # right after we finish the backoff/min-interval checks.
        source_jobs.append((source, bindings, now, prev_checked_at))

    # Only mark a source as checked once its fetch attempt has actually completed.
    # Otherwise a crash between scheduling and completion creates false freshness and
    # can silently suppress the next tick via min-interval skipping.

    # Discourse-specific recall: when a Discourse source is stale (service downtime),
    # merge a Top Daily RSS feed on the next fetch to avoid missing older-but-important posts
    # that fell out of latest.json.
    discourse_include_top_daily_by_source_id: dict[int, bool] = {}
    discourse_stale_threshold = max(
        0,
        int(getattr(settings, "discourse_recall_top_rss_if_stale_seconds", 3600) or 3600),
    )
    if discourse_stale_threshold > 0:
        for source, _bindings, now, prev_checked_at in source_jobs:
            if source.type != "discourse":
                continue
            include_top = False
            if prev_checked_at is None:
                include_top = True
            else:
                try:
                    include_top = (now - prev_checked_at) >= dt.timedelta(seconds=discourse_stale_threshold)
                except Exception:
                    include_top = True
            discourse_include_top_daily_by_source_id[source.id] = include_top

    logger.info(
        "tick start: bindings=%d sources=%d to_fetch=%d push=%s",
        len(pairs),
        len(by_source),
        len(source_jobs),
        push,
    )

    fetch_sem = asyncio.Semaphore(max(1, settings.max_concurrent_fetches))
    host_sems: dict[str, asyncio.Semaphore] = {}
    host_locks: dict[str, asyncio.Lock] = {}
    host_next_allowed: dict[str, float] = {}

    async def _fetch_one(
        src: Source,
        discourse_include_top_daily: bool,
    ) -> tuple[int, list[FetchedEntry] | None, str | None, dict[str, str] | None]:
        host = _host_key(src.url)
        host_sem = host_sems.setdefault(
            host, asyncio.Semaphore(max(1, settings.max_concurrent_fetches_per_host))
        )

        if settings.host_min_interval_seconds and settings.host_min_interval_seconds > 0:
            host_lock = host_locks.setdefault(host, asyncio.Lock())
            async with host_lock:
                now_mono = time.monotonic()
                next_allowed = host_next_allowed.get(host, 0.0)
                if now_mono < next_allowed:
                    await asyncio.sleep(next_allowed - now_mono)
                host_next_allowed[host] = time.monotonic() + float(settings.host_min_interval_seconds)

        async with host_sem:
            async with fetch_sem:
                try:
                    if src.type == "rss":
                        cookie = await _cookie_header_cb(src.url)
                        entries, update = await RssConnector(timeout_seconds=settings.http_timeout_seconds).fetch_with_state(
                            url=src.url,
                            etag=src.etag,
                            last_modified=src.last_modified,
                            cookie_header=cookie,
                        )
                        return src.id, entries, None, update

                    if src.type == "discourse":
                        entries = await fetch_entries_for_source(
                            source=src,
                            timeout_seconds=settings.http_timeout_seconds,
                            discourse_include_top_daily=bool(discourse_include_top_daily),
                            discourse_rss_catchup_pages=int(getattr(settings, "discourse_rss_catchup_pages", 1) or 1),
                            discourse_cookie=((getattr(settings, "discourse_cookie", "") or "").strip() or None),
                            cookie_header_cb=_cookie_header_cb,
                        )
                    else:
                        extra: dict[str, object] = {}
                        if src.type == "llm_models":
                            extra["llm_models_api_key"] = (settings.llm_api_key or None)
                        entries = await fetch_entries_for_source(
                            source=src,
                            timeout_seconds=settings.http_timeout_seconds,
                            cookie_header_cb=_cookie_header_cb,
                            **extra,
                        )
                    return src.id, entries, None, None
                except AuthRequiredError as exc:
                    return src.id, None, "auth_required", exc.meta()
                except Exception as exc:
                    return src.id, None, str(exc), None

    tasks = [
        asyncio.create_task(
            _fetch_one(
                job[0],
                discourse_include_top_daily_by_source_id.get(job[0].id, False),
            )
        )
        for job in source_jobs
    ]
    results = await asyncio.gather(*tasks) if tasks else []
    fetch_results: dict[int, tuple[list[FetchedEntry] | None, str | None, dict[str, str] | None]] = {
        sid: (e, err, upd) for sid, e, err, upd in results
    }

    for source, bindings, now, _prev_checked_at in source_jobs:
        entries, fetch_error, update = fetch_results.get(source.id, (None, "fetch missing", None))
        source.last_checked_at = now
        if fetch_error:
            error = fetch_error

            # Auth-required is not a "bad source": it means the operator needs to (re)login.
            if isinstance(update, dict) and update.get("error_type") == "auth_required":
                host = (update.get("host") or "").strip()
                if not host:
                    host = _url_host(source.url)
                host_norm = host.lower().split(":", 1)[0].lstrip(".")
                if host_norm.startswith("www."):
                    host_norm = host_norm[4:]

                # Remember domains that required auth so the operator can populate cookie jar entries later.
                # This is intentionally "best effort" and bounded (avoid unbounded growth in app config).
                try:
                    if host_norm:
                        raw_seen = (repo.get_app_config("auth_cookie_domains_seen") or "").strip()
                        seen = [p.strip() for p in raw_seen.split(",") if p.strip()]
                        if host_norm not in seen:
                            seen.append(host_norm)
                            if len(seen) > 200:
                                seen = seen[-200:]
                            repo.set_app_config("auth_cookie_domains_seen", ",".join(seen))
                except Exception:
                    pass

                logger.warning(
                    "source fetch auth_required: id=%s type=%s url=%s host=%s status=%s",
                    source.id,
                    source.type,
                    source.url,
                    host_norm or host,
                    update.get("status_code") or "",
                )

                health = repo.get_or_create_source_health(source_id=source.id)
                health.last_error = ("auth_required" if not error else str(error))[:4000]
                health.last_error_at = now
                # Do NOT increment error_count; just back off until next tick window.
                min_interval = {
                    "rss": settings.rss_min_interval_seconds,
                    "hn_search": settings.hn_min_interval_seconds,
                    "searxng_search": settings.searxng_min_interval_seconds,
                    "discourse": settings.discourse_min_interval_seconds,
                    "html_list": settings.rss_min_interval_seconds,
                }.get(source.type, settings.rss_min_interval_seconds)
                health.next_fetch_at = now + dt.timedelta(seconds=max(60, int(min_interval or 0)))
                session.commit()

                # OpenInfoMate OSS default: disable sources that require login/cookies.
                # (We don't want a permanently failing source to keep consuming budget.)
                try:
                    source.enabled = False
                    meta = repo.get_or_create_source_meta(source_id=int(source.id))
                    tag = "disabled:auth_required"
                    raw_tags = (meta.tags or "").strip()
                    tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
                    if tag not in tags:
                        tags.append(tag)
                        meta.tags = ",".join(tags)[:2000]
                    status_txt = (update.get("status_code") or "").strip()
                    final_url = (update.get("final_url") or "").strip()
                    note = f"[auto-disabled] auth_required status={status_txt or '-'} final_url={final_url or '-'}"
                    raw_notes = (meta.notes or "").strip()
                    meta.notes = ((raw_notes + "\n" + note).strip() if raw_notes else note)[:8000]
                    session.commit()
                except Exception:
                    try:
                        session.rollback()
                    except Exception:
                        pass

                if push and has_any_channel:
                    date_key = _local_day_iso(settings)
                    key_host = host_norm or host or f"source{source.id}"
                    id_key = f"auth_required:{key_host}:{date_key}"
                    status_txt = (update.get("status_code") or "").strip()
                    final_url = (update.get("final_url") or "").strip()
                    is_zh = out_lang == "zh"
                    md = "# Auth Required\n\n" if not is_zh else "# 需要重新登录（Auth Required）\n\n"
                    md += (
                        f"- host: {key_host}\n"
                        f"- source: #{source.id} ({source.type})\n"
                        f"- url: {source.url}\n"
                        + (f"- status: {status_txt}\n" if status_txt else "")
                        + (f"- final_url: {final_url}\n" if final_url else "")
                        + "- action: auto_disabled=true\n"
                    )
                    md += (
                        "\nTip: replace this source with a public feed/list page, or skip it.\n"
                        if not is_zh
                        else "\n提示：建议替换为公开可抓取的 feed/列表页来源，或暂时跳过。\n"
                    )

                    try:
                        await push_dingtalk_markdown(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            title=("Auth Required" if not is_zh else "需要重新登录"),
                            markdown=md,
                        )
                    except Exception:
                        pass
                    try:
                        await push_telegram_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            text=md,
                        )
                    except Exception:
                        pass
                    try:
                        push_email_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            subject=("[Auth Required]" if not is_zh else "[需要重新登录]") + f" {key_host}",
                            text=md,
                        )
                    except Exception:
                        pass
                    try:
                        await push_webhook_json(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            payload={
                                "type": "auth_required",
                                "host": key_host,
                                "source_id": source.id,
                                "source_type": source.type,
                                "source_url": source.url,
                                "status_code": status_txt,
                                "final_url": final_url,
                            },
                        )
                    except Exception:
                        pass

                for topic, _ts in bindings:
                    per_source.append(
                        TickSourceResult(
                            topic_name=topic.name,
                            source_url=source.url,
                            created=0,
                            pushed_alerts=0,
                            error="auth_required",
                        )
                    )
                continue

            logger.warning("source fetch failed: id=%s type=%s url=%s err=%s", source.id, source.type, source.url, error)
            health = repo.get_or_create_source_health(source_id=source.id)
            health.error_count += 1
            health.last_error = error[:4000]
            health.last_error_at = now

            backoff_seconds = min(
                settings.source_backoff_max_seconds,
                settings.source_backoff_base_seconds * (2 ** max(0, health.error_count - 1)),
            )
            health.next_fetch_at = now + dt.timedelta(seconds=backoff_seconds)

            disabled_now = False
            if health.error_count >= settings.source_disable_after_errors:
                if source.enabled:
                    source.enabled = False
                    disabled_now = True

            session.commit()

            if push and disabled_now and has_any_channel:
                date_key = _local_day_iso(settings)
                id_key = f"source_disabled:{source.id}:{date_key}"
                err_txt = (error or "").replace("\n", " ").strip()
                if len(err_txt) > 400:
                    err_txt = err_txt[:400] + "…"
                next_txt = health.next_fetch_at.isoformat() if health.next_fetch_at else ""

                md = (
                    "# Source Disabled\n\n"
                    f"- id: #{source.id}\n"
                    f"- type: {source.type}\n"
                    f"- url: {source.url}\n"
                    f"- errors: {health.error_count}\n"
                    f"- next_fetch_at: {next_txt}\n"
                    f"- last_error: {err_txt!r}\n"
                )

                try:
                    await push_dingtalk_markdown(
                        repo=repo,
                        settings=settings,
                        idempotency_key=id_key,
                        title=f"Source Disabled: #{source.id}",
                        markdown=md,
                    )
                except Exception:
                    logger.warning("push dingtalk failed: key=%s", id_key)
                    pass

                try:
                    await push_telegram_text(
                        repo=repo,
                        settings=settings,
                        idempotency_key=id_key,
                        text=md,
                    )
                except Exception:
                    logger.warning("push telegram failed: key=%s", id_key)
                    pass

                try:
                    push_email_text(
                        repo=repo,
                        settings=settings,
                        idempotency_key=id_key,
                        subject=f"[Source Disabled] {source.type} #{source.id}",
                        text=md,
                    )
                except Exception:
                    logger.warning("push email failed: key=%s", id_key)
                    pass

                try:
                    await push_webhook_json(
                        repo=repo,
                        settings=settings,
                        idempotency_key=id_key,
                        payload={
                            "type": "source_disabled",
                            "source_id": source.id,
                            "source_type": source.type,
                            "source_url": source.url,
                            "error_count": health.error_count,
                            "next_fetch_at": next_txt,
                            "last_error": err_txt,
                        },
                    )
                except Exception:
                    logger.warning("push webhook failed: key=%s", id_key)
                    pass

            for topic, _ts in bindings:
                per_source.append(
                    TickSourceResult(
                        topic_name=topic.name,
                        source_url=source.url,
                        created=0,
                        pushed_alerts=0,
                        error=error,
                    )
                )
            continue

        if update:
            if update.get("etag"):
                source.etag = update["etag"]
            if update.get("last_modified"):
                source.last_modified = update["last_modified"]

        # Success path: reset health (fetch succeeded).
        health = repo.get_or_create_source_health(source_id=source.id)
        health.error_count = 0
        health.last_error = ""
        health.last_error_at = None
        health.last_success_at = now
        health.next_fetch_at = None
        session.commit()

        for topic, ts in bindings:
            error: str | None = None
            pushed_alerts = 0
            try:
                policy = policies_by_topic_id.get(topic.id)
                use_llm_curation = bool(
                    settings.llm_curation_enabled
                    and settings.llm_base_url
                    and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
                    and policy
                    and policy.llm_curation_enabled
                )
                match_mode = "llm" if use_llm_curation else "keywords"
                include_keywords = ts.include_keywords
                if match_mode != "llm" and not bool(getattr(settings, "include_keywords_prefilter_enabled", False)):
                    include_keywords = ""
                created: list[CreatedDecision] = ingest_entries_for_topic_source(
                    session=session,
                    topic=topic,
                    source=source,
                    entries=entries or [],
                    include_keywords=include_keywords,
                    exclude_keywords=ts.exclude_keywords,
                    include_domains=getattr(settings, "include_domains", ""),
                    exclude_domains=getattr(settings, "exclude_domains", ""),
                    simhash_lookback_days=settings.simhash_lookback_days,
                    match_mode=match_mode,
                )
                for d in created:
                    try:
                        if int(d.item_id or 0) > 0:
                            created_item_ids.add(int(d.item_id))
                    except Exception:
                        pass
                if use_llm_curation and created:
                    topic_by_id[topic.id] = topic
                    ids = new_candidate_ids_by_topic.setdefault(topic.id, set())
                    for d in created:
                        if d.decision == "candidate":
                            ids.add(d.item_id)
            except Exception as exc:
                session.rollback()
                created = []
                error = str(exc)
                logger.warning(
                    "topic ingest failed: topic=%s source_id=%s err=%s", topic.name, source.id, error
                )

            if push and created:
                for d in created:
                    if d.decision != "alert":
                        continue
                    if not has_any_channel:
                        continue

                    final_decision = d.decision
                    final_reason = d.reason

                    if settings.llm_base_url and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None)):
                        used = llm_used_by_topic.get(d.topic_id, 0)
                        if used < max(0, settings.llm_max_candidates_per_tick):
                            llm_used_by_topic[d.topic_id] = used + 1
                            item = session.get(Item, d.item_id)
                            snippet = item.content_text if item else ""
                            try:
                                gate = await llm_gate_alert_candidate(
                                    repo=repo,
                                    settings=settings_out,
                                    topic=topic,
                                    title=d.title,
                                    url=d.canonical_url,
                                    content_text=snippet,
                                    usage_cb=llm_usage_cb,
                                )
                            except Exception as exc:
                                gate = None
                                logger.warning(
                                    "llm gate failed: topic=%s item_id=%s err=%s", topic.name, d.item_id, exc
                                )

                            if gate:
                                final_decision = gate.decision
                                llm_note = ""
                                if gate.reason:
                                    llm_note = f"llm_gate={gate.decision} llm_reason={gate.reason}"
                                else:
                                    llm_note = f"llm_gate={gate.decision}"
                                it_row = session.scalar(
                                    select(ItemTopic).where(
                                        and_(ItemTopic.item_id == d.item_id, ItemTopic.topic_id == d.topic_id)
                                    )
                                )
                                if it_row:
                                    if llm_note:
                                        existing_reason = (it_row.reason or "").strip()
                                        it_row.reason = (
                                            f"{existing_reason}; {llm_note}".strip("; ").strip()
                                            if existing_reason
                                            else llm_note
                                        )
                                    it_row.decision = final_decision
                                    session.commit()
                                    final_reason = it_row.reason

                    if final_decision != "alert":
                        continue

                    if bool(getattr(settings, "alert_global_dedupe_enabled", True)):
                        # Cross-topic de-dupe: if this item was already sent as an alert for any topic,
                        # do not re-send it (common when one source is bound to multiple topics).
                        if repo.any_push_sent_with_prefix(idempotency_prefix=f"alert:{d.item_id}:"):
                            continue

                    id_key = f"alert:{d.item_id}:{d.topic_id}"

                    # Ensure we never push an unopenable localhost URL.
                    push_url = (d.canonical_url or "").strip()
                    if _is_local_url(push_url):
                        item_for_url = repo.get_item_by_id(d.item_id)
                        src_for_url = repo.get_source_by_id(item_for_url.source_id) if item_for_url else None
                        push_url = _best_push_url_for_item(item=item_for_url, source=src_for_url) if item_for_url else push_url

                    # Respect explicit operator mutes (domain-level).
                    try:
                        host = (urlsplit((push_url or "").strip()).netloc or "").lower()
                        host = host.split(":", 1)[0].lstrip(".")
                        if host.startswith("www."):
                            host = host[4:]
                    except Exception:
                        host = ""
                    if host and host in active_mute_domains:
                        continue
                    try:
                        src_id = 0
                        src_url = ""
                        item_for_url = repo.get_item_by_id(d.item_id)
                        src_id = int(getattr(item_for_url, "source_id", 0) or 0) if item_for_url else 0
                        src_for_url = repo.get_source_by_id(src_id) if src_id > 0 else None
                        src_url = str(getattr(src_for_url, "url", "") or "") if src_for_url else ""
                        if min_source_score > 0 and _effective_source_score(source_id=src_id, source_url=src_url) < int(min_source_score):
                            continue
                    except Exception:
                        pass
                    if domain_policy and (not domain_policy.allows_push_url(str(push_url or ""))):
                        continue

                    first_success_for_key = not repo.any_push_sent(idempotency_key=id_key)
                    if first_success_for_key:
                        if not can_send_alert_under_budget(
                            session=session,
                            topic_id=d.topic_id,
                            daily_cap=topic.alert_daily_cap,
                            cooldown_minutes=topic.alert_cooldown_minutes,
                        ):
                            _fallback_alert_to_digest(
                                session=session,
                                repo=repo,
                                item_id=d.item_id,
                                topic_id=d.topic_id,
                                reason=final_reason,
                                note="delivery_note: alert_suppressed_by_budget (kept for digest)",
                            )
                            logger.info("alert suppressed by budget; downgraded to digest: item_id=%s topic_id=%s", d.item_id, d.topic_id)
                            continue

                    md = _format_alert_markdown(
                        topic_name=d.topic_name,
                        title=d.title,
                        url=push_url,
                        reason=final_reason,
                        lang=out_lang,
                    )
                    pushed_any = False
                    try:
                        pushed_any |= await push_dingtalk_markdown(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            title=f"Alert: {d.topic_name}",
                            markdown=md,
                        )
                    except Exception:
                        logger.warning("push dingtalk failed: key=%s", id_key)
                        pass

                    try:
                        pushed_any |= await push_telegram_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            text=_format_alert_text(title=d.title, url=push_url, reason=final_reason, lang=out_lang),
                        )
                    except Exception:
                        logger.warning("push telegram failed: key=%s", id_key)
                        pass

                    try:
                        pushed_any |= push_email_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            subject=f"[Alert] {d.topic_name}: {d.title}",
                            text=_format_alert_text(title=d.title, url=push_url, reason=final_reason, lang=out_lang),
                        )
                    except Exception:
                        logger.warning("push email failed: key=%s", id_key)
                        pass

                    try:
                        s, w = extract_llm_summary_why(final_reason)
                        pushed_any |= await push_webhook_json(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            payload={
                                "type": "alert",
                                "topic": d.topic_name,
                                "topic_id": d.topic_id,
                                "item_id": d.item_id,
                                "title": d.title,
                                "url": push_url,
                                "reason": final_reason,
                                "summary": s,
                                "why": w,
                            },
                        )
                    except Exception:
                        logger.warning("push webhook failed: key=%s", id_key)
                        pass

                    if pushed_any:
                        if first_success_for_key:
                            record_alert_delivery(session=session, topic_id=d.topic_id)
                        pushed_alerts += 1

            per_source.append(
                TickSourceResult(
                    topic_name=topic.name,
                    source_url=source.url,
                    created=len(created),
                    pushed_alerts=pushed_alerts,
                    error=error,
                )
            )
            total_created += len(created)
            total_pushed_alerts += pushed_alerts

    # Optional LLM-native curation (prompt-driven).
    # Tick mode: only promote alerts; keep digest decisions as "candidate" so daily digest can cap once/day.
    if settings.llm_curation_enabled and settings.llm_base_url and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None)):
        for topic_id in sorted(topic_by_id.keys()):
            topic = topic_by_id.get(topic_id)
            if not topic:
                continue
            policy = policies_by_topic_id.get(topic_id)
            if not policy or not policy.llm_curation_enabled:
                continue

            # Mix: new candidates + backlog "uncurated" candidates (e.g. downtime/backfill).
            new_item_ids = list(new_candidate_ids_by_topic.get(topic_id) or [])

            items: list[Item] = []
            seen_item_ids: set[int] = set()
            for item_id in new_item_ids:
                item = repo.get_item_by_id(item_id)
                if item and item.id not in seen_item_ids:
                    items.append(item)
                    seen_item_ids.add(int(item.id))

            def _item_key(it: Item) -> tuple[int, int]:
                when = it.published_at or it.created_at
                ts = int(when.timestamp()) if when else 0
                return (ts, int(it.id))

            final_max_candidates = max(1, int(settings.llm_curation_max_candidates or 1))
            triage_enabled = bool(getattr(settings, "llm_curation_triage_enabled", False)) and bool(
                (getattr(settings, "llm_model_mini", None) or "").strip()
            )
            pool_max_candidates = final_max_candidates
            if triage_enabled:
                try:
                    pool_max_candidates = int(getattr(settings, "llm_curation_triage_pool_max_candidates", 0) or 0)
                except Exception:
                    pool_max_candidates = 0
                if pool_max_candidates <= 0:
                    pool_max_candidates = final_max_candidates
                pool_max_candidates = max(pool_max_candidates, final_max_candidates)
                pool_max_candidates = min(pool_max_candidates, 500)

            history_days = max(0, int(getattr(settings, "llm_curation_history_dedupe_days", 0) or 0))
            backlog_days = max(7, history_days) if history_days > 0 else 7
            backlog_since = dt.datetime.utcnow() - dt.timedelta(days=backlog_days)
            # Always drain a little "uncurated" backlog even when new candidates are plentiful.
            # Otherwise long-running topics (e.g. Profile streams) can starve backfilled candidates
            # indefinitely and never surface important older items.
            items_new = list(items)
            items_new.sort(key=_item_key, reverse=True)

            backlog_quota = min(pool_max_candidates, max(1, min(5, pool_max_candidates // 4)))
            backlog_items: list[Item] = []
            try:
                extra = repo.list_uncurated_item_topics_for_topic(
                    topic=topic,
                    since=backlog_since,
                    limit=max(backlog_quota, 1),
                    exclude_item_ids=set(seen_item_ids),
                )
            except Exception:
                extra = []
            for _it_row, it in extra:
                if not it or it.id in seen_item_ids:
                    continue
                backlog_items.append(it)
                seen_item_ids.add(int(it.id))
                if len(backlog_items) >= backlog_quota:
                    break

            # Fairness: also drain at least one oldest uncurated candidate so long-running streams
            # don't starve older-but-important backlog items forever.
            if backlog_quota > 1 and len(backlog_items) < backlog_quota:
                try:
                    oldest = repo.list_uncurated_item_topics_for_topic(
                        topic=topic,
                        since=backlog_since,
                        limit=1,
                        exclude_item_ids=set(seen_item_ids),
                        order="asc",
                    )
                except Exception:
                    oldest = []
                for _it_row, it in oldest:
                    if not it or it.id in seen_item_ids:
                        continue
                    backlog_items.append(it)
                    seen_item_ids.add(int(it.id))
                    break

            new_quota = max(0, pool_max_candidates - len(backlog_items))
            items = items_new[:new_quota] + backlog_items
            if len(items) < pool_max_candidates:
                needed = pool_max_candidates - len(items)
                try:
                    extra2 = repo.list_uncurated_item_topics_for_topic(
                        topic=topic,
                        since=backlog_since,
                        limit=needed,
                        exclude_item_ids=set(seen_item_ids),
                    )
                except Exception:
                    extra2 = []
                for _it_row, it in extra2:
                    if it and it.id not in seen_item_ids:
                        items.append(it)
                        seen_item_ids.add(int(it.id))

            if not items:
                continue

            items.sort(key=_item_key, reverse=True)
            items = items[:pool_max_candidates]

            content_cache: dict[int, str] = {}

            def _best_text(item_id: int, fallback: str) -> str:
                cached = content_cache.get(item_id)
                if cached is not None:
                    return cached
                row = repo.get_item_content(item_id=item_id)
                txt = (row.content_text if row and row.content_text else fallback).strip()
                content_cache[item_id] = txt
                return txt

            # Optional full-text enrichment for better alert-time curation.
            if settings.fulltext_enabled:
                max_fetches = max(0, int(settings.fulltext_max_fetches_per_topic or 0))
                fetched = 0
                for it in items:
                    if fetched >= max_fetches:
                        break
                    url = (it.url or it.canonical_url or "").strip()
                    if not url.startswith(("http://", "https://")):
                        continue
                    try:
                        parts = urlsplit(url)
                        host = (parts.netloc or "").lower()
                        path = parts.path or ""
                    except Exception:
                        host = ""
                        path = ""
                    if host.endswith("nodeseek.com"):
                        continue
                    # Internal API-like endpoints aren't meaningful to fulltext-enrich.
                    if path.startswith("/v1/models"):
                        continue
                    existing = repo.get_item_content(item_id=it.id)
                    if existing and (existing.content_text or (existing.error or "").strip()):
                        continue
                    try:
                        cookie = await _cookie_header_cb(url)
                        text = await fetch_fulltext_for_url(
                            url=url,
                            timeout_seconds=int(settings.fulltext_timeout_seconds or settings.http_timeout_seconds),
                            max_chars=int(settings.fulltext_max_chars or 1),
                            discourse_cookie=((settings.discourse_cookie or "").strip() or cookie or None),
                            cookie_header=cookie,
                        )
                    except Exception as exc:
                        logger.info("fulltext fetch failed: url=%s err=%s", url, exc)
                        try:
                            err = str(exc or "").strip()
                            if len(err) > 400:
                                err = err[:400] + "…"
                            repo.upsert_item_content(item_id=it.id, url=url, content_text="", error=err)
                        except Exception:
                            pass
                        continue
                    try:
                        repo.upsert_item_content(item_id=it.id, url=url, content_text=text, error="")
                        content_cache[it.id] = text.strip()
                        fetched += 1
                    except Exception as exc:
                        logger.info("fulltext store failed: item_id=%s err=%s", it.id, exc)

            candidates = [
                {
                    "item_id": it.id,
                    "title": it.title,
                    "url": it.canonical_url,
                    "snippet": _best_text(it.id, (it.content_text or "")),
                }
                for it in items
            ]
            _annotate_candidates_domain_feedback(repo=repo, candidates=candidates)

            # Provide the model with anti-repeat context (recent digest/alert items),
            # and also pre-dedupe candidates so "same story" bursts don't spam alerts.
            recent_sent: list[dict[str, str]] = []
            history_seen: list[int] = []
            if history_days > 0:
                hist_since = dt.datetime.utcnow() - dt.timedelta(days=history_days)
                until = dt.datetime.utcnow()
                try:
                    # Global anti-repeat: avoid re-alerting the same story under different topics.
                    recent_sent = repo.list_recent_sent_items_window(
                        since=hist_since,
                        until=until,
                        decisions=["digest", "alert"],
                        limit=20,
                    )
                except Exception:
                    recent_sent = []
                try:
                    history_seen = repo.list_item_simhashes_window(
                        since=hist_since,
                        until=until,
                        decisions=["digest", "alert"],
                        limit=5000,
                    )
                except Exception:
                    history_seen = []

            history_urls: set[str] = {
                str(r.get("url") or "").strip() for r in (recent_sent or []) if str(r.get("url") or "").strip()
            }
            deduped: list[dict] = []
            seen: list[int] = list(history_seen or [])
            seen_story: list[int] = []
            history_snippets: dict[int, str] = {}

            def _history_snippet(item_id: int) -> str:
                cached = history_snippets.get(item_id)
                if cached is not None:
                    return cached
                try:
                    row = repo.get_item_content(item_id=item_id)
                except Exception:
                    row = None
                txt = (row.content_text if row and row.content_text else "").strip()
                if len(txt) > 2000:
                    txt = txt[:2000]
                history_snippets[item_id] = txt
                return txt

            for r in (recent_sent or [])[:50]:
                t = str(r.get("title") or "").strip()
                u = str(r.get("url") or "").strip()
                iid = 0
                try:
                    iid = int(r.get("item_id") or 0)
                except Exception:
                    iid = 0
                story = story_dedupe_text(
                    title=t,
                    url=u,
                    snippet=_history_snippet(iid) if iid > 0 else "",
                )
                if not story:
                    continue
                sh = simhash64(story)
                seen_story.append(int_to_signed64(sh))
            for c in candidates:
                url = str(c.get("url") or "").strip()
                if url and url in history_urls:
                    continue
                snippet = (str(c.get("snippet") or "")).strip()
                title = (str(c.get("title") or "")).strip()

                # Content-level near-dup (same excerpt/fulltext).
                text_for_dedupe = snippet or title
                if text_for_dedupe:
                    sh = simhash64(text_for_dedupe)
                    if is_near_duplicate(new_simhash=sh, existing_simhashes=seen):
                        continue
                    seen.append(int_to_signed64(sh))

                # Story-level near-dup (cross-outlet): title + notable links + strong tokens.
                story = story_dedupe_text(title=title, url=url, snippet=snippet)
                if story:
                    sh = simhash64(story)
                    if is_near_duplicate(new_simhash=sh, existing_simhashes=seen_story, max_distance=6):
                        continue
                    seen_story.append(int_to_signed64(sh))

                deduped.append(c)
            candidates = deduped

            if not candidates:
                continue

            # Optional cheap triage stage (mini model): reduce the pool to a bounded set before full curation.
            #
            # AI-only filtering requirement:
            # - If triage succeeds (even if it returns an empty list), respect it.
            # - If triage fails (None), do NOT deterministically slice candidates; instead pass the full
            #   bounded pool to the reasoning model so no relevance filtering happens outside AI.
            triage_keep_ids: list[int] | None = None
            if triage_enabled and len(candidates) > final_max_candidates:
                keep_max = 0
                try:
                    keep_max = int(getattr(settings, "llm_curation_triage_keep_candidates", 0) or 0)
                except Exception:
                    keep_max = 0
                if keep_max <= 0:
                    keep_max = final_max_candidates
                    keep_max = max(1, min(keep_max, len(candidates)))
                    try:
                        triage_keep_ids = await llm_triage_topic_items(
                            repo=repo,
                            settings=settings_out,
                            topic=topic,
                            policy_prompt=policy.llm_curation_prompt,
                            candidates=candidates,
                            recent_sent=recent_sent,
                            max_keep=keep_max,
                            usage_cb=llm_usage_cb,
                        )
                    except Exception as exc:
                        logger.info("llm triage failed (tick): topic=%s err=%s", topic.name, exc)
                        triage_keep_ids = None

                if triage_keep_ids is not None:
                    by_id: dict[int, dict] = {}
                    for c in candidates:
                        try:
                            cid = int(c.get("item_id"))
                        except Exception:
                            continue
                        if cid > 0 and cid not in by_id:
                            by_id[cid] = c
                    candidates = [by_id[i] for i in triage_keep_ids if i in by_id]

            if not candidates:
                continue

            try:
                decisions = await llm_curate_topic_items(
                    repo=repo,
                    settings=settings_out,
                    topic=topic,
                    policy_prompt=policy.llm_curation_prompt,
                    candidates=candidates,
                    recent_sent=recent_sent,
                    # do not cap digest in tick mode; digest is finalized in run_digest
                    max_digest=len(candidates),
                    max_alert=max(0, int(settings.llm_curation_max_alert or 0)),
                    usage_cb=llm_usage_cb,
                )
            except Exception as exc:
                decisions = None
                logger.warning("llm curation failed: topic=%s err=%s", topic.name, exc)

            if not decisions:
                continue

            # Apply decisions.
            alert_item_ids: list[int] = []
            for d in decisions:
                it_row = repo.get_item_topic(item_id=d.item_id, topic_id=topic_id)
                if not it_row:
                    continue
                if d.decision == "alert":
                    it_row.decision = "alert"
                    hint = "alert"
                    alert_item_ids.append(d.item_id)
                elif d.decision == "digest":
                    it_row.decision = "candidate"
                    hint = "digest_candidate"
                else:
                    it_row.decision = "ignore"
                    hint = "ignored"
                it_row.reason = _format_llm_curation_reason(summary=d.summary, why=d.why, hint=hint)
            session.commit()

            if push and alert_item_ids and has_any_channel:
                for item_id in alert_item_ids:
                    item = repo.get_item_by_id(item_id)
                    if not item:
                        continue
                    it_row = repo.get_item_topic(item_id=item_id, topic_id=topic_id)
                    if not it_row:
                        continue
                    id_key = f"alert:{item_id}:{topic_id}"

                    if bool(getattr(settings, "alert_global_dedupe_enabled", True)):
                        if repo.any_push_sent_with_prefix(idempotency_prefix=f"alert:{item_id}:"):
                            continue

                    push_url = _best_push_url_for_item(item=item, source=repo.get_source_by_id(item.source_id))

                    # Respect explicit operator mutes + domain quality policy (push surface).
                    try:
                        host = (urlsplit((push_url or "").strip()).netloc or "").lower()
                        host = host.split(":", 1)[0].lstrip(".")
                        if host.startswith("www."):
                            host = host[4:]
                    except Exception:
                        host = ""
                    if host and host in active_mute_domains:
                        continue
                    if domain_policy and (not domain_policy.allows_push_url(str(push_url or ""))):
                        continue

                    first_success_for_key = not repo.any_push_sent(idempotency_key=id_key)
                    if first_success_for_key:
                        if not can_send_alert_under_budget(
                            session=session,
                            topic_id=topic_id,
                            daily_cap=topic.alert_daily_cap,
                            cooldown_minutes=topic.alert_cooldown_minutes,
                        ):
                            _fallback_alert_to_digest(
                                session=session,
                                repo=repo,
                                item_id=item_id,
                                topic_id=topic_id,
                                reason=it_row.reason,
                                note="delivery_note: alert_suppressed_by_budget (kept for digest)",
                            )
                            logger.info("llm alert suppressed by budget; downgraded to digest: item_id=%s topic_id=%s", item_id, topic_id)
                            continue

                    md = _format_alert_markdown(
                        topic_name=topic.name,
                        title=item.title,
                        url=push_url,
                        reason=it_row.reason,
                        lang=out_lang,
                    )
                    pushed_any = False
                    try:
                        pushed_any |= await push_dingtalk_markdown(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            title=f"Alert: {topic.name}",
                            markdown=md,
                        )
                    except Exception:
                        logger.warning("push dingtalk failed: key=%s", id_key)
                        pass

                    try:
                        pushed_any |= await push_telegram_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            text=_format_alert_text(
                                title=item.title,
                                url=push_url,
                                reason=it_row.reason,
                                lang=out_lang,
                            ),
                        )
                    except Exception:
                        logger.warning("push telegram failed: key=%s", id_key)
                        pass

                    try:
                        pushed_any |= push_email_text(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            subject=f"[Alert] {topic.name}: {item.title}",
                            text=_format_alert_text(
                                title=item.title,
                                url=push_url,
                                reason=it_row.reason,
                                lang=out_lang,
                            ),
                        )
                    except Exception:
                        logger.warning("push email failed: key=%s", id_key)
                        pass

                    try:
                        s, w = extract_llm_summary_why(it_row.reason)
                        pushed_any |= await push_webhook_json(
                            repo=repo,
                            settings=settings,
                            idempotency_key=id_key,
                            payload={
                                "type": "alert",
                                "topic": topic.name,
                                "topic_id": topic_id,
                                "item_id": item_id,
                                "title": item.title,
                                "url": push_url,
                                "reason": it_row.reason,
                                "summary": s,
                                "why": w,
                            },
                        )
                    except Exception:
                        logger.warning("push webhook failed: key=%s", id_key)
                        pass

                    if pushed_any:
                        if first_success_for_key:
                            record_alert_delivery(session=session, topic_id=topic_id)
                        total_pushed_alerts += 1
                        pushed_alerts_by_topic[topic_id] = pushed_alerts_by_topic.get(topic_id, 0) + 1

    # Priority lane (optional): AI-native “must push” fast path.
    # This scans recent "candidate" items and promotes a few truly time-sensitive, high-impact
    # signals to alerts, so major model/tool releases don't wait for scheduled Curated Info.
    if bool(getattr(settings, "priority_lane_enabled", False)) and settings.llm_base_url and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None)):
        try:
            priority_hours = int(getattr(settings, "priority_lane_hours", 72) or 72)
        except Exception:
            priority_hours = 72
        priority_hours = max(1, min(priority_hours, 24 * 14))
        try:
            pool_max = int(getattr(settings, "priority_lane_pool_max_candidates", 200) or 200)
        except Exception:
            pool_max = 200
        pool_max = max(1, min(pool_max, 1000))
        try:
            triage_keep = int(getattr(settings, "priority_lane_triage_keep_candidates", 20) or 20)
        except Exception:
            triage_keep = 20
        triage_keep = max(1, min(triage_keep, 200))
        try:
            max_alert = int(getattr(settings, "priority_lane_max_alert", 2) or 2)
        except Exception:
            max_alert = 2
        max_alert = max(0, min(max_alert, 10))

        if max_alert > 0:
            # Cost guard: avoid re-triaging/re-curating the same pool every tick when nothing new was ingested.
            #
            # We keep a monotonic cursor (max seen item_id) in app_config, and only consider newer items
            # (plus any ids created in this tick) for Priority Lane. This keeps the fast path fast and
            # prevents background LLM "spam" when the stream is idle.
            last_seen_item_id = 0
            try:
                last_seen_item_id = int((repo.get_app_config("priority_lane_last_seen_item_id") or "").strip() or 0)
            except Exception:
                last_seen_item_id = 0

            now_utc = dt.datetime.utcnow()
            since_priority = now_utc - dt.timedelta(hours=priority_hours)

            # Candidate pool: recent candidate+alert events (deduped by item_id), annotated with topic ids.
            # We prefer newly ingested ids from this tick via a stable sort key.
            events = repo.list_recent_events(decisions=["candidate", "alert"], since=since_priority, limit=max(200, pool_max * 5))
            by_item_id: dict[int, dict] = {}
            topics_by_item_id: dict[int, set[int]] = {}

            for _it_row, item, topic, _source in events:
                if not item or not topic:
                    continue
                if not bool(getattr(topic, "enabled", True)):
                    continue
                try:
                    item_id_i = int(getattr(item, "id", 0) or 0)
                except Exception:
                    continue
                if item_id_i <= 0:
                    continue
                if last_seen_item_id > 0 and item_id_i <= last_seen_item_id and item_id_i not in created_item_ids:
                    continue

                topics_by_item_id.setdefault(item_id_i, set()).add(int(getattr(topic, "id", 0) or 0))

                if item_id_i in by_item_id:
                    continue

                snippet = (getattr(item, "content_text", "") or "").strip()
                if len(snippet) > 2000:
                    snippet = snippet[:2000]
                when = item.published_at or item.created_at
                by_item_id[item_id_i] = {
                    "item_id": item_id_i,
                    "title": item.title,
                    "url": item.canonical_url,
                    "snippet": snippet,
                    "published_at": when.isoformat() if when else "",
                    "_is_new": item_id_i in created_item_ids,
                }
                if len(by_item_id) >= pool_max:
                    # list_recent_events is ordered by recency; once we have enough unique items, stop.
                    break

            pool = list(by_item_id.values())
            if pool:
                pool_seen_max_item_id = 0
                try:
                    pool_seen_max_item_id = max(int(d.get("item_id") or 0) for d in pool)
                except Exception:
                    pool_seen_max_item_id = 0

                # Stable ordering: prefer newly ingested ids in this tick, then recency.
                pool.sort(
                    key=lambda d: (
                        1 if bool(d.get("_is_new")) else 0,
                        str(d.get("published_at") or ""),
                    ),
                    reverse=True,
                )
                pool = pool[:pool_max]
                _annotate_candidates_domain_feedback(repo=repo, candidates=pool)

                # Build a synthetic "topic" for LLM prompts.
                priority_topic = Topic(name="Priority Lane", query="", alert_keywords="")

                is_zh = (out_lang or "").strip().lower().startswith("zh")
                policy_prompt = _tpl(repo, settings, "llm.priority_lane.policy", language=out_lang)
                if not policy_prompt.strip():
                    # Best-effort profile block (compressed, delta-aware).
                    prof_lines: list[str] = []
                    try:
                        u = (repo.get_app_config("profile_understanding") or "").strip()
                        if u:
                            prof_lines.append("understanding:\n" + u)
                    except Exception:
                        pass
                    try:
                        axes = (repo.get_app_config("profile_interest_axes") or "").strip()
                        if axes:
                            prof_lines.append("interest_axes:\n" + axes)
                    except Exception:
                        pass
                    try:
                        kws = (repo.get_app_config("profile_interest_keywords") or "").strip()
                        if kws:
                            prof_lines.append("keywords:\n" + kws)
                    except Exception:
                        pass
                    try:
                        delta = (repo.get_app_config("profile_prompt_delta") or "").strip()
                        if delta:
                            prof_lines.append("delta_prompt:\n" + delta)
                    except Exception:
                        pass
                    profile_block = "\n\n".join([x for x in prof_lines if x.strip()]).strip()

                    # Fail-open fallback to a safe builtin policy (should be rare).
                    policy_prompt = (
                        (
                            "这是一个“快速消息/重大更新紧急通道”。只挑选必须立即提醒的信号（宁缺毋滥）。\n"
                            "你必须基于用户画像来判断“是否必须今天就知道”。\n"
                            + (f"\nUSER_PROFILE:\n{profile_block}\n\n" if profile_block else "\n")
                            + "不要按“安全/越狱/平台事故/灰色产业”等类别做默认屏蔽；只按信息质量、可验证性、时效性与影响筛选。\n"
                            + "若涉及风险/攻击/漏洞/越狱：允许提醒关键进展，但仅做客观摘要（影响/修复/出处），不输出可直接武器化的步骤/代码。\n"
                            + "低质量过滤：纯搬运/营销/标题党/无新增信息 → ignore。\n"
                            + "只要属于“今天你一定要知道”的新进展，就应该 alert；否则给 ignore。\n"
                        )
                        if is_zh
                        else (
                            "This is the Quick Messages / 'Breaking Updates' priority lane. Only pick signals that MUST be alerted immediately.\n"
                            "You MUST base decisions on the user's profile and whether it requires action today.\n"
                            + (f"\nUSER_PROFILE:\n{profile_block}\n\n" if profile_block else "\n")
                            + "Do NOT apply topic/category bans (security/jailbreak/outage/gray-market). Select by information quality, verifiability, timeliness, and impact.\n"
                            + "If it involves exploits/vulns/jailbreaks: alert key developments but summarize objectively (impact/patch/source) and avoid weaponizable step-by-step.\n"
                            + "Quality filter: reposts/marketing/clickbait/no-new-info -> ignore.\n"
                            + "If it's something the user should know TODAY, alert it; otherwise ignore.\n"
                        )
                    )

                # Provide a small anti-repeat context window.
                recent_sent: list[dict] = []
                try:
                    recent_sent = repo.list_recent_sent_items_window(
                        since=now_utc - dt.timedelta(days=3),
                        until=now_utc,
                        decisions=["digest", "alert"],
                        limit=20,
                    )
                except Exception:
                    recent_sent = []

                # Stage 1: cheap triage (mini model) to keep only a small pool.
                keep_ids: list[int] | None = None
                if len(pool) > triage_keep:
                    try:
                        keep_ids = await llm_triage_topic_items(
                            settings=settings_out,
                            topic=priority_topic,
                            policy_prompt=policy_prompt,
                            candidates=pool,
                            recent_sent=recent_sent,
                            max_keep=triage_keep,
                            usage_cb=llm_usage_cb,
                        )
                    except Exception as exc:
                        keep_ids = None
                        logger.info("priority lane triage failed: err=%s", exc)

                    # AI-only requirement: respect triage output if it returns a list (even empty).
                    if keep_ids is not None:
                        by_id = {int(d.get("item_id") or 0): d for d in pool if int(d.get("item_id") or 0) > 0}
                        pool = [by_id[i] for i in keep_ids if i in by_id]
                        # If triage explicitly returns an empty keep-list, we can safely advance the cursor
                        # without running the (more expensive) reasoning curation stage.
                        if (not pool) and pool_seen_max_item_id > 0:
                            try:
                                repo.set_app_config("priority_lane_last_seen_item_id", str(int(pool_seen_max_item_id)))
                                session.commit()
                                last_seen_item_id = int(pool_seen_max_item_id)
                            except Exception:
                                pass

                # Stage 2: reasoning curation, but only output alerts (digest cap=0).
                if pool:
                    try:
                        decisions = await llm_curate_topic_items(
                            settings=settings_out,
                            topic=priority_topic,
                            policy_prompt=policy_prompt,
                            candidates=pool,
                            recent_sent=recent_sent,
                            max_digest=0,
                            max_alert=max_alert,
                            usage_cb=llm_usage_cb,
                        )
                    except Exception as exc:
                        decisions = None
                        logger.info("priority lane curation failed: err=%s", exc)
                    else:
                        # Curation succeeded (even if it outputs no alerts): advance the cursor.
                        if pool_seen_max_item_id > 0:
                            try:
                                repo.set_app_config("priority_lane_last_seen_item_id", str(int(pool_seen_max_item_id)))
                                session.commit()
                                last_seen_item_id = int(pool_seen_max_item_id)
                            except Exception:
                                pass

                    # Apply + push.
                    if decisions:
                        profile_topic_id: int | None = None
                        try:
                            profile_name = (repo.get_app_config("profile_topic_name") or "").strip()
                            if profile_name:
                                profile_topic = repo.get_topic_by_name(profile_name)
                                if profile_topic:
                                    profile_topic_id = int(profile_topic.id)
                        except Exception:
                            profile_topic_id = None

                        for d in decisions:
                            if d.decision != "alert":
                                continue

                            item_id = int(d.item_id)
                            # If already pushed as an alert anywhere, skip.
                            if bool(getattr(settings, "alert_global_dedupe_enabled", True)):
                                if repo.any_push_sent_with_prefix(idempotency_prefix=f"alert:{item_id}:"):
                                    continue

                            topic_ids = {tid for tid in (topics_by_item_id.get(item_id) or set()) if int(tid or 0) > 0}
                            if not topic_ids:
                                continue
                            chosen_topic_id: int | None = None
                            if profile_topic_id and profile_topic_id in topic_ids:
                                chosen_topic_id = profile_topic_id
                            else:
                                chosen_topic_id = sorted(topic_ids)[0]
                            if not chosen_topic_id:
                                continue

                            topic = session.get(Topic, chosen_topic_id)
                            if not topic:
                                continue

                            it_row = repo.get_item_topic(item_id=item_id, topic_id=chosen_topic_id)
                            if not it_row:
                                continue

                            # Extra conservative gate for the priority lane: avoid alert spam.
                            item = repo.get_item_by_id(item_id)
                            if not item:
                                continue
                            try:
                                gate = await llm_gate_alert_candidate(
                                    repo=repo,
                                    settings=settings_out,
                                    topic=topic,
                                    title=item.title,
                                    url=item.canonical_url,
                                    content_text=item.content_text,
                                    usage_cb=llm_usage_cb,
                                )
                            except Exception as exc:
                                gate = None
                                logger.info("priority lane gate failed: item_id=%s err=%s", item_id, exc)
                            if gate and gate.decision != "alert":
                                continue

                            it_row.decision = "alert"
                            it_row.reason = _format_llm_curation_reason(
                                summary=d.summary,
                                why=d.why,
                                hint="priority_lane_alert",
                            )
                            session.commit()

                            if not (push and has_any_channel):
                                continue

                            id_key = f"alert:{item_id}:{chosen_topic_id}"
                            first_success_for_key = not repo.any_push_sent(idempotency_key=id_key)
                            if first_success_for_key:
                                if not can_send_alert_under_budget(
                                    session=session,
                                    topic_id=chosen_topic_id,
                                    daily_cap=topic.alert_daily_cap,
                                    cooldown_minutes=topic.alert_cooldown_minutes,
                                ):
                                    _fallback_alert_to_digest(
                                        session=session,
                                        repo=repo,
                                        item_id=item_id,
                                        topic_id=chosen_topic_id,
                                        reason=it_row.reason,
                                        note="delivery_note: alert_suppressed_by_budget (kept for digest)",
                                    )
                                    logger.info("priority-lane alert suppressed by budget; downgraded to digest: item_id=%s topic_id=%s", item_id, chosen_topic_id)
                                    continue

                            md = _format_alert_markdown(
                                topic_name=topic.name,
                                title=item.title,
                                url=item.canonical_url,
                                reason=it_row.reason,
                                lang=out_lang,
                            )
                            pushed_any = False
                            try:
                                pushed_any |= await push_dingtalk_markdown(
                                    repo=repo,
                                    settings=settings,
                                    idempotency_key=id_key,
                                    title=f"Alert: {topic.name}",
                                    markdown=md,
                                )
                            except Exception:
                                logger.warning("push dingtalk failed: key=%s", id_key)
                                pass

                            try:
                                pushed_any |= await push_telegram_text(
                                    repo=repo,
                                    settings=settings,
                                    idempotency_key=id_key,
                                    text=_format_alert_text(
                                        title=item.title,
                                        url=item.canonical_url,
                                        reason=it_row.reason,
                                        lang=out_lang,
                                    ),
                                )
                            except Exception:
                                logger.warning("push telegram failed: key=%s", id_key)
                                pass

                            try:
                                pushed_any |= push_email_text(
                                    repo=repo,
                                    settings=settings,
                                    idempotency_key=id_key,
                                    subject=f"[Alert] {topic.name}: {item.title}",
                                    text=_format_alert_text(
                                        title=item.title,
                                        url=item.canonical_url,
                                        reason=it_row.reason,
                                        lang=out_lang,
                                    ),
                                )
                            except Exception:
                                logger.warning("push email failed: key=%s", id_key)
                                pass

                            try:
                                s, w = extract_llm_summary_why(it_row.reason)
                                pushed_any |= await push_webhook_json(
                                    repo=repo,
                                    settings=settings,
                                    idempotency_key=id_key,
                                    payload={
                                        "type": "alert",
                                        "topic": topic.name,
                                        "topic_id": chosen_topic_id,
                                        "item_id": item_id,
                                        "title": item.title,
                                        "url": item.canonical_url,
                                        "reason": it_row.reason,
                                        "summary": s,
                                        "why": w,
                                    },
                                )
                            except Exception:
                                logger.warning("push webhook failed: key=%s", id_key)
                                pass

                            if pushed_any:
                                if first_success_for_key:
                                    record_alert_delivery(session=session, topic_id=chosen_topic_id)
                                total_pushed_alerts += 1
                                pushed_alerts_by_topic[chosen_topic_id] = pushed_alerts_by_topic.get(chosen_topic_id, 0) + 1

    logger.info(
        "tick done: total_created=%d pushed_alerts=%d per_binding=%d",
        total_created,
        total_pushed_alerts,
        len(per_source),
    )
    return TickResult(
        total_created=total_created,
        total_pushed_alerts=total_pushed_alerts,
        per_source=per_source,
    )


@dataclass(frozen=True)
class DiscoverSourcesTopicResult:
    topic_name: str
    pages_checked: int
    candidates_created: int
    candidates_found: int
    errors: int


@dataclass(frozen=True)
class DiscoverSourcesResult:
    per_topic: list[DiscoverSourcesTopicResult]


async def run_discover_sources(
    *,
    session: Session,
    settings: Settings,
    topic_ids: list[int] | None = None,
) -> DiscoverSourcesResult:
    """
    Discover RSS/Atom feeds from web-wide results (SearxNG), storing them as reviewable candidates.

    This never creates Sources automatically; operators accept/ignore candidates later.
    """
    repo = Repo(session)
    # Apply DB-backed dynamic overrides (include/exclude domains, discovery knobs, etc).
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass

    # Operator control: allow pausing discovery without restarting services.
    if not bool(getattr(settings, "discover_sources_enabled", True)):
        return DiscoverSourcesResult(per_topic=[])

    include_patterns = parse_domains_csv(getattr(settings, "include_domains", "") or "")
    exclude_patterns = parse_domains_csv(getattr(settings, "exclude_domains", "") or "")
    # Single operator-facing switch (Config/UI): auto-accept controls whether we automatically
    # turn high-scoring candidates into Sources. LLM screening/scoring still runs even when
    # auto-accept is off, so operators can review sorted candidates.
    auto_accept_sources = bool(getattr(settings, "discover_sources_auto_accept_enabled", True))
    try:
        min_source_score = int(getattr(settings, "source_quality_min_score", 50))
    except Exception:
        min_source_score = 50
    min_source_score = max(0, min(100, int(min_source_score)))
    try:
        max_sources_total = int(getattr(settings, "discover_sources_max_sources_total", 500) or 500)
    except Exception:
        max_sources_total = 500
    max_sources_total = max(50, min(5000, int(max_sources_total)))
    try:
        explore_weight = int(getattr(settings, "discover_sources_explore_weight", 2) or 2)
    except Exception:
        explore_weight = 2
    explore_weight = max(0, min(10, int(explore_weight)))
    try:
        exploit_weight = int(getattr(settings, "discover_sources_exploit_weight", 10 - explore_weight) or (10 - explore_weight))
    except Exception:
        exploit_weight = 10 - explore_weight
    exploit_weight = max(0, min(10, int(exploit_weight)))
    if explore_weight + exploit_weight <= 0:
        explore_weight, exploit_weight = 2, 8

    # Compact profile context for source curation/scoring.
    prof_lines: list[str] = []
    try:
        u = (repo.get_app_config("profile_understanding") or "").strip()
        if u:
            prof_lines.append("understanding:\n" + u)
    except Exception:
        pass
    try:
        axes = (repo.get_app_config("profile_interest_axes") or "").strip()
        if axes:
            prof_lines.append("interest_axes:\n" + axes)
    except Exception:
        pass
    try:
        kws = (repo.get_app_config("profile_interest_keywords") or "").strip()
        if kws:
            prof_lines.append("keywords:\n" + kws)
    except Exception:
        pass
    try:
        qs = (repo.get_app_config("profile_retrieval_queries") or "").strip()
        if qs:
            prof_lines.append("seed_queries:\n" + qs)
    except Exception:
        pass
    try:
        txt = (repo.get_app_config("profile_text") or "").strip()
        if txt and len("\n\n".join(prof_lines)) < 2000:
            prof_lines.append("raw_profile:\n" + txt[:5000])
    except Exception:
        pass
    profile_block = "\n\n".join([x for x in prof_lines if x.strip()]).strip()

    candidate_preview_limit = max(1, min(8, int(getattr(settings, "discover_sources_auto_accept_preview_entries", 3) or 3)))
    candidate_connector = RssConnector(timeout_seconds=settings.http_timeout_seconds)
    candidate_previews_by_id: dict[int, SourceCandidatePreview] = {}

    llm_usage_cb = make_llm_usage_recorder(session=session)
    # If the operator explicitly requests discovery for certain topics, allow it even when
    # a topic is currently disabled (e.g. Smart Config creates a draft topic first).
    ids = [int(x) for x in (topic_ids or []) if int(x or 0) > 0]
    if ids:
        topics = [t for t in repo.list_topics() if (t.id is not None and int(t.id) in ids)]
    else:
        topics = [t for t in repo.list_topics() if t.enabled]
    if not topics:
        return DiscoverSourcesResult(per_topic=[])

    policies_by_topic_id = {p.topic_id: p for p in repo.list_topic_policies()}

    fetch_sem = asyncio.Semaphore(max(1, settings.max_concurrent_fetches))
    host_sems: dict[str, asyncio.Semaphore] = {}
    host_locks: dict[str, asyncio.Lock] = {}
    host_next_allowed: dict[str, float] = {}

    def _host_key(url: str) -> str:
        parts = urlsplit(url)
        return parts.netloc or parts.scheme or "unknown"

    async with httpx.AsyncClient(timeout=settings.http_timeout_seconds, follow_redirects=True) as client:

        async def _discover_for_page(page_url: str) -> tuple[str, list[str], str, str | None]:
            host = _host_key(page_url)
            host_sem = host_sems.setdefault(
                host, asyncio.Semaphore(max(1, settings.max_concurrent_fetches_per_host))
            )

            if settings.host_min_interval_seconds and settings.host_min_interval_seconds > 0:
                host_lock = host_locks.setdefault(host, asyncio.Lock())
                async with host_lock:
                    now_mono = time.monotonic()
                    next_allowed = host_next_allowed.get(host, 0.0)
                    if now_mono < next_allowed:
                        await asyncio.sleep(next_allowed - now_mono)
                    host_next_allowed[host] = time.monotonic() + float(settings.host_min_interval_seconds)

            async with host_sem:
                async with fetch_sem:
                    try:
                        resp = await client.get(page_url, headers={"User-Agent": "tracker/0.1"})
                        resp.raise_for_status()
                        html = resp.text or ""
                        urls = discover_feed_urls_from_html(page_url=page_url, html=html)
                        if not urls:
                            headers = getattr(resp, "headers", None) or {}
                            try:
                                ct = str(headers.get("content-type") or headers.get("Content-Type") or "").lower()
                            except Exception:
                                ct = ""
                            head = (html.lstrip()[:200]).lower()
                            looks_xml = ("xml" in ct) or ("rss" in ct) or ("atom" in ct)
                            looks_xml = looks_xml or head.startswith("<?xml") or ("<rss" in head) or ("<feed" in head) or ("<rdf:rdf" in head)
                            if looks_xml:
                                # When the "page" is itself an RSS/Atom feed URL, accept it as-is.
                                # (SearxNG often returns feed links directly; HTML discovery would find nothing.)
                                urls = [page_url]
                        snippet = ""
                        if settings.discover_sources_ai_enabled:
                            snippet = html[: max(1, settings.discover_sources_ai_max_html_chars)]
                        return page_url, urls, snippet, None
                    except Exception as exc:
                        return page_url, [], "", str(exc)

        per_topic: list[DiscoverSourcesTopicResult] = []
        for topic in topics:
            # Find bound search sources for this topic.
            rows = repo.list_topic_sources(topic=topic)
            search_sources = [
                s
                for _t, s, _ts in rows
                if s.enabled and (s.type in {"searxng_search", "hn_search"})
            ]

            # Pull web-wide result pages (bounded).
            pages: list[str] = []
            seen_pages: set[str] = set()
            max_pages = max(1, settings.discover_sources_max_results_per_topic)

            def _try_add_page(u: str) -> None:
                url = (u or "").strip()
                if not url:
                    return
                parts = urlsplit(url)
                if parts.scheme not in {"http", "https"}:
                    return
                if include_patterns and not host_matches_any(host=parts.netloc or "", patterns=include_patterns):
                    return
                if exclude_patterns and host_matches_any(host=parts.netloc or "", patterns=exclude_patterns):
                    return
                # Skip HN item pages (not useful for feed discovery; we want the external story URL).
                if parts.netloc.lower() == "news.ycombinator.com":
                    return
                if url in seen_pages:
                    return
                seen_pages.add(url)
                pages.append(url)

            for src in search_sources:
                try:
                    entries = await fetch_entries_for_source(
                        source=src, timeout_seconds=settings.http_timeout_seconds
                    )
                except Exception:
                    continue
                for e in entries:
                    _try_add_page((e.url or "").strip())
                    if len(pages) >= max_pages:
                        break
                if len(pages) >= max_pages:
                    break

            # Fallback: seed from recent items already ingested for this topic.
            if len(pages) < max_pages:
                fallback_since = dt.datetime.utcnow() - dt.timedelta(hours=72)
                for u in repo.list_item_urls_for_discovery(
                    topic=topic,
                    since=fallback_since,
                    limit=max(10, max_pages * 10),
                    decisions=["candidate", "digest", "alert"],
                ):
                    _try_add_page(u)
                    if len(pages) >= max_pages:
                        break

            # Fallback/supplement: use configured web search directly (SearxNG).
            #
            # Rationale: AI Setup can create a new topic without any sources yet; relying purely on
            # "bound search sources" makes discovery brittle. This fallback keeps behavior generic
            # and does not hard-code any site list.
            if len(pages) < max_pages:
                searx_base = (getattr(settings, "searxng_base_url", "") or "").strip()
                # If the global SearxNG base URL isn't configured, derive it from any bound
                # `searxng_search` sources for this topic. This makes Smart Config work out-of-box
                # (AI Setup can add searxng_search seeds without requiring a separate global knob).
                if not searx_base:
                    try:
                        from tracker.connectors.searxng import normalize_searxng_base_url
                    except Exception:
                        normalize_searxng_base_url = None  # type: ignore[assignment]
                    if normalize_searxng_base_url:
                        for src in search_sources:
                            if (src.type or "").strip() != "searxng_search":
                                continue
                            try:
                                sp = urlsplit((src.url or "").strip())
                                if sp.scheme not in {"http", "https"} or not sp.netloc:
                                    continue
                                base_guess = urlunsplit((sp.scheme, sp.netloc, sp.path or "", "", ""))
                                searx_base = normalize_searxng_base_url(base_guess)
                            except Exception:
                                searx_base = ""
                            if searx_base:
                                break
                if searx_base:
                    try:
                        from tracker.connectors.searxng import build_searxng_search_url
                    except Exception:
                        build_searxng_search_url = None  # type: ignore[assignment]
                    q0 = normalize_search_query((topic.query or "").strip() or topic.name)
                    if q0:
                        # Exploit (high relevance): always try direct feed discovery.
                        exploit_queries: list[str] = []
                        exploit_queries.append(q0)
                        low = q0.lower()
                        if "rss" not in low and "atom" not in low and "feed" not in low:
                            exploit_queries.append(f"{q0} rss")
                            exploit_queries.append(f"{q0} atom feed")

                        # Explore (diversity): broaden queries a bit to avoid narrowing too fast.
                        explore_queries: list[str] = []
                        try:
                            words = [w for w in re.split(r"\\s+", q0) if w.strip()]
                        except Exception:
                            words = []
                        q_short = q0
                        if len(words) > 8:
                            q_short = " ".join(words[:8]).strip() or q0
                        if q_short:
                            explore_queries.extend(
                                [
                                    f"{q_short} blog rss",
                                    f"{q_short} newsletter rss",
                                    f"{q_short} releases atom",
                                ]
                            )

                        exploit_queries = list(dict.fromkeys([q.strip() for q in exploit_queries if q.strip()]))[:3]
                        explore_queries = list(dict.fromkeys([q.strip() for q in explore_queries if q.strip()]))[:3]

                        # Allocate remaining page budget by explore/exploit weights (default: 2/8).
                        try:
                            ew = int(getattr(settings, "discover_sources_explore_weight", 2) or 0)
                        except Exception:
                            ew = 2
                        try:
                            xw = int(getattr(settings, "discover_sources_exploit_weight", 8) or 0)
                        except Exception:
                            xw = 8
                        ew = max(0, min(10, ew))
                        xw = max(0, min(10, xw))
                        tot_w = ew + xw
                        remain = max(0, max_pages - len(pages))
                        explore_cap = 0
                        if tot_w > 0 and remain > 0:
                            explore_cap = int(round(remain * (ew / tot_w)))
                        explore_cap = max(0, min(remain, explore_cap))
                        exploit_cap = max(0, remain - explore_cap)

                        async def _add_pages_for_queries(qs: list[str], *, cap: int, time_range: str) -> None:
                            if not qs or cap <= 0:
                                return
                            target_len = min(max_pages, len(pages) + cap)
                            for q in qs:
                                if len(pages) >= target_len:
                                    break
                                if not build_searxng_search_url:
                                    break
                                if len(pages) == 0:
                                    logger.info(
                                        "discover-sources searxng fallback: topic=%s q=%r",
                                        topic.name,
                                        q,
                                    )
                                try:
                                    search_url = build_searxng_search_url(
                                        base_url=searx_base,
                                        query=q[:200],
                                        time_range=time_range,
                                        results=max_pages * 2,
                                    )
                                    resp = await client.get(search_url, headers={"User-Agent": "tracker/0.1"})
                                    resp.raise_for_status()
                                    data = resp.json()
                                except Exception:
                                    continue
                                rows2 = (data.get("results") if isinstance(data, dict) else None) or []
                                if not isinstance(rows2, list):
                                    continue
                                for r in rows2:
                                    if not isinstance(r, dict):
                                        continue
                                    _try_add_page(str(r.get("url") or "").strip())
                                    if len(pages) >= target_len:
                                        break

                        # Exploit first, then explore. Both are bounded and de-duplicated.
                        await _add_pages_for_queries(exploit_queries, cap=exploit_cap, time_range="week")
                        await _add_pages_for_queries(explore_queries, cap=explore_cap, time_range="year")

            if not pages:
                per_topic.append(
                    DiscoverSourcesTopicResult(
                        topic_name=topic.name,
                        pages_checked=0,
                        candidates_created=0,
                        candidates_found=0,
                        errors=0,
                    )
                )
                continue

            tasks = [asyncio.create_task(_discover_for_page(u)) for u in pages]
            results = await asyncio.gather(*tasks) if tasks else []

            created = 0
            found = 0
            errors = 0
            ai_pages_left = max(0, int(settings.discover_sources_ai_max_pages_per_topic or 0))
            for page_url, feed_urls, snippet, err in results:
                if err:
                    errors += 1
                    continue
                candidates = list(feed_urls)

                # Special-case: GitHub repos often expose only commit feeds in HTML. Prefer releases.atom.
                parts = urlsplit(page_url)
                if (parts.netloc or "").lower() == "github.com":
                    segs = [s for s in (parts.path or "").split("/") if s]
                    if len(segs) >= 2:
                        owner = segs[0].strip()
                        repo_name = segs[1].strip()
                        if repo_name.endswith(".git"):
                            repo_name = repo_name[: -len(".git")]
                        if owner and repo_name:
                            candidates.append(f"https://github.com/{owner}/{repo_name}/releases.atom")

                # De-dup, preserve order.
                candidates = list(dict.fromkeys(candidates))
                if not candidates and settings.discover_sources_ai_enabled and ai_pages_left > 0:
                    ai_pages_left -= 1
                    try:
                        guessed = await llm_guess_feed_urls(
                            settings=settings,
                            page_url=page_url,
                            html_snippet=snippet,
                            usage_cb=llm_usage_cb,
                        )
                        if guessed:
                            candidates.extend(list(guessed))
                    except Exception:
                        errors += 1

                # De-dup again after AI fallback.
                candidates = list(dict.fromkeys(candidates))
                if not candidates:
                    continue

                valid_found = 0
                seen_preview_signatures: set[tuple[str, str]] = set()
                for fu in candidates:
                    # Respect global domain filters (keep the candidate pool clean).
                    try:
                        fu_parts = urlsplit((fu or "").strip())
                    except Exception:
                        fu_parts = None
                    if not fu_parts or fu_parts.scheme not in {"http", "https"}:
                        continue
                    if include_patterns and not host_matches_any(host=fu_parts.netloc or "", patterns=include_patterns):
                        continue
                    if exclude_patterns and host_matches_any(host=fu_parts.netloc or "", patterns=exclude_patterns):
                        continue

                    preview = await _fetch_source_candidate_preview(
                        connector=candidate_connector,
                        candidate_url=str(fu or "").strip(),
                        discovered_from_url=page_url,
                        preview_limit=candidate_preview_limit,
                    )
                    if preview is None:
                        continue

                    sig_host = _url_host(preview.fetch_url or str(fu or "").strip()) or "_"
                    sig_key = (sig_host, preview.signature)
                    if sig_key in seen_preview_signatures:
                        continue
                    seen_preview_signatures.add(sig_key)

                    candidate_url = preview.fetch_url or str(fu or "").strip()
                    candidate_title = preview.titles[0] if preview.titles else ""
                    _cand, was_created = repo.add_source_candidate(
                        topic_id=topic.id,
                        source_type="rss",
                        url=candidate_url,
                        title=candidate_title,
                        discovered_from_url=page_url,
                    )
                    try:
                        cid = int(getattr(_cand, "id", 0) or 0)
                    except Exception:
                        cid = 0
                    if cid > 0:
                        candidate_previews_by_id[cid] = preview
                    valid_found += 1
                    if was_created:
                        created += 1

                found += valid_found

            per_topic.append(
                DiscoverSourcesTopicResult(
                    topic_name=topic.name,
                    pages_checked=len(pages),
                    candidates_created=created,
                    candidates_found=found,
                    errors=errors,
                )
            )

        # Optional: auto-accept some new candidates (prompt-driven, bounded).
        # Optional: LLM-screen some new candidates (score + recommend accept/ignore/skip).
        # This runs even when auto-accept is disabled, so operators can review high-signal sources first.
        if (
            settings.llm_base_url
            and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
            and int(settings.discover_sources_auto_accept_max_per_topic or 0) > 0
        ):
            from tracker.actions import (
                accept_source_candidate as accept_source_candidate_action,
                ignore_source_candidate as ignore_source_candidate_action,
            )

            preview_limit = candidate_preview_limit
            max_accept = max(0, int(settings.discover_sources_auto_accept_max_per_topic or 0))
            connector = candidate_connector

            for topic in topics:
                if max_accept <= 0:
                    continue
                policy = policies_by_topic_id.get(topic.id)
                policy_prompt = (policy.llm_curation_prompt if policy else "") or ""

                # Consider more candidates than we may accept, but keep it bounded.
                consider = max(10, max_accept * 10)
                cand_rows = repo.list_source_candidates(topic=topic, status="new", limit=consider)
                if not cand_rows:
                    continue

                candidates: list[dict] = []
                cand_by_id: dict[int, object] = {}
                for cand, _tt in cand_rows:
                    if (cand.source_type or "").strip().lower() != "rss":
                        continue
                    # Skip blocked domains early (avoid wasting preview fetch + LLM tokens).
                    try:
                        cand_parts = urlsplit((cand.url or "").strip())
                    except Exception:
                        cand_parts = None
                    if not cand_parts or cand_parts.scheme not in {"http", "https"}:
                        continue
                    if include_patterns and not host_matches_any(host=cand_parts.netloc or "", patterns=include_patterns):
                        continue
                    if exclude_patterns and host_matches_any(host=cand_parts.netloc or "", patterns=exclude_patterns):
                        continue
                    try:
                        cid = int(cand.id)
                    except Exception:
                        continue

                    preview = candidate_previews_by_id.get(cid)
                    if preview is None:
                        preview = await _fetch_source_candidate_preview(
                            connector=connector,
                            candidate_url=str(cand.url or "").strip(),
                            discovered_from_url=str(cand.discovered_from_url or "").strip(),
                            preview_limit=preview_limit,
                        )
                        if preview is not None:
                            candidate_previews_by_id[cid] = preview

                    if preview is None:
                        try:
                            cand.status = "ignored"
                            repo.upsert_source_candidate_eval(
                                candidate_id=cid,
                                decision="ignore",
                                score=0,
                                quality_score=0,
                                relevance_score=0,
                                novelty_score=0,
                                why=(
                                    "无法抓取到近期条目内容；自动扩源现在会直接过滤这类空内容候选。"
                                ),
                                model="system:preview_validation",
                                explore_weight=int(explore_weight),
                                exploit_weight=int(exploit_weight),
                            )
                            session.commit()
                        except Exception:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                        continue

                    if preview.fetch_url and preview.fetch_url != str(cand.url or "").strip():
                        try:
                            cand.url = canonicalize_url(preview.fetch_url, strip_www=False)
                            session.commit()
                        except Exception:
                            try:
                                session.rollback()
                            except Exception:
                                pass
                    if preview.titles and not str(getattr(cand, "title", "") or "").strip():
                        try:
                            cand.title = str(preview.titles[0] or "").strip()
                            session.commit()
                        except Exception:
                            try:
                                session.rollback()
                            except Exception:
                                pass

                    candidates.append(
                        {
                            "candidate_id": cid,
                            "url": str(cand.url or "").strip() or preview.fetch_url,
                            "discovered_from_url": cand.discovered_from_url,
                            "titles": list(preview.titles),
                            "source_content": preview.source_content,
                        }
                    )
                    cand_by_id[cid] = cand

                if not candidates:
                    continue

                try:
                    # Backward-compatible: tests may monkeypatch llm_decide_source_candidates with an older signature.
                    kwargs = {
                        "settings": settings,
                        "topic": topic,
                        "policy_prompt": policy_prompt,
                        "candidates": candidates,
                        "max_accept": max_accept,
                        "profile": profile_block,
                        "explore_weight": explore_weight,
                        "exploit_weight": exploit_weight,
                        "usage_cb": llm_usage_cb,
                    }
                    try:
                        import inspect

                        sig = inspect.signature(llm_decide_source_candidates)
                        if "profile" not in sig.parameters:
                            kwargs.pop("profile", None)
                        if "explore_weight" not in sig.parameters:
                            kwargs.pop("explore_weight", None)
                        if "exploit_weight" not in sig.parameters:
                            kwargs.pop("exploit_weight", None)
                    except Exception:
                        pass
                    decisions = await llm_decide_source_candidates(**kwargs)
                except Exception as exc:
                    logger.warning("llm auto-accept failed: topic=%s err=%r", topic.name, exc)
                    continue

                if not decisions:
                    continue

                for d in decisions:
                    try:
                        repo.upsert_source_candidate_eval(
                            candidate_id=int(d.candidate_id),
                            decision=str(d.decision or ""),
                            score=int(getattr(d, "score", 0) or 0),
                            quality_score=int(getattr(d, "quality_score", 0) or 0),
                            relevance_score=int(getattr(d, "relevance_score", 0) or 0),
                            novelty_score=int(getattr(d, "novelty_score", 0) or 0),
                            why=str(getattr(d, "why", "") or ""),
                            model=str(getattr(d, "model", "") or ""),
                            explore_weight=int(explore_weight),
                            exploit_weight=int(exploit_weight),
                        )
                    except Exception:
                        pass

                    if d.decision == "accept":
                        # Enforce the operator threshold regardless of model behavior.
                        try:
                            if int(getattr(d, "score", 0) or 0) < int(min_source_score):
                                try:
                                    ignore_source_candidate_action(session=session, candidate_id=d.candidate_id)
                                except Exception:
                                    pass
                                continue
                        except Exception:
                            pass

                        if not auto_accept_sources:
                            # Leave the candidate as `new` but keep the eval + score for UI sorting.
                            continue

                        try:
                            source = accept_source_candidate_action(session=session, candidate_id=d.candidate_id, enabled=True)
                        except Exception as exc:
                            logger.warning(
                                "auto-accept candidate failed: topic=%s candidate_id=%s err=%r",
                                topic.name,
                                d.candidate_id,
                                exc,
                            )
                            continue

                        try:
                            cand = cand_by_id.get(d.candidate_id)
                            discovered_from = getattr(cand, "discovered_from_url", "") if cand else ""
                            note = (
                                f"[auto-accept] topic={topic.name} candidate_id={d.candidate_id} "
                                f"score={int(getattr(d, 'score', 0) or 0)} why={d.why}"
                            ).strip()
                            if discovered_from:
                                note += f" discovered_from={discovered_from}"

                            existing = repo.get_source_meta(source_id=source.id)
                            prev = (existing.notes if existing else "") or ""
                            merged = (prev.strip() + ("\n" if prev.strip() else "") + note).strip()
                            repo.update_source_meta(source_id=source.id, notes=merged)
                        except Exception:
                            pass
                        try:
                            repo.upsert_source_score(
                                source_id=int(source.id),
                                score=int(getattr(d, "score", 0) or 0),
                                quality_score=int(getattr(d, "quality_score", 0) or 0),
                                relevance_score=int(getattr(d, "relevance_score", 0) or 0),
                                novelty_score=int(getattr(d, "novelty_score", 0) or 0),
                                origin="auto",
                                note=f"auto-accept topic={topic.name} why={str(getattr(d,'why','') or '')}"[:4000],
                            )
                        except Exception:
                            pass

                    elif d.decision == "ignore":
                        try:
                            ignore_source_candidate_action(session=session, candidate_id=d.candidate_id)
                        except Exception:
                            pass

            # Enforce global cap by evicting lowest-scoring enabled sources.
            try:
                if max_sources_total > 0:
                    # Exclude search seed sources (they are infra for discovery, not content sources).
                    excluded_types = {"searxng_search", "hn_search"}
                    enabled_sources = [s for s in repo.list_sources() if s.enabled and (s.type not in excluded_types)]
                    over = len(enabled_sources) - int(max_sources_total)
                    if over > 0:
                        # Prefer evicting auto-scored sources first; then any unlocked sources with missing score.
                        scores_by_id: dict[int, tuple[int, bool]] = {}
                        for sc in repo.list_source_scores(limit=10_000):
                            try:
                                sid = int(getattr(sc, "source_id", 0) or 0)
                            except Exception:
                                continue
                            scores_by_id[sid] = (int(getattr(sc, "score", 0) or 0), bool(getattr(sc, "locked", False)))

                        def _key(src: object) -> tuple[int, int]:
                            sid = int(getattr(src, "id", 0) or 0)
                            score = scores_by_id.get(sid, (0, False))[0]
                            locked = scores_by_id.get(sid, (0, False))[1]
                            # Locked sources should be last.
                            return (1 if locked else 0, score)

                        evictable = [s for s in enabled_sources if not scores_by_id.get(int(getattr(s, "id", 0) or 0), (0, False))[1]]
                        evictable.sort(key=_key)
                        for s in evictable[:over]:
                            sid = int(getattr(s, "id", 0) or 0)
                            sc = scores_by_id.get(sid, (0, False))[0]
                            try:
                                repo.set_source_enabled(sid, False)
                            except Exception:
                                continue
                            try:
                                existing = repo.get_source_meta(source_id=sid)
                                prev = (existing.notes if existing else "") or ""
                                note = f"[auto-evict] reason=cap max_sources_total={max_sources_total} score={sc}"
                                merged = (prev.strip() + ("\n" if prev.strip() else "") + note).strip()
                                repo.update_source_meta(source_id=sid, notes=merged)
                            except Exception:
                                pass
            except Exception:
                pass

        return DiscoverSourcesResult(per_topic=per_topic)


async def run_digest(
    *,
    session: Session,
    settings: Settings,
    hours: int,
    push: bool,
    topic_ids: list[int] | None = None,
    key_suffix: str | None = None,
) -> DigestResult:
    repo = Repo(session)
    # Apply DB-backed dynamic overrides for non-secret Settings fields.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass
    out_lang = _output_lang(repo=repo, settings=settings)
    try:
        settings_out = settings.model_copy(update={"output_language": out_lang})  # type: ignore[attr-defined]
    except Exception:
        settings_out = settings
    llm_usage_cb = make_llm_usage_recorder(session=session)
    now_utc = dt.datetime.utcnow()
    since = now_utc - dt.timedelta(hours=hours)
    policies_by_topic_id = {p.topic_id: p for p in repo.list_topic_policies()}

    # Explicit operator feedback: muted domains should not appear in digests.
    active_mute_domains: set[str] = set()
    try:
        active_mute_domains = {
            (m.key or "").strip().lower()
            for m in repo.list_active_mute_rules()
            if (getattr(m, "scope", "") or "").strip() == "domain" and (m.key or "").strip()
        }
    except Exception:
        active_mute_domains = set()

    # Quality tiering (optional): filter low-quality domains from digest output.
    try:
        from tracker.domain_quality import build_domain_quality_policy

        domain_policy = build_domain_quality_policy(settings=settings)
        if (
            (not domain_policy.low_patterns)
            and (not domain_policy.medium_patterns)
            and (not domain_policy.high_patterns)
            and int(domain_policy.min_push_rank) == 1
        ):
            domain_policy = None
    except Exception:
        domain_policy = None

    source_url_by_id: dict[int, str] = {int(s.id): str(s.url or "") for s in repo.list_sources()}

    # Source score hard filter (0..100). Falls back to a tier-derived numeric score when missing.
    try:
        min_source_score = int(getattr(settings, "source_quality_min_score", 0) or 0)
    except Exception:
        min_source_score = 0
    min_source_score = max(0, min(100, int(min_source_score)))
    scores_by_source_id: dict[int, int] = {}
    try:
        for sc in repo.list_source_scores(limit=10_000):
            sid = int(getattr(sc, "source_id", 0) or 0)
            if sid <= 0:
                continue
            scores_by_source_id[sid] = int(getattr(sc, "score", 0) or 0)
    except Exception:
        scores_by_source_id = {}

    def _tier_score_for_url(url: str) -> int:
        if not domain_policy:
            return 50
        try:
            tier = str(domain_policy.tier_for_url(url) or "unknown").strip().lower()
        except Exception:
            tier = "unknown"
        if tier == "high":
            return 75
        if tier == "medium":
            return 55
        if tier == "low":
            return 35
        return 45

    def _effective_source_score(source_id: int) -> int:
        sid = int(source_id or 0)
        if sid > 0 and sid in scores_by_source_id:
            return max(0, min(100, int(scores_by_source_id.get(sid) or 0)))
        return _tier_score_for_url(source_url_by_id.get(sid, ""))

    def _safe_url_for_item(item: Item) -> str:
        u = (str(getattr(item, "canonical_url", "") or "") or str(getattr(item, "url", "") or "")).strip()
        if _is_local_url(u):
            src_url = source_url_by_id.get(int(getattr(item, "source_id", 0) or 0), "")
            u = _rewrite_local_url_to_source_host(url=u, source_url=src_url)
        return u

    # --- Auth / Cookie jar (optional; reused by fulltext enrichment)
    cookie_jar = parse_cookie_jar_json(getattr(settings, "cookie_jar_json", "") or "")

    async def _cookie_header_cb(url: str) -> str | None:
        # Static cookie jar only.
        static_cookie = cookie_header_for_url(url=url, cookie_jar=cookie_jar)
        return static_cookie or None

    per_topic: list[DigestTopicResult] = []

    topics = repo.list_topics()
    logger.info("digest start: topics=%d hours=%d push=%s", len(topics), hours, push)
    for topic in topics:
        if not topic.enabled:
            continue
        if topic_ids is not None and topic.id not in topic_ids:
            continue

        # Optional LLM-native curation (prompt-driven): convert candidate → ignore|digest|alert.
        policy = policies_by_topic_id.get(topic.id)
        use_llm_curation = bool(
            settings.llm_curation_enabled
            and settings.llm_base_url
            and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
            and policy
            and policy.llm_curation_enabled
        )
        if use_llm_curation:
            # Include legacy heuristic "digest" decisions so LLM curation can cap the daily digest
            # even if the topic was enabled mid-day.
            pool_rows = repo.list_item_topics_for_curation(
                topic=topic,
                since=since,
                limit=500,
                decisions=["candidate", "digest"],
            )
            if pool_rows:
                content_cache: dict[int, str] = {}

                def _best_text(item_id: int, fallback: str) -> str:
                    cached = content_cache.get(item_id)
                    if cached is not None:
                        return cached
                    row = repo.get_item_content(item_id=item_id)
                    txt = (row.content_text if row and row.content_text else fallback).strip()
                    content_cache[item_id] = txt
                    return txt

                # Optional full-text enrichment for higher-quality curation.
                if settings.fulltext_enabled:
                    max_fetches = max(0, int(settings.fulltext_max_fetches_per_topic or 0))
                    prefetch_max_candidates = max(1, int(settings.llm_curation_max_candidates or 1))
                    fetched = 0
                    for _it_row, item in pool_rows[:prefetch_max_candidates]:
                        if fetched >= max_fetches:
                            break
                        url = (item.url or item.canonical_url or "").strip()
                        if not url.startswith(("http://", "https://")):
                            continue
                        try:
                            parts = urlsplit(url)
                            host = (parts.netloc or "").lower()
                            path = parts.path or ""
                        except Exception:
                            host = ""
                            path = ""
                        if host.endswith("nodeseek.com"):
                            continue
                        # Internal API-like endpoints aren't meaningful to fulltext-enrich.
                        if path.startswith("/v1/models"):
                            continue
                        existing = repo.get_item_content(item_id=item.id)
                        if existing and (existing.content_text or (existing.error or "").strip()):
                            continue
                        try:
                            cookie = await _cookie_header_cb(url)
                            text = await fetch_fulltext_for_url(
                                url=url,
                                timeout_seconds=int(settings.fulltext_timeout_seconds or settings.http_timeout_seconds),
                                max_chars=int(settings.fulltext_max_chars or 1),
                                discourse_cookie=((settings.discourse_cookie or "").strip() or cookie or None),
                                cookie_header=cookie,
                            )
                        except Exception as exc:
                            logger.info("fulltext fetch failed: url=%s err=%s", url, exc)
                            try:
                                err = str(exc or "").strip()
                                if len(err) > 400:
                                    err = err[:400] + "…"
                                repo.upsert_item_content(item_id=item.id, url=url, content_text="", error=err)
                            except Exception:
                                pass
                            continue
                        try:
                            repo.upsert_item_content(item_id=item.id, url=url, content_text=text, error="")
                            content_cache[item.id] = text.strip()
                            fetched += 1
                        except Exception as exc:
                            logger.info("fulltext store failed: item_id=%s err=%s", item.id, exc)

                # History-based anti-dup: seed simhashes from previous digested/alerted items so the
                # LLM doesn't repeatedly see the same story across days.
                #
                # Note: we only look at items *before* the current digest window (`until=since`),
                # so we don't accidentally filter the current pool against itself.
                history_seen: list[int] = []
                history_days = max(0, int(settings.llm_curation_history_dedupe_days or 0))
                recent_sent: list[dict[str, str]] = []
                if history_days > 0:
                    hist_since = dt.datetime.utcnow() - dt.timedelta(days=history_days)
                    try:
                        history_seen = repo.list_item_simhashes_for_topic_window(
                            topic=topic,
                            since=hist_since,
                            until=since,
                            decisions=["digest", "alert"],
                            limit=5000,
                        )
                    except Exception:
                        history_seen = []
                    try:
                        # Keep this bounded; it's only used as prompt context.
                        recent_sent = repo.list_recent_sent_items_for_topic_window(
                            topic=topic,
                            since=hist_since,
                            until=since,
                            decisions=["digest", "alert"],
                            limit=20,
                        )
                    except Exception:
                        recent_sent = []

                # Normalize the pool to "candidate" so non-selected items don't show up in the digest list.
                for it_row, _item in pool_rows:
                    it_row.decision = "candidate"

                final_max_candidates = max(1, int(settings.llm_curation_max_candidates or 1))
                triage_enabled = bool(getattr(settings, "llm_curation_triage_enabled", False)) and bool(
                    (getattr(settings, "llm_model_mini", None) or "").strip()
                )
                pool_max_candidates = final_max_candidates
                if triage_enabled:
                    try:
                        pool_max_candidates = int(getattr(settings, "llm_curation_triage_pool_max_candidates", 0) or 0)
                    except Exception:
                        pool_max_candidates = 0
                    if pool_max_candidates <= 0:
                        pool_max_candidates = final_max_candidates
                    pool_max_candidates = max(pool_max_candidates, final_max_candidates)
                    pool_max_candidates = min(pool_max_candidates, 500)

                # Topic-level anti-dup: avoid sending near-identical candidates to the LLM.
                # This reduces token waste and prevents the digest from containing the same story multiple times
                # (e.g., reposts / mirrors / multiple sources pointing to the same release).
                candidates: list[dict] = []
                seen: list[int] = list(history_seen)
                seen_story: list[int] = []
                history_snippets: dict[int, str] = {}

                def _history_snippet(item_id: int) -> str:
                    cached = history_snippets.get(item_id)
                    if cached is not None:
                        return cached
                    try:
                        row = repo.get_item_content(item_id=item_id)
                    except Exception:
                        row = None
                    txt = (row.content_text if row and row.content_text else "").strip()
                    if len(txt) > 2000:
                        txt = txt[:2000]
                    history_snippets[item_id] = txt
                    return txt

                for r in (recent_sent or [])[:50]:
                    t = str(r.get("title") or "").strip()
                    u = str(r.get("url") or "").strip()
                    iid = 0
                    try:
                        iid = int(r.get("item_id") or 0)
                    except Exception:
                        iid = 0
                    story = story_dedupe_text(
                        title=t,
                        url=u,
                        snippet=_history_snippet(iid) if iid > 0 else "",
                    )
                    if not story:
                        continue
                    sh = simhash64(story)
                    seen_story.append(int_to_signed64(sh))
                history_urls: set[str] = {
                    str(r.get("url") or "").strip()
                    for r in (recent_sent or [])
                    if str(r.get("url") or "").strip()
                }
                for _it, item in pool_rows:
                    if len(candidates) >= pool_max_candidates:
                        break
                    if str(item.canonical_url or "").strip() in history_urls:
                        continue
                    snippet = _best_text(item.id, (item.content_text or ""))
                    text_for_dedupe = (snippet or "").strip() or (item.title or "").strip()
                    if text_for_dedupe:
                        sh = simhash64(text_for_dedupe)
                        if is_near_duplicate(new_simhash=sh, existing_simhashes=seen):
                            continue
                        seen.append(int_to_signed64(sh))
                    story = story_dedupe_text(
                        title=(item.title or "").strip(),
                        url=str(item.canonical_url or "").strip(),
                        snippet=(snippet or "").strip(),
                    )
                    if story:
                        sh = simhash64(story)
                        if is_near_duplicate(new_simhash=sh, existing_simhashes=seen_story, max_distance=6):
                            continue
                        seen_story.append(int_to_signed64(sh))
                    candidates.append(
                        {
                            "item_id": item.id,
                            "title": item.title,
                            "url": item.canonical_url,
                            "snippet": snippet,
                        }
                    )

                _annotate_candidates_domain_feedback(repo=repo, candidates=candidates)

                # Optional cheap triage stage (mini model): reduce the pool to a bounded set before full curation.
                #
                # AI-only filtering requirement:
                # - If triage succeeds (even if it returns an empty list), respect it.
                # - If triage fails (None), do NOT deterministically slice candidates; instead pass the full
                #   bounded pool to the reasoning model so no relevance filtering happens outside AI.
                triage_keep_ids: list[int] | None = None
                if triage_enabled and len(candidates) > final_max_candidates:
                    keep_max = 0
                    try:
                        keep_max = int(getattr(settings, "llm_curation_triage_keep_candidates", 0) or 0)
                    except Exception:
                        keep_max = 0
                    if keep_max <= 0:
                        keep_max = final_max_candidates
                    keep_max = max(1, min(keep_max, len(candidates)))
                    try:
                        triage_keep_ids = await llm_triage_topic_items(
                            repo=repo,
                            settings=settings_out,
                            topic=topic,
                            policy_prompt=policy.llm_curation_prompt,
                            candidates=candidates,
                            recent_sent=recent_sent,
                            max_keep=keep_max,
                            usage_cb=llm_usage_cb,
                        )
                    except Exception as exc:
                        logger.info("llm triage failed (digest): topic=%s err=%s", topic.name, exc)
                        triage_keep_ids = None

                    if triage_keep_ids is not None:
                        by_id: dict[int, dict] = {}
                        for c in candidates:
                            try:
                                cid = int(c.get("item_id"))
                            except Exception:
                                continue
                            if cid > 0 and cid not in by_id:
                                by_id[cid] = c
                        candidates = [by_id[i] for i in triage_keep_ids if i in by_id]

                try:
                    decisions = await llm_curate_topic_items(
                        repo=repo,
                        settings=settings_out,
                        topic=topic,
                        policy_prompt=policy.llm_curation_prompt,
                        candidates=candidates,
                        recent_sent=recent_sent,
                        max_digest=max(0, int(settings.llm_curation_max_digest or 0)),
                        max_alert=max(0, int(settings.llm_curation_max_alert or 0)),
                        usage_cb=llm_usage_cb,
                    )
                except Exception as exc:
                    session.rollback()
                    decisions = None
                    logger.warning("llm curation failed (digest): topic=%s err=%r", topic.name, exc)

                if decisions is None:
                    session.rollback()
                    # Reliability: if the LLM is temporarily unavailable (e.g., tunnel drop),
                    # "fail open" with a tiny fallback digest so operators don't get silent days.
                    if bool(getattr(settings, "llm_curation_fail_open", False)):
                        try:
                            fail_open_max = max(0, int(getattr(settings, "llm_curation_fail_open_max_digest", 3) or 3))
                            cap = max(0, int(settings.llm_curation_max_digest or 0))
                            max_fb = min(fail_open_max, cap) if cap > 0 else 0
                            max_fb = max(0, min(max_fb, len(candidates)))
                            if max_fb > 0 and candidates:
                                # AI-only fallback: attempt to use the mini triage model to pick a tiny set.
                                keep_ids: list[int] | None = None
                                if triage_keep_ids is not None:
                                    keep_ids = triage_keep_ids
                                else:
                                    try:
                                        keep_ids = await llm_triage_topic_items(
                                            repo=repo,
                                            settings=settings_out,
                                            topic=topic,
                                            policy_prompt=policy.llm_curation_prompt,
                                            candidates=candidates,
                                            recent_sent=recent_sent,
                                            max_keep=max_fb,
                                            usage_cb=llm_usage_cb,
                                        )
                                    except Exception as exc:
                                        logger.info("llm triage failed (fail-open): topic=%s err=%s", topic.name, exc)
                                        keep_ids = None

                                if keep_ids:
                                    by_id: dict[int, dict] = {}
                                    for c in candidates:
                                        try:
                                            cid = int(c.get("item_id"))
                                        except Exception:
                                            continue
                                        if cid > 0 and cid not in by_id:
                                            by_id[cid] = c
                                    picked: list[int] = []
                                    for iid in keep_ids:
                                        if iid in by_id and iid not in picked:
                                            picked.append(iid)
                                        if len(picked) >= max_fb:
                                            break
                                    for iid in picked:
                                        it_row = repo.get_item_topic(item_id=iid, topic_id=topic.id)
                                        if not it_row:
                                            continue
                                        it_row.decision = "digest"
                                        it_row.reason = "llm_fallback: llm curation failed; triage-only digest"
                                    session.commit()
                        except Exception as exc:
                            session.rollback()
                            logger.info("llm curation fail-open fallback failed: topic=%s err=%s", topic.name, exc)
                else:
                    for d in decisions:
                        it_row = repo.get_item_topic(item_id=d.item_id, topic_id=topic.id)
                        if not it_row:
                            continue
                        it_row.decision = d.decision
                        it_row.reason = _format_llm_curation_reason(summary=d.summary, why=d.why, hint="digest")
                    session.commit()

        items = repo.list_item_topics_for_digest(topic=topic, since=since)

        prev_since = since - dt.timedelta(hours=hours)
        prev_items = repo.list_item_topics_for_digest_window(topic=topic, since=prev_since, until=since)
        url_overrides: dict[int, str] = {}
        for _it, item in items:
            try:
                iid = int(getattr(item, "id", 0) or 0)
            except Exception:
                iid = 0
            if iid > 0:
                url_overrides[iid] = _safe_url_for_item(item)
        for _it, item in prev_items:
            try:
                iid = int(getattr(item, "id", 0) or 0)
            except Exception:
                iid = 0
            if iid > 0 and iid not in url_overrides:
                url_overrides[iid] = _safe_url_for_item(item)
        if active_mute_domains or domain_policy or (min_source_score > 0):
            def _host(u: str) -> str:
                try:
                    h = (urlsplit((u or "").strip()).netloc or "").lower()
                    h = h.split(":", 1)[0].lstrip(".")
                    if h.startswith("www."):
                        h = h[4:]
                    return h
                except Exception:
                    return ""

            def _keep(url: str, *, source_id: int) -> bool:
                u = (url or "").strip()
                if active_mute_domains and _host(u) in active_mute_domains:
                    return False
                if domain_policy and (not domain_policy.allows_push_url(u)):
                    return False
                if min_source_score > 0 and _effective_source_score(int(source_id or 0)) < int(min_source_score):
                    return False
                return True

            items = [
                (it, item)
                for it, item in items
                if _keep(
                    str(
                        url_overrides.get(int(getattr(item, "id", 0) or 0), (item.canonical_url or "")).strip()
                    ),
                    source_id=int(getattr(item, "source_id", 0) or 0),
                )
            ]
            prev_items = [
                (it, item)
                for it, item in prev_items
                if _keep(
                    str(
                        url_overrides.get(int(getattr(item, "id", 0) or 0), (item.canonical_url or "")).strip()
                    ),
                    source_id=int(getattr(item, "source_id", 0) or 0),
                )
            ]

        def _dedupe_digest_rows(rows: list[tuple[ItemTopic, Item]]) -> list[tuple[ItemTopic, Item]]:
            """
            Curated Info is a de-dupe surface.

            Deduping is intentionally conservative:
            - primary key: canonicalized URL (if present)
            - fallback key: normalized title (only when URL is missing)
            """
            by_key: dict[str, tuple[ItemTopic, Item]] = {}

            def _pri(decision: str) -> int:
                d = (decision or "").strip().lower()
                return 0 if d == "alert" else 1

            for it_row, item in rows:
                try:
                    iid = int(getattr(item, "id", 0) or 0)
                except Exception:
                    iid = 0

                url_raw = ""
                try:
                    url_raw = str(
                        url_overrides.get(iid)
                        or getattr(item, "canonical_url", None)
                        or getattr(item, "url", None)
                        or ""
                    ).strip()
                except Exception:
                    url_raw = ""
                url_key = canonicalize_url(url_raw) if url_raw else ""

                title_raw = ""
                try:
                    title_raw = str(getattr(item, "title", "") or "").strip()
                except Exception:
                    title_raw = ""
                title_key = normalize_text(title_raw).casefold() if title_raw else ""

                key = url_key or (f"title:{title_key}" if title_key else f"id:{iid}")
                existing = by_key.get(key)
                if existing is None:
                    by_key[key] = (it_row, item)
                    continue

                ex_it, ex_item = existing
                if _pri(str(getattr(it_row, "decision", "") or "")) < _pri(str(getattr(ex_it, "decision", "") or "")):
                    by_key[key] = (it_row, item)
                    continue
                if _pri(str(getattr(it_row, "decision", "") or "")) > _pri(str(getattr(ex_it, "decision", "") or "")):
                    continue

                # Same priority: keep the newer published/created timestamp.
                try:
                    ts_new = getattr(item, "published_at", None) or getattr(item, "created_at", None)
                except Exception:
                    ts_new = None
                try:
                    ts_old = getattr(ex_item, "published_at", None) or getattr(ex_item, "created_at", None)
                except Exception:
                    ts_old = None
                try:
                    if isinstance(ts_new, dt.datetime) and isinstance(ts_old, dt.datetime) and ts_new > ts_old:
                        by_key[key] = (it_row, item)
                except Exception:
                    pass

            return list(by_key.values())

        items = _dedupe_digest_rows(items)
        prev_items = _dedupe_digest_rows(prev_items)

        prev_total = len(prev_items)
        prev_alerts = sum(1 for it, _item in prev_items if it.decision == "alert")

        # Digest is a de-dupe/aggregation surface (no long-form synthesis).
        # Any interpretive "survey" belongs in a separate report, not Digest.
        llm_summary = None

        markdown = format_digest_markdown(
            topic=topic,
            items=items,
            since=since,
            until=now_utc,
            tz_name=settings.cron_timezone,
            lang=out_lang,
            url_overrides_by_item_id=url_overrides,
            previous_total=prev_total,
            previous_alerts=prev_alerts,
            previous_items=prev_items,
            llm_summary=llm_summary,
        )
        markdown = _append_login_required_section(markdown=markdown, repo=repo, settings=settings, lang=out_lang)
        day = _local_day_iso(settings)
        digest_key = f"digest:{topic.id}:{day}"
        suffix = (key_suffix or "").strip()
        if not suffix:
                # Curated Info runs can happen multiple times per day; ensure each run is an immutable snapshot.
                # Use local HHMM by default.
                try:
                    tz, tz_ok = resolve_cron_timezone((settings.cron_timezone or "UTC").strip() or "UTC")
                    if not tz_ok:
                        tz = dt.timezone.utc
                    suffix = now_utc.replace(tzinfo=dt.timezone.utc).astimezone(tz).strftime("%H%M")
                except Exception:
                    suffix = now_utc.strftime("%H%M")
        if suffix:
            safe = suffix.replace(":", "-").replace("/", "-").replace("\\", "-").strip()
            if len(safe) > 40:
                safe = safe[:40]
            if safe:
                digest_key = f"{digest_key}:{safe}"
        repo.upsert_report(
            kind="digest",
            idempotency_key=digest_key,
            topic_id=topic.id,
            title=f"Curated Info: {topic.name}",
            markdown=markdown,
        )
        pushed = 0

        if push and (items or bool(getattr(settings, "digest_push_empty", True))):
            try:
                pushed += 1 if await push_dingtalk_markdown(
                    repo=repo,
                    settings=settings,
                    idempotency_key=digest_key,
                    title=(f"参考消息：{topic.name}" if out_lang == "zh" else f"Curated Info: {topic.name}"),
                    markdown=markdown,
                ) else 0
            except Exception:
                pass
            try:
                use_reader = bool(getattr(settings, "telegram_digest_reader_enabled", True))
                if use_reader:
                    pushed += 1 if await push_telegram_report_reader(
                        repo=repo,
                        settings=settings,
                        idempotency_key=digest_key,
                        markdown=markdown,
                    ) else 0
                else:
                    pushed += 1 if await push_telegram_text(
                        repo=repo,
                        settings=settings,
                        idempotency_key=digest_key,
                        text=format_im_text(markdown),
                    ) else 0
            except Exception:
                pass
            try:
                pushed += 1 if push_email_text(
                    repo=repo,
                    settings=settings,
                    idempotency_key=digest_key,
                    subject=f"[Curated] {topic.name}",
                    text=markdown,
                ) else 0
            except Exception:
                pass
            try:
                pushed += 1 if await push_webhook_json(
                    repo=repo,
                    settings=settings,
                    idempotency_key=digest_key,
                    payload={
                        "type": "curated",
                        "topic": topic.name,
                        "topic_id": topic.id,
                        "date": day,
                        "markdown": markdown,
                    },
                ) else 0
            except Exception:
                pass

        per_topic.append(
            DigestTopicResult(topic_name=topic.name, pushed=pushed, markdown=markdown, idempotency_key=digest_key)
        )

    logger.info("digest done: topics=%d", len(per_topic))
    return DigestResult(since=since, per_topic=per_topic)


async def run_curated_info(
    *,
    session: Session,
    settings: Settings,
    hours: int,
    push: bool,
    key_suffix: str | None = None,
    now: dt.datetime | None = None,
) -> CuratedInfoResult:
    """
    Build ONE cross-topic Curated Info batch (de-dupe only; no interpretation).
    """
    repo = Repo(session)
    # Apply DB-backed dynamic overrides for non-secret Settings fields.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass
    out_lang = _output_lang(repo=repo, settings=settings)
    llm_usage_cb = make_llm_usage_recorder(session=session)

    # Window: based on the provided `now` (if any) and `hours`.
    ref = now
    if ref is None:
        now_utc = dt.datetime.utcnow()
    else:
        if ref.tzinfo is None:
            now_utc = ref
        else:
            now_utc = ref.astimezone(dt.timezone.utc).replace(tzinfo=None)
    h = int(hours or 0)
    if h <= 0:
        h = 24
    since = now_utc - dt.timedelta(hours=h)

    def _recent_curated_rows() -> list[tuple[object, object, object, object]]:
        return repo.list_recent_events(
            topic=None,
            decisions=["alert", "digest"],
            since=since,
            limit=5000,
        )

    rows = _recent_curated_rows()
    stalled_topic_ids: list[int] = []
    try:
        for topic in repo.list_topics():
            if not getattr(topic, "enabled", True):
                continue
            pending = repo.list_uncurated_item_topics_for_topic(topic=topic, since=since, limit=1)
            if pending:
                stalled_topic_ids.append(int(topic.id))
    except Exception:
        stalled_topic_ids = []

    if stalled_topic_ids:
        log_fn = logger.warning if not rows else logger.info
        log_fn(
            "curated info found pending candidate backlog; auto-running digest repair: hours=%s topics=%s preexisting_rows=%s",
            h,
            ",".join(str(x) for x in stalled_topic_ids),
            len(rows),
        )
        try:
            await run_digest(
                session=session,
                settings=settings,
                hours=h,
                push=False,
                topic_ids=stalled_topic_ids,
                key_suffix=f"autorepair-{now_utc.strftime('%Y%m%d%H%M%S')}",
            )
            try:
                session.expire_all()
            except Exception:
                pass
            rows = _recent_curated_rows()
        except Exception as exc:
            logger.warning("curated info auto-repair failed: %s", exc)

    # Explicit operator feedback: muted domains should not appear in Curated Info.
    active_mute_domains: set[str] = set()
    try:
        active_mute_domains = {
            (m.key or "").strip().lower()
            for m in repo.list_active_mute_rules()
            if (getattr(m, "scope", "") or "").strip() == "domain" and (m.key or "").strip()
        }
    except Exception:
        active_mute_domains = set()

    # Quality tiering (optional): filter low-quality domains from Curated Info output.
    try:
        from tracker.domain_quality import build_domain_quality_policy

        domain_policy = build_domain_quality_policy(settings=settings)
        if (
            (not domain_policy.low_patterns)
            and (not domain_policy.medium_patterns)
            and (not domain_policy.high_patterns)
            and int(domain_policy.min_push_rank) == 1
        ):
            domain_policy = None
    except Exception:
        domain_policy = None

    # Build a cross-topic item map.
    #
    # Key: Item.id
    # Value: {title,url,ts,decision,topics(set)}
    by_item_id: dict[int, dict] = {}

    # NOTE: list_recent_events is ordered by recency; we still re-sort after grouping.
    for it_row, item, topic, source in rows:
        try:
            if not getattr(topic, "enabled", True):
                continue
        except Exception:
            continue
        iid = int(getattr(item, "id", 0) or 0)
        if iid <= 0:
            continue

        url = _best_push_url_for_item(item=item, source=source)
        if not url:
            continue

        host = _url_host(url)
        if active_mute_domains and host in active_mute_domains:
            continue
        if domain_policy and (not domain_policy.allows_push_url(url)):
            continue

        when = item.published_at or item.created_at
        ts = int(when.timestamp()) if when else 0
        dec = (str(getattr(it_row, "decision", "") or "")).strip().lower() or "digest"
        if dec not in {"alert", "digest"}:
            dec = "digest"

        summary, why = extract_llm_summary_why((getattr(it_row, "reason", "") or ""))
        entry = by_item_id.get(iid)
        if entry is None:
            entry = {
                "item_id": iid,
                "title": (item.title or "").strip(),
                "url": url,
                "ts": ts,
                "decision": dec,
                "topics": set(),
                "summary": summary,
                "why": why,
                "content_text": (item.content_text or "").strip(),
            }
            by_item_id[iid] = entry
        # Merge topics and decision priority (alert beats digest).
        try:
            entry["topics"].add((topic.name or "").strip())
        except Exception:
            pass
        if dec == "alert":
            entry["decision"] = "alert"
        # Keep the freshest title/url if the item row changes (best-effort).
        if ts and int(entry.get("ts") or 0) < ts:
            entry["ts"] = ts
            if (item.title or "").strip():
                entry["title"] = (item.title or "").strip()
            if url:
                entry["url"] = url
        if summary and not str(entry.get("summary") or "").strip():
            entry["summary"] = summary
        if why and not str(entry.get("why") or "").strip():
            entry["why"] = why
        if (item.content_text or "").strip() and not str(entry.get("content_text") or "").strip():
            entry["content_text"] = (item.content_text or "").strip()

    def _norm_lang(v: str) -> str:
        raw = (v or "").strip()
        low = raw.lower()
        if raw in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语"}:
            return "zh"
        if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
            return "zh"
        if low in {"en", "en-us", "english", "英文"} or low.startswith("en"):
            return "en"
        return "en"

    def _fmt_ts_local(ts_utc: dt.datetime) -> str:
        try:
            tz_raw = (settings.cron_timezone or "").strip()
            tz, tz_ok = resolve_cron_timezone(tz_raw)
            if not tz_ok:
                tz = dt.timezone.utc
            local = ts_utc.replace(tzinfo=dt.timezone.utc).astimezone(tz)
            return local.replace(second=0, microsecond=0).isoformat(timespec="minutes")
        except Exception:
            try:
                return ts_utc.replace(second=0, microsecond=0).isoformat(timespec="minutes")
            except Exception:
                return str(ts_utc)

    def _window_line(since_utc: dt.datetime, until_utc: dt.datetime) -> str:
        tz_raw = (settings.cron_timezone or "").strip()
        a = _fmt_ts_local(since_utc)
        b = _fmt_ts_local(until_utc)
        is_zh = _norm_lang(out_lang) == "zh"
        label = "窗口" if is_zh else "Window"
        if tz_raw and tz_raw.upper() != "UTC":
            return f"{label}: {a}–{b} ({tz_raw})"
        return f"{label}: {a}–{b} UTC"

    def _decision_label(decision: str) -> str:
        d = (decision or "").strip().lower()
        is_zh = _norm_lang(out_lang) == "zh"
        if not is_zh:
            return d or "digest"
        if d == "alert":
            return "告警"
        return "摘要"

    def _topics_short(topics: list[str], *, max_topics: int = 3) -> str:
        ts2 = [t for t in topics if (t or "").strip()]
        if not ts2:
            return ""
        ts2 = ts2[:]
        ts2 = sorted(set(ts2))
        more = max(0, len(ts2) - int(max_topics))
        shown = ts2[: int(max_topics)]
        if more <= 0:
            return ", ".join(shown)
        # Keep it short; this shows in References/cover.
        suffix = f"+{more}"
        return ", ".join(shown + [suffix])

    is_zh = _norm_lang(out_lang) == "zh"
    title = "参考消息" if is_zh else "Curated Info"

    # Dedupe across different item ids that end up pointing to the same canonical URL.
    #
    # Why: some feeds can emit duplicate items (same story) under different ids, and Curated Info
    # should not repeat them.
    by_url: dict[str, dict] = {}
    for e in by_item_id.values():
        u = str(e.get("url") or "").strip()
        u_key = canonicalize_url(u) if u else ""
        key = u_key or f"id:{int(e.get('item_id') or 0)}"
        existing = by_url.get(key)
        if existing is None:
            by_url[key] = e
            continue

        # Merge topics and decision priority.
        try:
            if isinstance(existing.get("topics"), set) and isinstance(e.get("topics"), set):
                existing["topics"].update(e["topics"])
        except Exception:
            pass
        if str(e.get("decision") or "").strip().lower() == "alert":
            existing["decision"] = "alert"

        # Prefer the newest representative title/url.
        try:
            if int(e.get("ts") or 0) > int(existing.get("ts") or 0):
                existing["ts"] = int(e.get("ts") or 0)
                if str(e.get("title") or "").strip():
                    existing["title"] = e.get("title", existing.get("title", ""))
                if u:
                    existing["url"] = u
        except Exception:
            pass
        for key2 in ("summary", "why", "content_text"):
            if str(e.get(key2) or "").strip() and not str(existing.get(key2) or "").strip():
                existing[key2] = e.get(key2, existing.get(key2, ""))

    items_all = list(by_url.values())
    items_all.sort(
        key=lambda e: (
            0 if str(e.get("decision") or "") == "alert" else 1,
            -int(e.get("ts") or 0),
            str(e.get("title") or ""),
        )
    )

    display_titles_by_item_id = await _localize_item_display_titles(
        repo=repo,
        settings=settings,
        entries=items_all,
        out_lang=out_lang,
        usage_cb=llm_usage_cb,
    )

    total = len(items_all)
    alerts = sum(1 for e in items_all if str(e.get("decision") or "") == "alert")
    digests = max(0, total - alerts)

    # Markdown output (kept compatible with the TG Reader parser).
    lines: list[str] = []
    lines.append(f"# {title}")
    lines.append("")
    lines.append(_window_line(since, now_utc))
    items_label = "条目" if is_zh else "Items"
    alerts_label = "告警" if is_zh else "alerts"
    digests_label = "摘要" if is_zh else "digests"
    lines.append(f"{items_label}: {total} ({alerts} {alerts_label}, {digests} {digests_label})")
    lines.append("")

    if total <= 0:
        lines.append("_暂无新条目。_" if is_zh else "_No new items._")
        markdown = "\n".join(lines).strip() + "\n"
    else:
        lines.append("## 条目" if is_zh else "## Items")
        lines.append("")

        refs: list[tuple[int, str, str]] = []
        for i, e in enumerate(items_all, start=1):
            item_id = int(e.get("item_id") or 0)
            t = str(display_titles_by_item_id.get(item_id) or e.get("title") or "").strip()
            u = str(e.get("url") or "").strip()
            topics = sorted([x for x in (e.get("topics") or set()) if str(x or "").strip()])
            topics_tag = _topics_short(topics)
            dec = _decision_label(str(e.get("decision") or "digest"))

            # Item list: clean, non-interpretive.
            cite = f" [{i}]" if u else ""
            tail = f"（{dec}）" if is_zh else f"({dec})"
            extra = f" · {topics_tag}" if topics_tag else ""
            # Keep the decision marker at the end so the TG reader can infer ALERT badges reliably.
            lines.append(f"- {t}{cite}{extra} {tail}".strip())

            # References: include a compact topic tag so the cover list is self-explanatory.
            ref_title = t
            if topics_tag:
                ref_title = f"{t}（{topics_tag}）" if is_zh else f"{t} ({topics_tag})"
            refs.append((i, ref_title, u))

        if refs:
            lines.append("")
            lines.append("References:")
            for n, t, u in refs:
                if not u:
                    continue
                title2 = t or f"Item {n}"
                lines.append(f"[{n}] {title2} — {u}")

        markdown = "\n".join(lines).strip() + "\n"

    markdown = _append_login_required_section(markdown=markdown, repo=repo, settings=settings, lang=out_lang)

    day = _local_day_iso(settings)
    key = f"digest:0:{day}"
    suffix = (key_suffix or "").strip()
    if not suffix:
        # Curated Info runs can happen multiple times per day; ensure each run is an immutable snapshot.
        try:
            tz, tz_ok = resolve_cron_timezone((settings.cron_timezone or "UTC").strip() or "UTC")
            if not tz_ok:
                tz = dt.timezone.utc
            suffix = now_utc.replace(tzinfo=dt.timezone.utc).astimezone(tz).strftime("%H%M")
        except Exception:
            suffix = now_utc.strftime("%H%M")
    if suffix:
        safe = suffix.replace(":", "-").replace("/", "-").replace("\\", "-").strip()
        if len(safe) > 40:
            safe = safe[:40]
        if safe:
            key = f"{key}:{safe}"

    repo.upsert_report(
        kind="digest",
        idempotency_key=key,
        topic_id=None,
        title=title,
        markdown=markdown,
    )

    pushed = 0
    if push and (total > 0 or bool(getattr(settings, "digest_push_empty", True))):
        try:
            pushed += 1 if await push_dingtalk_markdown(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                title=title,
                markdown=markdown,
            ) else 0
        except Exception:
            pass
        try:
            use_reader = bool(getattr(settings, "telegram_digest_reader_enabled", True))
            if use_reader:
                pushed += 1 if await push_telegram_report_reader(
                    repo=repo,
                    settings=settings,
                    idempotency_key=key,
                    markdown=markdown,
                ) else 0
            else:
                pushed += 1 if await push_telegram_text(
                    repo=repo,
                    settings=settings,
                    idempotency_key=key,
                    text=format_im_text(markdown),
                ) else 0
        except Exception:
            pass
        try:
            pushed += 1 if push_email_text(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                subject=(f"[Curated] {title}" if not is_zh else f"[参考消息] {title}"),
                text=markdown,
            ) else 0
        except Exception:
            pass
        try:
            pushed += 1 if await push_webhook_json(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                payload={
                    "type": "curated",
                    "date": day,
                    "markdown": markdown,
                },
            ) else 0
        except Exception:
            pass

    return CuratedInfoResult(since=since, pushed=pushed, markdown=markdown, idempotency_key=key)

async def run_health_report(*, session: Session, settings: Settings, push: bool) -> HealthResult:
    repo = Repo(session)
    # Apply DB-backed dynamic overrides for non-secret Settings fields.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass
    logger.info("health start: push=%s", push)
    stats = repo.get_stats()
    rows = repo.list_sources_with_health_and_meta()
    markdown = format_health_markdown(stats=stats, rows=rows)

    pushed = 0
    if push:
        day = _local_day_iso(settings)
        key = f"health:{day}"
        try:
                    pushed += 1 if await push_dingtalk_markdown(
                        repo=repo,
                        settings=settings,
                        idempotency_key=key,
                        title="OpenInfoMate Health",
                        markdown=markdown,
                    ) else 0
        except Exception:
            pass
        try:
            pushed += 1 if await push_telegram_text(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                text=markdown,
            ) else 0
        except Exception:
            pass
        try:
            pushed += 1 if push_email_text(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                subject="[Health] OpenInfoMate",
                text=markdown,
            ) else 0
        except Exception:
            pass
        try:
            pushed += 1 if await push_webhook_json(
                repo=repo,
                settings=settings,
                idempotency_key=key,
                payload={
                    "type": "health",
                    "date": day,
                    "markdown": markdown,
                    "stats": stats,
                },
            ) else 0
        except Exception:
            pass

    logger.info("health done: pushed=%d", pushed)
    key = f"health:{_local_day_iso(settings)}"
    repo.upsert_report(kind="health", idempotency_key=key, title="OpenInfoMate Health", markdown=markdown, topic_id=None)
    return HealthResult(pushed=pushed, markdown=markdown)
