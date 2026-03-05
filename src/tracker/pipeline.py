from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

from dateutil import parser as date_parser
from sqlalchemy import select
from sqlalchemy.orm import Session

from tracker.connectors.base import FetchedEntry
from tracker.connectors.discourse import DiscourseConnector
from tracker.connectors.html_list import HtmlListConnector
from tracker.connectors.html_list import parse_html_list_url
from tracker.connectors.hn_algolia import HnAlgoliaConnector
from tracker.connectors.llm_models import LlmModelsConnector
from tracker.connectors.rss import RssConnector
from tracker.connectors.searxng import SearxngConnector
from tracker.models import Item, ItemTopic, Source, Topic
from tracker.normalize import canonicalize_url, html_to_text, normalize_text, sha256_hex
from tracker.repo import Repo
from tracker.simhash import hamming_distance64, int_to_signed64, simhash64


@dataclass(frozen=True)
class CreatedDecision:
    topic_id: int
    topic_name: str
    decision: str
    reason: str
    item_id: int
    title: str
    canonical_url: str


async def fetch_entries_for_source(
    *,
    source: Source,
    timeout_seconds: int = 20,
    discourse_include_top_daily: bool = False,
    discourse_rss_catchup_pages: int = 1,
    discourse_cookie: str | None = None,
    llm_models_api_key: str | None = None,
    cookie_header_cb: Callable[[str], Awaitable[str | None]] | None = None,
) -> list[FetchedEntry]:
    if source.type == "rss":
        cookie = await cookie_header_cb(source.url) if cookie_header_cb else None
        return await RssConnector(timeout_seconds=timeout_seconds).fetch_with_cookie(url=source.url, cookie_header=cookie)
    if source.type == "hn_search":
        return await HnAlgoliaConnector(timeout_seconds=timeout_seconds).fetch(url=source.url)
    if source.type == "searxng_search":
        return await SearxngConnector(timeout_seconds=timeout_seconds).fetch(url=source.url)
    if source.type == "discourse":
        cookie = (discourse_cookie or "").strip() or (await cookie_header_cb(source.url) if cookie_header_cb else None)
        return await DiscourseConnector(
            timeout_seconds=timeout_seconds,
            rss_catchup_pages=discourse_rss_catchup_pages,
            cookie=cookie,
        ).fetch(
            url=source.url,
            include_top_daily=bool(discourse_include_top_daily),
        )
    if source.type == "html_list":
        cookie = None
        if cookie_header_cb:
            try:
                spec = parse_html_list_url(source.url)
                cookie = await cookie_header_cb(spec.page_url)
            except Exception:
                cookie = None
        return await HtmlListConnector(timeout_seconds=timeout_seconds).fetch(url=source.url, cookie_header=cookie)
    if source.type == "llm_models":
        return await LlmModelsConnector(timeout_seconds=timeout_seconds, api_key=llm_models_api_key).fetch(url=source.url)
    raise ValueError(f"unsupported source type: {source.type}")


def _parse_datetime_maybe(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = date_parser.parse(value)
        if not parsed.tzinfo:
            return parsed
        return parsed.astimezone(dt.timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _contains_any(text: str, needles_csv: str) -> bool:
    needles = [n.strip().lower() for n in needles_csv.split(",") if n.strip()]
    hay = text.lower()
    return any(n in hay for n in needles)


def _parse_domains_csv(value: str) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []
    out: list[str] = []
    for part in raw.split(","):
        p = (part or "").strip().lower()
        if not p:
            continue
        # Allow operators to paste full URLs; normalize to host-ish patterns.
        if "://" in p:
            try:
                p = urlsplit(p).netloc.lower()
            except Exception:
                pass
        p = p.split("/", 1)[0]
        p = p.split(":", 1)[0]  # strip port if any
        p = p.lstrip(".")
        if p.startswith("www."):
            p = p[4:]
        if p:
            out.append(p)
    return out


def _host_matches_domain_patterns(host: str, patterns: list[str]) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return False
    h = h.split(":", 1)[0]
    if h.startswith("www."):
        h = h[4:]
    for p in patterns:
        if not p:
            continue
        if h == p:
            return True
        if h.endswith("." + p):
            return True
    return False


def _extract_host_for_filter(canonical_url: str) -> str:
    u = (canonical_url or "").strip()
    if not u:
        return ""
    try:
        parts = urlsplit(u)
    except Exception:
        return ""
    if parts.scheme not in {"http", "https"}:
        return ""
    return parts.netloc or ""


def decide_item_for_topic(
    *,
    topic: Topic,
    title: str,
    content_text: str,
    canonical_url: str = "",
    include_keywords: str = "",
    exclude_keywords: str = "",
    include_domains: str = "",
    exclude_domains: str = "",
    match_mode: str = "keywords",  # keywords|llm
) -> tuple[str, str]:
    # v1 heuristic: keyword match ⇒ digest; alert_keywords ⇒ alert
    # optional LLM mode: pass filters ⇒ candidate (later curated by LLM)
    combined = f"{title}\n{content_text}"
    host = _extract_host_for_filter(canonical_url)
    include_patterns = _parse_domains_csv(include_domains)
    if include_patterns and not _host_matches_domain_patterns(host, include_patterns):
        return "ignore", "filtered by include_domains"
    exclude_patterns = _parse_domains_csv(exclude_domains)
    if exclude_patterns and _host_matches_domain_patterns(host, exclude_patterns):
        return "ignore", "filtered by exclude_domains"

    mode = (match_mode or "keywords").strip().lower()
    # In LLM curation mode, relevance is decided by the model prompt; avoid keyword prefilters
    # (both include/exclude) which can cause false negatives and violate "AI-only" filtering.
    if mode != "llm":
        if include_keywords and not _contains_any(combined, include_keywords):
            return "ignore", "filtered by include_keywords"
        if exclude_keywords and _contains_any(combined, exclude_keywords):
            return "ignore", "filtered by exclude_keywords"
    if mode == "llm":
        return "candidate", "llm curation candidate"

    if _contains_any(combined, topic.alert_keywords):
        return "alert", "matched alert_keywords"
    if _contains_any(combined, topic.query):
        return "digest", "matched topic query"
    return "ignore", "no match"


def is_near_duplicate(*, new_simhash: int, existing_simhashes: list[int], max_distance: int = 3) -> bool:
    return any(hamming_distance64(new_simhash, s) <= max_distance for s in existing_simhashes)


async def ingest_rss_source_for_topic(
    *,
    session: Session,
    topic: Topic,
    source: Source,
    timeout_seconds: int = 20,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> list[CreatedDecision]:
    connector = RssConnector(timeout_seconds=timeout_seconds)
    fetched = await connector.fetch(url=source.url)

    return ingest_entries_for_topic_source(
        session=session,
        topic=topic,
        source=source,
        entries=fetched,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


async def ingest_hn_search_source_for_topic(
    *,
    session: Session,
    topic: Topic,
    source: Source,
    timeout_seconds: int = 20,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> list[CreatedDecision]:
    connector = HnAlgoliaConnector(timeout_seconds=timeout_seconds)
    fetched = await connector.fetch(url=source.url)

    return ingest_entries_for_topic_source(
        session=session,
        topic=topic,
        source=source,
        entries=fetched,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


async def ingest_searxng_search_source_for_topic(
    *,
    session: Session,
    topic: Topic,
    source: Source,
    timeout_seconds: int = 20,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> list[CreatedDecision]:
    connector = SearxngConnector(timeout_seconds=timeout_seconds)
    fetched = await connector.fetch(url=source.url)

    return ingest_entries_for_topic_source(
        session=session,
        topic=topic,
        source=source,
        entries=fetched,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


async def ingest_discourse_source_for_topic(
    *,
    session: Session,
    topic: Topic,
    source: Source,
    timeout_seconds: int = 20,
    include_keywords: str = "",
    exclude_keywords: str = "",
) -> list[CreatedDecision]:
    connector = DiscourseConnector(timeout_seconds=timeout_seconds)
    fetched = await connector.fetch(url=source.url)

    return ingest_entries_for_topic_source(
        session=session,
        topic=topic,
        source=source,
        entries=fetched,
        include_keywords=include_keywords,
        exclude_keywords=exclude_keywords,
    )


def ingest_entries_for_topic_source(
    *,
    session: Session,
    topic: Topic,
    source: Source,
    entries: list[FetchedEntry],
    include_keywords: str = "",
    exclude_keywords: str = "",
    include_domains: str = "",
    exclude_domains: str = "",
    simhash_lookback_days: int = 30,
    match_mode: str = "keywords",
) -> list[CreatedDecision]:
    repo = Repo(session)

    created: list[CreatedDecision] = []
    # naive in-process simhash cache (v1)
    # note: by default we only compare against recent items to keep long-running DBs fast.
    stmt = select(Item.simhash64)
    if simhash_lookback_days > 0:
        cutoff = dt.datetime.utcnow() - dt.timedelta(days=simhash_lookback_days)
        stmt = stmt.where(Item.created_at >= cutoff)
    existing_simhashes = [row[0] for row in session.execute(stmt).all()]

    for entry in entries:
        canonical = canonicalize_url(entry.url)
        existing_item = repo.get_item_by_canonical_url(canonical)
        if existing_item:
            if repo.item_topic_exists(item_id=existing_item.id, topic_id=topic.id):
                continue

            decision, reason = decide_item_for_topic(
                topic=topic,
                title=existing_item.title,
                content_text=existing_item.content_text,
                canonical_url=existing_item.canonical_url,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                include_domains=include_domains,
                exclude_domains=exclude_domains,
                match_mode=match_mode,
            )
            it = ItemTopic(item_id=existing_item.id, topic_id=topic.id, decision=decision, reason=reason)
            session.add(it)
            session.flush()

            created.append(
                CreatedDecision(
                    topic_id=topic.id,
                    topic_name=topic.name,
                    decision=decision,
                    reason=reason,
                    item_id=existing_item.id,
                    title=existing_item.title,
                    canonical_url=existing_item.canonical_url,
                )
            )
            continue

        content_text = html_to_text(entry.summary or "")
        # Some sources (especially search connectors) may not provide a snippet.
        # Use the title as a minimal text payload so dedupe + LLM curation have something to work with.
        if not content_text:
            content_text = normalize_text(entry.title)
        content_hash = sha256_hex(content_text)
        sh = simhash64(content_text)

        # near-dup gate
        if content_text and is_near_duplicate(new_simhash=sh, existing_simhashes=existing_simhashes):
            continue

        item = Item(
            source_id=source.id,
            url=entry.url,
            canonical_url=canonical,
            title=normalize_text(entry.title),
            published_at=_parse_datetime_maybe(entry.published_at_iso),
            content_text=content_text,
            content_hash=content_hash,
            simhash64=int_to_signed64(sh),
        )
        session.add(item)
        session.flush()

        decision, reason = decide_item_for_topic(
            topic=topic,
            title=item.title,
            content_text=item.content_text,
            canonical_url=item.canonical_url,
            include_keywords=include_keywords,
            exclude_keywords=exclude_keywords,
            include_domains=include_domains,
            exclude_domains=exclude_domains,
            match_mode=match_mode,
        )
        it = ItemTopic(item_id=item.id, topic_id=topic.id, decision=decision, reason=reason)
        session.add(it)
        session.flush()

        existing_simhashes.append(int_to_signed64(sh))
        created.append(
            CreatedDecision(
                topic_id=topic.id,
                topic_name=topic.name,
                decision=decision,
                reason=reason,
                item_id=item.id,
                title=item.title,
                canonical_url=item.canonical_url,
            )
        )

    session.commit()
    return created
