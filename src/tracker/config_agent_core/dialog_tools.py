from __future__ import annotations

import datetime as dt
import json
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import and_, func, or_, select

from tracker.formatting import curated_priority_score, extract_llm_summary_why
from tracker.fulltext import fetch_fulltext_for_url
from tracker.models import Item, ItemContent, ItemTopic, Report, Source, Topic
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_report_reader import parse_reference_entries, parse_report_markdown

_URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)
_WORD_RE = re.compile(r"[a-z0-9][a-z0-9+_.:/-]{1,}", re.IGNORECASE)
_CJK_RE = re.compile(r"[\u4e00-\u9fff]+")

_RECENT_REPORTS_TOOL = "mcp.reports.recent"
_ITEM_SEARCH_TOOL = "mcp.items.search"
_ITEM_EXPLAIN_TOOL = "mcp.items.explain"
_EXTERNAL_FETCH_TOOL = "mcp.external.fetch_url"
_DIALOG_TOOLS = {
    _RECENT_REPORTS_TOOL,
    _ITEM_SEARCH_TOOL,
    _ITEM_EXPLAIN_TOOL,
    _EXTERNAL_FETCH_TOOL,
}


@dataclass(frozen=True)
class DialogToolExecution:
    tool: str
    args: dict[str, Any]
    result: dict[str, Any]


def _norm_text(value: object) -> str:
    return str(value or "").strip()


def _safe_excerpt(value: object, *, limit: int = 400) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, int(limit) - 1)].rstrip() + "…"


def _iso(value: dt.datetime | None) -> str:
    if value is None:
        return ""
    return value.replace(microsecond=0).isoformat() + "Z"


def _extract_urls(text: str) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for match in _URL_RE.findall(text or ""):
        url = str(match or "").strip().rstrip(".,);]>")
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(url)
    return out


def _tokenize(text: str) -> list[str]:
    raw = _norm_text(text).lower()
    if not raw:
        return []
    out: list[str] = []
    seen: set[str] = set()
    for token in _WORD_RE.findall(raw):
        if len(token) < 2:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    for seq in _CJK_RE.findall(raw):
        seq = seq.strip()
        if len(seq) < 2:
            continue
        if len(seq) <= 8:
            if seq not in seen:
                seen.add(seq)
                out.append(seq)
            continue
        for n in (2, 3, 4):
            for idx in range(0, max(0, len(seq) - n + 1)):
                piece = seq[idx : idx + n]
                if piece not in seen:
                    seen.add(piece)
                    out.append(piece)
    return out


def _domain(url: str) -> str:
    try:
        return (urlsplit(url).hostname or "").strip().lower()
    except Exception:
        return ""


def _is_collect_report_key(key: str) -> bool:
    return _norm_text(key).startswith("digest:collect.")


def dialog_tool_catalog_text(*, lang: str) -> str:
    is_zh = _norm_text(lang).lower().startswith("zh")
    if is_zh:
        return (
            "可用缓存优先工具（默认只读站内缓存；只有用户显式给出 URL 并要求访问网页时，才允许 external.fetch_url）：\n"
            "- mcp.reports.recent: 读取最近 N 小时内的参考消息 / collect 报告。参数：hours(1-168), limit(1-6), only_collect(bool), include_items(bool), title_query(optional)\n"
            "- mcp.items.search: 在最近 N 小时已入选 digest/alert 的条目里按标题/URL/摘要/缓存正文搜索。参数：query, hours(1-168), limit(1-8)\n"
            "- mcp.items.explain: 按 item_id 或 url 读取单条条目的缓存详情。参数：item_id 或 url\n"
            "- mcp.external.fetch_url: 仅在用户显式给出 URL 且明确要求访问该网页时使用；返回实时抓取正文摘要。参数：url, max_chars(optional)\n"
        )
    return (
        "Available cache-first tools (default: cached/internal data only; use external.fetch_url only when the user explicitly provides a URL and asks to inspect it):\n"
        "- mcp.reports.recent: read recent digest / collect reports. Args: hours(1-168), limit(1-6), only_collect(bool), include_items(bool), title_query(optional)\n"
        "- mcp.items.search: search recently curated digest/alert items by title/URL/summary/cached content. Args: query, hours(1-168), limit(1-8)\n"
        "- mcp.items.explain: read one cached item in detail by item_id or url. Args: item_id or url\n"
        "- mcp.external.fetch_url: only when the user explicitly provides a URL and explicitly wants that webpage inspected; returns live fetched text. Args: url, max_chars(optional)\n"
    )


def normalize_dialog_tool_calls(tool_calls: object) -> list[dict[str, Any]]:
    if not isinstance(tool_calls, list):
        return []
    out: list[dict[str, Any]] = []
    for raw in tool_calls[:4]:
        if not isinstance(raw, dict):
            continue
        tool = _norm_text(raw.get("tool"))
        if tool not in _DIALOG_TOOLS:
            continue
        args = raw.get("args")
        if not isinstance(args, dict):
            args = {}
        out.append({"tool": tool, "args": dict(args)})
    return out


def _coerce_hours(value: object, *, default: int) -> int:
    try:
        hours = int(value or default)
    except Exception:
        hours = default
    return max(1, min(168, hours))


def _coerce_limit(value: object, *, default: int, hi: int) -> int:
    try:
        limit = int(value or default)
    except Exception:
        limit = default
    return max(1, min(hi, limit))


def _coerce_bool(value: object, *, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    raw = _norm_text(value).lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return bool(default)


def _find_item_by_url(repo: Repo, url: str) -> Item | None:
    exact = _norm_text(url)
    if not exact:
        return None
    item = repo.get_item_by_canonical_url(exact)
    if item is not None:
        return item
    stmt = (
        select(Item)
        .where(or_(Item.canonical_url == exact, Item.url == exact))
        .order_by(Item.id.desc())
        .limit(1)
    )
    return repo.session.scalar(stmt)


def _best_item_topics(repo: Repo, *, item_id: int) -> list[tuple[ItemTopic, Topic]]:
    stmt = (
        select(ItemTopic, Topic)
        .join(Topic, Topic.id == ItemTopic.topic_id)
        .where(ItemTopic.item_id == int(item_id))
        .order_by(ItemTopic.created_at.desc(), ItemTopic.id.desc())
    )
    return list(repo.session.execute(stmt).all())


def _serialize_item(
    repo: Repo,
    *,
    item: Item,
    source: Source | None = None,
    item_content: ItemContent | None = None,
    topics: list[tuple[ItemTopic, Topic]] | None = None,
) -> dict[str, Any]:
    src = source
    if src is None:
        src = repo.session.get(Source, int(getattr(item, "source_id", 0) or 0))
    content_row = item_content if item_content is not None else repo.get_item_content(item_id=int(item.id))
    topic_rows = list(topics or _best_item_topics(repo, item_id=int(item.id)))
    decision_rows: list[dict[str, Any]] = []
    best_summary = ""
    best_why = ""
    best_rank = 0
    for item_topic, topic in topic_rows[:8]:
        summary, why = extract_llm_summary_why(getattr(item_topic, "reason", "") or "")
        rank_score = curated_priority_score(reason=getattr(item_topic, "reason", "") or "", decision=getattr(item_topic, "decision", "") or "")
        decision_rows.append(
            {
                "topic_name": _norm_text(getattr(topic, "name", "")),
                "decision": _norm_text(getattr(item_topic, "decision", "")),
                "rank_score": int(rank_score or 0),
                "summary": summary,
                "why": why,
            }
        )
        if int(rank_score or 0) > int(best_rank):
            best_rank = int(rank_score or 0)
            best_summary = summary
            best_why = why
    raw_content = _norm_text(getattr(content_row, "content_text", "") or getattr(item, "content_text", ""))
    return {
        "item_id": int(item.id),
        "title": _norm_text(getattr(item, "title", "")),
        "url": _norm_text(getattr(item, "canonical_url", "") or getattr(item, "url", "")),
        "domain": _domain(_norm_text(getattr(item, "canonical_url", "") or getattr(item, "url", ""))),
        "published_at": _iso(getattr(item, "published_at", None)),
        "created_at": _iso(getattr(item, "created_at", None)),
        "source": {
            "id": int(getattr(src, "id", 0) or 0) if src is not None else 0,
            "type": _norm_text(getattr(src, "type", "") if src is not None else ""),
            "url": _norm_text(getattr(src, "url", "") if src is not None else ""),
        },
        "best_summary": best_summary,
        "best_why": best_why,
        "rank_score": int(best_rank or 0),
        "decisions": decision_rows,
        "content_excerpt": _safe_excerpt(raw_content, limit=900),
        "cached_fetch_error": _norm_text(getattr(content_row, "error", "")) if content_row is not None else "",
    }


def _extract_report_takeaways(markdown: str) -> list[str]:
    doc = parse_report_markdown(markdown or "")
    lines: list[str] = []
    for section in doc.sections[:4]:
        body = _safe_excerpt(section.body, limit=260)
        if section.title and body:
            lines.append(f"{section.title}: {body}")
        elif body:
            lines.append(body)
        if len(lines) >= 3:
            break
    return lines[:3]


def _tool_recent_reports(repo: Repo, *, args: dict[str, Any]) -> dict[str, Any]:
    hours = _coerce_hours(args.get("hours"), default=24)
    limit = _coerce_limit(args.get("limit"), default=3, hi=6)
    only_collect = _coerce_bool(args.get("only_collect"), default=False)
    include_items = _coerce_bool(args.get("include_items"), default=True)
    title_query = _norm_text(args.get("title_query")).casefold()
    since = dt.datetime.utcnow() - dt.timedelta(hours=hours)
    stmt = (
        select(Report, Topic)
        .outerjoin(Topic, Topic.id == Report.topic_id)
        .where(and_(Report.kind == "digest", Report.created_at >= since))
        .order_by(Report.created_at.desc(), Report.id.desc())
        .limit(max(1, limit * 4))
    )
    rows = list(repo.session.execute(stmt).all())
    reports: list[dict[str, Any]] = []
    for report, topic in rows:
        key = _norm_text(getattr(report, "idempotency_key", ""))
        is_collect = _is_collect_report_key(key)
        if only_collect and not is_collect:
            continue
        if not only_collect and is_collect:
            continue
        hay = " ".join([_norm_text(getattr(report, "title", "")), key]).casefold()
        if title_query and title_query not in hay:
            continue
        refs = parse_reference_entries(parse_report_markdown(getattr(report, "markdown", "") or "").references)
        item_rows: list[dict[str, Any]] = []
        if include_items:
            for _, _, ref_url in refs[:12]:
                item = _find_item_by_url(repo, ref_url)
                if item is None:
                    continue
                item_rows.append(_serialize_item(repo, item=item))
        reports.append(
            {
                "report_id": int(getattr(report, "id", 0) or 0),
                "kind": _norm_text(getattr(report, "kind", "")),
                "title": _norm_text(getattr(report, "title", "")),
                "idempotency_key": key,
                "topic_name": _norm_text(getattr(topic, "name", "")) or ("collect" if is_collect else "all"),
                "created_at": _iso(getattr(report, "created_at", None)),
                "is_collect": bool(is_collect),
                "reference_count": len(refs),
                "takeaways": _extract_report_takeaways(getattr(report, "markdown", "") or ""),
                "items": item_rows,
            }
        )
        if len(reports) >= limit:
            break
    return {
        "hours": hours,
        "only_collect": bool(only_collect),
        "title_query": _norm_text(args.get("title_query")),
        "reports": reports,
    }


def _score_item_match(*, item_payload: dict[str, Any], query: str, query_tokens: list[str], query_urls: list[str]) -> int:
    title = _norm_text(item_payload.get("title")).lower()
    url = _norm_text(item_payload.get("url")).lower()
    summary = _norm_text(item_payload.get("best_summary")).lower()
    why = _norm_text(item_payload.get("best_why")).lower()
    content_excerpt = _norm_text(item_payload.get("content_excerpt")).lower()
    blob = "\n".join([title, url, summary, why, content_excerpt])
    score = 0
    for ref_url in query_urls:
        low = ref_url.lower()
        if url == low:
            score += 200
        elif low and low in url:
            score += 120
    for token in query_tokens:
        if token and token in title:
            score += 40
        if token and token in summary:
            score += 24
        if token and token in why:
            score += 18
        if token and token in url:
            score += 14
        if token and token in content_excerpt:
            score += 8
    if _norm_text(query).lower() and _norm_text(query).lower() in blob:
        score += 10
    score += min(30, int(item_payload.get("rank_score") or 0) // 3)
    return int(score)


def _tool_search_items(repo: Repo, *, args: dict[str, Any]) -> dict[str, Any]:
    query = _norm_text(args.get("query"))
    if not query:
        raise ValueError("missing query")
    hours = _coerce_hours(args.get("hours"), default=48)
    limit = _coerce_limit(args.get("limit"), default=5, hi=8)
    since = dt.datetime.utcnow() - dt.timedelta(hours=hours)
    stmt = (
        select(ItemTopic, Item, Topic, Source, ItemContent)
        .join(Item, Item.id == ItemTopic.item_id)
        .join(Topic, Topic.id == ItemTopic.topic_id)
        .join(Source, Source.id == Item.source_id)
        .outerjoin(ItemContent, ItemContent.item_id == Item.id)
        .where(
            and_(
                ItemTopic.decision.in_(["digest", "alert"]),
                func.coalesce(Item.published_at, Item.created_at) >= since,
            )
        )
        .order_by(func.coalesce(Item.published_at, Item.created_at).desc(), ItemTopic.id.desc())
        .limit(600)
    )
    rows = list(repo.session.execute(stmt).all())
    by_item_id: dict[int, dict[str, Any]] = {}
    query_tokens = _tokenize(query)
    query_urls = _extract_urls(query)
    for item_topic, item, topic, source, item_content in rows:
        item_id = int(getattr(item, "id", 0) or 0)
        if item_id <= 0:
            continue
        existing = by_item_id.get(item_id)
        if existing is None:
            payload = _serialize_item(
                repo,
                item=item,
                source=source,
                item_content=item_content,
                topics=[(item_topic, topic)],
            )
            payload["match_score"] = _score_item_match(
                item_payload=payload,
                query=query,
                query_tokens=query_tokens,
                query_urls=query_urls,
            )
            by_item_id[item_id] = payload
            continue
        existing_topics = list(existing.get("decisions") or [])
        summary, why = extract_llm_summary_why(getattr(item_topic, "reason", "") or "")
        existing_topics.append(
            {
                "topic_name": _norm_text(getattr(topic, "name", "")),
                "decision": _norm_text(getattr(item_topic, "decision", "")),
                "rank_score": int(curated_priority_score(reason=getattr(item_topic, "reason", "") or "", decision=getattr(item_topic, "decision", "") or "") or 0),
                "summary": summary,
                "why": why,
            }
        )
        existing["decisions"] = existing_topics[:8]
    ranked = [payload for payload in by_item_id.values() if int(payload.get("match_score") or 0) > 0]
    ranked.sort(
        key=lambda payload: (
            -int(payload.get("match_score") or 0),
            -int(payload.get("rank_score") or 0),
            str(payload.get("published_at") or payload.get("created_at") or ""),
        )
    )
    return {
        "query": query,
        "hours": hours,
        "items": ranked[:limit],
    }


def _tool_explain_item(repo: Repo, *, args: dict[str, Any]) -> dict[str, Any]:
    item_id = 0
    try:
        item_id = int(args.get("item_id") or 0)
    except Exception:
        item_id = 0
    url = _norm_text(args.get("url"))
    item: Item | None = None
    if item_id > 0:
        item = repo.get_item_by_id(int(item_id))
    if item is None and url:
        item = _find_item_by_url(repo, url)
    if item is None:
        raise ValueError("item not found")
    return {
        "requested_item_id": int(item_id or 0),
        "requested_url": url,
        "item": _serialize_item(repo, item=item),
    }


async def _tool_fetch_external_url(settings: Settings, *, user_prompt: str, args: dict[str, Any]) -> dict[str, Any]:
    url = _norm_text(args.get("url"))
    if not url:
        raise ValueError("missing url")
    explicit_urls = set(_extract_urls(user_prompt))
    if url not in explicit_urls:
        raise ValueError("external fetch requires an explicit URL in the user prompt")
    max_chars = _coerce_limit(args.get("max_chars"), default=3000, hi=6000)
    text = await fetch_fulltext_for_url(
        url=url,
        timeout_seconds=int(getattr(settings, "http_timeout_seconds", 20) or 20),
        max_chars=max_chars,
    )
    return {
        "url": url,
        "content_excerpt": _safe_excerpt(text, limit=max_chars),
    }


async def execute_dialog_tool_calls(
    *,
    repo: Repo,
    settings: Settings,
    user_prompt: str,
    tool_calls: object,
) -> tuple[list[DialogToolExecution], list[str]]:
    executions: list[DialogToolExecution] = []
    warnings: list[str] = []
    for call in normalize_dialog_tool_calls(tool_calls):
        tool = str(call.get("tool") or "")
        args = dict(call.get("args") or {})
        try:
            if tool == _RECENT_REPORTS_TOOL:
                result = _tool_recent_reports(repo, args=args)
            elif tool == _ITEM_SEARCH_TOOL:
                result = _tool_search_items(repo, args=args)
            elif tool == _ITEM_EXPLAIN_TOOL:
                result = _tool_explain_item(repo, args=args)
            elif tool == _EXTERNAL_FETCH_TOOL:
                result = await _tool_fetch_external_url(settings, user_prompt=user_prompt, args=args)
            else:
                warnings.append(f"unsupported dialog tool: {tool}")
                continue
            executions.append(DialogToolExecution(tool=tool, args=args, result=result))
        except Exception as exc:
            warnings.append(f"{tool}: {exc}")
    return executions, warnings


def serialize_dialog_tool_results(executions: list[DialogToolExecution]) -> list[dict[str, Any]]:
    return [{"tool": row.tool, "args": row.args, "result": row.result} for row in executions]

