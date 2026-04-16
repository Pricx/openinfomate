from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import urlsplit, urlunsplit

from tracker.connectors.discourse import build_discourse_json_url
from tracker.connectors.searxng import build_searxng_search_url, normalize_searxng_base_url
from tracker.search_query import normalize_search_query

MCP_SOURCE_BINDING_ENSURE_OP = "mcp.source_binding.ensure"
MCP_SOURCE_DISABLE_OP = "mcp.source.disable"
MCP_BINDING_REMOVE_OP = "mcp.binding.remove"
MCP_ALLOWED_OPS = (
    MCP_SOURCE_BINDING_ENSURE_OP,
    MCP_SOURCE_DISABLE_OP,
    MCP_BINDING_REMOVE_OP,
)

_AUTO_TOPIC_SENTINELS = {"", "__auto__", "auto", "AUTO"}
_CJK_RE = re.compile(r"[\u4e00-\u9fff]+")
_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9+_.:-]*")
_KNOWN_DISCOURSE_HOSTS = {
    "linux.do",
    "meta.discourse.org",
}


def _norm_text(v: object) -> str:
    return str(v or "").strip()


def _compact_text(v: object) -> str:
    raw = _norm_text(v).lower()
    if not raw:
        return ""
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", raw)



def _extract_tokens(v: object) -> set[str]:
    raw = _norm_text(v).lower()
    out: set[str] = set()
    for tok in _TOKEN_RE.findall(raw):
        if len(tok) >= 2:
            out.add(tok)
    for seq in _CJK_RE.findall(raw):
        seq = seq.strip()
        if not seq:
            continue
        if len(seq) <= 8:
            out.add(seq)
            continue
        for n in (2, 3, 4):
            for idx in range(0, max(0, len(seq) - n + 1)):
                out.add(seq[idx : idx + n])
    return out



def _site_host(site: str) -> str:
    raw = _norm_text(site)
    if not raw:
        return ""
    probe = raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
    try:
        host = (urlsplit(probe).hostname or "").strip().lower().rstrip(".")
    except Exception:
        host = ""
    return host



def _site_base_url(site: str) -> str:
    raw = _norm_text(site)
    if not raw:
        return ""
    probe = raw if raw.startswith(("http://", "https://")) else f"https://{raw}"
    try:
        parts = urlsplit(probe)
    except Exception:
        return ""
    if (parts.scheme or "") not in {"http", "https"} or not (parts.netloc or ""):
        return ""
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _normalize_discourse_json_path(path: str) -> str:
    raw = _norm_text(path) or "/latest.json"
    if not raw.startswith("/"):
        raw = f"/{raw}"
    if raw == "/":
        return "/latest.json"
    if raw.endswith(".rss"):
        raw = raw[: -len(".rss")] + ".json"
    elif not raw.endswith(".json"):
        raw = raw.rstrip("/") + ".json"
    return raw



def _infer_searxng_base_url(snapshot_before: dict[str, Any], explicit_base_url: str) -> str:
    base = normalize_searxng_base_url(_norm_text(explicit_base_url)) or ""
    if base:
        return base
    for s in (snapshot_before.get("sources") or [])[:4000]:
        if not isinstance(s, dict):
            continue
        if _norm_text(s.get("type")) != "searxng_search":
            continue
        raw_url = _norm_text(s.get("url"))
        if not raw_url:
            continue
        try:
            parts = urlsplit(raw_url)
        except Exception:
            continue
        if (parts.scheme or "") not in {"http", "https"} or not (parts.netloc or ""):
            continue
        guess = normalize_searxng_base_url(urlunsplit((parts.scheme, parts.netloc, parts.path or "", "", ""))) or ""
        if guess:
            return guess
    return "http://127.0.0.1:8888"



def _topic_rows(snapshot_before: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for t in (snapshot_before.get("topics") or [])[:4000]:
        if not isinstance(t, dict):
            continue
        name = _norm_text(t.get("name"))
        if not name:
            continue
        row = dict(t)
        row["name"] = name
        out.append(row)
    return out



def _find_existing_topic_name(snapshot_before: dict[str, Any], topic_name: str) -> str:
    want = _norm_text(topic_name)
    if not want:
        return ""
    low = want.casefold()
    for row in _topic_rows(snapshot_before):
        if row["name"].casefold() == low:
            return row["name"]
    return ""



def _score_topic(row: dict[str, Any], *, query: str, topic_hint: str, site: str) -> int:
    name = _norm_text(row.get("name"))
    query_text = _norm_text(row.get("query"))
    name_compact = _compact_text(name)
    topic_compact = _compact_text(topic_hint)
    query_compact = _compact_text(query)
    site_host = _site_host(site)

    score = 0
    if topic_compact and topic_compact == name_compact:
        score += 100
    if topic_compact and topic_compact in name_compact:
        score += 40
    if topic_compact and name_compact in topic_compact:
        score += 20
    if query_compact and name_compact and name_compact in query_compact:
        score += 18
    if query_compact and _compact_text(query_text) and _compact_text(query_text) in query_compact:
        score += 14
    if site_host and site_host in f"{name.lower()} {query_text.lower()}":
        score += 8

    cand_tokens = _extract_tokens(" ".join([topic_hint, query, site_host]))
    topic_tokens = _extract_tokens(" ".join([name, query_text]))
    if cand_tokens and topic_tokens:
        score += len(cand_tokens & topic_tokens) * 6

    if not bool(row.get("enabled", True)):
        score -= 2
    return score



def _auto_select_topic(snapshot_before: dict[str, Any], *, query: str, topic_hint: str, site: str, profile_topic_name: str) -> str:
    rows = _topic_rows(snapshot_before)
    if not rows:
        return _norm_text(profile_topic_name) or "Profile"
    best_name = ""
    best_score = -10_000
    for row in rows:
        score = _score_topic(row, query=query, topic_hint=topic_hint, site=site)
        if score > best_score:
            best_score = score
            best_name = row["name"]
    profile_name = _find_existing_topic_name(snapshot_before, profile_topic_name)
    fallback_profile_name = profile_name or (_norm_text(profile_topic_name) or "Profile")
    if best_score < 6:
        if profile_name:
            return profile_name
        return best_name or fallback_profile_name
    return best_name or fallback_profile_name



def _ensure_topic_action(*, snapshot_before: dict[str, Any], topic_name: str, query: str, emitted_topic_names: set[str], profile_topic_name: str) -> list[dict[str, Any]]:
    resolved = _norm_text(topic_name)
    if not resolved:
        return []
    existing = _find_existing_topic_name(snapshot_before, resolved)
    if existing:
        return []
    low = resolved.casefold()
    if low in emitted_topic_names:
        return []
    emitted_topic_names.add(low)
    profile_name = _norm_text(profile_topic_name) or "Profile"
    topic_query = (normalize_search_query(query or resolved) or _norm_text(resolved)) if resolved.casefold() != profile_name.casefold() else ""
    return [{"op": "topic.upsert", "name": resolved, "query": topic_query or "", "enabled": True}]



def _should_map_site_stream_to_discourse(*, source_type: str, site: str) -> bool:
    st = _norm_text(source_type).lower() or "auto"
    if st == "discourse":
        return True
    host = _site_host(site)
    if not host:
        return False
    if host in _KNOWN_DISCOURSE_HOSTS:
        return True
    return host.startswith("forum.") or host.startswith("community.") or ("discourse" in host)



def _build_site_search_query(site: str, query: str) -> str:
    host = _site_host(site)
    q = normalize_search_query(query) or _norm_text(query)
    if host and q:
        return f"site:{host} {q}".strip()
    if host:
        return f"site:{host}"
    return q



def _bind_filters_from_action(action: dict[str, Any]) -> tuple[str, str]:
    bind = action.get("bind") or {}
    include_keywords = _norm_text(action.get("include_keywords"))
    exclude_keywords = _norm_text(action.get("exclude_keywords"))
    if isinstance(bind, dict):
        include_keywords = include_keywords or str(bind.get("include_keywords") or "")
        exclude_keywords = exclude_keywords or str(bind.get("exclude_keywords") or "")
    return include_keywords, exclude_keywords



def _topic_from_action(action: dict[str, Any]) -> str:
    bind = action.get("bind") or {}
    topic = _norm_text(action.get("topic") or action.get("topic_name"))
    if isinstance(bind, dict):
        topic = topic or _norm_text(bind.get("topic"))
    return topic



def _expand_ensure_action(
    *,
    action: dict[str, Any],
    snapshot_before: dict[str, Any],
    searxng_base_url: str,
    profile_topic_name: str,
    emitted_topic_names: set[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    site = _norm_text(action.get("site") or action.get("domain") or action.get("base_url"))
    source_type = _norm_text(action.get("source_type") or action.get("connector") or action.get("source_kind") or "auto").lower() or "auto"
    intent = _norm_text(action.get("intent") or ("search" if _norm_text(action.get("query")) else "site_stream")).lower()
    explicit_url = _norm_text(action.get("url") or action.get("feed_url") or action.get("source_url"))
    topic_hint = _topic_from_action(action)
    query = _norm_text(action.get("query"))
    include_keywords, exclude_keywords = _bind_filters_from_action(action)
    resolved_topic = topic_hint
    if topic_hint.casefold() in {s.casefold() for s in _AUTO_TOPIC_SENTINELS} or not topic_hint:
        resolved_topic = _auto_select_topic(
            snapshot_before,
            query=query,
            topic_hint=_norm_text(action.get("topic_hint") or action.get("topic_query_hint")),
            site=site,
            profile_topic_name=profile_topic_name,
        )
    elif not _find_existing_topic_name(snapshot_before, resolved_topic):
        resolved_topic = topic_hint
    else:
        resolved_topic = _find_existing_topic_name(snapshot_before, resolved_topic) or resolved_topic

    out: list[dict[str, Any]] = []
    out.extend(
        _ensure_topic_action(
            snapshot_before=snapshot_before,
            topic_name=resolved_topic,
            query=query or resolved_topic,
            emitted_topic_names=emitted_topic_names,
            profile_topic_name=profile_topic_name,
        )
    )

    bind = {"topic": resolved_topic, "include_keywords": include_keywords, "exclude_keywords": exclude_keywords}

    if explicit_url:
        if source_type == "discourse" or _should_map_site_stream_to_discourse(source_type=source_type, site=explicit_url):
            parts = urlsplit(explicit_url)
            base_url = urlunsplit((parts.scheme, parts.netloc, "", "", ""))
            json_path = _normalize_discourse_json_path(parts.path or "/latest.json")
            out.append({"op": "source.add_discourse", "base_url": base_url, "json_path": json_path, "bind": bind})
            return out, warnings
        out.append({"op": "source.add_rss", "url": explicit_url, "bind": bind})
        return out, warnings

    if intent in {"site_stream", "stream", "rss"} and site and _should_map_site_stream_to_discourse(source_type=source_type, site=site):
        base_url = _site_base_url(site)
        if base_url:
            out.append(
                {
                    "op": "source.add_discourse",
                    "base_url": base_url,
                    "json_path": _normalize_discourse_json_path(_norm_text(action.get("json_path") or "/latest.json") or "/latest.json"),
                    "bind": bind,
                }
            )
            if source_type in {"auto", "rss"}:
                warnings.append(f"mcp: mapped site stream request to discourse latest.json: {base_url}")
            return out, warnings

    if source_type == "hn_search" and query:
        out.append(
            {
                "op": "source.add_hn_search",
                "query": normalize_search_query(query) or query,
                "tags": _norm_text(action.get("tags") or "story") or "story",
                "hits_per_page": int(action.get("hits_per_page") or 50),
                "bind": bind,
            }
        )
        return out, warnings

    searx_query = _build_site_search_query(site, query)
    if not searx_query:
        site_host = _site_host(site)
        searx_query = f"site:{site_host}" if site_host else ""
    if searx_query:
        out.append(
            {
                "op": "source.add_searxng_search",
                "base_url": _infer_searxng_base_url(snapshot_before, searxng_base_url),
                "query": searx_query,
                "time_range": _norm_text(action.get("time_range") or "week") or "week",
                "results": int(action.get("results") or 10),
                "bind": bind,
            }
        )
        if site and intent in {"site_stream", "stream", "rss"} and source_type not in {"searxng_search", "search"}:
            warnings.append(f"mcp: no native stream mapping found for {site}; fell back to searxng_search")
    return out, warnings



def _expand_disable_action(
    *,
    action: dict[str, Any],
    snapshot_before: dict[str, Any],
    searxng_base_url: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    source_type = _norm_text(action.get("source_type") or action.get("connector") or action.get("source_kind") or "auto").lower() or "auto"
    explicit_type = _norm_text(action.get("type"))
    explicit_url = _norm_text(action.get("url") or action.get("feed_url") or action.get("source_url"))
    site = _norm_text(action.get("site") or action.get("domain") or action.get("base_url"))
    query = _norm_text(action.get("query"))

    if explicit_type and explicit_url:
        return ([{"op": "source.disable", "type": explicit_type, "url": explicit_url}], warnings)
    if explicit_url and source_type == "discourse":
        parts = urlsplit(explicit_url)
        return ([{"op": "source.disable", "type": "discourse", "url": build_discourse_json_url(base_url=urlunsplit((parts.scheme, parts.netloc, "", "", "")), json_path=_normalize_discourse_json_path(parts.path or "/latest.json"))}], warnings)
    if explicit_url:
        return ([{"op": "source.disable", "type": "rss", "url": explicit_url}], warnings)
    if site and _should_map_site_stream_to_discourse(source_type=source_type, site=site) and not query:
        base_url = _site_base_url(site)
        if base_url:
            return ([{"op": "source.disable", "type": "discourse", "url": build_discourse_json_url(base_url=base_url, json_path=_normalize_discourse_json_path(_norm_text(action.get("json_path") or "/latest.json") or "/latest.json"))}], warnings)
    searx_query = _build_site_search_query(site, query)
    if searx_query:
        url = build_searxng_search_url(
            base_url=_infer_searxng_base_url(snapshot_before, searxng_base_url),
            query=searx_query,
            time_range=_norm_text(action.get("time_range") or "week") or "week",
            results=int(action.get("results") or 10),
        )
        return ([{"op": "source.disable", "type": "searxng_search", "url": url}], warnings)
    warnings.append("mcp: unable to resolve source.disable target; ignored")
    return ([], warnings)



def _expand_binding_remove_action(
    *,
    action: dict[str, Any],
    snapshot_before: dict[str, Any],
    searxng_base_url: str,
    profile_topic_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    warnings: list[str] = []
    topic_hint = _topic_from_action(action)
    query = _norm_text(action.get("query"))
    site = _norm_text(action.get("site") or action.get("domain") or action.get("base_url"))
    resolved_topic = topic_hint
    if topic_hint.casefold() in {s.casefold() for s in _AUTO_TOPIC_SENTINELS} or not topic_hint:
        resolved_topic = _auto_select_topic(
            snapshot_before,
            query=query,
            topic_hint=_norm_text(action.get("topic_hint") or ""),
            site=site,
            profile_topic_name=profile_topic_name,
        )
    else:
        resolved_topic = _find_existing_topic_name(snapshot_before, topic_hint) or topic_hint

    disable_actions, extra = _expand_disable_action(action=action, snapshot_before=snapshot_before, searxng_base_url=searxng_base_url)
    warnings.extend(extra)
    if not disable_actions:
        return [], warnings
    first = disable_actions[0]
    return (
        [
            {
                "op": "binding.remove",
                "topic": resolved_topic,
                "source": {"type": _norm_text(first.get("type")), "url": _norm_text(first.get("url"))},
            }
        ],
        warnings,
    )



def expand_source_binding_mcp_actions(
    *,
    snapshot_before: dict[str, Any],
    actions: list[dict[str, Any]],
    searxng_base_url: str = "",
    profile_topic_name: str = "Profile",
) -> tuple[list[dict[str, Any]], list[str]]:
    out: list[dict[str, Any]] = []
    warnings: list[str] = []
    emitted_keys: set[str] = set()
    emitted_topic_names: set[str] = set()
    existing_topic_upserts = {
        _norm_text(a.get("name") or a.get("topic")).casefold()
        for a in actions
        if isinstance(a, dict) and _norm_text(a.get("op")) == "topic.upsert" and _norm_text(a.get("name") or a.get("topic"))
    }
    emitted_topic_names.update(existing_topic_upserts)

    def _emit(action: dict[str, Any]) -> None:
        key = json.dumps(action, ensure_ascii=False, sort_keys=True)
        if key in emitted_keys:
            return
        emitted_keys.add(key)
        out.append(action)

    for raw in actions:
        if not isinstance(raw, dict):
            continue
        op = _norm_text(raw.get("op"))
        expanded: list[dict[str, Any]] = []
        more: list[str] = []
        if op == MCP_SOURCE_BINDING_ENSURE_OP:
            expanded, more = _expand_ensure_action(
                action=raw,
                snapshot_before=snapshot_before,
                searxng_base_url=searxng_base_url,
                profile_topic_name=profile_topic_name,
                emitted_topic_names=emitted_topic_names,
            )
        elif op == MCP_SOURCE_DISABLE_OP:
            expanded, more = _expand_disable_action(
                action=raw,
                snapshot_before=snapshot_before,
                searxng_base_url=searxng_base_url,
            )
        elif op == MCP_BINDING_REMOVE_OP:
            expanded, more = _expand_binding_remove_action(
                action=raw,
                snapshot_before=snapshot_before,
                searxng_base_url=searxng_base_url,
                profile_topic_name=profile_topic_name,
            )
        else:
            expanded = [raw]
        for w in more:
            if w and w not in warnings:
                warnings.append(w)
        for item in expanded:
            _emit(item)
    return out, warnings



def has_source_binding_mcp_actions(plan: dict[str, Any]) -> bool:
    for a in (plan.get("actions") or [])[:4000]:
        if not isinstance(a, dict):
            continue
        if _norm_text(a.get("op")) in MCP_ALLOWED_OPS:
            return True
    return False



def source_binding_mcp_tool_catalog_text(*, lang: str = "zh") -> str:
    if (lang or "").strip().lower().startswith("zh"):
        return (
            "MCP 工具动作（优先用于‘加入某站点信源/加入对某关键词的搜索/删除或禁用某个追踪源’这类请求）：\n"
            "1) mcp.source_binding.ensure：新增或复用 source，并确保 binding 存在/更新。\n"
            "   常用字段：intent(site_stream|search|rss)、source_type(auto|discourse|rss|searxng_search|hn_search)、site、query、topic(可填 __auto__)、include_keywords、exclude_keywords。\n"
            "   规则：若 topic=__auto__，运行时会优先匹配最相关的现有 topic；实在匹配不到才回退到 Profile。若 source 已存在，则复用并更新 binding，而不是强制新建。\n"
            "2) mcp.source.disable：按 site/query/source_type 解析出已有 source 后执行禁用。\n"
            "3) mcp.binding.remove：按 site/query/topic 解析出 binding 后移除。\n"
            "   规则：用户说\"不要再把这个源推给某个 topic\"时优先用它；用户说\"整个源停掉/禁用\"时用 mcp.source.disable。\n"
            "示例 A：加入 linux.do 的站点流\n"
            '{"op":"mcp.source_binding.ensure","intent":"site_stream","source_type":"discourse","site":"linux.do","topic":"__auto__"}\n'
            "示例 B：加入对 codex fast 的搜索，并限制在 linux.do\n"
            '{"op":"mcp.source_binding.ensure","intent":"search","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}\n'
            "示例 C：把 linux.do 上的 codex fast 搜索从当前 topic 移除\n"
            '{"op":"mcp.binding.remove","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}'
        )
    return (
        "MCP tool actions (prefer these for requests like 'add a site source', 'add a search for X', 'disable/remove an existing tracked source').\n"
        "1) mcp.source_binding.ensure: add-or-reuse a source and ensure/update its binding.\n"
        "   Common fields: intent(site_stream|search|rss), source_type(auto|discourse|rss|searxng_search|hn_search), site, query, topic(use __auto__ when unsure), include_keywords, exclude_keywords.\n"
        "   Runtime behavior: topic=__auto__ first tries the most relevant existing topic, then falls back to Profile. Existing sources/bindings are reused and updated instead of blindly creating duplicates.\n"
        "2) mcp.source.disable: resolve an existing source from site/query/source_type and disable it.\n"
        "3) mcp.binding.remove: resolve an existing binding from site/query/topic and remove it.\n"
        "   Rule: use mcp.binding.remove when the user wants to stop sending one source to one topic but keep the source; use mcp.source.disable when the source itself should be turned off.\n"
        "Example A: add linux.do site stream\n"
        '{"op":"mcp.source_binding.ensure","intent":"site_stream","source_type":"discourse","site":"linux.do","topic":"__auto__"}\n'
        "Example B: add a search for codex fast on linux.do\n"
        '{"op":"mcp.source_binding.ensure","intent":"search","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}\n'
        "Example C: remove the linux.do codex fast search from the current topic\n"
        '{"op":"mcp.binding.remove","source_type":"searxng_search","site":"linux.do","query":"codex fast","topic":"__auto__"}'
    )
