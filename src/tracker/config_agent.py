from __future__ import annotations

import copy
import datetime as dt
import json
import re
from dataclasses import dataclass
from typing import Any, Literal

from sqlalchemy.orm import Session

from tracker.actions import (
    SourceBindingSpec,
    create_discourse_source,
    create_hn_search_source,
    create_html_list_source,
    create_rss_source,
    create_searxng_search_source,
)
from tracker.connectors.discourse import build_discourse_json_url
from tracker.connectors.hn_algolia import build_hn_search_url
from tracker.connectors.html_list import build_html_list_url
from tracker.connectors.searxng import build_searxng_search_url, normalize_searxng_base_url
from tracker.integrations.source_binding_mcp import expand_source_binding_mcp_actions, has_source_binding_mcp_actions
from tracker.repo import Repo
from tracker.search_query import normalize_search_query


AI_SETUP_KIND = "tracking_ai_setup"
AI_SETUP_BASELINE_APP_CONFIG_KEY = "tracking_ai_setup_baseline_json"

_HN_ALLOWED_TAG_TOKENS = {
    "story",
    "comment",
    "poll",
    "pollopt",
    "show_hn",
    "ask_hn",
    "front_page",
}
_HN_TAG_TOKEN_RE = re.compile(r"[a-z0-9_]+")
_URL_RE = re.compile(r"https?://[^\s<>()]+", re.IGNORECASE)


@dataclass(frozen=True)
class PlanResult:
    ok: bool
    plan: dict[str, Any]
    preview_markdown: str
    snapshot_before: dict[str, Any]
    snapshot_preview: dict[str, Any]
    warnings: list[str]


def export_tracking_snapshot(*, session: Session) -> dict[str, Any]:
    """
    Export a non-secret snapshot of tracking config for AI planning / undo / restore.

    Scope (v1):
    - topics
    - topic_policies
    - sources (+ meta)
    - bindings
    """
    repo = Repo(session)
    topic_name_by_id = {int(t.id): t.name for t in repo.list_topics() if t and t.id is not None}
    meta_map = {s.id: m for s, _h, m in repo.list_sources_with_health_and_meta() if m}

    topics: list[dict[str, Any]] = []
    for t in repo.list_topics():
        topics.append(
            {
                "name": t.name,
                "query": t.query,
                "enabled": bool(t.enabled),
                "digest_cron": t.digest_cron,
                "alert_keywords": t.alert_keywords,
                "alert_cooldown_minutes": int(t.alert_cooldown_minutes),
                "alert_daily_cap": int(t.alert_daily_cap),
            }
        )

    topic_policies: list[dict[str, Any]] = []
    for p in repo.list_topic_policies():
        name = topic_name_by_id.get(int(p.topic_id))
        if not name:
            continue
        topic_policies.append(
            {
                "topic": name,
                "llm_curation_enabled": bool(p.llm_curation_enabled),
                "llm_curation_prompt": p.llm_curation_prompt,
            }
        )

    sources: list[dict[str, Any]] = []
    for s in repo.list_sources():
        m = meta_map.get(s.id)
        sources.append(
            {
                "type": s.type,
                "url": s.url,
                "enabled": bool(s.enabled),
                "tags": (m.tags if m else ""),
                "notes": (m.notes if m else ""),
            }
        )

    bindings: list[dict[str, Any]] = []
    for t, s, ts in repo.list_topic_sources():
        bindings.append(
            {
                "topic": t.name,
                "source": {"type": s.type, "url": s.url},
                "include_keywords": ts.include_keywords,
                "exclude_keywords": ts.exclude_keywords,
            }
        )

    return {
        "version": 1,
        "exported_at": dt.datetime.utcnow().isoformat() + "Z",
        "topics": topics,
        "topic_policies": topic_policies,
        "sources": sources,
        "bindings": bindings,
    }


def snapshot_compact_text(snapshot: dict[str, Any], *, max_topics: int = 60, max_sources: int = 120, max_bindings: int = 180) -> str:
    """
    Compact, prompt-friendly view of the current tracking config.

    Keep this bounded: the goal is planning, not dumping the whole DB.
    """
    topics = snapshot.get("topics") or []
    sources = snapshot.get("sources") or []
    bindings = snapshot.get("bindings") or []

    out: list[str] = []
    out.append(f"Topics({len(topics)}):")
    for t in list(topics)[: max(0, int(max_topics))]:
        if not isinstance(t, dict):
            continue
        name = str(t.get("name") or "").strip()
        query = str(t.get("query") or "").strip()
        enabled = bool(t.get("enabled", True))
        if name:
            out.append(f"- {name} [{'on' if enabled else 'off'}] query={query!r}")

    out.append(f"Sources({len(sources)}):")
    for s in list(sources)[: max(0, int(max_sources))]:
        if not isinstance(s, dict):
            continue
        st = str(s.get("type") or "").strip()
        url = str(s.get("url") or "").strip()
        enabled = bool(s.get("enabled", True))
        if st and url:
            out.append(f"- {st} {url} [{'on' if enabled else 'off'}]")

    out.append(f"Bindings({len(bindings)}):")
    for b in list(bindings)[: max(0, int(max_bindings))]:
        if not isinstance(b, dict):
            continue
        topic = str(b.get("topic") or "").strip()
        src = b.get("source") or {}
        st = str((src.get("type") if isinstance(src, dict) else "") or "").strip()
        url = str((src.get("url") if isinstance(src, dict) else "") or "").strip()
        if topic and st and url:
            out.append(f"- {topic} <= {st} {url}")

    # Truncation hint (helps LLM avoid assuming completeness).
    if len(topics) > max_topics or len(sources) > max_sources or len(bindings) > max_bindings:
        out.append("")
        out.append("NOTE: lists are truncated for brevity; do NOT assume missing entries do not exist.")

    return "\n".join(out).strip()


def _norm_text(v: object) -> str:
    return str(v or "").strip()


def _norm_bool(v: object, default: bool | None = None) -> bool | None:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in {"1", "true", "yes", "y", "on"}:
        return True
    if s in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _norm_int(v: object, default: int | None = None) -> int | None:
    if v is None:
        return default
    try:
        return int(v)
    except Exception:
        return default


AllowedOp = Literal[
    "topic.upsert",
    "topic.disable",
    "source.add_rss",
    "source.add_hn_search",
    "source.add_searxng_search",
    "source.add_discourse",
    "source.add_html_list",
    "source.disable",
    "source.set_meta",
    "binding.remove",
    "binding.set_filters",
    "mcp.source_binding.ensure",
    "mcp.source.disable",
    "mcp.binding.remove",
]


def validate_ai_setup_plan(obj: object) -> tuple[dict[str, Any], list[str]]:
    """
    Validate+normalize an AI Setup plan.

    The goal is to keep execution bounded and safe (no arbitrary code paths).
    """
    if not isinstance(obj, dict):
        raise ValueError("plan must be a JSON object")
    raw_actions = obj.get("actions")
    if not isinstance(raw_actions, list) or not raw_actions:
        raise ValueError("plan.actions must be a non-empty list")

    warnings: list[str] = []
    actions: list[dict[str, Any]] = []

    allowed: set[str] = set(getattr(AllowedOp, "__args__", ()))  # type: ignore[attr-defined]

    # Keep execution bounded (avoid abuse), but allow Smart Config to emit a reasonably large
    # plan when the operator provides a big profile dump.
    max_actions = 2000
    if len(raw_actions) > max_actions:
        warnings.append(f"plan.actions truncated: {len(raw_actions)} -> {max_actions}")
    for idx, a in enumerate(raw_actions[:max_actions]):
        if not isinstance(a, dict):
            raise ValueError(f"action[{idx}] must be an object")
        op = _norm_text(a.get("op"))
        if op not in allowed:
            raise ValueError(f"action[{idx}] invalid op: {op!r}")

        clean: dict[str, Any] = {"op": op}

        if op in {"topic.upsert", "topic.disable"}:
            name = _norm_text(a.get("name") or a.get("topic"))
            if not name:
                raise ValueError(f"action[{idx}] missing topic name")
            clean["name"] = name
            if op == "topic.upsert":
                if "query" in a:
                    clean["query"] = str(a.get("query") or "")
                if "enabled" in a:
                    clean["enabled"] = bool(_norm_bool(a.get("enabled"), True))
                for k in ("digest_cron", "alert_keywords"):
                    if k in a:
                        clean[k] = str(a.get(k) or "")
                for k in ("alert_cooldown_minutes", "alert_daily_cap"):
                    if k in a:
                        vv = _norm_int(a.get(k))
                        if vv is not None:
                            clean[k] = int(vv)

        elif op == "source.add_rss":
            url = _norm_text(a.get("url"))
            if not url:
                raise ValueError(f"action[{idx}] missing url")
            clean["url"] = url
            if "tags" in a:
                clean["tags"] = str(a.get("tags") or "")
            if "notes" in a:
                clean["notes"] = str(a.get("notes") or "")
            bind = a.get("bind")
            if isinstance(bind, dict):
                topic = _norm_text(bind.get("topic"))
                if topic:
                    clean["bind"] = {
                        "topic": topic,
                        "include_keywords": str(bind.get("include_keywords") or ""),
                        "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                    }

        elif op == "source.add_hn_search":
            q = _norm_text(a.get("query"))
            if not q:
                raise ValueError(f"action[{idx}] missing query")
            clean["query"] = q
            raw_tags = _norm_text(a.get("tags") or "story") or "story"
            tokens = [t for t in _HN_TAG_TOKEN_RE.findall(raw_tags.lower()) if t]
            if not any(t in _HN_ALLOWED_TAG_TOKENS for t in tokens):
                warnings.append(f"action[{idx}] hn_search.tags invalid; defaulted to 'story'")
                raw_tags = "story"
            clean["tags"] = raw_tags
            clean["hits_per_page"] = int(_norm_int(a.get("hits_per_page"), 50) or 50)
            bind = a.get("bind")
            if isinstance(bind, dict):
                topic = _norm_text(bind.get("topic"))
                if topic:
                    clean["bind"] = {
                        "topic": topic,
                        "include_keywords": str(bind.get("include_keywords") or ""),
                        "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                    }

        elif op == "source.add_searxng_search":
            base_url = _norm_text(a.get("base_url"))
            q = _norm_text(a.get("query"))
            if not base_url or not q:
                raise ValueError(f"action[{idx}] missing base_url/query")
            norm_base_url = normalize_searxng_base_url(base_url)
            if norm_base_url and norm_base_url != base_url:
                warnings.append(f"action[{idx}] searxng.base_url normalized: {base_url!r} -> {norm_base_url!r}")
                base_url = norm_base_url
            clean["base_url"] = base_url
            clean["query"] = q
            for k in ("categories", "time_range", "language"):
                if k in a:
                    clean[k] = _norm_text(a.get(k))
            if "results" in a:
                vv = _norm_int(a.get("results"))
                if vv is not None:
                    clean["results"] = int(vv)
            bind = a.get("bind")
            if isinstance(bind, dict):
                topic = _norm_text(bind.get("topic"))
                if topic:
                    clean["bind"] = {
                        "topic": topic,
                        "include_keywords": str(bind.get("include_keywords") or ""),
                        "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                    }

        elif op == "source.add_discourse":
            base_url = _norm_text(a.get("base_url"))
            if not base_url:
                raise ValueError(f"action[{idx}] missing base_url")
            clean["base_url"] = base_url
            clean["json_path"] = _norm_text(a.get("json_path") or "/latest.json") or "/latest.json"
            bind = a.get("bind")
            if isinstance(bind, dict):
                topic = _norm_text(bind.get("topic"))
                if topic:
                    clean["bind"] = {
                        "topic": topic,
                        "include_keywords": str(bind.get("include_keywords") or ""),
                        "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                    }

        elif op == "source.add_html_list":
            page_url = _norm_text(a.get("page_url"))
            item_selector = _norm_text(a.get("item_selector"))
            if not page_url or not item_selector:
                raise ValueError(f"action[{idx}] missing page_url/item_selector")
            clean["page_url"] = page_url
            clean["item_selector"] = item_selector
            clean["title_selector"] = _norm_text(a.get("title_selector") or "a") or "a"
            clean["summary_selector"] = _norm_text(a.get("summary_selector") or "")
            clean["max_items"] = int(_norm_int(a.get("max_items"), 30) or 30)
            bind = a.get("bind")
            if isinstance(bind, dict):
                topic = _norm_text(bind.get("topic"))
                if topic:
                    clean["bind"] = {
                        "topic": topic,
                        "include_keywords": str(bind.get("include_keywords") or ""),
                        "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                    }

        elif op in {"source.disable", "source.set_meta"}:
            st = _norm_text(a.get("type") or a.get("source_type"))
            url = _norm_text(a.get("url") or a.get("source_url"))
            if not st or not url:
                raise ValueError(f"action[{idx}] missing source type/url")
            clean["type"] = st
            clean["url"] = url
            if op == "source.set_meta":
                if "tags" in a:
                    clean["tags"] = str(a.get("tags") or "")
                if "notes" in a:
                    clean["notes"] = str(a.get("notes") or "")

        elif op in {"binding.remove", "binding.set_filters"}:
            topic = _norm_text(a.get("topic"))
            st = _norm_text(a.get("source_type") or a.get("type"))
            url = _norm_text(a.get("source_url") or a.get("url"))
            src = a.get("source")
            if isinstance(src, dict):
                st = st or _norm_text(src.get("type"))
                url = url or _norm_text(src.get("url"))
            if not topic or not st or not url:
                raise ValueError(f"action[{idx}] missing topic/source")
            clean["topic"] = topic
            clean["source"] = {"type": st, "url": url}
            if op == "binding.set_filters":
                if "include_keywords" in a:
                    clean["include_keywords"] = str(a.get("include_keywords") or "")
                if "exclude_keywords" in a:
                    clean["exclude_keywords"] = str(a.get("exclude_keywords") or "")


        elif op == "mcp.source_binding.ensure":
            source_type = _norm_text(a.get("source_type") or a.get("connector") or a.get("source_kind") or "auto")
            intent = _norm_text(a.get("intent") or ("search" if _norm_text(a.get("query")) else "site_stream"))
            topic = _norm_text(a.get("topic") or a.get("topic_name"))
            site = _norm_text(a.get("site") or a.get("domain") or a.get("base_url"))
            query = _norm_text(a.get("query"))
            raw_url = _norm_text(a.get("url") or a.get("feed_url") or a.get("source_url"))
            if not any([site, query, raw_url]):
                raise ValueError(f"action[{idx}] missing site/query/url for mcp.source_binding.ensure")
            clean["source_type"] = source_type or "auto"
            clean["intent"] = intent or ("search" if query else "site_stream")
            if topic:
                clean["topic"] = topic
            if site:
                clean["site"] = site
            if query:
                clean["query"] = query
            if raw_url:
                clean["url"] = raw_url
            if "json_path" in a:
                clean["json_path"] = _norm_text(a.get("json_path") or "/latest.json") or "/latest.json"
            if "time_range" in a:
                clean["time_range"] = _norm_text(a.get("time_range") or "week") or "week"
            if "results" in a:
                vv = _norm_int(a.get("results"))
                if vv is not None:
                    clean["results"] = int(vv)
            if "tags" in a:
                clean["tags"] = str(a.get("tags") or "")
            if "notes" in a:
                clean["notes"] = str(a.get("notes") or "")
            bind = a.get("bind")
            if isinstance(bind, dict):
                b_topic = _norm_text(bind.get("topic"))
                if b_topic and "topic" not in clean:
                    clean["topic"] = b_topic
                clean["bind"] = {
                    "topic": _norm_text(bind.get("topic") or clean.get("topic") or ""),
                    "include_keywords": str(bind.get("include_keywords") or ""),
                    "exclude_keywords": str(bind.get("exclude_keywords") or ""),
                }
            else:
                bind_obj: dict[str, Any] = {}
                if "topic" in clean:
                    bind_obj["topic"] = str(clean.get("topic") or "")
                if "include_keywords" in a:
                    bind_obj["include_keywords"] = str(a.get("include_keywords") or "")
                if "exclude_keywords" in a:
                    bind_obj["exclude_keywords"] = str(a.get("exclude_keywords") or "")
                if bind_obj:
                    bind_obj.setdefault("include_keywords", "")
                    bind_obj.setdefault("exclude_keywords", "")
                    clean["bind"] = bind_obj

        elif op == "mcp.source.disable":
            source_type = _norm_text(a.get("source_type") or a.get("connector") or a.get("source_kind") or a.get("type") or "auto")
            site = _norm_text(a.get("site") or a.get("domain") or a.get("base_url"))
            query = _norm_text(a.get("query"))
            raw_url = _norm_text(a.get("url") or a.get("feed_url") or a.get("source_url"))
            if not any([site, query, raw_url]):
                raise ValueError(f"action[{idx}] missing site/query/url for mcp.source.disable")
            clean["source_type"] = source_type or "auto"
            if site:
                clean["site"] = site
            if query:
                clean["query"] = query
            if raw_url:
                clean["url"] = raw_url
            if "json_path" in a:
                clean["json_path"] = _norm_text(a.get("json_path") or "/latest.json") or "/latest.json"
            if "time_range" in a:
                clean["time_range"] = _norm_text(a.get("time_range") or "week") or "week"
            if "results" in a:
                vv = _norm_int(a.get("results"))
                if vv is not None:
                    clean["results"] = int(vv)

        elif op == "mcp.binding.remove":
            source_type = _norm_text(a.get("source_type") or a.get("connector") or a.get("source_kind") or "auto")
            topic = _norm_text(a.get("topic") or a.get("topic_name"))
            site = _norm_text(a.get("site") or a.get("domain") or a.get("base_url"))
            query = _norm_text(a.get("query"))
            raw_url = _norm_text(a.get("url") or a.get("feed_url") or a.get("source_url"))
            if not any([topic, site, query, raw_url]):
                raise ValueError(f"action[{idx}] missing locator for mcp.binding.remove")
            clean["source_type"] = source_type or "auto"
            if topic:
                clean["topic"] = topic
            if site:
                clean["site"] = site
            if query:
                clean["query"] = query
            if raw_url:
                clean["url"] = raw_url
            if "json_path" in a:
                clean["json_path"] = _norm_text(a.get("json_path") or "/latest.json") or "/latest.json"
            if "time_range" in a:
                clean["time_range"] = _norm_text(a.get("time_range") or "week") or "week"
            if "results" in a:
                vv = _norm_int(a.get("results"))
                if vv is not None:
                    clean["results"] = int(vv)

        actions.append(clean)

    # Optional high-level summary/questions.
    summary = _norm_text(obj.get("summary"))
    questions = obj.get("questions")
    qs: list[str] = []
    if isinstance(questions, list):
        for q in questions[:10]:
            s = _norm_text(q)
            if s:
                qs.append(s)
    if qs:
        warnings.append("questions: " + " | ".join(qs))

    plan: dict[str, Any] = {"actions": actions}
    if summary:
        plan["summary"] = summary
    return plan, warnings


def materialize_ai_setup_mcp_plan(
    *,
    snapshot_before: dict[str, Any],
    plan: dict[str, Any],
    searxng_base_url: str = "",
    profile_topic_name: str = "Profile",
) -> tuple[dict[str, Any], list[str]]:
    """Expand high-level MCP source/binding actions into concrete tracking actions."""
    warnings: list[str] = []
    try:
        base_plan, base_warnings = validate_ai_setup_plan(plan)
        warnings.extend(base_warnings)
    except Exception:
        base_plan = {"actions": list((plan or {}).get("actions") or [])}

    if not has_source_binding_mcp_actions(base_plan):
        return base_plan, warnings

    expanded_actions, more = expand_source_binding_mcp_actions(
        snapshot_before=snapshot_before,
        actions=list((base_plan.get("actions") or [])[:2000]),
        searxng_base_url=searxng_base_url,
        profile_topic_name=_norm_text(profile_topic_name) or "Profile",
    )
    warnings.extend([w for w in more if w])

    out: dict[str, Any] = {"actions": expanded_actions}
    if isinstance(base_plan.get("summary"), str) and base_plan.get("summary"):
        out["summary"] = str(base_plan.get("summary") or "")

    normalized, more_warnings = validate_ai_setup_plan(out)
    warnings.extend([w for w in more_warnings if w])
    return normalized, warnings


def autofix_ai_setup_plan_for_source_expansion(
    *,
    snapshot_before: dict[str, Any],
    plan: dict[str, Any],
    user_prompt: str,
    searxng_base_url: str = "",
    profile_topic_name: str = "",
) -> tuple[dict[str, Any], list[str]]:
    """
    Best-effort post-processing for AI Setup plans to make source expansion reliable.

    Goals:
    - Avoid "hallucinated" RSS sources for broad interest prompts.
    - Ensure at least one SearxNG web-search seed is present for newly created topics,
      so discover-sources can expand into reviewable RSS/Atom candidates.

    This is intentionally conservative and never guesses secrets.
    """
    warnings: list[str] = []
    profile_topic = _norm_text(profile_topic_name) or "Profile"

    # Normalize the incoming plan first (defensive: caller may pass raw JSON).
    try:
        base_plan, base_warnings = validate_ai_setup_plan(plan)
        warnings.extend(base_warnings)
    except Exception:
        base_plan = {"actions": list((plan or {}).get("actions") or [])}

    actions0 = list((base_plan.get("actions") or [])[:600])

    def _extract_prompt_urls(text: str) -> set[str]:
        raw = (text or "").strip()
        if not raw:
            return set()
        urls: list[str] = []
        for m in _URL_RE.finditer(raw):
            u = _norm_text(m.group(0))
            # Common trailing punctuation from copy/paste.
            while u and u[-1] in ")]}>,.;":
                u = u[:-1]
            u = u.strip()
            if u.startswith(("http://", "https://")):
                urls.append(u)
            if len(urls) >= 30:
                break
        return set(urls)

    prompt_urls = _extract_prompt_urls(user_prompt or "")

    # Snapshot topic names (to detect newly created topics).
    before_topic_names: set[str] = set()
    try:
        for t in (snapshot_before.get("topics") or [])[:2000]:
            if not isinstance(t, dict):
                continue
            n = _norm_text(t.get("name"))
            if n:
                before_topic_names.add(n)
    except Exception:
        before_topic_names = set()

    topic_query_by_name: dict[str, str] = {}
    new_topic_names: list[str] = []
    for a in actions0:
        if not isinstance(a, dict):
            continue
        if str(a.get("op") or "").strip() != "topic.upsert":
            continue
        name = _norm_text(a.get("name") or a.get("topic"))
        if not name:
            continue
        topic_query_by_name[name] = _norm_text(a.get("query"))
        if name not in before_topic_names and name not in new_topic_names:
            new_topic_names.append(name)

    def _axis_to_topic_name(axis: str) -> str:
        raw = (axis or "").strip()
        s = " ".join(raw.split()).strip()
        if not s:
            return ""
        # Common shape: "Axis Name: long explanation…" / "Axis Name：long explanation…" -> keep the short name.
        for sep in (":", "："):
            if sep in s:
                left, _right = s.split(sep, 1)
                left = left.strip()
                if 2 <= len(left) <= 120:
                    s = left
                break
        # Drop parenthetical details when present.
        for open_br in ("（", "("):
            if open_br in s:
                s = s.split(open_br, 1)[0].strip()
        s = s.strip()
        if not s:
            s = raw.strip()
        if len(s) > 80:
            s = s[:80].rstrip()
        return s

    def _axis_to_topic_query(axis: str) -> str:
        raw = (axis or "").strip()
        s = " ".join(raw.split()).strip()
        if not s:
            return ""
        # Keep the full axis text as the topic query so web-search seeding stays specific,
        # but avoid unbounded UI noise.
        if len(s) > 800:
            s = s[:800].rstrip()
        return s

    def _extract_bullets(text: str, header: str) -> list[str]:
        """
        Extract a "- ..." bullet list under a header line like "INTEREST_AXES:".
        """
        raw = str(text or "")
        # Support UI/paste artifacts that contain literal "\\n" sequences instead of real newlines.
        raw = raw.replace("\\r\\n", "\n").replace("\\n", "\n").replace("\\r", "\n")
        raw = raw.replace("\r", "")
        if not raw:
            return []
        want = (header or "").strip().rstrip(":").upper()
        if not want:
            return []
        out: list[str] = []
        seen: set[str] = set()
        in_section = False
        for ln in raw.splitlines():
            s = (ln or "").strip()
            if not s:
                if in_section:
                    continue
                continue
            up = s.upper().rstrip(":").strip()
            if up == want:
                in_section = True
                continue
            if in_section and re.match(r"^[A-Z_]{3,}[A-Z0-9_ ]*:$", s):
                break
            if in_section and (s.startswith("- ") or s.startswith("• ")):
                v = s[2:].strip()
                if not v:
                    continue
                key = " ".join(v.split()).strip().lower()
                if key in seen:
                    continue
                seen.add(key)
                out.append(v)
                if len(out) >= 10_000:
                    break
        return out

    # Operators can paste explicit feed URLs; those are the only RSS sources we accept in a plan.
    #
    # Rationale: Without explicit feed URLs, `source.add_rss` is often an LLM hallucination
    # (or "picked from memory"). For broad interest prompts, prefer web-search seeds +
    # discover-sources so the operator can review candidates.
    actions: list[dict[str, Any]] = []
    removed_rss = 0
    removed_rss_topics: list[str] = []
    for a in actions0:
        if not isinstance(a, dict):
            continue
        op = str(a.get("op") or "").strip()
        if op == "source.add_rss":
            url = _norm_text(a.get("url"))
            if url and url in prompt_urls:
                actions.append(a)
                continue
            removed_rss += 1
            try:
                bind = a.get("bind")
                if isinstance(bind, dict):
                    t = _norm_text(bind.get("topic"))
                    if t and t not in removed_rss_topics:
                        removed_rss_topics.append(t)
            except Exception:
                pass
            continue
        actions.append(a)
    if removed_rss:
        warnings.append(
            f"autofix: removed {removed_rss} source.add_rss actions (feed URL not explicitly provided in user prompt); prefer web search seeds + discovery"
        )

    # Keep the profile topic query empty (profile is curation-only; not keyword matching).
    for a in actions:
        if not isinstance(a, dict):
            continue
        if str(a.get("op") or "").strip() != "topic.upsert":
            continue
        name = _norm_text(a.get("name") or a.get("topic"))
        if not name or name != profile_topic:
            continue
        q0 = str(a.get("query") or "").strip()
        if q0:
            a["query"] = ""
            warnings.append(f"autofix: kept profile topic query empty: {profile_topic}")

    # Ensure new topics are explicitly enabled on apply.
    #
    # Rationale: Smart Config may create draft topics (disabled) to run source discovery before Apply.
    # If the plan omits `enabled`, applying it would keep the draft disabled and nothing would run.
    for a in actions:
        if not isinstance(a, dict):
            continue
        if str(a.get("op") or "").strip() != "topic.upsert":
            continue
        name = _norm_text(a.get("name") or a.get("topic"))
        if not name or name not in new_topic_names:
            continue
        if "enabled" not in a:
            a["enabled"] = True
            warnings.append(f"autofix: added topic.upsert.enabled=true for new topic: {name}")

    def _snapshot_has_searx_binding(topic_name: str) -> bool:
        t = (topic_name or "").strip()
        if not t:
            return False
        try:
            for b in (snapshot_before.get("bindings") or [])[:4000]:
                if not isinstance(b, dict):
                    continue
                if _norm_text(b.get("topic")) != t:
                    continue
                src = b.get("source") or {}
                if not isinstance(src, dict):
                    continue
                if _norm_text(src.get("type")) == "searxng_search":
                    return True
        except Exception:
            return False
        return False

    def _snapshot_searx_queries(topic_name: str) -> set[str]:
        """
        Best-effort: parse existing searxng_search bindings' `q=` query strings for a topic.
        """
        t = (topic_name or "").strip()
        if not t:
            return set()
        out: set[str] = set()
        try:
            from urllib.parse import parse_qs, urlsplit

            for b in (snapshot_before.get("bindings") or [])[:6000]:
                if not isinstance(b, dict):
                    continue
                if _norm_text(b.get("topic")) != t:
                    continue
                src = b.get("source") or {}
                if not isinstance(src, dict):
                    continue
                if _norm_text(src.get("type")) != "searxng_search":
                    continue
                u = _norm_text(src.get("url"))
                if not u:
                    continue
                try:
                    qs = parse_qs(urlsplit(u).query or "")
                except Exception:
                    qs = {}
                q = _norm_text((qs.get("q") or [""])[0])
                if q:
                    out.add(normalize_search_query(q) or q)
        except Exception:
            return out
        return out

    def _has_searx_seed(topic_name: str) -> bool:
        t = (topic_name or "").strip()
        if not t:
            return False
        for a in actions:
            if not isinstance(a, dict):
                continue
            if str(a.get("op") or "").strip() != "source.add_searxng_search":
                continue
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")) == t:
                return True
        return False

    def _action_searx_queries(topic_name: str) -> set[str]:
        t = (topic_name or "").strip()
        if not t:
            return set()
        out: set[str] = set()
        for a in actions:
            if not isinstance(a, dict):
                continue
            if str(a.get("op") or "").strip() != "source.add_searxng_search":
                continue
            bind = a.get("bind")
            if not (isinstance(bind, dict) and _norm_text(bind.get("topic")) == t):
                continue
            q = _norm_text(a.get("query"))
            if q:
                out.add(normalize_search_query(q) or q)
        return out

    # Seed topics:
    # - new topics
    # - topics where hallucinated RSS sources were removed
    # - *any* topic.upsert in the plan (even if the topic already existed), because discover-sources
    #   needs at least one web-search seed per touched topic to reliably expand sources.
    need_seed_topics: list[str] = list(
        dict.fromkeys([*new_topic_names, *removed_rss_topics, *list(topic_query_by_name.keys())])
    )
    # Never seed web-search sources for the Profile topic (profile is curation-only).
    need_seed_topics = [t for t in need_seed_topics if t and t != profile_topic]

    # Infer SearxNG base URL (prefer explicit settings, fall back to any existing searxng_search source).
    searx_base = (searxng_base_url or "").strip()
    if not searx_base:
        try:
            from urllib.parse import urlsplit, urlunsplit

            for s in (snapshot_before.get("sources") or [])[:2000]:
                if not isinstance(s, dict):
                    continue
                if _norm_text(s.get("type")) != "searxng_search":
                    continue
                u = _norm_text(s.get("url"))
                if not u:
                    continue
                parts = urlsplit(u)
                if (parts.scheme or "") not in {"http", "https"} or not (parts.netloc or ""):
                    continue
                base_guess = urlunsplit((parts.scheme, parts.netloc, parts.path or "", "", ""))
                searx_base = normalize_searxng_base_url(base_guess)
                if searx_base:
                    break
        except Exception:
            searx_base = ""

    if not searx_base:
        # Best-effort: probe a couple common local ports so Smart Config doesn't silently
        # seed an unusable URL (port drift is a common docker-compose operator tweak).
        def _probe_searxng_base_url(candidates: list[str]) -> str:
            try:
                import httpx  # local import: avoid making httpx a hard dependency of planning
            except Exception:
                return ""
            for u0 in candidates:
                base0 = normalize_searxng_base_url(u0)
                if not base0:
                    continue
                try:
                    url = build_searxng_search_url(base_url=base0, query="openinfomate", results=1, time_range="day")
                    resp = httpx.get(
                        url,
                        timeout=0.6,
                        follow_redirects=True,
                        headers={"User-Agent": "tracker/0.1"},
                    )
                    if int(getattr(resp, "status_code", 0) or 0) != 200:
                        continue
                    try:
                        obj = resp.json()
                    except Exception:
                        obj = None
                    if isinstance(obj, dict) and isinstance(obj.get("results"), list):
                        return base0
                except Exception:
                    continue
            return ""

        probed = _probe_searxng_base_url(["http://127.0.0.1:8888", "http://127.0.0.1:8889"])
        searx_base = probed or "http://127.0.0.1:8888"
        warnings.append(
            f"autofix: searxng base_url defaulted to {searx_base} (set TRACKER_SEARXNG_BASE_URL to override)"
        )

    # If the operator explicitly configured a SearxNG base URL, treat it as the source of truth
    # and normalize *all* searxng_search actions to use it. This avoids plans that hard-code
    # the wrong local port (e.g. 8888 vs 8889) and then get auto-disabled after repeated errors.
    if (searxng_base_url or "").strip():
        rewrites = 0
        for a in actions:
            if not isinstance(a, dict):
                continue
            if str(a.get("op") or "").strip() != "source.add_searxng_search":
                continue
            old_base = _norm_text(a.get("base_url"))
            if not old_base:
                a["base_url"] = searx_base
                rewrites += 1
                continue
            old_norm = normalize_searxng_base_url(old_base) or old_base
            if old_norm != searx_base:
                a["base_url"] = searx_base
                rewrites += 1
        if rewrites:
            warnings.append(f"autofix: normalized {rewrites} searxng_search base_url actions to {searx_base}")

    for tname in need_seed_topics:
        desired_query = normalize_search_query(topic_query_by_name.get(tname) or tname) or tname
        snapshot_qs = _snapshot_searx_queries(tname)
        action_qs = _action_searx_queries(tname)
        # Avoid searx seed explosion: allow a few distinct queries per topic, but keep bounded.
        if len(snapshot_qs | action_qs) >= 4:
            continue
        if desired_query and desired_query in (snapshot_qs | action_qs):
            continue
        # If the topic has some searx binding but it does not match the (new) query,
        # we still add one seed so discover-sources can expand along the updated query.
        if _has_searx_seed(tname):
            continue
        q = normalize_search_query(topic_query_by_name.get(tname) or tname)
        if not q:
            q = tname
        actions.append(
            {
                "op": "source.add_searxng_search",
                "base_url": searx_base,
                "query": q,
                # Keep it recent by default; discovery will still fetch pages and derive feeds.
                "time_range": "week",
                "results": 10,
                "bind": {"topic": tname, "include_keywords": "", "exclude_keywords": ""},
            }
        )
        if _snapshot_has_searx_binding(tname):
            warnings.append(f"autofix: added extra searxng_search seed for updated topic: {tname}")
        else:
            warnings.append(f"autofix: added searxng_search seed for topic: {tname}")

    # If the plan only touches the Profile topic, expand topics/seeds from profile axes/queries.
    plan_topic_names: list[str] = []
    for a in actions:
        if not isinstance(a, dict):
            continue
        if str(a.get("op") or "").strip() != "topic.upsert":
            continue
        n = _norm_text(a.get("name") or a.get("topic"))
        if n and n not in plan_topic_names:
            plan_topic_names.append(n)
    non_profile_topics = [n for n in plan_topic_names if n != profile_topic]
    if not non_profile_topics:
        axes = _extract_bullets(user_prompt, "INTEREST_AXES")
        seed_queries = _extract_bullets(user_prompt, "RETRIEVAL_QUERIES") + _extract_bullets(user_prompt, "SEED_QUERIES")

        # De-dup seed queries (case/whitespace-insensitive), keep order.
        sq_seen: set[str] = set()
        sq: list[str] = []
        for q0 in seed_queries:
            q = normalize_search_query(q0) or " ".join(str(q0 or "").split()).strip()
            if not q:
                continue
            key = q.lower()
            if key in sq_seen:
                continue
            sq_seen.add(key)
            sq.append(q)
            if len(sq) >= 600:
                break

        # Build topic list from interest axes.
        t_seen: set[str] = set(n.lower() for n in plan_topic_names)
        extra_topics: list[str] = []
        axis_query_by_topic_name: dict[str, str] = {}
        remaining = max(0, 2000 - len(actions))
        max_topics = min(60, max(0, remaining // 2))
        for ax in axes:
            if len(extra_topics) >= max_topics:
                break
            name = _axis_to_topic_name(ax)
            if not name:
                continue
            if name.lower() == profile_topic.lower():
                continue
            if name.lower() in t_seen:
                continue
            t_seen.add(name.lower())
            extra_topics.append(name)
            # Preserve full axis text as query for better seeding.
            axis_q = _axis_to_topic_query(ax) or name
            axis_query_by_topic_name.setdefault(name, axis_q)

        if extra_topics:
            warnings.append(
                f"autofix: expanded plan from INTEREST_AXES into topics+seeds: topics={len(extra_topics)} queries={len(sq)}"
            )
            for name in extra_topics:
                actions.append(
                    {
                        "op": "topic.upsert",
                        "name": name,
                        "query": axis_query_by_topic_name.get(name) or name,
                        "enabled": True,
                    }
                )

            # Add many short search seeds (SearxNG).
            base = searx_base
            for i, q in enumerate(sq[:240] or []):
                if len(actions) >= 2000:
                    break
                tname = extra_topics[i % len(extra_topics)]
                actions.append(
                    {
                        "op": "source.add_searxng_search",
                        "base_url": base,
                        "query": q,
                        "time_range": "week",
                        "results": 10,
                        "bind": {"topic": tname, "include_keywords": "", "exclude_keywords": ""},
                    }
                )

    out: dict[str, Any] = {"actions": actions}
    if isinstance(base_plan.get("summary"), str) and base_plan.get("summary"):
        out["summary"] = str(base_plan.get("summary") or "")

    try:
        normalized, more_warnings = validate_ai_setup_plan(out)
        warnings.extend(more_warnings)
        return normalized, warnings
    except Exception:
        return out, warnings


def apply_plan_to_snapshot(*, snapshot: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any]:
    """
    Apply a validated plan to an exported snapshot (preview-only; no DB writes).
    """
    if has_source_binding_mcp_actions(plan):
        try:
            plan, _warnings = materialize_ai_setup_mcp_plan(snapshot_before=snapshot or {}, plan=plan)
        except Exception:
            pass

    cur = copy.deepcopy(snapshot or {})

    topics = cur.get("topics")
    if not isinstance(topics, list):
        topics = []
        cur["topics"] = topics
    sources = cur.get("sources")
    if not isinstance(sources, list):
        sources = []
        cur["sources"] = sources
    bindings = cur.get("bindings")
    if not isinstance(bindings, list):
        bindings = []
        cur["bindings"] = bindings

    topic_by_name: dict[str, dict[str, Any]] = {}
    for t in topics:
        if isinstance(t, dict) and _norm_text(t.get("name")):
            topic_by_name[_norm_text(t.get("name"))] = t

    source_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for s in sources:
        if not isinstance(s, dict):
            continue
        st = _norm_text(s.get("type"))
        url = _norm_text(s.get("url"))
        if st and url:
            source_by_key[(st, url)] = s

    def _upsert_topic(name: str) -> dict[str, Any]:
        if name in topic_by_name:
            return topic_by_name[name]
        t = {
            "name": name,
            "query": "",
            "enabled": True,
            "digest_cron": "0 9 * * *",
            "alert_keywords": "",
            "alert_cooldown_minutes": 120,
            "alert_daily_cap": 5,
        }
        topics.append(t)
        topic_by_name[name] = t
        return t

    def _upsert_source(st: str, url: str) -> dict[str, Any]:
        key = (st, url)
        if key in source_by_key:
            return source_by_key[key]
        s = {"type": st, "url": url, "enabled": True, "tags": "", "notes": ""}
        sources.append(s)
        source_by_key[key] = s
        return s

    def _bind(topic: str, st: str, url: str, include: str = "", exclude: str = "") -> None:
        for b in bindings:
            if not isinstance(b, dict):
                continue
            if _norm_text(b.get("topic")) != topic:
                continue
            src = b.get("source") or {}
            if not isinstance(src, dict):
                continue
            if _norm_text(src.get("type")) == st and _norm_text(src.get("url")) == url:
                b["include_keywords"] = include
                b["exclude_keywords"] = exclude
                return
        bindings.append(
            {
                "topic": topic,
                "source": {"type": st, "url": url},
                "include_keywords": include,
                "exclude_keywords": exclude,
            }
        )

    def _unbind(topic: str, st: str, url: str) -> None:
        keep: list[dict[str, Any]] = []
        for b in bindings:
            if not isinstance(b, dict):
                continue
            bt = _norm_text(b.get("topic"))
            src = b.get("source") or {}
            bst = _norm_text(src.get("type")) if isinstance(src, dict) else ""
            burl = _norm_text(src.get("url")) if isinstance(src, dict) else ""
            if bt == topic and bst == st and burl == url:
                continue
            keep.append(b)
        bindings[:] = keep

    for a in plan.get("actions") or []:
        if not isinstance(a, dict):
            continue
        op = _norm_text(a.get("op"))

        if op == "topic.upsert":
            name = _norm_text(a.get("name"))
            if not name:
                continue
            t = _upsert_topic(name)
            if "query" in a:
                t["query"] = str(a.get("query") or "")
            if "enabled" in a:
                t["enabled"] = bool(a.get("enabled", True))
            for k in ("digest_cron", "alert_keywords", "alert_cooldown_minutes", "alert_daily_cap"):
                if k in a:
                    t[k] = a.get(k)

        elif op == "topic.disable":
            name = _norm_text(a.get("name"))
            if not name:
                continue
            t = _upsert_topic(name)
            t["enabled"] = False

        elif op == "source.add_rss":
            url = _norm_text(a.get("url"))
            if not url:
                continue
            s = _upsert_source("rss", url)
            if "tags" in a:
                s["tags"] = str(a.get("tags") or "")
            if "notes" in a:
                s["notes"] = str(a.get("notes") or "")
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")):
                topic = _norm_text(bind.get("topic"))
                _upsert_topic(topic)
                _bind(
                    topic,
                    "rss",
                    url,
                    include=str(bind.get("include_keywords") or ""),
                    exclude=str(bind.get("exclude_keywords") or ""),
                )

        elif op == "source.add_hn_search":
            q = normalize_search_query(_norm_text(a.get("query")))
            tags = _norm_text(a.get("tags") or "story") or "story"
            hits = int(_norm_int(a.get("hits_per_page"), 50) or 50)
            url = build_hn_search_url(query=q, tags=tags, hits_per_page=hits)
            _upsert_source("hn_search", url)
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")):
                topic = _norm_text(bind.get("topic"))
                _upsert_topic(topic)
                _bind(
                    topic,
                    "hn_search",
                    url,
                    include=str(bind.get("include_keywords") or ""),
                    exclude=str(bind.get("exclude_keywords") or ""),
                )

        elif op == "source.add_searxng_search":
            base_url = _norm_text(a.get("base_url"))
            q = normalize_search_query(_norm_text(a.get("query")))
            url = build_searxng_search_url(
                base_url=base_url,
                query=q,
                categories=_norm_text(a.get("categories")) or None,
                time_range=_norm_text(a.get("time_range")) or None,
                language=_norm_text(a.get("language")) or None,
                results=_norm_int(a.get("results")) if a.get("results") is not None else None,
            )
            _upsert_source("searxng_search", url)
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")):
                topic = _norm_text(bind.get("topic"))
                _upsert_topic(topic)
                _bind(
                    topic,
                    "searxng_search",
                    url,
                    include=str(bind.get("include_keywords") or ""),
                    exclude=str(bind.get("exclude_keywords") or ""),
                )

        elif op == "source.add_discourse":
            base_url = _norm_text(a.get("base_url"))
            json_path = _norm_text(a.get("json_path") or "/latest.json") or "/latest.json"
            url = build_discourse_json_url(base_url=base_url, json_path=json_path)
            _upsert_source("discourse", url)
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")):
                topic = _norm_text(bind.get("topic"))
                _upsert_topic(topic)
                _bind(
                    topic,
                    "discourse",
                    url,
                    include=str(bind.get("include_keywords") or ""),
                    exclude=str(bind.get("exclude_keywords") or ""),
                )

        elif op == "source.add_html_list":
            page_url = _norm_text(a.get("page_url"))
            item_selector = _norm_text(a.get("item_selector"))
            title_selector = _norm_text(a.get("title_selector") or "a") or "a"
            summary_selector = _norm_text(a.get("summary_selector") or "") or None
            max_items = int(_norm_int(a.get("max_items"), 30) or 30)
            url = build_html_list_url(
                page_url=page_url,
                item_selector=item_selector,
                title_selector=title_selector,
                summary_selector=summary_selector,
                max_items=max_items,
            )
            _upsert_source("html_list", url)
            bind = a.get("bind")
            if isinstance(bind, dict) and _norm_text(bind.get("topic")):
                topic = _norm_text(bind.get("topic"))
                _upsert_topic(topic)
                _bind(
                    topic,
                    "html_list",
                    url,
                    include=str(bind.get("include_keywords") or ""),
                    exclude=str(bind.get("exclude_keywords") or ""),
                )

        elif op == "source.disable":
            st = _norm_text(a.get("type"))
            url = _norm_text(a.get("url"))
            if not st or not url:
                continue
            s = _upsert_source(st, url)
            s["enabled"] = False

        elif op == "source.set_meta":
            st = _norm_text(a.get("type"))
            url = _norm_text(a.get("url"))
            if not st or not url:
                continue
            s = _upsert_source(st, url)
            if "tags" in a:
                s["tags"] = str(a.get("tags") or "")
            if "notes" in a:
                s["notes"] = str(a.get("notes") or "")

        elif op == "binding.remove":
            topic = _norm_text(a.get("topic"))
            src = a.get("source") or {}
            st = _norm_text(src.get("type")) if isinstance(src, dict) else ""
            url = _norm_text(src.get("url")) if isinstance(src, dict) else ""
            if topic and st and url:
                _unbind(topic, st, url)

        elif op == "binding.set_filters":
            topic = _norm_text(a.get("topic"))
            src = a.get("source") or {}
            st = _norm_text(src.get("type")) if isinstance(src, dict) else ""
            url = _norm_text(src.get("url")) if isinstance(src, dict) else ""
            if not (topic and st and url):
                continue
            _bind(
                topic,
                st,
                url,
                include=str(a.get("include_keywords") or ""),
                exclude=str(a.get("exclude_keywords") or ""),
            )

    cur["exported_at"] = dt.datetime.utcnow().isoformat() + "Z"
    return cur


def diff_tracking_snapshots(*, before: dict[str, Any], after: dict[str, Any]) -> str:
    """
    Human-friendly diff for preview/audit (Markdown-ish).
    """
    b_topics = {str(t.get("name") or ""): t for t in (before.get("topics") or []) if isinstance(t, dict) and str(t.get("name") or "").strip()}
    a_topics = {str(t.get("name") or ""): t for t in (after.get("topics") or []) if isinstance(t, dict) and str(t.get("name") or "").strip()}

    b_sources = {(str(s.get("type") or ""), str(s.get("url") or "")): s for s in (before.get("sources") or []) if isinstance(s, dict)}
    a_sources = {(str(s.get("type") or ""), str(s.get("url") or "")): s for s in (after.get("sources") or []) if isinstance(s, dict)}

    def _short_text(v: object, limit: int = 120) -> str:
        s = str(v if v is not None else "")
        s = " ".join(s.split())
        if len(s) > limit:
            return s[: max(0, limit - 1)] + "…"
        return s

    def _search_query_from_url(*, st: str, url: str) -> str:
        """
        Best-effort: extract the human query string from a search connector URL.

        We intentionally hide connector implementation details (hn_search/searxng_search) in previews
        and show `search: <keywords>` instead.
        """
        try:
            from urllib.parse import parse_qs, urlsplit

            parts = urlsplit((url or "").strip())
            qs = parse_qs(parts.query or "")
            if st == "searxng_search":
                q = (qs.get("q") or [""])[0]
                return _short_text(q, 160)
            if st == "hn_search":
                q = (qs.get("query") or [""])[0]
                return _short_text(q, 160)
        except Exception:
            return ""
        return ""

    def _format_source(st: str, url: str) -> str:
        st2 = str(st or "").strip()
        url2 = str(url or "").strip()
        if st2 in {"searxng_search", "hn_search"}:
            q = _search_query_from_url(st=st2, url=url2)
            if q:
                return f"search: {q}"
            return "search"
        return f"{st2} {url2}".strip()

    def _binding_key(b: dict[str, Any]) -> tuple[str, str, str]:
        topic = str(b.get("topic") or "").strip()
        src = b.get("source") or {}
        st = str((src.get("type") if isinstance(src, dict) else "") or "").strip()
        url = str((src.get("url") if isinstance(src, dict) else "") or "").strip()
        return topic, st, url

    b_bind = {_binding_key(b): b for b in (before.get("bindings") or []) if isinstance(b, dict)}
    a_bind = {_binding_key(b): b for b in (after.get("bindings") or []) if isinstance(b, dict)}

    lines: list[str] = []
    lines.append("# Preview Diff")

    # Topics
    added_topics = sorted([k for k in a_topics.keys() if k and k not in b_topics])
    removed_topics = sorted([k for k in b_topics.keys() if k and k not in a_topics])
    changed_topics: list[str] = []
    for k in sorted(set(b_topics.keys()) & set(a_topics.keys())):
        bt = b_topics.get(k) or {}
        at = a_topics.get(k) or {}
        for field in ("enabled", "query", "digest_cron", "alert_keywords", "alert_cooldown_minutes", "alert_daily_cap"):
            if str(bt.get(field)) != str(at.get(field)):
                changed_topics.append(k)
                break

    lines.append("")
    lines.append("## Topics")
    if added_topics:
        lines.append("- Added: " + ", ".join(added_topics[:40]) + ("…" if len(added_topics) > 40 else ""))
    if removed_topics:
        lines.append("- Removed: " + ", ".join(removed_topics[:40]) + ("…" if len(removed_topics) > 40 else ""))
    if changed_topics:
        lines.append("- Changed: " + ", ".join(changed_topics[:40]) + ("…" if len(changed_topics) > 40 else ""))
        # Show a small, bounded field-level preview so operators can understand "Changed".
        for name in changed_topics[:6]:
            bt = b_topics.get(name) or {}
            at = a_topics.get(name) or {}
            fields = ("enabled", "query", "digest_cron", "alert_keywords", "alert_cooldown_minutes", "alert_daily_cap")
            diffs: list[tuple[str, str, str]] = []
            for f in fields:
                if str(bt.get(f)) != str(at.get(f)):
                    diffs.append((f, _short_text(bt.get(f), 160), _short_text(at.get(f), 160)))
            if not diffs:
                continue
            lines.append(f"  - {name}:")
            for f, old, new in diffs[:6]:
                lines.append(f"    - {f}: {old} -> {new}")
    if not (added_topics or removed_topics or changed_topics):
        lines.append("- (no topic changes)")

    # Sources
    added_sources = sorted([k for k in a_sources.keys() if k not in b_sources and k[0] and k[1]])
    removed_sources = sorted([k for k in b_sources.keys() if k not in a_sources and k[0] and k[1]])
    changed_sources: list[tuple[str, str]] = []
    for k in sorted(set(b_sources.keys()) & set(a_sources.keys())):
        bs = b_sources.get(k) or {}
        a_s = a_sources.get(k) or {}
        for field in ("enabled", "tags", "notes"):
            if str(bs.get(field)) != str(a_s.get(field)):
                changed_sources.append(k)
                break

    lines.append("")
    lines.append("## Sources")
    if added_sources:
        lines.append(
            "- Added: "
            + ", ".join([_format_source(t, u) for t, u in added_sources[:20]])
            + ("…" if len(added_sources) > 20 else "")
        )
    if removed_sources:
        lines.append(
            "- Removed: "
            + ", ".join([_format_source(t, u) for t, u in removed_sources[:20]])
            + ("…" if len(removed_sources) > 20 else "")
        )
    if changed_sources:
        lines.append(
            "- Changed: "
            + ", ".join([_format_source(t, u) for t, u in changed_sources[:10]])
            + ("…" if len(changed_sources) > 10 else "")
        )
    if not (added_sources or removed_sources or changed_sources):
        lines.append("- (no source changes)")

    # Bindings
    added_bind = sorted([k for k in a_bind.keys() if k not in b_bind and k[0] and k[1] and k[2]])
    removed_bind = sorted([k for k in b_bind.keys() if k not in a_bind and k[0] and k[1] and k[2]])
    changed_bind: list[tuple[str, str, str]] = []
    for k in sorted(set(b_bind.keys()) & set(a_bind.keys())):
        bb = b_bind.get(k) or {}
        ab = a_bind.get(k) or {}
        if str(bb.get("include_keywords") or "") != str(ab.get("include_keywords") or ""):
            changed_bind.append(k)
            continue
        if str(bb.get("exclude_keywords") or "") != str(ab.get("exclude_keywords") or ""):
            changed_bind.append(k)

    lines.append("")
    lines.append("## Bindings")
    if added_bind:
        items: list[str] = []
        for t, st, u in added_bind[:20]:
            if st in {"searxng_search", "hn_search"}:
                q = _search_query_from_url(st=st, url=u)
                items.append(f"{t}<=search: {q}".strip() if q else f"{t}<=search")
            else:
                items.append(f"{t}<=({st})")
        lines.append("- Added: " + ", ".join(items) + ("…" if len(added_bind) > 20 else ""))
    if removed_bind:
        items: list[str] = []
        for t, st, u in removed_bind[:20]:
            if st in {"searxng_search", "hn_search"}:
                q = _search_query_from_url(st=st, url=u)
                items.append(f"{t}<=search: {q}".strip() if q else f"{t}<=search")
            else:
                items.append(f"{t}<=({st})")
        lines.append("- Removed: " + ", ".join(items) + ("…" if len(removed_bind) > 20 else ""))
    if changed_bind:
        items2: list[str] = []
        for t, st, u in changed_bind[:20]:
            if st in {"searxng_search", "hn_search"}:
                q = _search_query_from_url(st=st, url=u)
                items2.append(f"{t}<=search: {q}".strip() if q else f"{t}<=search")
            else:
                items2.append(f"{t}<=({st})")
        lines.append("- Changed: " + ", ".join(items2) + ("…" if len(changed_bind) > 20 else ""))
    if not (added_bind or removed_bind or changed_bind):
        lines.append("- (no binding changes)")

    return "\n".join(lines).strip()


def apply_plan_to_db(*, session: Session, plan: dict[str, Any]) -> list[str]:
    """
    Execute a validated plan (DB writes).

    Returns a list of human-readable change notes (best-effort).
    """
    repo = Repo(session)
    if has_source_binding_mcp_actions(plan):
        try:
            snapshot_before = export_tracking_snapshot(session=session)
            plan, _warnings = materialize_ai_setup_mcp_plan(
                snapshot_before=snapshot_before,
                plan=plan,
                profile_topic_name=(repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile",
            )
        except Exception:
            pass
    notes: list[str] = []

    for a in plan.get("actions") or []:
        if not isinstance(a, dict):
            continue
        op = _norm_text(a.get("op"))

        if op == "topic.upsert":
            name = _norm_text(a.get("name"))
            if not name:
                continue
            topic = repo.get_topic_by_name(name)
            if not topic:
                topic = repo.add_topic(name=name, query=str(a.get("query") or ""), digest_cron=str(a.get("digest_cron") or "0 9 * * *"))
                notes.append(f"topic added: {name}")
            changed = False
            if "query" in a and topic.query != str(a.get("query") or ""):
                topic.query = str(a.get("query") or "")
                changed = True
            if "enabled" in a:
                en = bool(a.get("enabled", True))
                if bool(topic.enabled) != en:
                    topic.enabled = en
                    changed = True
            if "digest_cron" in a and str(a.get("digest_cron") or "") and topic.digest_cron != str(a.get("digest_cron") or ""):
                topic.digest_cron = str(a.get("digest_cron") or "")
                changed = True
            if "alert_keywords" in a and topic.alert_keywords != str(a.get("alert_keywords") or ""):
                topic.alert_keywords = str(a.get("alert_keywords") or "")
                changed = True
            if "alert_cooldown_minutes" in a and isinstance(a.get("alert_cooldown_minutes"), int):
                v = int(a.get("alert_cooldown_minutes") or topic.alert_cooldown_minutes)
                if int(topic.alert_cooldown_minutes) != v:
                    topic.alert_cooldown_minutes = v
                    changed = True
            if "alert_daily_cap" in a and isinstance(a.get("alert_daily_cap"), int):
                v = int(a.get("alert_daily_cap") or topic.alert_daily_cap)
                if int(topic.alert_daily_cap) != v:
                    topic.alert_daily_cap = v
                    changed = True
            if changed:
                session.commit()
                notes.append(f"topic updated: {name}")

        elif op == "topic.disable":
            name = _norm_text(a.get("name"))
            if not name:
                continue
            topic = repo.get_topic_by_name(name)
            if topic and topic.enabled:
                topic.enabled = False
                session.commit()
                notes.append(f"topic disabled: {name}")

        elif op == "source.add_rss":
            url = _norm_text(a.get("url"))
            if not url:
                continue
            bind = None
            b = a.get("bind")
            if isinstance(b, dict) and _norm_text(b.get("topic")):
                bind = SourceBindingSpec(
                    topic=_norm_text(b.get("topic")),
                    include_keywords=str(b.get("include_keywords") or ""),
                    exclude_keywords=str(b.get("exclude_keywords") or ""),
                )
            src = create_rss_source(session=session, url=url, bind=bind)
            if "tags" in a or "notes" in a:
                try:
                    repo.update_source_meta(source_id=int(src.id), tags=str(a.get("tags") or ""), notes=str(a.get("notes") or ""))
                except Exception:
                    pass
            notes.append(f"source ensured: rss {url}")

        elif op == "source.add_hn_search":
            q = _norm_text(a.get("query"))
            if not q:
                continue
            bind = None
            b = a.get("bind")
            if isinstance(b, dict) and _norm_text(b.get("topic")):
                bind = SourceBindingSpec(
                    topic=_norm_text(b.get("topic")),
                    include_keywords=str(b.get("include_keywords") or ""),
                    exclude_keywords=str(b.get("exclude_keywords") or ""),
                )
            src = create_hn_search_source(
                session=session,
                query=q,
                tags=_norm_text(a.get("tags") or "story") or "story",
                hits_per_page=int(_norm_int(a.get("hits_per_page"), 50) or 50),
                bind=bind,
            )
            # Smart Config / "ensure" semantics: if the source already existed but was disabled,
            # bringing it back into the plan should re-enable it by default.
            try:
                if not bool(getattr(src, "enabled", True)):
                    src.enabled = True
                    session.commit()
            except Exception:
                try:
                    session.rollback()
                except Exception:
                    pass
            notes.append(f"source ensured: hn_search {src.url}")

        elif op == "source.add_searxng_search":
            base_url = _norm_text(a.get("base_url"))
            q = _norm_text(a.get("query"))
            if not base_url or not q:
                continue
            bind = None
            b = a.get("bind")
            if isinstance(b, dict) and _norm_text(b.get("topic")):
                bind = SourceBindingSpec(
                    topic=_norm_text(b.get("topic")),
                    include_keywords=str(b.get("include_keywords") or ""),
                    exclude_keywords=str(b.get("exclude_keywords") or ""),
                )
            src = create_searxng_search_source(
                session=session,
                base_url=base_url,
                query=q,
                categories=_norm_text(a.get("categories")) or None,
                time_range=_norm_text(a.get("time_range")) or None,
                language=_norm_text(a.get("language")) or None,
                results=_norm_int(a.get("results")) if a.get("results") is not None else None,
                bind=bind,
            )
            # Smart Config / "ensure" semantics: if the source already existed but was disabled,
            # bringing it back into the plan should re-enable it by default.
            try:
                if not bool(getattr(src, "enabled", True)):
                    src.enabled = True
                    session.commit()
            except Exception:
                try:
                    session.rollback()
                except Exception:
                    pass
            notes.append(f"source ensured: searxng_search {src.url}")

        elif op == "source.add_discourse":
            base_url = _norm_text(a.get("base_url"))
            if not base_url:
                continue
            bind = None
            b = a.get("bind")
            if isinstance(b, dict) and _norm_text(b.get("topic")):
                bind = SourceBindingSpec(
                    topic=_norm_text(b.get("topic")),
                    include_keywords=str(b.get("include_keywords") or ""),
                    exclude_keywords=str(b.get("exclude_keywords") or ""),
                )
            src = create_discourse_source(
                session=session,
                base_url=base_url,
                json_path=_norm_text(a.get("json_path") or "/latest.json") or "/latest.json",
                bind=bind,
            )
            notes.append(f"source ensured: discourse {src.url}")

        elif op == "source.add_html_list":
            page_url = _norm_text(a.get("page_url"))
            item_selector = _norm_text(a.get("item_selector"))
            if not page_url or not item_selector:
                continue
            bind = None
            b = a.get("bind")
            if isinstance(b, dict) and _norm_text(b.get("topic")):
                bind = SourceBindingSpec(
                    topic=_norm_text(b.get("topic")),
                    include_keywords=str(b.get("include_keywords") or ""),
                    exclude_keywords=str(b.get("exclude_keywords") or ""),
                )
            src = create_html_list_source(
                session=session,
                page_url=page_url,
                item_selector=item_selector,
                title_selector=_norm_text(a.get("title_selector") or "a") or "a",
                summary_selector=_norm_text(a.get("summary_selector") or "") or None,
                max_items=int(_norm_int(a.get("max_items"), 30) or 30),
                bind=bind,
            )
            notes.append(f"source ensured: html_list {src.url}")

        elif op == "source.disable":
            st = _norm_text(a.get("type"))
            url = _norm_text(a.get("url"))
            if not st or not url:
                continue
            src = repo.get_source(type=st, url=url)
            if src and src.enabled:
                src.enabled = False
                session.commit()
                notes.append(f"source disabled: {st} {url}")

        elif op == "source.set_meta":
            st = _norm_text(a.get("type"))
            url = _norm_text(a.get("url"))
            if not st or not url:
                continue
            src = repo.get_source(type=st, url=url)
            if not src:
                continue
            tags = (str(a.get("tags") or "") if "tags" in a else None)
            notes0 = (str(a.get("notes") or "") if "notes" in a else None)
            if tags is not None or notes0 is not None:
                repo.update_source_meta(source_id=int(src.id), tags=tags, notes=notes0)
                notes.append(f"source meta updated: {st} {url}")

        elif op == "binding.remove":
            topic_name = _norm_text(a.get("topic"))
            src_ref = a.get("source") or {}
            st = _norm_text(src_ref.get("type")) if isinstance(src_ref, dict) else ""
            url = _norm_text(src_ref.get("url")) if isinstance(src_ref, dict) else ""
            if not (topic_name and st and url):
                continue
            topic = repo.get_topic_by_name(topic_name)
            src = repo.get_source(type=st, url=url)
            if topic and src:
                if repo.unbind_topic_source(topic=topic, source=src):
                    notes.append(f"binding removed: {topic_name} <= {st} {url}")

        elif op == "binding.set_filters":
            topic_name = _norm_text(a.get("topic"))
            src_ref = a.get("source") or {}
            st = _norm_text(src_ref.get("type")) if isinstance(src_ref, dict) else ""
            url = _norm_text(src_ref.get("url")) if isinstance(src_ref, dict) else ""
            if not (topic_name and st and url):
                continue
            topic = repo.get_topic_by_name(topic_name)
            src = repo.get_source(type=st, url=url)
            if not (topic and src):
                continue
            ts = repo.bind_topic_source(topic=topic, source=src)
            ts.include_keywords = str(a.get("include_keywords") or "")
            ts.exclude_keywords = str(a.get("exclude_keywords") or "")
            session.commit()
            notes.append(f"binding updated: {topic_name} <= {st} {url}")

    return notes


def restore_tracking_snapshot_to_db(*, session: Session, snapshot: dict[str, Any]) -> list[str]:
    """
    Restore tracking config to match a snapshot (best-effort, non-destructive).

    Policy:
    - Topics/sources missing from snapshot are DISABLED (not deleted).
    - Bindings missing from snapshot are removed (bindings are cheap and safe to recreate).
    - Policies not present in snapshot are reset to disabled/empty prompt.
    """
    repo = Repo(session)
    notes: list[str] = []

    desired_topics: dict[str, dict[str, Any]] = {}
    for t in snapshot.get("topics") or []:
        if not isinstance(t, dict):
            continue
        name = _norm_text(t.get("name"))
        if not name:
            continue
        desired_topics[name] = t

    desired_sources: dict[tuple[str, str], dict[str, Any]] = {}
    for s in snapshot.get("sources") or []:
        if not isinstance(s, dict):
            continue
        st = _norm_text(s.get("type"))
        url = _norm_text(s.get("url"))
        if not st or not url:
            continue
        desired_sources[(st, url)] = s

    desired_bindings: dict[tuple[str, str, str], dict[str, Any]] = {}
    for b in snapshot.get("bindings") or []:
        if not isinstance(b, dict):
            continue
        topic = _norm_text(b.get("topic"))
        src = b.get("source") or {}
        st = _norm_text(src.get("type")) if isinstance(src, dict) else ""
        url = _norm_text(src.get("url")) if isinstance(src, dict) else ""
        if not (topic and st and url):
            continue
        desired_bindings[(topic, st, url)] = b

    # Topics: update existing, disable missing, create missing.
    existing_topics = {t.name: t for t in repo.list_topics()}
    for name, topic in existing_topics.items():
        want = desired_topics.get(name)
        if not want:
            if topic.enabled:
                topic.enabled = False
                notes.append(f"topic disabled: {name}")
            continue
        topic.query = str(want.get("query") or "")
        topic.enabled = bool(want.get("enabled", True))
        topic.digest_cron = str(want.get("digest_cron") or topic.digest_cron)
        topic.alert_keywords = str(want.get("alert_keywords") or "")
        topic.alert_cooldown_minutes = int(want.get("alert_cooldown_minutes") or topic.alert_cooldown_minutes)
        topic.alert_daily_cap = int(want.get("alert_daily_cap") or topic.alert_daily_cap)

    for name, want in desired_topics.items():
        if name in existing_topics:
            continue
        try:
            t = repo.add_topic(name=name, query=str(want.get("query") or ""), digest_cron=str(want.get("digest_cron") or "0 9 * * *"))
        except ValueError:
            t = repo.get_topic_by_name(name)
        if t:
            t.enabled = bool(want.get("enabled", True))
            t.alert_keywords = str(want.get("alert_keywords") or "")
            t.alert_cooldown_minutes = int(want.get("alert_cooldown_minutes") or t.alert_cooldown_minutes)
            t.alert_daily_cap = int(want.get("alert_daily_cap") or t.alert_daily_cap)
            session.commit()
            notes.append(f"topic added: {name}")

    session.commit()

    # Sources: update existing, disable missing, create missing.
    existing_sources = {(s.type, s.url): s for s in repo.list_sources()}
    for (st, url), src in existing_sources.items():
        want = desired_sources.get((st, url))
        if not want:
            if src.enabled:
                src.enabled = False
                notes.append(f"source disabled: {st} {url}")
            continue
        src.enabled = bool(want.get("enabled", True))
        # Meta
        try:
            repo.update_source_meta(
                source_id=int(src.id),
                tags=str(want.get("tags") or ""),
                notes=str(want.get("notes") or ""),
            )
        except Exception:
            pass

    for (st, url), want in desired_sources.items():
        if (st, url) in existing_sources:
            continue
        src = repo.add_source(type=st, url=url)
        src.enabled = bool(want.get("enabled", True))
        session.commit()
        try:
            repo.update_source_meta(
                source_id=int(src.id),
                tags=str(want.get("tags") or ""),
                notes=str(want.get("notes") or ""),
            )
        except Exception:
            pass
        notes.append(f"source added: {st} {url}")

    session.commit()

    # Bindings: ensure desired, remove extra.
    existing_bindings = repo.list_topic_sources()
    desired_keys = set(desired_bindings.keys())
    for t, s, ts in existing_bindings:
        key = (t.name, s.type, s.url)
        if key in desired_keys:
            want = desired_bindings.get(key) or {}
            ts.include_keywords = str(want.get("include_keywords") or "")
            ts.exclude_keywords = str(want.get("exclude_keywords") or "")
        else:
            session.delete(ts)
            notes.append(f"binding removed: {t.name} <= {s.type} {s.url}")
    session.commit()

    # Add missing bindings.
    for (topic_name, st, url), want in desired_bindings.items():
        topic = repo.get_topic_by_name(topic_name)
        src = repo.get_source(type=st, url=url)
        if not (topic and src):
            continue
        ts = repo.bind_topic_source(topic=topic, source=src)
        ts.include_keywords = str(want.get("include_keywords") or "")
        ts.exclude_keywords = str(want.get("exclude_keywords") or "")
        session.commit()

    return notes


def load_baseline_snapshot(repo: Repo) -> dict[str, Any] | None:
    raw = (repo.get_app_config(AI_SETUP_BASELINE_APP_CONFIG_KEY) or "").strip()
    if not raw:
        return None
    try:
        obj = json.loads(raw)
    except Exception:
        return None
    return obj if isinstance(obj, dict) else None


def save_baseline_snapshot(repo: Repo, snapshot: dict[str, Any]) -> None:
    repo.set_app_config(AI_SETUP_BASELINE_APP_CONFIG_KEY, json.dumps(snapshot, ensure_ascii=False))
