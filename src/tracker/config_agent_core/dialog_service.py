from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

from tracker.config_agent_core.dialog_tools import (
    dialog_tool_catalog_text,
    execute_dialog_tool_calls,
    serialize_dialog_tool_results,
)
from tracker.llm import llm_answer_config_agent_dialog, llm_route_config_agent_dialog
from tracker.llm_usage import UsageCallback
from tracker.repo import Repo
from tracker.settings import Settings

_URL_RE = re.compile(r"https?://[^\s)>\]]+", re.IGNORECASE)
_HOURS_RE = re.compile(
    r"(?P<num>\d{1,3})\s*(?:hours?|hrs?|hr|h|小时|小時)",
    re.IGNORECASE,
)
_ITEM_ID_RE = re.compile(r"#(?P<item_id>\d+)")

_INFO_KEYWORDS = (
    "参考消息",
    "digest",
    "report",
    "recent",
    "最近",
    "过去",
    "总结",
    "概括",
    "回顾",
    "recap",
    "summar",
    "explain",
    "解释",
    "什么意思",
    "论文",
    "专题",
    "why",
)
_CONFIG_ACTION_KEYWORDS = (
    "apply",
    "配置",
    "改配置",
    "改成",
    "更新配置",
    "set ",
    "enable",
    "disable",
    "新增",
    "添加来源",
    "加来源",
    "绑定",
    "topic",
    "source",
    "setting",
    "llm",
    "push",
)
_SELF_DEFERRING_REPLY_MARKERS = (
    "贴过来",
    "贴给我",
    "把那期内容贴过来",
    "需要先看到",
    "我需要先看到",
    "you need to paste",
    "paste it here",
    "paste the list",
    "i need to see",
    "need to see the list",
)
_CACHE_DENIAL_REPLY_MARKERS = (
    "没有看到 arxiv",
    "没有 arxiv",
    "看不到 arxiv",
    "并没有看到",
    "didn't see arxiv",
    "do not see arxiv",
    "no arxiv",
)


@dataclass(frozen=True)
class ConfigAgentDialogResult:
    plan: dict[str, Any]
    warnings: list[str]


def _norm_text(value: object) -> str:
    return str(value or "").strip()


def _contains_cjk(text: str) -> bool:
    for ch in str(text or ""):
        code = ord(ch)
        if 0x4E00 <= code <= 0x9FFF:
            return True
    return False


def _dialog_lang(*, settings: Settings, user_prompt: str) -> str:
    output_language = _norm_text(getattr(settings, "output_language", ""))
    if _contains_cjk(user_prompt):
        return "zh"
    if output_language.lower().startswith("zh"):
        return "zh"
    return "en"


def _looks_like_cached_info_request(prompt: str) -> bool:
    raw = _norm_text(prompt)
    if not raw:
        return False
    low = raw.casefold()
    if not any(keyword in low for keyword in [k.casefold() for k in _INFO_KEYWORDS]):
        return False
    summary_markers = ("参考消息", "digest", "report", "总结", "概括", "回顾", "recap", "summar", "explain", "解释", "为什么", "why")
    if any(keyword.casefold() in low for keyword in _CONFIG_ACTION_KEYWORDS) and not any(marker.casefold() in low for marker in summary_markers):
        return False
    return True


def _extract_explicit_url(prompt: str) -> str:
    match = _URL_RE.search(prompt or "")
    if not match:
        return ""
    return _norm_text(match.group(0)).rstrip(".,);]>")


def _extract_hours(prompt: str, *, default: int) -> int:
    match = _HOURS_RE.search(prompt or "")
    if match is not None:
        try:
            value = int(match.group("num") or default)
        except Exception:
            value = default
        return max(1, min(168, value))
    low = _norm_text(prompt).casefold()
    if "一天" in low or "1 day" in low:
        return 24
    if "两天" in low or "2 day" in low:
        return 48
    return max(1, min(168, default))


def _heuristic_route_plan(*, settings: Settings, user_prompt: str) -> dict[str, Any] | None:
    prompt = _norm_text(user_prompt)
    if not _looks_like_cached_info_request(prompt):
        return None
    low = prompt.casefold()
    explicit_url = _extract_explicit_url(prompt)
    item_match = _ITEM_ID_RE.search(prompt)
    if any(word in low for word in ("arxiv", "论文", "paper")) and any(word in low for word in ("19:00", "19点", "19 点", "collect", "专题")):
        return {
            "mode": "info_reply",
            "assistant_reply": "",
            "questions": [],
            "tool_calls": [
                {
                    "tool": "mcp.reports.recent",
                    "args": {
                        "hours": max(36, _extract_hours(prompt, default=36)),
                        "limit": 2,
                        "only_collect": True,
                        "include_items": True,
                        "title_query": "arxiv",
                    },
                }
            ],
        }
    if any(word in low for word in ("参考消息", "digest", "report", "最近", "过去", "recap", "summar")):
        return {
            "mode": "info_reply",
            "assistant_reply": "",
            "questions": [],
            "tool_calls": [
                {
                    "tool": "mcp.reports.recent",
                    "args": {
                        "hours": _extract_hours(prompt, default=24),
                        "limit": 6,
                        "only_collect": False,
                        "include_items": True,
                    },
                }
            ],
        }
    if item_match is not None:
        return {
            "mode": "info_reply",
            "assistant_reply": "",
            "questions": [],
            "tool_calls": [
                {"tool": "mcp.items.explain", "args": {"item_id": int(item_match.group("item_id"))}},
            ],
        }
    if explicit_url and any(word in low for word in ("解释", "explain", "是什么", "what is", "看看", "analyze", "分析")):
        return {
            "mode": "info_reply",
            "assistant_reply": "",
            "questions": [],
            "tool_calls": [
                {"tool": "mcp.items.explain", "args": {"url": explicit_url}},
                {"tool": "mcp.external.fetch_url", "args": {"url": explicit_url, "max_chars": 2500}},
            ],
        }
    return {
        "mode": "info_reply",
        "assistant_reply": "",
        "questions": [],
        "tool_calls": [
            {
                "tool": "mcp.items.search",
                "args": {
                    "query": prompt,
                    "hours": _extract_hours(prompt, default=72),
                    "limit": 6,
                },
            }
        ],
    }


def _is_strong_cache_backed_request(prompt: str) -> bool:
    low = _norm_text(prompt).casefold()
    if not low:
        return False
    summary_markers = ("总结", "概括", "回顾", "推荐", "解释", "summar", "recap", "recommend", "explain")
    if not any(marker.casefold() in low for marker in summary_markers):
        return False
    if any(word in low for word in ("arxiv", "论文", "paper", "专题", "参考消息", "digest", "report", "最近", "过去")):
        return True
    if _extract_explicit_url(prompt):
        return True
    if _ITEM_ID_RE.search(prompt):
        return True
    return False


def _normalize_route_plan(route_obj: object) -> dict[str, Any] | None:
    if not isinstance(route_obj, dict):
        return None
    mode = _norm_text(route_obj.get("mode")).lower()
    if mode not in {"info_reply", "config_plan"}:
        return None
    tool_calls = route_obj.get("tool_calls")
    if not isinstance(tool_calls, list):
        tool_calls = []
    questions = route_obj.get("questions")
    if not isinstance(questions, list):
        questions = []
    return {
        "mode": mode,
        "assistant_reply": _norm_text(route_obj.get("assistant_reply")),
        "questions": [str(q or "").strip() for q in questions[:3] if str(q or "").strip()],
        "tool_calls": tool_calls[:4],
    }


def _collect_items_from_tool_results(tool_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items_by_key: dict[str, dict[str, Any]] = {}
    for row in tool_results:
        result = row.get("result")
        if not isinstance(result, dict):
            continue
        if _norm_text(row.get("tool")) == "mcp.reports.recent":
            reports = result.get("reports")
            if isinstance(reports, list):
                for report in reports:
                    if not isinstance(report, dict):
                        continue
                    for item in (report.get("items") or [])[:100]:
                        if not isinstance(item, dict):
                            continue
                        key = _norm_text(item.get("url")) or f"item:{int(item.get('item_id') or 0)}"
                        existing = items_by_key.get(key)
                        if existing is None or int(item.get("rank_score") or 0) > int(existing.get("rank_score") or 0):
                            items_by_key[key] = dict(item)
        elif _norm_text(row.get("tool")) in {"mcp.items.search", "mcp.items.explain"}:
            rows = result.get("items") if isinstance(result.get("items"), list) else []
            single = result.get("item")
            if isinstance(single, dict):
                rows = list(rows) + [single]
            for item in rows[:100]:
                if not isinstance(item, dict):
                    continue
                key = _norm_text(item.get("url")) or f"item:{int(item.get('item_id') or 0)}"
                existing = items_by_key.get(key)
                if existing is None or int(item.get("rank_score") or 0) > int(existing.get("rank_score") or 0):
                    items_by_key[key] = dict(item)
    items = list(items_by_key.values())
    items.sort(
        key=lambda item: (
            -int(item.get("rank_score") or 0),
            _norm_text(item.get("published_at") or item.get("created_at")),
            _norm_text(item.get("title")),
        )
    )
    return items


def _fallback_dialog_plan(*, lang: str, prompt: str, tool_results: list[dict[str, Any]], warnings: list[str]) -> dict[str, Any]:
    is_zh = lang.startswith("zh")
    prompt_low = _norm_text(prompt).casefold()
    lines: list[str] = []
    collected_items = _collect_items_from_tool_results(tool_results)

    if collected_items and any(word in prompt_low for word in ("arxiv", "论文", "paper", "专题", "推荐", "recommend")):
        if is_zh:
            lines.append("基于最近缓存下来的 arXiv 专题，我优先推荐这些信息量/价值更高的论文：")
        else:
            lines.append("From the latest cached arXiv collect, these are the most information-dense / high-value papers I would prioritize:")
        for idx, item in enumerate(collected_items[:5], start=1):
            title = _norm_text(item.get("title")) or _norm_text(item.get("url")) or f"item #{idx}"
            why = _norm_text(item.get("best_why"))
            summary = _norm_text(item.get("best_summary"))
            detail = why or summary or _norm_text(item.get("content_excerpt"))
            if is_zh:
                lines.append(f"{idx}. {title}")
                if detail:
                    lines.append(f"   - 值得看：{detail}")
                if summary and summary != detail:
                    lines.append(f"   - 核心信息：{summary}")
            else:
                lines.append(f"{idx}. {title}")
                if detail:
                    lines.append(f"   - Why it matters: {detail}")
                if summary and summary != detail:
                    lines.append(f"   - Core point: {summary}")

    for row in tool_results:
        tool = _norm_text(row.get("tool"))
        result = row.get("result")
        if not isinstance(result, dict):
            continue
        if tool == "mcp.reports.recent":
            reports = result.get("reports")
            if isinstance(reports, list) and reports:
                header = "我先按缓存中的参考消息整理：" if is_zh else "Here is a cache-based recap:"
                if not lines:
                    lines.append(header)
                for report in reports[:3]:
                    if not isinstance(report, dict):
                        continue
                    title = _norm_text(report.get("title") or report.get("idempotency_key"))
                    count = int(report.get("reference_count") or 0)
                    takeaways = report.get("takeaways")
                    takeaway_text = ""
                    if isinstance(takeaways, list):
                        takeaway_text = "；".join([_norm_text(x) for x in takeaways[:2] if _norm_text(x)])
                    lines.append(
                        f"- {title} · {count} 条"
                        + (f" · {takeaway_text}" if takeaway_text else "")
                    )
        elif tool == "mcp.items.search":
            items = result.get("items")
            if isinstance(items, list) and items:
                if not lines:
                    lines.append("我先按缓存命中的条目回答：" if is_zh else "Here are the best cached matches:")
                for item in items[:3]:
                    if not isinstance(item, dict):
                        continue
                    lines.append(
                        f"- {_norm_text(item.get('title'))}: "
                        f"{_norm_text(item.get('best_summary') or item.get('best_why') or item.get('content_excerpt'))}"
                    )
        elif tool == "mcp.items.explain":
            item = result.get("item")
            if isinstance(item, dict):
                title = _norm_text(item.get("title"))
                detail = _norm_text(item.get("best_summary") or item.get("best_why") or item.get("content_excerpt"))
                block = f"{title}\n{detail}".strip()
                if block and block not in lines:
                    lines.append(block)
        elif tool == "mcp.external.fetch_url":
            excerpt = _norm_text(result.get("content_excerpt"))
            if excerpt:
                lines.append(excerpt)
    if not lines:
        lines.append(
            "我已经进入缓存优先智能对话模式，但这次没有拿到足够的缓存数据；请给我更具体的条目名、URL 或时间范围。"
            if is_zh
            else "I switched to cache-first smart dialog mode, but did not get enough cached data this time. Please give me a clearer item name, URL, or time range."
        )
    if warnings:
        lines.append("\n".join([f"⚠️ {warning}" for warning in warnings[:3]]))
    return {
        "assistant_reply": "\n\n".join([line for line in lines if _norm_text(line)]).strip(),
        "summary": "cache-first reply" if not is_zh else "缓存优先答复",
        "questions": [],
        "actions": [],
    }


def _reply_defers_back_to_user(reply: str) -> bool:
    low = _norm_text(reply).casefold()
    if not low:
        return False
    return any(marker.casefold() in low for marker in _SELF_DEFERRING_REPLY_MARKERS)


def _reply_denies_cache_data(*, prompt: str, reply: str, tool_results: list[dict[str, Any]]) -> bool:
    low = _norm_text(reply).casefold()
    prompt_low = _norm_text(prompt).casefold()
    if not low or not tool_results:
        return False
    collected_items = _collect_items_from_tool_results(tool_results)
    if not collected_items:
        return False
    if any(word in prompt_low for word in ("arxiv", "论文", "paper", "专题")):
        return any(marker.casefold() in low for marker in _CACHE_DENIAL_REPLY_MARKERS)
    return False


async def maybe_answer_config_agent_dialog_request(
    *,
    repo: Repo,
    settings: Settings,
    user_prompt: str,
    conversation_history_text: str = "",
    page_context_text: str = "",
    usage_cb: UsageCallback | None = None,
) -> ConfigAgentDialogResult | None:
    prompt = _norm_text(user_prompt)
    if not _looks_like_cached_info_request(prompt):
        return None

    lang = _dialog_lang(settings=settings, user_prompt=prompt)
    heuristic_route = _normalize_route_plan(_heuristic_route_plan(settings=settings, user_prompt=prompt))
    strong_cache_request = _is_strong_cache_backed_request(prompt)
    route_plan = None
    try:
        route_plan = await llm_route_config_agent_dialog(
            repo=repo,
            settings=settings,
            user_prompt=prompt,
            conversation_history_text=conversation_history_text,
            page_context_text=page_context_text,
            dialog_tools_text=dialog_tool_catalog_text(lang=lang),
            usage_cb=usage_cb,
        )
    except Exception:
        route_plan = None

    normalized_route = _normalize_route_plan(route_plan) if route_plan is not None else None
    if strong_cache_request and heuristic_route is not None:
        normalized_route = heuristic_route
    if normalized_route is None:
        normalized_route = heuristic_route
    if normalized_route is None:
        return None
    if str(normalized_route.get("mode") or "") != "info_reply":
        return None

    if not list(normalized_route.get("tool_calls") or []):
        plan = {
            "assistant_reply": _norm_text(normalized_route.get("assistant_reply"))
            or (
                "我可以基于已缓存的参考消息、专题和条目来总结与解释；也可以继续帮你改配置。"
                if lang.startswith("zh")
                else "I can summarize and explain cached digests, collect messages, and items, or keep helping you change configuration."
            ),
            "summary": "smart dialog reply" if not lang.startswith("zh") else "智能对话答复",
            "questions": list(normalized_route.get("questions") or []),
            "actions": [],
        }
        return ConfigAgentDialogResult(plan=plan, warnings=[])

    executions, tool_warnings = await execute_dialog_tool_calls(
        repo=repo,
        settings=settings,
        user_prompt=prompt,
        tool_calls=normalized_route.get("tool_calls") or [],
    )
    tool_results = serialize_dialog_tool_results(executions)

    try:
        planned = await llm_answer_config_agent_dialog(
            repo=repo,
            settings=settings,
            user_prompt=prompt,
            conversation_history_text=conversation_history_text,
            page_context_text=page_context_text,
            tool_results_json=json.dumps(tool_results, ensure_ascii=False),
            usage_cb=usage_cb,
        )
        if isinstance(planned, dict):
            planned.setdefault("actions", [])
            reply_text = _norm_text(planned.get("assistant_reply"))
            if tool_results and (
                _reply_defers_back_to_user(reply_text)
                or _reply_denies_cache_data(prompt=prompt, reply=reply_text, tool_results=tool_results)
            ):
                return ConfigAgentDialogResult(
                    plan=_fallback_dialog_plan(lang=lang, prompt=prompt, tool_results=tool_results, warnings=tool_warnings),
                    warnings=tool_warnings,
                )
            return ConfigAgentDialogResult(plan=planned, warnings=tool_warnings)
    except Exception:
        pass

    return ConfigAgentDialogResult(
        plan=_fallback_dialog_plan(lang=lang, prompt=prompt, tool_results=tool_results, warnings=tool_warnings),
        warnings=tool_warnings,
    )
