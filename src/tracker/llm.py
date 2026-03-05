from __future__ import annotations

import json
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlsplit, urlunsplit

import httpx

from tracker.models import Topic
from tracker.repo import Repo
from tracker.prompt_templates import resolve_prompt_best_effort
from tracker.settings import Settings
from tracker.story import extract_notable_links

logger = logging.getLogger(__name__)

UsageCallback = Callable[[dict], None]

_CJK_RE = re.compile(r"[\u4e00-\u9fff]")


@dataclass(frozen=True)
class LlmGateResult:
    decision: str  # alert|digest
    reason: str


@dataclass(frozen=True)
class LlmDigestSummary:
    summary: str
    highlights: list[str]
    risks: list[str]
    next_actions: list[str]


@dataclass(frozen=True)
class LlmCurationDecision:
    item_id: int
    decision: str  # ignore|digest|alert
    why: str
    summary: str


@dataclass(frozen=True)
class LlmTopicSourceHints:
    """
    Conservative, UI-friendly suggestions for initial sources.

    This is intentionally limited to Tracker's built-in source types (no arbitrary URLs).
    """

    add_hn: bool = True
    add_searxng: bool = True
    add_discourse: bool = False
    discourse_base_url: str = ""
    discourse_json_path: str = "/latest.json"
    add_nodeseek: bool = False


@dataclass(frozen=True)
class LlmTopicProposal:
    topic_name: str
    query_keywords: str
    alert_keywords: str
    ai_prompt: str
    source_hints: LlmTopicSourceHints | None = None


@dataclass(frozen=True)
class LlmProfileProposal:
    understanding: str
    ai_prompt: str
    interest_axes: list[str] = field(default_factory=list)
    interest_keywords: list[str] = field(default_factory=list)
    retrieval_queries: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class LlmProfileDeltaUpdate:
    delta_prompt: str
    note: str = ""


@dataclass(frozen=True)
class LlmPromptDeltaUpdate:
    delta_prompt: str
    note: str = ""


def _normalize_http_origin(value: str) -> str:
    """
    Normalize a user/LLM-provided base URL to an http(s) origin.

    Accepts inputs like "https://forum.example.com" (preferred). Returns "" when invalid.
    """
    raw = (value or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return ""
    scheme = (parts.scheme or "").strip().lower()
    if scheme not in {"http", "https"}:
        return ""
    host = (parts.hostname or "").strip()
    if not host:
        return ""
    port = parts.port
    netloc = host
    if port:
        netloc = f"{host}:{port}"
    return urlunsplit((scheme, netloc, "", "", "")).rstrip("/")


def _output_lang(settings: Settings) -> str:
    """
    Normalize server-side output language for LLM-generated text.

    Note: UI language is cookie-based; background jobs use Settings/app_config output_language.
    """
    raw0 = (getattr(settings, "output_language", "") or "").strip()
    low = raw0.lower()
    # Operators often enter human labels via UI/TG.
    if raw0 in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语", "漢語"}:
        return "zh"
    if raw0 in {"英文", "英语", "英語"}:
        return "en"
    if low in {"en", "en-us", "en-gb", "english"} or low.startswith("en"):
        return "en"
    if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
        return "zh"
    return "en"


def _contains_cjk(text: str) -> bool:
    return bool(_CJK_RE.search(text or ""))


def _tpl(repo: Repo | None, settings: Settings, slot_id: str, context: dict[str, object] | None = None) -> str:
    """
    Resolve an operator-configurable prompt template.

    Keep this lightweight: log template warnings, but never hard-fail background jobs.
    """
    try:
        res = resolve_prompt_best_effort(repo=repo, settings=settings, slot_id=slot_id, context=context or {})
    except Exception as exc:
        logger.warning("prompt resolve failed: slot=%s err=%s", slot_id, exc)
        return ""
    for w in res.warnings:
        logger.warning("prompt warning: slot=%s warn=%s", slot_id, w)
    return res.text or ""


def _ensure_non_codex_model(model: str, *, allow_codex: bool = False) -> None:
    # NOTE: OSS Core: do not enforce model branding/policy here.
    # Operators may choose any OpenAI-compatible model name (including "*codex*").
    return


def _select_model_for_kind(settings: Settings, *, kind: str) -> str | None:
    base = (settings.llm_model or "").strip()
    reasoning = ((settings.llm_model_reasoning or "").strip() if hasattr(settings, "llm_model_reasoning") else "")
    mini = ((settings.llm_model_mini or "").strip() if hasattr(settings, "llm_model_mini") else "")
    if _kind_uses_mini_provider(kind):
        return mini or reasoning or base or None
    return reasoning or base or None


def _kind_uses_mini_provider(kind: str) -> bool:
    return (kind or "").strip().lower() in {"digest_summary", "triage_items", "prompt_template_translate"}


def _select_llm_base_url_for_kind(settings: Settings, *, kind: str) -> str:
    base = (settings.llm_base_url or "").strip()
    if _kind_uses_mini_provider(kind):
        return ((getattr(settings, "llm_mini_base_url", "") or "").strip() or base)
    return base


def _select_llm_api_key_for_kind(settings: Settings, *, kind: str) -> str:
    key = (settings.llm_api_key or "").strip()
    if _kind_uses_mini_provider(kind):
        return ((getattr(settings, "llm_mini_api_key", "") or "").strip() or key)
    return key


def _select_llm_proxy_for_kind(settings: Settings, *, kind: str) -> str | None:
    if _kind_uses_mini_provider(kind):
        raw = (getattr(settings, "llm_mini_proxy", "") or "").strip()
        if raw:
            return raw
    raw = (getattr(settings, "llm_proxy", "") or "").strip()
    return raw or None


def _load_llm_extra_body(settings: Settings, *, kind: str = "") -> dict:
    raw_primary = (getattr(settings, "llm_extra_body_json", "") or "").strip()
    raw_mini = (getattr(settings, "llm_mini_extra_body_json", "") or "").strip()
    raw = raw_primary
    if _kind_uses_mini_provider(kind) and raw_mini:
        raw = raw_mini
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        logger.warning("invalid TRACKER_LLM_EXTRA_BODY_JSON (expected JSON object)")
        return {}
    if not isinstance(obj, dict):
        logger.warning("invalid TRACKER_LLM_EXTRA_BODY_JSON (expected JSON object)")
        return {}
    # Guardrails: these would break our request/response parsing.
    for k in ("model", "messages", "stream"):
        if k in obj:
            logger.warning("ignoring forbidden key in TRACKER_LLM_EXTRA_BODY_JSON: %s", k)
            obj.pop(k, None)
    return obj


async def llm_plan_tracking_ai_setup(
    *,
    repo: Repo | None = None,
    settings: Settings,
    user_prompt: str,
    tracking_snapshot_text: str,
    web_context: str = "",
    web_search_context: str = "",
    max_tokens_override: int | None = None,
    usage_cb: UsageCallback | None = None,
) -> tuple[dict, list[str]] | None:
    """
    Generate a bounded JSON plan for tracking config changes (topics/sources/bindings).

    Contract:
    - Returns (plan, warnings) where `plan` is normalized by `validate_ai_setup_plan()`.
    - Returns None if LLM is not configured.
    """
    if not settings.llm_base_url:
        return None
    kind = "tracking_ai_setup_plan"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "config_agent.tracking_ai_setup.plan.system")
    # NOTE: AI Setup inputs can be very long (profile dumps, bookmarks, notes).
    # Prefer transforming/structuring upstream (caller) instead of hard-truncating.
    # We still keep a high safety cap here to avoid exceeding provider context limits.
    user_prompt_raw = (user_prompt or "").strip()
    user_prompt_cap = 80_000
    user_prompt_for_llm = user_prompt_raw
    truncated = False
    if len(user_prompt_for_llm) > user_prompt_cap:
        user_prompt_for_llm = user_prompt_for_llm[:user_prompt_cap] + "…"
        truncated = True
    user = _tpl(
        repo,
        settings,
        "config_agent.tracking_ai_setup.plan.user",
        {
            "user_prompt": user_prompt_for_llm,
            "tracking_snapshot_text": _truncate((tracking_snapshot_text or "").strip(), 12_000),
            "web_context": _truncate((web_context or "").strip(), 20_000),
            "web_search_context": _truncate((web_search_context or "").strip(), 12_000),
        },
    )

    max_tokens = 1400
    if max_tokens_override is not None:
        try:
            max_tokens = int(max_tokens_override)
        except Exception:
            max_tokens = 1400
    else:
        try:
            max_tokens = int(getattr(settings, "ai_setup_plan_max_tokens", 1400) or 1400)
        except Exception:
            max_tokens = 1400
    # Allow large planning outputs (many topics + many search seeds).
    max_tokens = max(400, min(50_000, max_tokens))

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind=kind, model=resp_model, data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    from tracker.config_agent import validate_ai_setup_plan

    plan, warnings = validate_ai_setup_plan(obj)
    if truncated:
        warnings = list(warnings or [])
        warnings.append(
            f"USER_PROMPT truncated at {user_prompt_cap} chars before planning. "
            "Consider using Profile → One-click config (AI Setup) which transforms large inputs."
        )
    return plan, warnings


async def llm_transform_tracking_ai_setup_input(
    *,
    repo: Repo | None = None,
    settings: Settings,
    user_prompt_chunk: str,
    usage_cb: UsageCallback | None = None,
) -> dict[str, Any] | None:
    """
    Transform a chunk of USER_PROMPT into structured JSON for planning.

    Contract:
    - Returns a JSON object with keys like understanding/interest_axes/keywords/seed_queries.
    - Returns None if LLM is not configured.
    """
    if not settings.llm_base_url:
        return None
    kind = "tracking_ai_setup_transform"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "config_agent.tracking_ai_setup.transform.system")
    user = _tpl(
        repo,
        settings,
        "config_agent.tracking_ai_setup.transform.user",
        {"user_prompt_chunk": _truncate((user_prompt_chunk or "").strip(), 20_000)},
    )

    max_tokens = 1600
    try:
        # Reuse the AI Setup plan budget as an upper bound for transform work.
        max_tokens = max(800, min(3000, int(getattr(settings, "ai_setup_plan_max_tokens", 1600) or 1600)))
    except Exception:
        max_tokens = 1600

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": int(max_tokens),
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind=kind, model=resp_model, data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    # Keep only the expected shape, best-effort.
    out: dict[str, Any] = {}
    for k in ("understanding", "interest_axes", "keywords", "seed_queries"):
        if k in obj:
            out[k] = obj.get(k)
    if not out:
        return obj  # fall back to raw object for debugging
    return out


async def llm_translate_prompt_template(
    *,
    repo: Repo | None = None,
    settings: Settings,
    source_lang: str,
    target_lang: str,
    updated_source_text: str,
    previous_target_text: str,
    usage_cb: UsageCallback | None = None,
) -> str | None:
    """
    Translate a prompt template between zh/en using the mini provider/model.

    Contract:
    - Returns translated text (target language) or None if LLM not configured.
    - Never returns JSON; callers treat output as plain text.
    """
    if not settings.llm_base_url:
        return None
    kind = "prompt_template_translate"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.prompt_template_translate.system")
    user = _tpl(
        repo,
        settings,
        "llm.prompt_template_translate.user",
        {
            "source_lang": (source_lang or "").strip().lower(),
            "target_lang": (target_lang or "").strip().lower(),
            "updated_source_text": _truncate((updated_source_text or "").strip(), 18_000),
            "previous_target_text": _truncate((previous_target_text or "").strip(), 18_000),
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 1600,
    }

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind=kind, model=resp_model, data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")
    return (content or "").strip()


def _openai_compat_chat_completions_url(base_url: str) -> str:
    base = (base_url or "").rstrip("/")
    if base.endswith("/v1"):
        return f"{base}/chat/completions"
    return f"{base}/v1/chat/completions"


def _extract_first_json_object(text: str) -> dict | None:
    """
    Best-effort extraction of the first JSON object from an LLM response.

    Many providers occasionally wrap JSON in code fences or add a short preface.
    This helper is intentionally tolerant: it scans for '{' and uses raw_decode
    so trailing text doesn't break parsing.
    """
    raw = (text or "").strip()
    if not raw:
        return None

    # Prefer the first fenced block content if present (often ```json ... ```).
    if "```" in raw:
        parts = raw.split("```")
        for i in range(1, len(parts), 2):
            block = parts[i]
            if not block:
                continue
            lines = block.splitlines()
            if lines and lines[0].strip().lower() in {"json", "javascript"}:
                block = "\n".join(lines[1:])
            if "{" in block and "}" in block:
                raw = block.strip()
                break

    dec = json.JSONDecoder()
    for idx, ch in enumerate(raw):
        if ch != "{":
            continue
        try:
            obj, _end = dec.raw_decode(raw[idx:])
        except Exception:
            continue
        if isinstance(obj, dict):
            return obj
    return None


def _extract_usage(data: object) -> tuple[int, int, int]:
    """
    Extract OpenAI-compatible token usage from a response payload.

    Returns (prompt_tokens, completion_tokens, total_tokens); zeros if absent.
    """
    if not isinstance(data, dict):
        return 0, 0, 0
    usage = data.get("usage")
    if not isinstance(usage, dict):
        return 0, 0, 0
    try:
        prompt_tokens = int(usage.get("prompt_tokens") or 0)
    except Exception:
        prompt_tokens = 0
    try:
        completion_tokens = int(usage.get("completion_tokens") or 0)
    except Exception:
        completion_tokens = 0
    try:
        total_tokens = int(usage.get("total_tokens") or 0)
    except Exception:
        total_tokens = 0
    if total_tokens <= 0 and (prompt_tokens > 0 or completion_tokens > 0):
        total_tokens = prompt_tokens + completion_tokens
    return max(0, prompt_tokens), max(0, completion_tokens), max(0, total_tokens)


def _emit_usage(
    usage_cb: UsageCallback | None,
    *,
    kind: str,
    model: str,
    topic: str = "",
    data: object,
) -> None:
    if usage_cb is None:
        return
    pt, ct, tt = _extract_usage(data)
    try:
        usage_cb(
            {
                "kind": (kind or "").strip(),
                "model": (model or "").strip(),
                "topic": (topic or "").strip(),
                "prompt_tokens": pt,
                "completion_tokens": ct,
                "total_tokens": tt,
            }
        )
    except Exception:
        # Usage tracking must never break the main pipeline.
        logger.debug("usage_cb failed", exc_info=True)


def _truncate(text: str, limit: int) -> str:
    s = (text or "").strip()
    if len(s) <= limit:
        return s
    return s[:limit] + "…"


_REDUNDANT_TOKEN_RE = re.compile(r"[A-Za-z0-9]+|[\u4e00-\u9fff]")


def _is_redundant_pair(a: str, b: str) -> bool:
    """
    Best-effort redundancy detector for (summary, why) pairs.

    If the model repeats itself, it's better to drop the redundant field than to
    spam operators with two paraphrases of the same sentence.
    """
    x = (a or "").strip()
    y = (b or "").strip()
    if not (x and y):
        return False
    if x == y:
        return True
    if len(x) >= 12 and x in y:
        return True
    if len(y) >= 12 and y in x:
        return True

    tx = set(_REDUNDANT_TOKEN_RE.findall(x.lower()))
    ty = set(_REDUNDANT_TOKEN_RE.findall(y.lower()))
    if not tx or not ty:
        return False
    union = tx | ty
    if len(union) < 8:
        return False
    sim = len(tx & ty) / max(1, len(union))
    return sim >= 0.85


def _coerce_str_list(value: object, *, max_items: int) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [value.strip()] if value.strip() else []
    if isinstance(value, list):
        out: list[str] = []
        for v in value[:max_items]:
            if isinstance(v, str) and v.strip():
                out.append(v.strip())
            else:
                s = str(v).strip()
                if s:
                    out.append(s)
        return out
    s = str(value).strip()
    return [s] if s else []


def _normalize_keywords_csv(value: str, *, max_items: int = 24) -> str:
    """
    Normalize a (mostly) comma-separated keyword string into Tracker's v1 CSV format.
    """
    raw = (value or "").strip()
    if not raw:
        return ""
    # Tolerate newlines/semicolons/Chinese commas.
    s = raw.replace("，", ",").replace("；", ",").replace(";", ",").replace("\n", ",")
    parts = [p.strip() for p in s.split(",")]
    out: list[str] = []
    seen: set[str] = set()
    for p in parts:
        if not p:
            continue
        if p in seen:
            continue
        seen.add(p)
        out.append(p)
        if len(out) >= max_items:
            break
    return ",".join(out)


async def llm_propose_topic_setup(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic_name: str,
    brief: str,
    usage_cb: UsageCallback | None = None,
) -> LlmTopicProposal | None:
    """
    AI-assisted topic onboarding: propose topic query keywords + AI policy prompt.

    This is intentionally conservative:
    - It does NOT invent RSS URLs.
    - It only proposes a tighter search query (CSV keywords) + a curation prompt.
    """
    if not settings.llm_base_url:
        return None
    kind = "propose_topic_setup"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.propose_topic_setup.system")

    tn = (topic_name or "").strip()
    br = (brief or "").strip()
    if not br:
        br = tn

    user = _tpl(
        repo,
        settings,
        "llm.propose_topic_setup.user",
        {"topic_name": tn, "brief": _truncate(br, 1200)},
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 1200,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind="propose_topic_setup", model=resp_model, topic=tn, data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    out_name = str(obj.get("topic_name") or tn).strip() or tn
    out_query = _normalize_keywords_csv(str(obj.get("query_keywords") or ""), max_items=24) or tn
    out_alert = _normalize_keywords_csv(str(obj.get("alert_keywords") or ""), max_items=16)
    out_prompt = str(obj.get("ai_prompt") or "").strip()

    # Optional source hints (bounded / conservative).
    hints = None
    raw_hints = obj.get("source_hints")
    if isinstance(raw_hints, dict):
        add_hn = bool(raw_hints.get("add_hn", True))
        add_searxng = bool(raw_hints.get("add_searxng", True))
        add_discourse = bool(raw_hints.get("add_discourse", False))
        add_nodeseek = bool(raw_hints.get("add_nodeseek", False))
        discourse_base_url = _normalize_http_origin(str(raw_hints.get("discourse_base_url") or ""))
        discourse_json_path = str(raw_hints.get("discourse_json_path") or "/latest.json").strip() or "/latest.json"
        if add_discourse and not discourse_base_url:
            add_discourse = False

        hints = LlmTopicSourceHints(
            add_hn=add_hn,
            add_searxng=add_searxng,
            add_discourse=add_discourse,
            discourse_base_url=discourse_base_url,
            discourse_json_path=discourse_json_path,
            add_nodeseek=add_nodeseek,
        )

    if not out_prompt:
        out_prompt = _tpl(
            repo,
            settings,
            "llm.propose_topic_setup.fallback_ai_prompt",
            {"topic_name": out_name, "brief": _truncate(br, 1200)},
        )

    return LlmTopicProposal(
        topic_name=_truncate(out_name, 200),
        query_keywords=_truncate(out_query, 1200),
        alert_keywords=_truncate(out_alert, 400),
        ai_prompt=_truncate(out_prompt, 8000),
        source_hints=hints,
    )


async def llm_propose_profile_setup(
    *,
    repo: Repo | None = None,
    settings: Settings,
    profile_text: str,
    usage_cb: UsageCallback | None = None,
) -> LlmProfileProposal | None:
    """
    AI-assisted profile onboarding: turn arbitrary user text into a strict, AI-native curation prompt.

    Important: this is NOT a keyword matcher. The output prompt is meant to guide the LLM
    to read candidate items and decide ignore|digest|alert based on content quality + user intent.
    """
    if not settings.llm_base_url:
        return None
    kind = "propose_profile_setup"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    is_zh = _output_lang(settings) == "zh"
    system = _tpl(repo, settings, "llm.propose_profile_setup.system")

    raw = (profile_text or "").strip()
    if not raw:
        raise RuntimeError("missing profile_text")

    def _sample_chunks(text: str, *, size: int, limit: int) -> list[str]:
        raw2 = (text or "").strip()
        if not raw2:
            return []
        all_chunks: list[str] = []
        i = 0
        n = len(raw2)
        hard_cap = 5000  # safety bound
        while i < n and len(all_chunks) < hard_cap:
            j = min(n, i + size)
            cut = raw2.rfind("\n", i, j)
            if cut > i + int(size * 0.4):
                j = cut
            all_chunks.append(raw2[i:j].strip())
            i = j
        all_chunks = [c for c in all_chunks if c]
        if len(all_chunks) <= limit:
            return all_chunks
        if limit <= 1:
            return [all_chunks[0]]
        idxs: list[int] = []
        last = len(all_chunks) - 1
        for k in range(limit):
            try:
                idx = int(round(k * last / (limit - 1)))
            except Exception:
                idx = 0
            if idx < 0:
                idx = 0
            if idx > last:
                idx = last
            if not idxs or idxs[-1] != idx:
                idxs.append(idx)
        return [all_chunks[ii] for ii in idxs if 0 <= ii < len(all_chunks)]

    # If the operator pastes a huge profile dump, transform it into a structured brief first,
    # so downstream prompts are not brittle to truncation.
    profile_for_llm = raw
    try:
        chunk_chars = int(getattr(settings, "ai_setup_transform_chunk_chars", 10_000) or 10_000)
    except Exception:
        chunk_chars = 10_000
    try:
        max_chunks = int(getattr(settings, "ai_setup_transform_max_chunks", 20) or 20)
    except Exception:
        max_chunks = 20
    chunk_chars = max(2000, min(50_000, chunk_chars))
    max_chunks = max(1, min(200, max_chunks))

    if len(raw) > chunk_chars:
        try:
            axes: list[str] = []
            axes_seen: set[str] = set()
            keywords: list[str] = []
            keywords_seen: set[str] = set()
            seed_queries: list[str] = []
            seed_queries_seen: set[str] = set()
            understanding = ""

            def _add_many(dst: list[str], seen: set[str], items: object, *, max_items: int) -> None:
                if not isinstance(items, list):
                    return
                for x in items:
                    s = str(x or "").strip()
                    if not s:
                        continue
                    key = " ".join(s.split()).strip().lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    dst.append(s)
                    if len(dst) >= max_items:
                        break

            for ch in _sample_chunks(raw, size=chunk_chars, limit=max_chunks):
                obj = await llm_transform_tracking_ai_setup_input(
                    repo=repo,
                    settings=settings,
                    user_prompt_chunk=ch,
                    usage_cb=usage_cb,
                )
                if not obj or not isinstance(obj, dict):
                    continue
                if not understanding:
                    u0 = str(obj.get("understanding") or "").strip()
                    if u0:
                        understanding = u0
                _add_many(axes, axes_seen, obj.get("interest_axes"), max_items=2000)
                _add_many(keywords, keywords_seen, obj.get("keywords"), max_items=5000)
                _add_many(seed_queries, seed_queries_seen, obj.get("seed_queries"), max_items=2000)

            if understanding or axes or keywords or seed_queries:
                lines: list[str] = []
                lines.append("PROFILE_TEXT (transformed from a large input; raw is stored separately):")
                if understanding:
                    lines.append("")
                    lines.append("UNDERSTANDING:")
                    lines.append(understanding)
                if axes:
                    lines.append("")
                    lines.append("INTEREST_AXES:")
                    for a in axes[:2000]:
                        lines.append(f"- {a}")
                if keywords:
                    lines.append("")
                    lines.append("KEYWORDS:")
                    for k in keywords[:5000]:
                        lines.append(f"- {k}")
                if seed_queries:
                    lines.append("")
                    lines.append("RETRIEVAL_QUERIES:")
                    for q in seed_queries[:2000]:
                        lines.append(f"- {q}")
                profile_for_llm = "\n".join(lines).strip()
        except Exception:
            profile_for_llm = raw

    user = _tpl(repo, settings, "llm.propose_profile_setup.user", {"profile_text": _truncate(profile_for_llm, 80_000)})

    max_tokens = 1800
    try:
        max_tokens = int(getattr(settings, "ai_setup_plan_max_tokens", 1800) or 1800)
    except Exception:
        max_tokens = 1800
    max_tokens = max(1200, min(8000, max_tokens))

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": int(max_tokens),
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind="propose_profile_setup", model=resp_model, topic="profile", data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    def _parse_string_list(value: object, *, max_items: int, max_len: int) -> list[str]:
        parts: list[str] = []
        if isinstance(value, list):
            parts = [str(x or "").strip() for x in value if str(x or "").strip()]
        elif isinstance(value, str):
            raw = value.replace("，", ",").replace("；", ",").replace(";", ",").replace("\n", ",")
            parts = [p.strip() for p in raw.split(",") if p.strip()]
        else:
            parts = []
        out: list[str] = []
        seen: set[str] = set()
        for p in parts:
            s = " ".join(p.split()).strip()
            if not s:
                continue
            s = s[:max_len]
            key = s.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(s)
            if len(out) >= max_items:
                break
        return out

    understanding = str(obj.get("understanding") or "").strip()
    if not understanding:
        understanding = (
            "基于你提供的资料，你希望长期跟踪前沿进展，并以极简高信号方式接收定期汇总与少量警报。"
            if is_zh
            else "From your materials, you want to track cutting-edge progress long-term and receive small, high-signal periodic summaries plus rare alerts."
        )

    interest_axes = _parse_string_list(obj.get("interest_axes"), max_items=120, max_len=220)
    interest_keywords = _parse_string_list(obj.get("interest_keywords"), max_items=600, max_len=100)
    retrieval_queries = _parse_string_list(obj.get("retrieval_queries"), max_items=200, max_len=260)

    prompt = str(obj.get("ai_prompt") or "").strip()
    if not prompt:
        prompt = _tpl(
            repo,
            settings,
            "llm.propose_profile_setup.fallback_ai_prompt",
            {"profile_text": _truncate(raw, 4000)},
        )

    return LlmProfileProposal(
        understanding=_truncate(understanding, 800),
        interest_axes=interest_axes,
        interest_keywords=interest_keywords,
        retrieval_queries=retrieval_queries,
        ai_prompt=_truncate(prompt, 8000),
    )


async def llm_update_profile_delta_from_feedback(
    *,
    repo: Repo | None = None,
    settings: Settings,
    core_prompt: str,
    delta_prompt: str,
    feedback_events: list[dict[str, object]],
    usage_cb: UsageCallback | None = None,
) -> LlmProfileDeltaUpdate | None:
    """
    Update the *delta* part of the Profile prompt from explicit user feedback events.

    Design goal: keep the profile stable and controllable.
    - CORE_PROMPT is treated as immutable.
    - DELTA_PROMPT is small and can be updated often.
    """
    if not settings.llm_base_url:
        return None
    kind = "profile_delta_update"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.profile_delta_update.system")

    core = (core_prompt or "").strip()
    delta = (delta_prompt or "").strip()
    events = feedback_events or []
    if not core or not events:
        return None

    # Keep the input compact and stable.
    safe_events: list[dict[str, object]] = []
    for e in events[:50]:
        if not isinstance(e, dict):
            continue
        safe_events.append(
            {
                "id": int(e.get("id") or 0),
                "kind": str(e.get("kind") or ""),
                "value_int": int(e.get("value_int") or 0),
                "domain": str(e.get("domain") or ""),
                "url": str(e.get("url") or ""),
                "note": str(e.get("note") or ""),
                "text": _truncate(str(e.get("text") or ""), 600),
                "created_at": str(e.get("created_at") or ""),
            }
        )

    user = _tpl(
        repo,
        settings,
        "llm.profile_delta_update.user",
        {
            "core_prompt": _truncate(core, 6000),
            "current_delta_prompt": _truncate(delta, 3000),
            "feedback_events_json": _truncate(json.dumps(safe_events, ensure_ascii=False), 12_000),
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 900,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind="profile_delta_update", model=resp_model, topic="profile", data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    out_delta = str(obj.get("delta_prompt") or "").strip()
    note = str(obj.get("note") or "").strip()
    if not out_delta:
        return None
    return LlmProfileDeltaUpdate(delta_prompt=_truncate(out_delta, 2000), note=_truncate(note, 800))


async def llm_update_prompt_delta_from_feedback(
    *,
    repo: Repo | None = None,
    settings: Settings,
    target_slot_id: str,
    current_delta_prompt: str,
    feedback_events: list[dict[str, object]],
    usage_cb: UsageCallback | None = None,
) -> LlmPromptDeltaUpdate | None:
    """
    Update an operator-controlled "prompt delta" (small, auditable) from explicit feedback events.

    This is used for report-quality/style corrections without rewriting the whole base prompt slot.
    """
    if not settings.llm_base_url:
        return None
    kind = "prompt_delta_update"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.prompt_delta_update.system")

    slot = (target_slot_id or "").strip()
    cur = (current_delta_prompt or "").strip()
    events = feedback_events or []
    if not (slot and events):
        return None

    safe_events: list[dict[str, object]] = []
    for e in events[:50]:
        if not isinstance(e, dict):
            continue
        safe_events.append(
            {
                "id": int(e.get("id") or 0),
                "kind": str(e.get("kind") or ""),
                "domain": str(e.get("domain") or ""),
                "url": str(e.get("url") or ""),
                "note": str(e.get("note") or ""),
                "text": _truncate(str(e.get("text") or ""), 800),
                "created_at": str(e.get("created_at") or ""),
            }
        )

    user = _tpl(
        repo,
        settings,
        "llm.prompt_delta_update.user",
        {
            "target_slot_id": _truncate(slot, 200),
            "current_delta_prompt": _truncate(cur, 3000),
            "feedback_events_json": _truncate(json.dumps(safe_events, ensure_ascii=False), 12_000),
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 900,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind=kind, model=resp_model, topic="prompts", data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    out_delta = str(obj.get("delta_prompt") or "").strip()
    note = str(obj.get("note") or "").strip()
    if not out_delta:
        return None
    return LlmPromptDeltaUpdate(delta_prompt=_truncate(out_delta, 2000), note=_truncate(note, 800))


def _looks_like_comment_feed_url(url: str) -> bool:
    parts = urlsplit(url)
    path = (parts.path or "").lower()
    query = (parts.query or "").lower()
    if "/comments/" in path:
        return True
    if "comments/feed" in path:
        return True
    if "comment" in query and "feed" in query:
        return True
    if "withcomments=1" in query:
        return True
    return False


async def llm_gate_alert_candidate(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic: Topic,
    title: str,
    url: str,
    content_text: str,
    usage_cb: UsageCallback | None = None,
) -> LlmGateResult | None:
    """
    Optional “hybrid” gate to reduce alert spam.

    If LLM is not configured, returns None.
    If configured, returns an (alert|digest) decision plus a short reason.
    """
    if not settings.llm_base_url:
        return None
    kind = "gate_alert"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.gate_alert.system")

    profile_understanding = ""
    try:
        if repo is not None:
            profile_understanding = (repo.get_app_config("profile_understanding") or "").strip()
    except Exception:
        profile_understanding = ""

    user = _tpl(
        repo,
        settings,
        "llm.gate_alert.user",
        {
            "profile_understanding": _truncate(profile_understanding, 1200),
            "topic_name": topic.name,
            "topic_query_keywords": topic.query,
            "topic_alert_keywords": topic.alert_keywords,
            "item_title": _truncate(title, 300),
            "item_url": url,
            "item_snippet": _truncate(content_text, 1200),
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 200,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="gate_alert",
        model=resp_model,
        topic=topic.name,
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    decision = str(obj.get("decision", "digest")).strip().lower()
    if decision not in {"alert", "digest"}:
        decision = "digest"
    reason = str(obj.get("reason", "")).strip()
    return LlmGateResult(decision=decision, reason=reason)


async def llm_summarize_digest(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic: Topic,
    policy_prompt: str = "",
    since: str,
    items: list[dict],
    previous_items: list[dict] | None,
    metrics: dict,
    usage_cb: UsageCallback | None = None,
) -> LlmDigestSummary | None:
    """
    Optional digest summary. Intended to be *bounded* (top-N items) and safe to skip.

    Returns a structured summary that the caller can render into markdown.
    """
    if not settings.llm_base_url:
        return None
    kind = "digest_summary"
    model = _select_model_for_kind(settings, kind=kind)
    if not model:
        return None
    _ensure_non_codex_model(model, allow_codex=True)
    extra_body = _load_llm_extra_body(settings, kind=kind)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    system = _tpl(repo, settings, "llm.digest_summary.system")

    def _fmt_item(d: dict) -> str:
        title = _truncate(str(d.get("title", "")), 220)
        url = str(d.get("url", ""))
        decision = str(d.get("decision", "digest"))
        snippet = _truncate(str(d.get("snippet", "")), 240)
        if snippet:
            return f"[{decision}] {title} — {url}\n   snippet: {snippet}"
        return f"[{decision}] {title} — {url}"

    topic_policy_prompt_block = ""
    if policy_prompt and policy_prompt.strip():
        topic_policy_prompt_block = "\nTOPIC_POLICY_PROMPT:\n" + policy_prompt.strip()

    metrics_lines: list[str] = []
    for k in sorted(metrics.keys()):
        metrics_lines.append(f"- {k}: {metrics[k]}")
    metrics_block = "\n".join(metrics_lines).strip()

    items_lines: list[str] = []
    for i, d in enumerate(items[: max(1, settings.llm_digest_max_items)], start=1):
        items_lines.append(f"{i}. {_fmt_item(d)}")
    items_block = "\n".join(items_lines).strip()

    previous_items_block = ""
    if previous_items:
        prev_lines: list[str] = ["", "PREVIOUS_ITEMS (context; most recent first):"]
        for i, d in enumerate(previous_items[:10], start=1):
            prev_lines.append(f"{i}. {_fmt_item(d)}")
        previous_items_block = "\n".join(prev_lines).rstrip()

    user = _tpl(
        repo,
        settings,
        "llm.digest_summary.user",
        {
            "topic_name": topic.name,
            "topic_query_keywords": topic.query,
            "topic_alert_keywords": topic.alert_keywords,
            "topic_policy_prompt_block": topic_policy_prompt_block,
            "since": since,
            "metrics_block": metrics_block,
            "items_block": items_block,
            "previous_items_block": previous_items_block,
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 350,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="digest_summary",
        model=resp_model,
        topic=topic.name,
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    summary = str(obj.get("summary", "")).strip()
    highlights = _coerce_str_list(obj.get("highlights"), max_items=5)
    risks = _coerce_str_list(obj.get("risks"), max_items=3)
    next_actions = _coerce_str_list(obj.get("next_actions"), max_items=3)
    if not summary:
        raise RuntimeError("LLM JSON missing 'summary'")

    return LlmDigestSummary(
        summary=_truncate(summary, 1200),
        highlights=[_truncate(x, 300) for x in highlights],
        risks=[_truncate(x, 300) for x in risks],
        next_actions=[_truncate(x, 300) for x in next_actions],
    )


async def llm_triage_topic_items(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic: Topic,
    policy_prompt: str,
    candidates: list[dict],
    recent_sent: list[dict] | None = None,
    max_keep: int,
    usage_cb: UsageCallback | None = None,
) -> list[int] | None:
    """
    Cheap pre-filter step (optional).

    Use the configured mini model to drop obvious junk/off-topic/duplicates from a larger candidate pool,
    then keep a bounded set for the main reasoning model to do final ignore|digest|alert decisions.
    """
    kind = "triage_items"
    model = _select_model_for_kind(settings, kind=kind)
    if not (settings.llm_base_url and model):
        return None
    _ensure_non_codex_model(model, allow_codex=True)

    max_keep_i = max(1, int(max_keep or 1))

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    extra_body = _load_llm_extra_body(settings, kind=kind)

    system = _tpl(repo, settings, "llm.triage_items.system")

    profile_understanding = ""
    try:
        if repo is not None:
            profile_understanding = (repo.get_app_config("profile_understanding") or "").strip()
    except Exception:
        profile_understanding = ""

    def _fmt_item(c: dict) -> str:
        try:
            item_id = int(c.get("item_id") or 0)
        except Exception:
            item_id = 0
        title = _truncate(str(c.get("title", "") or ""), 220)
        url = str(c.get("url", "") or "").strip()
        domain = str(c.get("domain", "") or "").strip().lower()
        if not domain and url:
            try:
                host = (urlsplit(url).netloc or "").strip().lower()
                host = host.split(":", 1)[0].lstrip(".")
                if host.startswith("www."):
                    host = host[4:]
                domain = host
            except Exception:
                domain = ""
        try:
            likes = int(c.get("domain_likes") or 0)
        except Exception:
            likes = 0
        try:
            dislikes = int(c.get("domain_dislikes") or 0)
        except Exception:
            dislikes = 0
        raw_snippet = str(c.get("snippet", "") or "")
        snippet = _truncate(raw_snippet, 420)
        links = extract_notable_links(text=raw_snippet, url=url, max_links=3)
        parts = [f"item_id={item_id}", f"title={title}", f"url={url}"]
        if domain:
            parts.append(f"domain={domain}")
        if likes or dislikes:
            parts.append(f"domain_feedback=+{max(0, likes)}/-{max(0, dislikes)}")
        if links:
            parts.append("links=" + ", ".join(links))
        if snippet:
            parts.append("snippet=" + snippet)
        return "\n  ".join(parts)

    topic_policy_prompt_block = ""
    if policy_prompt and policy_prompt.strip():
        topic_policy_prompt_block = "\nTOPIC_POLICY_PROMPT:\n" + policy_prompt.strip()

    recent_sent_block = ""
    recent = recent_sent or []
    if recent:
        rs_lines: list[str] = ["", "RECENT_SENT (avoid repeats unless materially new):"]
        for row in recent[:20]:
            title = _truncate(str(row.get("title", "") or ""), 220)
            url = str(row.get("url", "") or "").strip()
            when = str(row.get("published_at", "") or row.get("created_at", "") or "").strip()
            if not url:
                continue
            if when:
                rs_lines.append(f"- {title} | {url} | {when}")
            else:
                rs_lines.append(f"- {title} | {url}")
        recent_sent_block = "\n".join(rs_lines).rstrip() + "\n"

    cand_lines: list[str] = []
    for i, c in enumerate(candidates, start=1):
        cand_lines.append(f"{i}. {_fmt_item(c)}")
    candidates_block = "\n".join(cand_lines).strip()

    user = _tpl(
        repo,
        settings,
        "llm.triage_items.user",
        {
            "profile_understanding": _truncate(profile_understanding, 1200),
            "topic_name": topic.name,
            "topic_query_keywords": topic.query,
            "topic_alert_keywords": topic.alert_keywords,
            "max_keep": max_keep_i,
            "topic_policy_prompt_block": topic_policy_prompt_block,
            "recent_sent_block": recent_sent_block,
            "candidates_block": candidates_block,
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 600,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(usage_cb, kind="triage_items", model=resp_model, topic=topic.name, data=data)

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")
    keep = obj.get("keep_item_ids")
    if keep is None:
        keep = []
    if not isinstance(keep, list):
        raise RuntimeError("LLM JSON missing 'keep_item_ids' list")

    # Normalize and filter to known candidate ids.
    candidate_ids: list[int] = []
    seen_ids: set[int] = set()
    for c in candidates:
        try:
            cid = int(c.get("item_id"))
        except Exception:
            continue
        if cid <= 0 or cid in seen_ids:
            continue
        seen_ids.add(cid)
        candidate_ids.append(cid)
    candidate_set = set(candidate_ids)

    out: list[int] = []
    seen: set[int] = set()
    for x in keep:
        try:
            item_id = int(x)
        except Exception:
            continue
        if item_id <= 0 or item_id not in candidate_set:
            continue
        if item_id in seen:
            continue
        seen.add(item_id)
        out.append(item_id)
        if len(out) >= max_keep_i:
            break

    return out[:max_keep_i]


async def llm_curate_topic_items(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic: Topic,
    policy_prompt: str,
    candidates: list[dict],
    recent_sent: list[dict] | None = None,
    max_digest: int,
    max_alert: int,
    usage_cb: UsageCallback | None = None,
) -> list[LlmCurationDecision] | None:
    """
    Prompt-driven curation: given candidate items (title/url/snippet), decide ignore|digest|alert.

    - Returns None if LLM isn't configured.
    - Returns one decision per candidate item_id.
    """
    kind = "curate_items"
    model = _select_model_for_kind(settings, kind=kind)
    if not (settings.llm_base_url and model):
        return None
    _ensure_non_codex_model(model)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    extra_body = _load_llm_extra_body(settings, kind=kind)

    system = _tpl(repo, settings, "llm.curate_items.system")

    profile_understanding = ""
    try:
        if repo is not None:
            profile_understanding = (repo.get_app_config("profile_understanding") or "").strip()
    except Exception:
        profile_understanding = ""

    topic_policy_prompt_block = ""
    if policy_prompt and policy_prompt.strip():
        topic_policy_prompt_block = "\nTOPIC_POLICY_PROMPT:\n" + policy_prompt.strip()

    # Anti-repeat context: provide a short list of recently sent items (digest/alert),
    # so the model can ignore "same story" reposts or unchanged updates.
    #
    # Note: this is not a hard filter; the model may still select an item if it is a
    # materially new development. Keep this bounded to limit token usage.
    recent_sent_block = ""
    recent = recent_sent or []
    if recent:
        rs_lines: list[str] = ["", "RECENT_SENT (digest/alert; avoid repeating unless materially new):"]
        for row in recent[:20]:
            title = _truncate(str(row.get("title", "")), 220)
            url = str(row.get("url", "")).strip()
            when = str(row.get("published_at", "") or row.get("created_at", "") or "").strip()
            if when:
                rs_lines.append(f"- {title} | {url} | {when}")
            else:
                rs_lines.append(f"- {title} | {url}")
        recent_sent_block = "\n".join(rs_lines).rstrip() + "\n"

    cand_lines: list[str] = []
    for i, c in enumerate(candidates, start=1):
        item_id = c.get("item_id")
        title = _truncate(str(c.get("title", "")), 220)
        url = str(c.get("url", ""))
        domain = str(c.get("domain", "") or "").strip().lower()
        if not domain and url:
            try:
                host = (urlsplit(url).netloc or "").strip().lower()
                host = host.split(":", 1)[0].lstrip(".")
                if host.startswith("www."):
                    host = host[4:]
                domain = host
            except Exception:
                domain = ""
        try:
            likes = int(c.get("domain_likes") or 0)
        except Exception:
            likes = 0
        try:
            dislikes = int(c.get("domain_dislikes") or 0)
        except Exception:
            dislikes = 0
        raw_snippet = str(c.get("snippet", "") or "")
        links = extract_notable_links(text=raw_snippet, url=url, max_links=4)
        snippet = _truncate(raw_snippet, 1200)
        cand_lines.append(f"{i}. item_id={item_id}")
        cand_lines.append(f"   title={title}")
        cand_lines.append(f"   url={url}")
        if domain:
            cand_lines.append(f"   domain={domain}")
        if likes or dislikes:
            cand_lines.append(f"   domain_feedback=+{max(0, likes)}/-{max(0, dislikes)}")
        if links:
            cand_lines.append(f"   links={', '.join(links)}")
        if snippet:
            cand_lines.append(f"   snippet={snippet}")

    candidates_block = "\n".join(cand_lines).strip()
    user = _tpl(
        repo,
        settings,
        "llm.curate_items.user",
        {
            "profile_understanding": _truncate(profile_understanding, 1200),
            "topic_name": topic.name,
            "topic_query_keywords": topic.query,
            "topic_alert_keywords": topic.alert_keywords,
            "max_digest": max(0, int(max_digest)),
            "max_alert": max(0, int(max_alert)),
            "topic_policy_prompt_block": topic_policy_prompt_block,
            "recent_sent_block": recent_sent_block,
            "candidates_block": candidates_block,
        },
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 1200,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="curate_items",
        model=resp_model,
        topic=topic.name,
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")
    rows = obj.get("decisions")
    if not rows or not isinstance(rows, list):
        raise RuntimeError("LLM JSON missing 'decisions' list")

    # Normalize decisions.
    out_by_id: dict[int, LlmCurationDecision] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            item_id = int(row.get("item_id"))
        except Exception:
            continue
        decision = str(row.get("decision", "")).strip().lower()
        if decision not in {"ignore", "digest", "alert"}:
            decision = "ignore"
        why = _truncate(str(row.get("why", "")).strip(), 600)
        summary = _truncate(str(row.get("summary", "")).strip(), 600)
        if _is_redundant_pair(summary, why):
            why = ""
        out_by_id[item_id] = LlmCurationDecision(item_id=item_id, decision=decision, why=why, summary=summary)

    # Ensure every candidate has a decision (safety): default to ignore.
    ordered_ids: list[int] = []
    for c in candidates:
        try:
            item_id = int(c.get("item_id"))  # type: ignore[arg-type]
        except Exception:
            continue
        ordered_ids.append(item_id)
        if item_id not in out_by_id:
            out_by_id[item_id] = LlmCurationDecision(item_id=item_id, decision="ignore", why="", summary="")

    # Enforce caps regardless of model behavior.
    max_alert_i = max(0, int(max_alert))
    max_digest_i = max(0, int(max_digest))

    # Keep input order for tie-breaking (newer first).
    alert_ids = [item_id for item_id in ordered_ids if out_by_id[item_id].decision == "alert"]
    for item_id in alert_ids[max_alert_i:]:
        cur = out_by_id[item_id]
        out_by_id[item_id] = LlmCurationDecision(item_id=item_id, decision="digest", why=cur.why, summary=cur.summary)

    digest_ids = [item_id for item_id in ordered_ids if out_by_id[item_id].decision == "digest"]
    for item_id in digest_ids[max_digest_i:]:
        cur = out_by_id[item_id]
        out_by_id[item_id] = LlmCurationDecision(item_id=item_id, decision="ignore", why=cur.why, summary=cur.summary)

    # Rebuild in input order.
    return [out_by_id[item_id] for item_id in ordered_ids]


async def llm_guess_feed_urls(
    *,
    repo: Repo | None = None,
    settings: Settings,
    page_url: str,
    html_snippet: str,
    usage_cb: UsageCallback | None = None,
) -> list[str] | None:
    """
    Optional AI-assisted feed discovery.

    If LLM is not configured, returns None.
    If configured, asks the model to propose RSS/Atom feed URLs for a webpage.
    """
    kind = "guess_feed_urls"
    model = _select_model_for_kind(settings, kind=kind)
    if not (settings.llm_base_url and model):
        return None
    _ensure_non_codex_model(model)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    extra_body = _load_llm_extra_body(settings, kind=kind)

    system = _tpl(repo, settings, "llm.guess_feed_urls.system")
    user = _tpl(
        repo,
        settings,
        "llm.guess_feed_urls.user",
        {"page_url": page_url, "html_snippet": _truncate(html_snippet, settings.discover_sources_ai_max_html_chars)},
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 400,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="guess_feed_urls",
        model=resp_model,
        topic="",
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    urls = _coerce_str_list(obj.get("feed_urls"), max_items=max(1, settings.discover_sources_ai_max_feed_urls))
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        resolved = urljoin(page_url, u.strip())
        if not resolved.startswith("http://") and not resolved.startswith("https://"):
            continue
        if _looks_like_comment_feed_url(resolved):
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(resolved)

    return out


async def llm_guess_api_endpoints(
    *,
    repo: Repo | None = None,
    settings: Settings,
    page_url: str,
    html_snippet: str,
    usage_cb: UsageCallback | None = None,
) -> list[str] | None:
    """
    Optional AI-assisted API discovery.

    If LLM is not configured, returns None.
    If configured, asks the model to propose likely public web API endpoints used by a webpage.
    """
    kind = "guess_api_endpoints"
    model = _select_model_for_kind(settings, kind=kind)
    if not (settings.llm_base_url and model):
        return None
    _ensure_non_codex_model(model)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    extra_body = _load_llm_extra_body(settings, kind=kind)

    system = _tpl(repo, settings, "llm.guess_api_endpoints.system")
    user = _tpl(
        repo,
        settings,
        "llm.guess_api_endpoints.user",
        {"page_url": page_url, "html_snippet": _truncate(html_snippet, settings.discover_sources_ai_max_html_chars)},
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 400,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="guess_api_endpoints",
        model=resp_model,
        topic="",
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")

    urls = _coerce_str_list(obj.get("api_endpoints"), max_items=max(1, settings.discover_sources_ai_max_feed_urls))
    out: list[str] = []
    seen: set[str] = set()
    for u in urls:
        resolved = urljoin(page_url, u.strip())
        if not resolved.startswith("http://") and not resolved.startswith("https://"):
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        out.append(resolved)

    return out


@dataclass(frozen=True)
class LlmSourceCandidateDecision:
    candidate_id: int
    decision: str  # accept|ignore|skip
    score: int = 0
    quality_score: int = 0
    relevance_score: int = 0
    novelty_score: int = 0
    why: str = ""
    model: str = ""


async def llm_decide_source_candidates(
    *,
    repo: Repo | None = None,
    settings: Settings,
    topic: Topic,
    policy_prompt: str,
    candidates: list[dict],
    max_accept: int,
    profile: str = "",
    explore_weight: int = 2,
    exploit_weight: int = 8,
    usage_cb: UsageCallback | None = None,
) -> list[LlmSourceCandidateDecision] | None:
    """
    Prompt-driven source curation: decide which RSS candidates to accept for a topic.

    Returns None if LLM isn't configured.
    Returns decisions in input order.
    """
    kind = "curate_sources"
    model = _select_model_for_kind(settings, kind=kind)
    if not (settings.llm_base_url and model):
        return None
    _ensure_non_codex_model(model)

    base_url = _select_llm_base_url_for_kind(settings, kind=kind)
    api_key = _select_llm_api_key_for_kind(settings, kind=kind)
    proxy = _select_llm_proxy_for_kind(settings, kind=kind)

    endpoint = _openai_compat_chat_completions_url(base_url)
    headers: dict[str, str] = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"
    extra_body = _load_llm_extra_body(settings, kind=kind)

    system = _tpl(repo, settings, "llm.curate_sources.system")

    topic_policy_prompt_block = ""
    if policy_prompt and policy_prompt.strip():
        topic_policy_prompt_block = "\nTOPIC_POLICY_PROMPT:\n" + policy_prompt.strip()

    cand_lines: list[str] = []
    for i, c in enumerate(candidates, start=1):
        cid = c.get("candidate_id")
        url = str(c.get("url", ""))
        discovered_from = str(c.get("discovered_from_url", ""))
        titles = c.get("titles") or []
        source_content = str(c.get("source_content", "") or "").strip()
        cand_lines.append(f"{i}. candidate_id={cid}")
        cand_lines.append(f"   url={url}")
        if discovered_from:
            cand_lines.append(f"   discovered_from={discovered_from}")
        if source_content:
            cand_lines.append("   source_content:")
            for ln in (source_content.splitlines() or [])[:80]:
                s = (ln or "").strip()
                if not s:
                    continue
                cand_lines.append(f"   {s[:320]}")
        elif titles and isinstance(titles, list):
            for t in titles[:8]:
                if not t:
                    continue
                cand_lines.append(f"   - {str(t)[:220]}")

    candidates_block = "\n".join(cand_lines).strip()
    user = _tpl(
        repo,
        settings,
        "llm.curate_sources.user",
        {
            "topic_name": topic.name,
            "topic_query_keywords": topic.query,
            "topic_alert_keywords": topic.alert_keywords,
            "max_accept": max(0, int(max_accept)),
            "explore_weight": max(0, min(10, int(explore_weight or 0))),
            "exploit_weight": max(0, min(10, int(exploit_weight or 0))),
            "profile": _truncate(str(profile or "").strip(), 6000),
            "topic_policy_prompt_block": topic_policy_prompt_block,
            "candidates_block": candidates_block,
        },
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": 900,
    }
    payload.update(extra_body)

    async with httpx.AsyncClient(timeout=settings.llm_timeout_seconds, proxy=proxy) as client:
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
    resp_model = str((data.get("model") if isinstance(data, dict) else None) or model or "")
    _emit_usage(
        usage_cb,
        kind="curate_sources",
        model=resp_model,
        topic=topic.name,
        data=data,
    )

    choices = data.get("choices") if isinstance(data, dict) else None
    if not choices or not isinstance(choices, list):
        raise RuntimeError("LLM response missing choices")
    msg = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = msg.get("content") if isinstance(msg, dict) else None
    if not content or not isinstance(content, str):
        raise RuntimeError("LLM response missing message.content")

    obj = _extract_first_json_object(content)
    if not obj or not isinstance(obj, dict):
        raise RuntimeError("LLM did not return valid JSON object")
    rows = obj.get("decisions")
    if not rows or not isinstance(rows, list):
        raise RuntimeError("LLM JSON missing 'decisions' list")

    out_by_id: dict[int, LlmSourceCandidateDecision] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        try:
            cid = int(row.get("candidate_id"))
        except Exception:
            continue
        decision = str(row.get("decision", "")).strip().lower()
        if decision not in {"accept", "ignore", "skip"}:
            decision = "skip"
        def _clamp01(x: object) -> int:
            try:
                v = int(x)  # type: ignore[arg-type]
            except Exception:
                v = 0
            return max(0, min(100, v))

        score = _clamp01(row.get("score"))
        q = _clamp01(row.get("quality_score"))
        r = _clamp01(row.get("relevance_score"))
        n = _clamp01(row.get("novelty_score"))
        why = _truncate(str(row.get("why", "")).strip(), 600)
        out_by_id[cid] = LlmSourceCandidateDecision(
            candidate_id=cid,
            decision=decision,
            score=score,
            quality_score=q,
            relevance_score=r,
            novelty_score=n,
            why=why,
            model=resp_model,
        )

    ordered_ids: list[int] = []
    for c in candidates:
        try:
            cid = int(c.get("candidate_id"))  # type: ignore[arg-type]
        except Exception:
            continue
        ordered_ids.append(cid)
        if cid not in out_by_id:
            out_by_id[cid] = LlmSourceCandidateDecision(candidate_id=cid, decision="skip", why="", model=resp_model)

    # Enforce accept cap regardless of model behavior.
    max_accept_i = max(0, int(max_accept))
    accept_ids = [cid for cid in ordered_ids if out_by_id[cid].decision == "accept"]
    for cid in accept_ids[max_accept_i:]:
        cur = out_by_id[cid]
        out_by_id[cid] = LlmSourceCandidateDecision(
            candidate_id=cid,
            decision="skip",
            score=cur.score,
            quality_score=cur.quality_score,
            relevance_score=cur.relevance_score,
            novelty_score=cur.novelty_score,
            why=cur.why,
            model=cur.model,
        )

    return [out_by_id[cid] for cid in ordered_ids]
