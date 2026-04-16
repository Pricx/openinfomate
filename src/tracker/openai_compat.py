from __future__ import annotations

import asyncio
import json
import logging
import re
from typing import Any, Literal

import httpx

from tracker.repo import Repo

logger = logging.getLogger(__name__)

OpenAiCompatMode = Literal["chat_completions", "responses"]
OpenAiCompatModePreference = Literal["auto", "chat_completions", "responses"]

_MODE_CACHE_KEY = "llm_api_mode_cache_json"
_TRANSIENT_HTTP_STATUSES = {408, 429, 500, 502, 503, 504}
_ALTERNATE_MODE_HTTP_STATUSES = {400, 401, 403, 404, 405, *_TRANSIENT_HTTP_STATUSES}


def normalize_openai_compat_base_url(base_url: str) -> str:
    """
    Normalize an OpenAI-compatible base URL.

    Examples:
    - https://example.com/v1 -> https://example.com
    - https://example.com/v1/ -> https://example.com
    - https://example.com -> https://example.com
    """
    b = (base_url or "").strip().rstrip("/")
    if b.endswith("/v1"):
        b = b[: -len("/v1")]
    return b.rstrip("/")


def openai_compat_chat_completions_url(base_url: str) -> str:
    base = normalize_openai_compat_base_url(base_url)
    return f"{base}/v1/chat/completions"


def openai_compat_responses_url(base_url: str) -> str:
    base = normalize_openai_compat_base_url(base_url)
    return f"{base}/v1/responses"


# Keep this heuristic permissive: many OpenAI-compatible providers return an error
# message like "Please use /v1/responses" when they drop `/v1/chat/completions`.
_RESPONSES_HINT_RE = re.compile(r"(/v1/responses\b|\bresponses\b)", re.IGNORECASE)


def _looks_like_responses_required(status_code: int, body: str) -> bool:
    # Some OpenAI-compatible providers return 400, others may return 404/405 while still
    # indicating that `/v1/responses` should be used.
    if status_code not in {400, 404, 405}:
        return False
    raw = (body or "").strip()
    if not raw:
        return False
    # Some providers escape slashes inside JSON strings.
    norm = raw.replace("\\/", "/")
    if _RESPONSES_HINT_RE.search(norm):
        return True
    # Best-effort: check common JSON error fields.
    try:
        obj = json.loads(norm)
    except Exception:
        obj = None
    if isinstance(obj, dict):
        msg = ""
        try:
            if isinstance(obj.get("error"), dict):
                msg = str(obj["error"].get("message") or "")
            if not msg:
                msg = str(obj.get("message") or obj.get("detail") or "")
        except Exception:
            msg = ""
        if msg and _RESPONSES_HINT_RE.search(msg.replace("\\/", "/")):
            return True
    return False


def _load_mode_cache(repo: Repo) -> dict[str, str]:
    try:
        raw = (repo.get_app_config(_MODE_CACHE_KEY) or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    if not isinstance(obj, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in obj.items():
        kk = str(k or "").strip()
        vv = str(v or "").strip()
        if not kk or vv not in {"chat_completions", "responses"}:
            continue
        out[kk] = vv
    return out


def _save_mode_cache(repo: Repo, cache: dict[str, str]) -> None:
    # Best-effort: never hard-fail background jobs.
    try:
        repo.set_app_config(_MODE_CACHE_KEY, json.dumps(cache, ensure_ascii=False, sort_keys=True))
    except Exception:
        logger.debug("failed to persist llm mode cache", exc_info=True)


def get_cached_mode(repo: Repo | None, *, base_url: str) -> OpenAiCompatMode | None:
    if repo is None:
        return None
    b = normalize_openai_compat_base_url(base_url)
    if not b:
        return None
    cache = _load_mode_cache(repo)
    v = cache.get(b)
    if v in {"chat_completions", "responses"}:
        return v  # type: ignore[return-value]
    return None


def set_cached_mode(repo: Repo | None, *, base_url: str, mode: OpenAiCompatMode) -> None:
    if repo is None:
        return
    b = normalize_openai_compat_base_url(base_url)
    if not b:
        return
    cache = _load_mode_cache(repo)
    cache[b] = mode
    _save_mode_cache(repo, cache)


def normalize_openai_compat_mode_preference(value: object) -> OpenAiCompatModePreference:
    raw = str(value or "").strip().lower()
    if raw == "responses":
        return "responses"
    if raw in {"chat_completions", "chat-completions", "chat"}:
        return "chat_completions"
    return "auto"


def chat_payload_to_responses_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """
    Convert an OpenAI Chat Completions payload into a best-effort Responses payload.

    We keep it conservative and mostly-compatible with OpenAI's `/v1/responses`:
    - `messages` -> `input` as role+input_text parts
    - `max_tokens` -> `max_output_tokens`
    """
    model = str(payload.get("model") or "").strip()
    messages = payload.get("messages")

    inp: list[dict[str, Any]] = []
    if isinstance(messages, list):
        for m in messages:
            if not isinstance(m, dict):
                continue
            role = str(m.get("role") or "").strip() or "user"
            content = m.get("content")
            if isinstance(content, str):
                text = content
            else:
                text = str(content or "")
            inp.append(
                {
                    "role": role,
                    "content": [{"type": "input_text", "text": text}],
                }
            )

    out: dict[str, Any] = {"model": model, "input": inp}

    if "temperature" in payload:
        out["temperature"] = payload.get("temperature")
    if "max_tokens" in payload:
        out["max_output_tokens"] = payload.get("max_tokens")

    # Carry over a few common knobs if present (best-effort).
    for k in ("top_p", "presence_penalty", "frequency_penalty", "reasoning", "metadata"):
        if k in payload:
            out[k] = payload.get(k)

    return out


def extract_text_from_openai_compat_response(data: object) -> str:
    """
    Extract text from either Chat Completions or Responses payload.
    """
    if not isinstance(data, dict):
        return ""

    # Responses API convenience field (some providers).
    v = data.get("output_text")
    if isinstance(v, str) and v.strip():
        return v.strip()

    # Responses API: output[...].content[...].text
    out = data.get("output")
    if isinstance(out, list):
        for item in out:
            if not isinstance(item, dict):
                continue
            content = item.get("content")
            if not isinstance(content, list):
                continue
            for c in content:
                if isinstance(c, dict):
                    txt = c.get("text")
                    if isinstance(txt, str) and txt.strip():
                        return txt.strip()

    # Chat Completions: choices[0].message.content
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        ch0 = choices[0] if isinstance(choices[0], dict) else None
        msg = ch0.get("message") if isinstance(ch0, dict) else None
        content = msg.get("content") if isinstance(msg, dict) else None
        if isinstance(content, str):
            return content.strip()

    return ""


def extract_usage_tokens(data: object) -> tuple[int, int, int]:
    """
    Extract usage tokens from OpenAI-compatible responses.

    Returns (prompt_tokens, completion_tokens, total_tokens).
    """
    if not isinstance(data, dict):
        return 0, 0, 0
    usage = data.get("usage")
    if not isinstance(usage, dict):
        return 0, 0, 0

    # Chat Completions style.
    pt = usage.get("prompt_tokens")
    ct = usage.get("completion_tokens")
    tt = usage.get("total_tokens")

    # Responses style.
    if pt is None:
        pt = usage.get("input_tokens")
    if ct is None:
        ct = usage.get("output_tokens")
    if tt is None:
        tt = usage.get("total_tokens") or usage.get("input_tokens") or 0

    def _i(x: object) -> int:
        try:
            return int(x or 0)
        except Exception:
            return 0

    prompt_tokens = max(0, _i(pt))
    completion_tokens = max(0, _i(ct))
    total_tokens = max(0, _i(tt))
    if total_tokens <= 0 and (prompt_tokens > 0 or completion_tokens > 0):
        total_tokens = prompt_tokens + completion_tokens
    return prompt_tokens, completion_tokens, total_tokens


async def post_openai_compat_json(
    *,
    repo: Repo | None,
    client: httpx.AsyncClient,
    base_url: str,
    headers: dict[str, str],
    payload_chat: dict[str, Any],
    preferred_mode: OpenAiCompatModePreference | None = None,
) -> tuple[dict[str, Any], OpenAiCompatMode]:
    """
    Post a request using Chat Completions, with auto-fallback to Responses if required.

    Returns (data, mode_used).
    """
    preferred_mode_normalized = normalize_openai_compat_mode_preference(preferred_mode)
    preferred: OpenAiCompatMode = (
        preferred_mode_normalized
        if preferred_mode_normalized in {"chat_completions", "responses"}
        else (get_cached_mode(repo, base_url=base_url) or "chat_completions")
    )

    async def _post(mode: OpenAiCompatMode) -> dict[str, Any]:
        if mode == "responses":
            endpoint = openai_compat_responses_url(base_url)
            payload = chat_payload_to_responses_payload(payload_chat)
        else:
            endpoint = openai_compat_chat_completions_url(base_url)
            payload = payload_chat
        resp = await client.post(endpoint, headers=headers, json=payload)
        resp.raise_for_status()
        obj = resp.json()
        return obj if isinstance(obj, dict) else {"raw": obj}

    async def _post_with_retry(mode: OpenAiCompatMode) -> dict[str, Any]:
        attempts = 2
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return await _post(mode)
            except httpx.HTTPStatusError as exc:
                last_error = exc
                status = int(exc.response.status_code or 0) if exc.response is not None else 0
                if status not in _TRANSIENT_HTTP_STATUSES or attempt + 1 >= attempts:
                    raise
            except httpx.RequestError as exc:
                last_error = exc
                if attempt + 1 >= attempts:
                    raise
            if attempt + 1 < attempts:
                await asyncio.sleep(0.25)
        if last_error is not None:
            raise last_error
        raise RuntimeError("openai-compat request failed without an error")

    try:
        data = await _post_with_retry(preferred)
        set_cached_mode(repo, base_url=base_url, mode=preferred)
        return data, preferred
    except httpx.HTTPStatusError as exc:
        body = ""
        try:
            body = (exc.response.text or "").strip()
        except Exception:
            body = ""
        status = int(exc.response.status_code or 0) if exc.response is not None else 0

        # Main compatibility goal:
        # - Respect explicit operator mode preference when provided.
        # - Otherwise keep using the cached/default path, but try the alternate endpoint when
        #   the provider likely rejects only the current transport.
        alternate_mode: OpenAiCompatMode = "responses" if preferred == "chat_completions" else "chat_completions"
        should_try_alternate = False
        if preferred == "chat_completions":
            if _looks_like_responses_required(status, body):
                should_try_alternate = True
            elif status in _ALTERNATE_MODE_HTTP_STATUSES:
                # Some providers/proxies reject only one transport path. This includes generic
                # 4xx transport mismatches (even when surfaced as 401/403) and transient errors.
                should_try_alternate = True
        elif status in _ALTERNATE_MODE_HTTP_STATUSES:
            should_try_alternate = True

        if should_try_alternate:
            try:
                data2 = await _post_with_retry(alternate_mode)
            except httpx.HTTPStatusError:
                raise
            set_cached_mode(repo, base_url=base_url, mode=alternate_mode)
            return data2, alternate_mode
        raise
