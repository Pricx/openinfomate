from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import re
import secrets
from typing import Any
from urllib.parse import urlsplit

import httpx

from tracker.i18n import t as ui_t
from tracker.repo import Repo
from tracker.settings import Settings


logger = logging.getLogger(__name__)


_TG_HTTP_CLIENT: httpx.AsyncClient | None = None
_TG_HTTP_CLIENT_LOCK = asyncio.Lock()


async def _tg_http_client() -> httpx.AsyncClient:
    """
    Shared Telegram HTTP client (connection reuse).

    Why:
    - Inline button UX is extremely sensitive to round-trip time.
    - Re-creating a new client per request pays extra TLS handshake + TCP setup cost.
    """
    global _TG_HTTP_CLIENT
    if _TG_HTTP_CLIENT and not _TG_HTTP_CLIENT.is_closed:
        return _TG_HTTP_CLIENT
    async with _TG_HTTP_CLIENT_LOCK:
        if _TG_HTTP_CLIENT and not _TG_HTTP_CLIENT.is_closed:
            return _TG_HTTP_CLIENT
        limits = httpx.Limits(max_connections=40, max_keepalive_connections=20)
        _TG_HTTP_CLIENT = httpx.AsyncClient(follow_redirects=True, limits=limits)
        return _TG_HTTP_CLIENT


_REACTION_LIKE = {"👍", "❤️", "🔥", "⭐", "🌟"}
_REACTION_DISLIKE = {"👎", "💩", "😡", "🤮", "❌"}
_REACTION_MUTE = {"🔕"}

_CMD_ITEM_ID_RE = re.compile(r"(?:#|item[_ ]?id[:= ]|item[:= ]|id[:= ])(\d{1,12})", re.IGNORECASE)
_URL_RE = re.compile(r"https?://[^\s<>\")\]]+", re.IGNORECASE)

_TG_AUTH_ALLOWED_KEYS: set[str] = {
    "TRACKER_DISCOURSE_COOKIE",
    "TRACKER_COOKIE_JAR_JSON",
}


def _parse_emojis_csv(raw: str, *, fallback: set[str]) -> set[str]:
    """
    Parse a comma/space-separated emoji list into a set.

    Keep it permissive: operators may paste with commas, spaces, or newlines.
    """
    s = (raw or "").strip()
    if not s:
        return set(fallback)
    out: set[str] = set()
    for part in re.split(r"[\s,]+", s):
        p = (part or "").strip()
        if p:
            out.add(p)
    return out or set(fallback)


def _telegram_welcome_text() -> str:
    return (
        "OpenInfoMate 已连接到这个聊天。\n\n"
        "后续我会在这里推送：\n"
        "- 快速消息（单条、重大更新）\n"
        "- 参考消息（批次、去重、不解读）\n"
        "如需解绑：打开 /setup/push 或 /admin → Telegram → Disconnect。"
    ).strip()


def _telegram_status_text(*, repo: Repo, settings: Settings) -> str:
    """
    Minimal interactive response for private bots.

    Tracker is primarily a push-only bot, but a short `/status` reply prevents
    confusion when operators expect an acknowledgment after clicking /start.
    """
    chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
    curated_enabled = bool(getattr(settings, "digest_scheduler_enabled", False))
    curated_push = bool(getattr(settings, "digest_push_enabled", False))
    priority_enabled = bool(getattr(settings, "priority_lane_enabled", False))
    curated_hint = "已启用" if (curated_enabled and curated_push) else ("仅归档" if curated_enabled else "未启用")
    return (
        "OpenInfoMate ✅\n"
        f"- connected: {bool(chat_id)}\n"
        f"- chat_id: {chat_id or '-'}\n"
        f"- curated_info: {curated_hint}\n"
        f"- quick_messages: {'已启用' if priority_enabled else '未启用'}\n"
        "Commands: /status /setup /why /t /s /bindings /config /llm /prompts /profile /push /auth /api /env"
    ).strip()


async def _telegram_send_welcome(*, settings: Settings, chat_id: str) -> None:
    token = (settings.telegram_bot_token or "").strip()
    cid = (chat_id or "").strip()
    if not (token and cid):
        return
    # Reuse the shared pusher so behavior (timeouts/splitting) is consistent with push delivery.
    from tracker.push.telegram import TelegramPusher

    p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))
    await p.send_text(chat_id=cid, text=_telegram_welcome_text(), disable_preview=True)


def telegram_bot_username(*, repo: Repo, settings: Settings) -> str:
    raw = (repo.get_app_config("telegram_bot_username") or settings.telegram_bot_username or "OpenInfoMateBot").strip()
    raw = raw.lstrip("@").strip()
    return raw or "OpenInfoMateBot"


def telegram_extract_start_payload(text: str) -> str | None:
    s = (text or "").strip()
    if not s.startswith("/start"):
        return None
    parts = s.split(maxsplit=1)
    if len(parts) < 2:
        return ""
    return parts[1].strip()


async def telegram_get_updates(
    *,
    bot_token: str,
    offset: int | None,
    timeout_seconds: int,
    client_timeout_seconds: int,
) -> list[dict[str, Any]]:
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    params: dict[str, str | int] = {
        "timeout": int(timeout_seconds),
        "limit": 50,
        "allowed_updates": '["message","message_reaction","callback_query"]',
    }
    if offset is not None:
        params["offset"] = int(offset)

    client = await _tg_http_client()
    resp = await client.get(url, params=params, timeout=client_timeout_seconds)
    resp.raise_for_status()
    data = resp.json()

    if not isinstance(data, dict) or not data.get("ok"):
        desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
        raise RuntimeError(str(desc))

    res = data.get("result")
    return list(res) if isinstance(res, list) else []


async def telegram_answer_callback_query(
    *,
    bot_token: str,
    callback_query_id: str,
    text: str = "",
    show_alert: bool = False,
    client_timeout_seconds: int,
) -> None:
    """
    Best-effort: acknowledge an inline button click (callback_query) to stop the loading spinner.
    """
    token = (bot_token or "").strip()
    qid = (callback_query_id or "").strip()
    if not (token and qid):
        return
    url = f"https://api.telegram.org/bot{token}/answerCallbackQuery"
    payload: dict[str, object] = {"callback_query_id": qid, "show_alert": bool(show_alert)}
    if (text or "").strip():
        payload["text"] = (text or "").strip()[:180]
    client = await _tg_http_client()
    resp = await client.post(url, json=payload, timeout=client_timeout_seconds)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("ok"):
        desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
        raise RuntimeError(str(desc))


async def telegram_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:
    """
    Ensure getUpdates polling works even if the bot previously had a webhook set.

    Telegram returns a conflict error for getUpdates when a webhook is active.
    `deleteWebhook` is idempotent and safe to call in connect flows.
    """
    url = f"https://api.telegram.org/bot{bot_token}/deleteWebhook"
    client = await _tg_http_client()
    resp = await client.post(url, timeout=client_timeout_seconds)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict) or not data.get("ok"):
        desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
        raise RuntimeError(str(desc))


def telegram_status(*, repo: Repo, settings: Settings) -> dict[str, Any]:
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass
    chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
    code = (repo.get_app_config("telegram_setup_code") or "").strip()
    bot_username = telegram_bot_username(repo=repo, settings=settings)
    return {
        "bot_username": bot_username,
        "has_bot_token": bool(settings.telegram_bot_token),
        "connected": bool(chat_id),
        "chat_id": chat_id or None,
        "pending_code": code or None,
    }


def telegram_link(
    *,
    repo: Repo,
    settings: Settings,
    bot_username_override: str | None = None,
    force_rebind: bool = False,
) -> dict[str, Any]:
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass
    existing = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
    if existing:
        if not force_rebind:
            raise RuntimeError("Telegram is already connected. Disconnect first.")
        telegram_disconnect(repo=repo, settings=settings)

    code = secrets.token_urlsafe(12).replace("-", "").replace("_", "")
    repo.set_app_config("telegram_setup_code", code)
    # Reset connect state so the next poll can consume the operator's /start message.
    repo.delete_app_config("telegram_update_offset")
    repo.delete_app_config("telegram_connected_notified")

    bot_username = (bot_username_override or "").strip()
    if bot_username:
        repo.set_app_config("telegram_bot_username", bot_username)
    bot_username = telegram_bot_username(repo=repo, settings=settings)
    link = f"https://t.me/{bot_username}?start={code}"
    return {"code": code, "link": link, "bot_username": bot_username}


async def telegram_poll(*, repo: Repo, settings: Settings, code: str | None = None) -> dict[str, Any]:
    # Apply runtime-effective settings so `.env` edits take effect without restart.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass

    token = (settings.telegram_bot_token or "").strip()
    if not token:
        raise RuntimeError("TRACKER_TELEGRAM_BOT_TOKEN is not configured")

    reactions_enabled = bool(getattr(settings, "telegram_feedback_reactions_enabled", True))
    replies_enabled = bool(getattr(settings, "telegram_feedback_replies_enabled", True))
    like_emojis = _parse_emojis_csv(getattr(settings, "telegram_feedback_like_emojis", ""), fallback=_REACTION_LIKE)
    dislike_emojis = _parse_emojis_csv(
        getattr(settings, "telegram_feedback_dislike_emojis", ""), fallback=_REACTION_DISLIKE
    )
    mute_emojis = _parse_emojis_csv(getattr(settings, "telegram_feedback_mute_emojis", ""), fallback=_REACTION_MUTE)

    existing_chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
    owner_user_id = (
        (repo.get_app_config("telegram_owner_user_id") or settings.telegram_owner_user_id or "").strip()
    )
    if existing_chat_id:
        # If this is a private chat, chat_id == user_id. Use it as a best-effort owner lock
        # so the bot behaves "private" without requiring manual configuration.
        if not owner_user_id:
            try:
                cid_i = int(existing_chat_id)
            except Exception:
                cid_i = 0
            if cid_i > 0:
                owner_user_id = str(cid_i)
                repo.set_app_config("telegram_owner_user_id", owner_user_id)

        # One-time welcome (best-effort).
        notified = (repo.get_app_config("telegram_connected_notified") or "").strip()
        if notified != "1":
            try:
                await _telegram_send_welcome(settings=settings, chat_id=existing_chat_id)
                repo.set_app_config("telegram_connected_notified", "1")
            except Exception:
                # Never block connect status on a welcome message failure.
                pass

        # Also poll for simple operator commands like `/status` to provide a visible ack.
        raw_off = (repo.get_app_config("telegram_update_offset") or "").strip()
        try:
            offset = int(raw_off) if raw_off else None
        except Exception:
            offset = None

        poll_timeout = int(getattr(settings, "telegram_connect_poll_seconds", 0) or 0)
        poll_timeout = max(0, min(25, poll_timeout))

        # Clear webhook only when needed (avoid adding an extra Telegram RTT per poll).
        try:
            cleared = (repo.get_app_config("telegram_webhook_cleared") or "").strip()
            if cleared != "1":
                await telegram_delete_webhook(bot_token=token, client_timeout_seconds=settings.http_timeout_seconds)
                repo.set_app_config("telegram_webhook_cleared", "1")
        except Exception:
            pass

        try:
            updates = await telegram_get_updates(
                bot_token=token,
                offset=offset,
                timeout_seconds=poll_timeout,
                client_timeout_seconds=settings.http_timeout_seconds,
            )
            try:
                repo.set_app_config("telegram_last_polled_at_utc", dt.datetime.utcnow().isoformat() + "Z")
            except Exception:
                pass
        except RuntimeError as exc:
            # If a webhook was configured, getUpdates returns a conflict error.
            msg = str(exc or "")
            if "webhook" in msg.lower():
                try:
                    await telegram_delete_webhook(bot_token=token, client_timeout_seconds=settings.http_timeout_seconds)
                    repo.set_app_config("telegram_webhook_cleared", "1")
                except Exception:
                    pass
                updates = await telegram_get_updates(
                    bot_token=token,
                    offset=offset,
                    timeout_seconds=poll_timeout,
                    client_timeout_seconds=settings.http_timeout_seconds,
                )
            else:
                raise

        def _out_lang() -> str:
            raw = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
            low = raw.lower()
            if raw in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语"}:
                return "zh"
            if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
                return "zh"
            return "en"

        def _default_mute_days() -> int:
            raw = (repo.get_app_config("telegram_feedback_mute_days_default") or "").strip()
            try:
                n = int(raw) if raw else 7
            except Exception:
                n = 7
            return max(1, min(365, n))

        def _domain_from_url(url: str) -> str:
            try:
                host = (urlsplit((url or "").strip()).netloc or "").strip().lower()
                host = host.split(":", 1)[0].lstrip(".")
                if host.startswith("www."):
                    host = host[4:]
                return host
            except Exception:
                return ""

        def _apply_source_score_feedback(*, item_id: int | None, feedback_event_id: int | None, kind: str) -> None:
            """
            Best-effort source score adjustment from TG feedback.

            Notes:
            - Does NOT mark feedback events as applied (profile delta can still consume them).
            - Respects operator locks (SourceScore.locked).
            - Uses a small deterministic delta to keep behavior predictable.
            """
            try:
                iid = int(item_id or 0)
            except Exception:
                iid = 0
            try:
                fid = int(feedback_event_id or 0)
            except Exception:
                fid = 0
            if iid <= 0 or fid <= 0:
                return
            item = None
            try:
                item = repo.get_item_by_id(iid)
            except Exception:
                item = None
            if not item:
                return
            try:
                sid = int(getattr(item, "source_id", 0) or 0)
            except Exception:
                sid = 0
            if sid <= 0:
                return

            marker = f"fb_id={fid}"
            cur = None
            try:
                cur = repo.get_source_score(source_id=sid)
            except Exception:
                cur = None
            prev_note = str(getattr(cur, "note", "") or "") if cur else ""
            if marker in prev_note:
                return

            base = 50
            try:
                if cur:
                    base = int(getattr(cur, "score", 50) or 50)
            except Exception:
                base = 50
            base = max(0, min(100, int(base)))

            k = (kind or "").strip().lower()
            delta = 0
            if k == "like":
                delta = 5
            elif k == "dislike":
                delta = -12
            elif k == "mute":
                delta = -40
            new_score = max(0, min(100, int(base + delta)))

            note_line = f"[feedback] {marker} kind={k} delta={delta} new_score={new_score}"
            merged = (prev_note.strip() + ("\n" if prev_note.strip() else "") + note_line).strip()
            try:
                repo.upsert_source_score(
                    source_id=sid,
                    score=new_score,
                    origin="feedback",
                    note=merged[:4000],
                    force=False,
                )
            except Exception:
                pass

        def _extract_first_url(text: str) -> str:
            # Very small heuristic: look for the first http(s):// token.
            for tok in (text or "").split():
                if tok.startswith("http://") or tok.startswith("https://"):
                    return tok.strip()
            return ""

        def _parse_reply_item_selector(text: str) -> tuple[str | None, int | None, str]:
            """
            Parse a reply prefix like `👎2 ...`, `👍 3 ...`, `#2 ...`, `第2条 ...`, or bare `2`.

            Returns: (action_kind, ref_index, remainder_text)
              - action_kind: like|dislike|None
              - ref_index: 1-based index into a report's References list
            """
            import re

            s = (text or "").strip()
            if not s:
                return (None, None, "")

            m = re.match(r"^(👍|👎)\s*(\d{1,3})(?:\s*[:：\\-])?\s*(.*)$", s)
            if m:
                emoji = (m.group(1) or "").strip()
                try:
                    idx = int(m.group(2))
                except Exception:
                    idx = 0
                rest = (m.group(3) or "").strip()
                if idx > 0:
                    return ("like" if emoji == "👍" else "dislike", idx, rest)

            m = re.match(r"^#\s*(\d{1,3})(?:\s*[:：\\-])?\s*(.*)$", s)
            if m:
                try:
                    idx = int(m.group(1))
                except Exception:
                    idx = 0
                rest = (m.group(2) or "").strip()
                if idx > 0:
                    return (None, idx, rest)

            m = re.match(r"^第\s*(\d{1,3})\s*(?:条|项|条目)\s*(.*)$", s)
            if m:
                try:
                    idx = int(m.group(1))
                except Exception:
                    idx = 0
                rest = (m.group(2) or "").strip()
                if idx > 0:
                    return (None, idx, rest)

            # Bare number (replying "2" to a Curated Info reader should mean "item #2", not a rating).
            if s.isdigit():
                try:
                    idx = int(s)
                except Exception:
                    idx = 0
                if idx > 0:
                    return (None, idx, "")

            return (None, None, s)

        def _report_kind_from_message_key_or_kind(*, msg_kind: str, idempotency_key: str) -> str | None:
            k = (msg_kind or "").strip()
            key = (idempotency_key or "").strip()
            if k == "digest" or key.startswith("digest:"):
                return "digest"
            return None

        def _resolve_reference_anchor(
            *,
            report_kind: str,
            report_key: str,
            ref_index: int,
            message_created_at: dt.datetime | None = None,
        ) -> tuple[int | None, str, str, str]:
            """
            Resolve a `References:` entry `#n` to (item_id, url, domain, title).

            For digest reports, `item_id` can be resolved if the URL matches an Item.
            """
            try:
                idx = int(ref_index or 0)
            except Exception:
                idx = 0
            if idx <= 0:
                return (None, "", "", "")

            rep = None
            try:
                rep = repo.get_report_by_key(kind=report_kind, idempotency_key=report_key)
            except Exception:
                rep = None
            if not rep:
                # Best-effort: if Telegram collapsed the key, look up the report that was current
                # at the time the message was sent (avoid jumping to a newer batch).
                try:
                    msg_at = message_created_at
                    for r, _t in repo.list_reports(kind=report_kind, limit=200):
                        k2 = (getattr(r, "idempotency_key", "") or "").strip()
                        if k2 == report_key or k2.startswith(report_key + ":"):
                            if msg_at and isinstance(getattr(r, "created_at", None), dt.datetime):
                                if r.created_at > msg_at:
                                    continue
                            rep = r
                            break
                except Exception:
                    rep = None

            md = (getattr(rep, "markdown", "") or "").strip() if rep else ""
            if not md:
                return (None, "", "", "")

            try:
                from tracker.telegram_report_reader import parse_reference_entries, parse_report_markdown

                doc = parse_report_markdown(md)
                refs = parse_reference_entries(doc.references)
            except Exception:
                refs = []

            title = ""
            url = ""
            for n, t, u in refs:
                if int(n) == int(idx):
                    title = (t or "").strip()
                    url = (u or "").strip()
                    break
            if not url:
                return (None, "", "", "")

            domain = _domain_from_url(url)
            item_id: int | None = None
            if report_kind == "digest":
                try:
                    item = repo.get_item_by_canonical_url(url)
                    iid = int(getattr(item, "id", 0) or 0) if item else 0
                    if iid > 0:
                        item_id = iid
                except Exception:
                    item_id = None
            return (item_id, url, domain, title)

        def _item_from_id(item_id: int) -> tuple[str, str]:
            """
            Return (url, domain) for an item id; empty strings if not found.
            """
            try:
                from tracker.models import Item

                item = repo.session.get(Item, int(item_id))
                if not item:
                    return ("", "")
                url = (getattr(item, "canonical_url", "") or "").strip() or (getattr(item, "url", "") or "").strip()
                return (url, _domain_from_url(url))
            except Exception:
                return ("", "")

        async def _send_ack(text: str) -> None:
            msg = (text or "").strip()
            if not msg:
                return
            try:
                from tracker.push.telegram import TelegramPusher

                p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))
                await p.send_text(chat_id=existing_chat_id, text=msg, disable_preview=True)
            except Exception:
                return

        async def _send_with_markup(*, text: str, reply_markup: dict | None) -> int:
            msg = (text or "").strip()
            if not msg:
                return 0
            try:
                from tracker.push.telegram import TelegramPusher

                p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))
                return int(
                    await p.send_raw_text(
                        chat_id=existing_chat_id,
                        text=msg,
                        disable_preview=True,
                        reply_markup=reply_markup,
                    )
                    or 0
                )
            except Exception:
                return 0

        def _env_path() -> str:
            from pathlib import Path

            return str(Path(getattr(settings, "env_path", "") or ".env"))

        def _prompt_slots_all() -> list[dict[str, Any]]:
            try:
                from dataclasses import asdict

                from tracker.prompt_templates import builtin_slots

                slots = [asdict(s) for s in builtin_slots()]
                slots = [s for s in slots if isinstance(s, dict) and str(s.get("id") or "").strip()]
                slots.sort(key=lambda d: str(d.get("id") or ""))
                return slots
            except Exception:
                return []

        def _prompt_templates_all() -> list[dict[str, Any]]:
            try:
                from tracker.prompt_templates import list_all_templates

                merged = list_all_templates(repo=repo)
                arr: list[dict[str, Any]] = []
                for tpl in merged.values():
                    arr.append({"id": tpl.id, "builtin": bool(getattr(tpl, "builtin", True)), "title": tpl.title})
                # Prefer custom/overrides first.
                arr.sort(key=lambda d: (0 if not bool(d.get("builtin", True)) else 1, str(d.get("id") or "")))
                return arr
            except Exception:
                return []

        def _prompts_menu() -> tuple[str, dict]:
            is_zh = _out_lang() == "zh"
            text = (
                "Prompts（可配置提示词）\n\n"
                "用途：管理 LLM 的“系统提示词/合成策略”。\n"
                "- Slot bindings：把某个 slot 绑定到一个 template_id\n"
                "- Edit templates：通过“自定义模板覆盖”内置模板（同 id 即 override）\n\n"
                "提示：模板不含密钥；保存后对下一轮任务生效。"
                if is_zh
                else (
                    "Prompts (operator-configurable)\n\n"
                    "Use: manage LLM prompts (system/synthesis policies).\n"
                    "- Slot bindings: bind a slot -> template_id\n"
                    "- Edit templates: override built-ins by saving a custom template with the same id\n\n"
                    "Note: templates contain no secrets; changes apply to the next run."
                )
            )
            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("🔗 Slot bindings" if not is_zh else "🔗 Slot 绑定"), "callback_data": "pr:slots:0"},
                        {"text": ("✏️ Templates" if not is_zh else "✏️ 模板"), "callback_data": "pr:tpl:0"},
                    ],
                    [{"text": ("🔄 Refresh" if not is_zh else "🔄 刷新"), "callback_data": "pr:menu"}],
                ]
            }
            return (text, kb)

        def _prompts_slots_menu(*, page: int) -> tuple[str, dict]:
            slots = _prompt_slots_all()
            page_size = 8
            total = len(slots)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = slots[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            header = (f"Slots（{start + 1}-{start + len(chunk)} / {total}）" if is_zh else f"Slots ({start + 1}-{start + len(chunk)} / {total})") if total else ("Slots（0）" if is_zh else "Slots (0)")
            lines.append(header)
            lines.append("点 slot 进入绑定/编辑。" if is_zh else "Tap a slot to bind/edit.")
            lines.append("")
            for idx, s in enumerate(chunk, start=start):
                sid = str(s.get("id") or "").strip()
                title = ui_t(_out_lang(), str(s.get("title") or "").strip())
                lines.append(f"- #{idx} {sid}" + (f" — {_short(title, 48)}" if title else ""))

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for idx, s in enumerate(chunk, start=start):
                sid = str(s.get("id") or "").strip()
                label = _short(sid, 28) or sid
                row.append({"text": label, "callback_data": f"pr:slot:{idx}:{page_i}:0"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav: list[dict[str, str]] = []
            if page_i > 0:
                nav.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"pr:slots:{page_i - 1}"})
            if page_i < max_page:
                nav.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"pr:slots:{page_i + 1}"})
            if nav:
                kb_rows.append(nav)
            kb_rows.append([{"text": ("🏠 菜单" if is_zh else "🏠 Menu"), "callback_data": "pr:menu"}])
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _prompts_slot_detail(*, slot_index: int, slots_page: int, tpl_page: int) -> tuple[str, dict]:
            slots = _prompt_slots_all()
            templates = _prompt_templates_all()
            try:
                from tracker.prompt_templates import load_bindings

                bindings = load_bindings(repo)
            except Exception:
                bindings = {}

            idx = int(slot_index or 0)
            if idx < 0 or idx >= len(slots):
                return _prompts_slots_menu(page=slots_page)
            s = slots[idx]
            sid = str(s.get("id") or "").strip()
            title = ui_t(_out_lang(), str(s.get("title") or "").strip())
            desc = ui_t(_out_lang(), str(s.get("description") or "").strip())
            fmt = str(s.get("output_format") or "").strip()
            placeholders = s.get("placeholders") or []
            ph = [str(x or "").strip() for x in placeholders if str(x or "").strip()] if isinstance(placeholders, list) else []

            bound = str(bindings.get(sid) or "").strip()
            current_tpl_id = bound or sid

            # Find current template index for edit shortcuts.
            cur_tpl_idx = -1
            for j, t in enumerate(templates):
                if str(t.get("id") or "").strip() == current_tpl_id:
                    cur_tpl_idx = j
                    break

            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            lines.append(("Slot 绑定" if is_zh else "Slot Binding"))
            lines.append(f"- slot: {sid}")
            if title:
                lines.append(f"- title: {title}")
            if fmt:
                lines.append(f"- format: {fmt}")
            if ph:
                lines.append(f"- placeholders: {', '.join(ph[:12])}")
            if desc:
                lines.append("")
                lines.append(desc)
            lines.append("")
            lines.append((f"当前绑定：{current_tpl_id}" if is_zh else f"Current: {current_tpl_id}"))
            lines.append(("选择 template 进行绑定；或选 Default 取消绑定（回到内置默认）。" if is_zh else "Pick a template to bind; Default unbinds (builtin default)."))

            page_size = 8
            total = len(templates)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(tpl_page or 0), max_page))
            start = page_i * page_size
            chunk = templates[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            kb_rows: list[list[dict[str, str]]] = []
            kb_rows.append([{"text": ("Default" if not is_zh else "Default（默认）"), "callback_data": f"pr:bind:{idx}:-1:{int(slots_page or 0)}:{int(page_i)}"}])
            for j, t in enumerate(chunk, start=start):
                tid = str(t.get("id") or "").strip()
                if not tid:
                    continue
                mark = "*" if not bool(t.get("builtin", True)) else ""
                label = _short(tid, 30) + mark
                kb_rows.append([{"text": label, "callback_data": f"pr:bind:{idx}:{j}:{int(slots_page or 0)}:{int(page_i)}"}])

            nav: list[dict[str, str]] = []
            if page_i > 0:
                nav.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"pr:slot:{idx}:{int(slots_page or 0)}:{page_i - 1}"})
            if page_i < max_page:
                nav.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"pr:slot:{idx}:{int(slots_page or 0)}:{page_i + 1}"})
            if nav:
                kb_rows.append(nav)

            # Edit current template (reply-based).
            if cur_tpl_idx >= 0:
                kb_rows.append(
                    [
                        {"text": ("✏️ 编辑中文" if is_zh else "✏️ Edit ZH"), "callback_data": f"pr:edit:{cur_tpl_idx}:zh:{idx}:{int(slots_page or 0)}:{int(page_i)}"},
                        {"text": ("✏️ Edit EN" if is_zh else "✏️ Edit EN"), "callback_data": f"pr:edit:{cur_tpl_idx}:en:{idx}:{int(slots_page or 0)}:{int(page_i)}"},
                    ]
                )
            kb_rows.append(
                [
                    {"text": ("⬅️ 返回 Slots" if is_zh else "⬅️ Back"), "callback_data": f"pr:slots:{int(slots_page or 0)}"},
                    {"text": ("🏠 菜单" if is_zh else "🏠 Menu"), "callback_data": "pr:menu"},
                ]
            )
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _prompts_templates_menu(*, page: int) -> tuple[str, dict]:
            templates = _prompt_templates_all()
            page_size = 8
            total = len(templates)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = templates[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            is_zh = _out_lang() == "zh"
            header = (f"Templates（{start + 1}-{start + len(chunk)} / {total}）" if is_zh else f"Templates ({start + 1}-{start + len(chunk)} / {total})") if total else ("Templates（0）" if is_zh else "Templates (0)")
            lines = [header, ("点模板查看/编辑。" if is_zh else "Tap a template to view/edit."), ""]
            for idx, t in enumerate(chunk, start=start):
                tid = str(t.get("id") or "").strip()
                mark = "*" if not bool(t.get("builtin", True)) else ""
                lines.append(f"- #{idx} {tid}{mark}")

            kb_rows: list[list[dict[str, str]]] = []
            for idx, t in enumerate(chunk, start=start):
                tid = str(t.get("id") or "").strip()
                mark = "*" if not bool(t.get("builtin", True)) else ""
                kb_rows.append([{"text": _short(tid, 34) + mark, "callback_data": f"pr:tplv:{idx}:{page_i}"}])

            nav: list[dict[str, str]] = []
            if page_i > 0:
                nav.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"pr:tpl:{page_i - 1}"})
            if page_i < max_page:
                nav.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"pr:tpl:{page_i + 1}"})
            if nav:
                kb_rows.append(nav)
            kb_rows.append([{"text": ("🏠 菜单" if is_zh else "🏠 Menu"), "callback_data": "pr:menu"}])
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _prompts_template_detail(*, template_index: int, page: int) -> tuple[str, dict]:
            templates = _prompt_templates_all()
            idx = int(template_index or 0)
            page_i = max(0, int(page or 0))
            if idx < 0 or idx >= len(templates):
                return _prompts_templates_menu(page=page_i)
            t = templates[idx]
            tid = str(t.get("id") or "").strip()
            is_custom = not bool(t.get("builtin", True))
            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            lines.append("Template" + ("（自定义）" if is_custom and is_zh else (" (custom)" if is_custom else "")))
            lines.append(f"- id: {tid}")
            ttl = str(t.get("title") or "").strip()
            if ttl:
                lines.append(f"- title: {ttl}")
            lines.append("")
            lines.append(("操作：点按钮编辑（回复粘贴内容）。" if is_zh else "Actions: tap to edit (reply paste)."))

            kb_rows: list[list[dict[str, str]]] = []
            kb_rows.append(
                [
                    {"text": ("✏️ 编辑中文" if is_zh else "✏️ Edit ZH"), "callback_data": f"pr:edit2:{idx}:zh:{page_i}"},
                    {"text": ("✏️ Edit EN" if is_zh else "✏️ Edit EN"), "callback_data": f"pr:edit2:{idx}:en:{page_i}"},
                ]
            )
            if is_custom:
                kb_rows.append([{"text": ("🗑️ 删除覆盖" if is_zh else "🗑️ Delete override"), "callback_data": f"pr:del:{idx}:{page_i}"}])
            kb_rows.append([{"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": f"pr:tpl:{page_i}"}])
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _topic_menu(*, page: int) -> tuple[str, dict]:
            topics = repo.list_topics()
            page_size = 8
            total = len(topics)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = topics[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            if is_zh:
                header = f"Topics（{start + 1}-{start + len(chunk)} / {total}）" if total else "Topics（0）"
                lines.append(header)
                lines.append("点按钮切换启用状态；添加：点 ➕ 添加 或发送 /t add；编辑：点 ✏️ 编辑 或发送 /t edit。")
            else:
                header = f"Topics ({start + 1}-{start + len(chunk)} / {total})" if total else "Topics (0)"
                lines.append(header)
                lines.append("Tap to toggle; add via ➕ Add or /t add; edit via ✏️ Edit or /t edit.")
            lines.append("")

            for t in chunk:
                mark = "✅" if getattr(t, "enabled", False) else "⬜"
                q = _short(str(getattr(t, "query", "") or ""), 72)
                lines.append(f"{mark} {t.name} — {q}")

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for t in chunk:
                mark = "✅" if getattr(t, "enabled", False) else "⬜"
                label = _short(f"{mark} {t.name}", 28) or mark
                row.append({"text": label, "callback_data": f"t:toggle:{int(t.id)}:{page_i}"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav1: list[dict[str, str]] = []
            if page_i > 0:
                nav1.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"t:page:{page_i - 1}"})
            if page_i < max_page:
                nav1.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"t:page:{page_i + 1}"})
            if nav1:
                kb_rows.append(nav1)
            kb_rows.append(
                [
                    {"text": ("✏️ 编辑" if is_zh else "✏️ Edit"), "callback_data": "t:edit"},
                    {"text": ("➕ 添加" if is_zh else "➕ Add"), "callback_data": "t:add"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"t:page:{page_i}"},
                ]
            )

            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _sources_menu(*, page: int) -> tuple[str, dict]:
            sources = repo.list_sources()
            page_size = 8
            total = len(sources)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = sources[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            if is_zh:
                header = f"Sources（{start + 1}-{start + len(chunk)} / {total}）" if total else "Sources（0）"
                lines.append(header)
                lines.append("点条目看详情；添加：点 ➕ 添加 或发送 /s add；绑定：点某个 source 的 🔗 绑定。")
            else:
                header = f"Sources ({start + 1}-{start + len(chunk)} / {total})" if total else "Sources (0)"
                lines.append(header)
                lines.append("Tap an item for details; add via ➕ Add or /s add; bind via 🔗 Bindings.")
            lines.append("")

            for src in chunk:
                mark = "✅" if getattr(src, "enabled", False) else "⬜"
                sid = int(getattr(src, "id", 0) or 0)
                typ = str(getattr(src, "type", "") or "").strip()
                url = str(getattr(src, "url", "") or "").strip()
                dom = _domain_from_url(url)
                tail = dom or _short(url, 44)
                lines.append(f"{mark} #{sid} [{typ}] {tail}".strip())

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for src in chunk:
                mark = "✅" if getattr(src, "enabled", False) else "⬜"
                sid = int(getattr(src, "id", 0) or 0)
                url = str(getattr(src, "url", "") or "").strip()
                dom = _domain_from_url(url)
                label = _short(f"{mark}#{sid} {dom or 'source'}", 28) or mark
                row.append({"text": label, "callback_data": f"s:detail:{sid}:{page_i}"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav1: list[dict[str, str]] = []
            if page_i > 0:
                nav1.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"s:page:{page_i - 1}"})
            if page_i < max_page:
                nav1.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"s:page:{page_i + 1}"})
            if nav1:
                kb_rows.append(nav1)
            kb_rows.append(
                [
                    {"text": ("➕ 添加" if is_zh else "➕ Add"), "callback_data": "s:add"},
                    {"text": ("🔗 绑定" if is_zh else "🔗 Bindings"), "callback_data": "b:page:0"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"s:page:{page_i}"},
                ]
            )

            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _source_details(*, source_id: int, page: int) -> tuple[str, dict]:
            sid = int(source_id or 0)
            src_page = max(0, int(page or 0))
            src = repo.get_source_by_id(sid)
            is_zh = _out_lang() == "zh"
            if not src:
                text = f"⚠️ Source not found: {sid}" if not is_zh else f"⚠️ 未找到 Source: {sid}"
                return (
                    text,
                    {"inline_keyboard": [[{"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": f"s:page:{src_page}"}]]},
                )

            url = str(getattr(src, "url", "") or "").strip()
            dom = _domain_from_url(url)
            typ = str(getattr(src, "type", "") or "").strip()
            enabled = bool(getattr(src, "enabled", False))

            # Best-effort health snapshot.
            health_line = "-"
            topics: list[str] = []
            try:
                from sqlalchemy import select

                from tracker.models import SourceHealth, Topic, TopicSource

                h = repo.session.scalar(select(SourceHealth).where(SourceHealth.source_id == sid))
                if h:
                    last_err = str(getattr(h, "last_error", "") or "").strip()
                    err_cnt = int(getattr(h, "error_count", 0) or 0)
                    if last_err:
                        health_line = f"{last_err} (errors={err_cnt})"
                    else:
                        health_line = "ok"
                rows = repo.session.execute(
                    select(Topic)
                    .join(TopicSource, TopicSource.topic_id == Topic.id)
                    .where(TopicSource.source_id == sid)
                    .order_by(Topic.id.asc())
                ).scalars()
                topics = [str(getattr(t, "name", "") or "").strip() for t in rows if str(getattr(t, "name", "") or "").strip()]
            except Exception:
                health_line = "-"
                topics = []

            header = f"Source #{sid}" if not is_zh else f"Source #{sid}"
            lines: list[str] = [header, ""]
            lines.append(f"- enabled: {enabled}")
            lines.append(f"- type: {typ or '-'}")
            lines.append(f"- domain: {dom or '-'}")
            lines.append(f"- health: {health_line}")
            lines.append(f"- url: {url}")
            if topics:
                show = topics[:8]
                tail = f" …(+{len(topics) - len(show)})" if len(topics) > len(show) else ""
                lines.append(f"- topics: {', '.join(show)}{tail}")
            else:
                lines.append("- topics: -")

            toggle_label = ("Disable" if enabled else "Enable") if not is_zh else ("停用" if enabled else "启用")
            kb = {
                "inline_keyboard": [
                    [{"text": f"✅ {toggle_label}", "callback_data": f"s:toggle:{sid}:{src_page}"}],
                    [{"text": ("🔗 绑定" if is_zh else "🔗 Bindings"), "callback_data": f"s:bind:menu:{sid}:{src_page}:0"}],
                    [
                        {"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": f"s:page:{src_page}"},
                        {"text": ("➕ 添加" if is_zh else "➕ Add"), "callback_data": "s:add"},
                    ],
                ]
            }
            return ("\n".join(lines).strip(), kb)

        def _source_bind_menu(*, source_id: int, src_page: int, page: int) -> tuple[str, dict]:
            sid = int(source_id or 0)
            page_size = 8
            topics = repo.list_topics()
            total = len(topics)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = topics[start : start + page_size]

            bound: set[int] = set()
            try:
                from sqlalchemy import select

                from tracker.models import TopicSource

                bound = set(
                    int(x or 0)
                    for x in repo.session.scalars(
                        select(TopicSource.topic_id).where(TopicSource.source_id == sid)
                    ).all()
                    if int(x or 0) > 0
                )
            except Exception:
                bound = set()

            is_zh = _out_lang() == "zh"

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            lines: list[str] = []
            header = f"Bindings: Source #{sid}（{start + 1}-{start + len(chunk)} / {total}）" if is_zh else f"Bindings: Source #{sid} ({start + 1}-{start + len(chunk)} / {total})"
            lines.append(header if total else (f"Bindings: Source #{sid}（0）" if is_zh else f"Bindings: Source #{sid} (0)"))
            lines.append("点按钮切换绑定/解绑。" if is_zh else "Tap to bind/unbind.")
            lines.append("")

            for t in chunk:
                tid = int(getattr(t, "id", 0) or 0)
                mark = "🔗" if tid in bound else "⬜"
                lines.append(f"{mark} {t.name}")

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for t in chunk:
                tid = int(getattr(t, "id", 0) or 0)
                mark = "🔗" if tid in bound else "⬜"
                label = _short(f"{mark} {t.name}", 28) or mark
                row.append({"text": label, "callback_data": f"s:bind:toggle:{sid}:{tid}:{int(src_page or 0)}:{page_i}"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav1: list[dict[str, str]] = []
            if page_i > 0:
                nav1.append(
                    {
                        "text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"),
                        "callback_data": f"s:bind:page:{sid}:{int(src_page or 0)}:{page_i - 1}",
                    }
                )
            if page_i < max_page:
                nav1.append(
                    {
                        "text": ("下一页 ➡️" if is_zh else "Next ➡️"),
                        "callback_data": f"s:bind:page:{sid}:{int(src_page or 0)}:{page_i + 1}",
                    }
                )
            if nav1:
                kb_rows.append(nav1)
            kb_rows.append(
                [
                    {"text": ("⬅️ 来源" if is_zh else "⬅️ Source"), "callback_data": f"s:detail:{sid}:{int(src_page or 0)}"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"s:bind:page:{sid}:{int(src_page or 0)}:{page_i}"},
                ]
            )

            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _bindings_topic_menu(*, page: int) -> tuple[str, dict]:
            topics = repo.list_topics()
            page_size = 8
            total = len(topics)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = topics[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            is_zh = _out_lang() == "zh"
            lines: list[str] = []
            header = f"Bindings（选 Topic）（{start + 1}-{start + len(chunk)} / {total}）" if is_zh else f"Bindings (pick a Topic) ({start + 1}-{start + len(chunk)} / {total})"
            lines.append(header if total else ("Bindings（0）" if is_zh else "Bindings (0)"))
            lines.append("点一个 Topic 查看/解绑已绑定 sources。" if is_zh else "Tap a topic to view/unbind bound sources.")
            lines.append("")
            for t in chunk:
                mark = "✅" if getattr(t, "enabled", False) else "⬜"
                lines.append(f"{mark} {t.name}")

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for t in chunk:
                mark = "✅" if getattr(t, "enabled", False) else "⬜"
                label = _short(f"{mark} {t.name}", 28) or mark
                row.append({"text": label, "callback_data": f"b:topic:{int(t.id)}:0"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav1: list[dict[str, str]] = []
            if page_i > 0:
                nav1.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"b:page:{page_i - 1}"})
            if page_i < max_page:
                nav1.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"b:page:{page_i + 1}"})
            if nav1:
                kb_rows.append(nav1)
            kb_rows.append(
                [
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"b:page:{page_i}"},
                    {"text": ("来源" if is_zh else "Sources"), "callback_data": "s:page:0"},
                ]
            )
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _topic_bindings_menu(*, topic_id: int, page: int) -> tuple[str, dict]:
            tid = int(topic_id or 0)
            page_size = 8
            is_zh = _out_lang() == "zh"

            try:
                from sqlalchemy import select

                from tracker.models import Source, Topic, TopicSource

                topic = repo.session.get(Topic, tid)
                sources = (
                    repo.session.execute(
                        select(Source)
                        .join(TopicSource, TopicSource.source_id == Source.id)
                        .where(TopicSource.topic_id == tid)
                        .order_by(Source.id.asc())
                    )
                    .scalars()
                    .all()
                )
            except Exception:
                topic = None
                sources = []

            total = len(sources)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = sources[start : start + page_size]

            def _short(s: str, n: int) -> str:
                s2 = (s or "").strip()
                if len(s2) <= n:
                    return s2
                return s2[: max(0, n - 1)] + "…"

            tname = str(getattr(topic, "name", "") or "").strip() if topic else str(tid)

            lines: list[str] = []
            header = (
                f"Topic Sources: {tname}（{start + 1}-{start + len(chunk)} / {total}）"
                if is_zh
                else f"Topic Sources: {tname} ({start + 1}-{start + len(chunk)} / {total})"
            )
            lines.append(header if total else (f"Topic Sources: {tname}（0）" if is_zh else f"Topic Sources: {tname} (0)"))
            lines.append("点按钮解绑。" if is_zh else "Tap to unbind.")
            lines.append("")
            for src in chunk:
                sid = int(getattr(src, "id", 0) or 0)
                mark = "✅" if getattr(src, "enabled", False) else "⬜"
                url = str(getattr(src, "url", "") or "").strip()
                dom = _domain_from_url(url)
                lines.append(f"{mark} #{sid} {dom or _short(url, 44)}")

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for src in chunk:
                sid = int(getattr(src, "id", 0) or 0)
                url = str(getattr(src, "url", "") or "").strip()
                dom = _domain_from_url(url)
                label = _short(f"🔗#{sid} {dom or 'source'}", 28) or f"#{sid}"
                row.append({"text": label, "callback_data": f"b:unbind:{tid}:{sid}:{page_i}"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav1: list[dict[str, str]] = []
            if page_i > 0:
                nav1.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"b:topic:{tid}:{page_i - 1}"})
            if page_i < max_page:
                nav1.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"b:topic:{tid}:{page_i + 1}"})
            if nav1:
                kb_rows.append(nav1)
            kb_rows.append(
                [
                    {"text": ("⬅️ 主题" if is_zh else "⬅️ Topics"), "callback_data": "b:page:0"},
                    {"text": ("➕ 绑定（用 /s）" if is_zh else "➕ Bind (use /s)"), "callback_data": "s:page:0"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"b:topic:{tid}:{page_i}"},
                ]
            )

            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _config_menu() -> tuple[str, dict]:
            out_lang = _out_lang()
            tz = (repo.get_app_config("cron_timezone") or getattr(settings, "cron_timezone", "") or "").strip() or "UTC"
            ol = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip() or "-"

            def _is_on(v: object) -> bool:
                s = str(v or "").strip().lower()
                if s in {"1", "true", "yes", "on"}:
                    return True
                if s in {"0", "false", "no", "off"}:
                    return False
                return bool(v)

            mute_days = (repo.get_app_config("telegram_feedback_mute_days_default") or "").strip()
            try:
                mute_days_n = int(mute_days) if mute_days else 7
            except Exception:
                mute_days_n = 7
            mute_days_n = max(1, min(365, int(mute_days_n)))
            if out_lang == "zh":
                text = (
                    "配置\n"
                    f"- output_language: {ol}\n"
                    f"- cron_timezone: {tz}\n\n"
                    f"- mute_days_default: {mute_days_n}\n\n"
                    "提示：cron 相关修改通常需要 /restart 才会生效；高级项可用 /env 粘贴 env 块（不会回显密钥）。"
                )
                kb = {
                    "inline_keyboard": [
                        [
                            {"text": "语言：中文", "callback_data": "cfg:lang:zh"},
                            {"text": "语言：英文", "callback_data": "cfg:lang:en"},
                        ],
                        [
                            {"text": "时区：上海", "callback_data": "cfg:tz:Asia/Shanghai"},
                            {"text": "时区：自定义", "callback_data": "cfg:tz:custom"},
                        ],
                        [
                            {"text": "静音默认：7天", "callback_data": "cfg:mute:7"},
                            {"text": "静音默认：14天", "callback_data": "cfg:mute:14"},
                        ],
                        [
                            {"text": "静音默认：自定义", "callback_data": "cfg:mute:custom"},
                        ],
                        [{"text": "🔄 刷新", "callback_data": "cfg:menu"}],
                    ]
                }
                return (text, kb)

            text = (
                "Config\n"
                f"- output_language: {ol}\n"
                f"- cron_timezone: {tz}\n\n"
                f"- mute_days_default: {mute_days_n}\n\n"
                "Tip: cron changes usually require /restart; for advanced keys, paste an env block via /env (secrets are not echoed)."
            )
            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("语言：中文" if _out_lang() == "zh" else "Lang: ZH"), "callback_data": "cfg:lang:zh"},
                        {"text": ("语言：英文" if _out_lang() == "zh" else "Lang: EN"), "callback_data": "cfg:lang:en"},
                    ],
                    [
                        {"text": ("时区：上海" if _out_lang() == "zh" else "TZ: Asia/Shanghai"), "callback_data": "cfg:tz:Asia/Shanghai"},
                        {"text": ("时区：自定义" if _out_lang() == "zh" else "TZ: custom"), "callback_data": "cfg:tz:custom"},
                    ],
                    [
                        {"text": ("Mute default: 7d" if _out_lang() != "zh" else "静音默认：7天"), "callback_data": "cfg:mute:7"},
                        {"text": ("Mute default: 14d" if _out_lang() != "zh" else "静音默认：14天"), "callback_data": "cfg:mute:14"},
                    ],
                    [
                        {"text": ("Mute default: custom" if _out_lang() != "zh" else "静音默认：自定义"), "callback_data": "cfg:mute:custom"},
                    ],
                    [{"text": ("🔄 刷新" if _out_lang() == "zh" else "🔄 Refresh"), "callback_data": "cfg:menu"}],
                ]
            }
            return (text, kb)

        # --- Config Center (v2): build TG menus from the same registry used by Web Admin.
        _CFG_C_DANGEROUS_FIELDS: set[str] = {"db_url", "env_path", "api_host", "api_port"}

        def _cfgc_build_ui() -> dict[str, Any] | None:
            try:
                from pathlib import Path

                from tracker.admin_settings import build_settings_view, default_settings_sections

                return build_settings_view(
                    repo=repo,
                    settings=settings,
                    env_path=Path(_env_path()),
                    sections=default_settings_sections(),
                )
            except Exception:
                return None

        def _cfgc_menu() -> tuple[str, dict]:
            lang = _out_lang()
            is_zh = lang == "zh"
            ui = _cfgc_build_ui()
            secs = (ui or {}).get("sections") if isinstance(ui, dict) else None
            if not isinstance(secs, list):
                secs = []

            lines: list[str] = []
            lines.append("配置中心（TG）" if is_zh else "Config Center (TG)")
            lines.append("点分组 → 点字段 → 设置值。" if is_zh else "Tap a section → tap a field → set a value.")
            lines.append("危险项（db_url/env_path/api_host/api_port）请用 /api 或 SSH/CLI。" if is_zh else "Dangerous keys (db_url/env_path/api_host/api_port): use /api or SSH/CLI.")
            lines.append("")

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for s in secs[:20]:
                sid = str((s or {}).get("id") or "").strip()
                title = str((s or {}).get("title") or "").strip() or sid
                if not sid:
                    continue
                label = ui_t(lang, title)
                row.append({"text": label, "callback_data": f"cfgc:sec:{sid}:0"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)
            kb_rows.append(
                [
                    {"text": ("♻️ 重启服务" if is_zh else "♻️ Restart services"), "callback_data": "cfgc:restart"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": "cfgc:menu"},
                ]
            )

            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _cfgc_section_menu(*, section_id: str, page: int) -> tuple[str, dict]:
            lang = _out_lang()
            is_zh = lang == "zh"
            ui = _cfgc_build_ui()
            secs = (ui or {}).get("sections") if isinstance(ui, dict) else None
            views = (ui or {}).get("views") if isinstance(ui, dict) else None
            if not isinstance(secs, list) or not isinstance(views, dict):
                return (
                    ("⚠️ 配置中心不可用（请用 /env 或 Web Admin）" if is_zh else "⚠️ Config Center unavailable (use /env or Web Admin)"),
                    {"inline_keyboard": [[{"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": "cfgc:menu"}]]},
                )

            sid = (section_id or "").strip()
            sec = None
            for s in secs:
                if str((s or {}).get("id") or "").strip() == sid:
                    sec = s
                    break
            if not isinstance(sec, dict):
                return _cfgc_menu()

            fields = sec.get("fields") if isinstance(sec.get("fields"), list) else []
            page_size = 8
            total = len(fields)
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(page or 0), max_page))
            start = page_i * page_size
            chunk = fields[start : start + page_size]

            title = ui_t(lang, str(sec.get("title") or sid))
            lines: list[str] = []
            header = (
                f"{title}（{start + 1}-{start + len(chunk)} / {total}）"
                if is_zh
                else f"{title} ({start + 1}-{start + len(chunk)} / {total})"
            )
            lines.append(header if total else (f"{title}（0）" if is_zh else f"{title} (0)"))
            lines.append("点字段进入详情/修改。" if is_zh else "Tap a field to view/edit.")
            lines.append("")

            def _short(s: str, n: int) -> str:
                ss = (s or "").strip()
                if len(ss) <= n:
                    return ss
                return ss[: max(0, n - 1)] + "…"

            for f in chunk:
                field = str((f or {}).get("field") or "").strip()
                v = views.get(field) if field else None
                if not isinstance(v, dict):
                    continue
                label = ui_t(lang, str(v.get("label") or field))
                kind = str(v.get("kind") or "")
                secret = bool(v.get("secret"))
                restart = bool(v.get("restart_required"))
                cur = ""
                if secret:
                    cur = ("set" if bool(v.get("secret_is_set")) else "unset")
                else:
                    cur = str(v.get("current_value_str") or "").strip() or "-"
                    if kind == "toggle":
                        cur = "ON" if str(cur).strip().lower() in {"true", "1", "yes", "on"} else "OFF"
                mark = "♻️" if restart else ""
                lines.append(f"- {label}: {_short(cur, 38)} {mark}".rstrip())

            kb_rows: list[list[dict[str, str]]] = []
            row: list[dict[str, str]] = []
            for f in chunk:
                field = str((f or {}).get("field") or "").strip()
                v = views.get(field) if field else None
                if not field or not isinstance(v, dict):
                    continue
                label = ui_t(lang, str(v.get("label") or field))
                row.append({"text": _short(label, 28) or field, "callback_data": f"cfgc:field:{sid}:{field}:{page_i}"})
                if len(row) >= 2:
                    kb_rows.append(row)
                    row = []
            if row:
                kb_rows.append(row)

            nav: list[dict[str, str]] = []
            if page_i > 0:
                nav.append({"text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"), "callback_data": f"cfgc:sec:{sid}:{page_i - 1}"})
            if page_i < max_page:
                nav.append({"text": ("下一页 ➡️" if is_zh else "Next ➡️"), "callback_data": f"cfgc:sec:{sid}:{page_i + 1}"})
            if nav:
                kb_rows.append(nav)
            kb_rows.append(
                [
                    {"text": ("⬅️ 分组" if is_zh else "⬅️ Sections"), "callback_data": "cfgc:menu"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"cfgc:sec:{sid}:{page_i}"},
                ]
            )
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _cfgc_field_menu(*, section_id: str, field: str, section_page: int) -> tuple[str, dict]:
            lang = _out_lang()
            is_zh = lang == "zh"
            ui = _cfgc_build_ui()
            views = (ui or {}).get("views") if isinstance(ui, dict) else None
            if not isinstance(views, dict):
                return _cfgc_menu()

            f = (field or "").strip()
            v = views.get(f) if f else None
            if not f or not isinstance(v, dict):
                return _cfgc_section_menu(section_id=section_id, page=int(section_page or 0))

            label = ui_t(lang, str(v.get("label") or f))
            desc = ui_t(lang, str(v.get("description") or "")) if str(v.get("description") or "").strip() else ""
            env_key = str(v.get("env_key") or "").strip()
            kind = str(v.get("kind") or "").strip()
            secret = bool(v.get("secret"))
            restart = bool(v.get("restart_required"))
            source = str(v.get("source") or "").strip() or "-"
            example = str(v.get("example") or "").strip()

            cur = ""
            if secret:
                cur = ("set" if bool(v.get("secret_is_set")) else "unset")
            else:
                cur = str(v.get("current_value_str") or "").strip() or "-"
                if kind == "toggle":
                    cur = "ON" if str(cur).strip().lower() in {"true", "1", "yes", "on"} else "OFF"

            lines: list[str] = []
            lines.append(f"{label}")
            if desc:
                lines.append(desc)
            lines.append("")
            lines.append(("当前：" if is_zh else "Current: ") + cur)
            lines.append(("来源：" if is_zh else "Source: ") + source)
            lines.append(("env：" if is_zh else "env: ") + (env_key or "-"))
            lines.append(("需要重启：是" if is_zh else "Restart required: yes") if restart else ("需要重启：否" if is_zh else "Restart required: no"))
            if example:
                lines.append(("示例：" if is_zh else "Example: ") + example)
            if f in _CFG_C_DANGEROUS_FIELDS:
                lines.append("")
                lines.append("⚠️ 该字段为危险项，请用 /api 或 SSH/CLI。" if is_zh else "⚠️ Dangerous key: use /api or SSH/CLI.")

            kb_rows: list[list[dict[str, str]]] = []
            if f not in _CFG_C_DANGEROUS_FIELDS:
                if kind == "toggle":
                    kb_rows.append(
                        [
                            {"text": ("开" if is_zh else "ON"), "callback_data": f"cfgc:set:{section_id}:{f}:true:{int(section_page or 0)}"},
                            {"text": ("关" if is_zh else "OFF"), "callback_data": f"cfgc:set:{section_id}:{f}:false:{int(section_page or 0)}"},
                        ]
                    )
                elif kind == "select" and isinstance(v.get("options"), list):
                    opts = [o for o in (v.get("options") or []) if isinstance(o, (list, tuple)) and len(o) >= 1]
                    row: list[dict[str, str]] = []
                    for o in opts[:8]:
                        val = str(o[0] or "").strip()
                        lab = str(o[1] if len(o) >= 2 else val).strip()
                        if not val:
                            continue
                        row.append(
                            {
                                "text": ui_t(lang, lab) if lab else val,
                                "callback_data": f"cfgc:set:{section_id}:{f}:{val}:{int(section_page or 0)}",
                            }
                        )
                        if len(row) >= 2:
                            kb_rows.append(row)
                            row = []
                    if row:
                        kb_rows.append(row)
                else:
                    kb_rows.append(
                        [
                            {
                                "text": ("✏️ 设置" if is_zh else "✏️ Set"),
                                "callback_data": f"cfgc:edit:{section_id}:{f}:{int(section_page or 0)}",
                            }
                        ]
                    )
                    if secret:
                        kb_rows.append([{"text": ("（密钥）推荐用 /env" if is_zh else "(secret) prefer /env"), "callback_data": "cfgc:menu"}])

            kb_rows.append(
                [
                    {"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": f"cfgc:sec:{section_id}:{int(section_page or 0)}"},
                    {"text": ("🔄 刷新" if is_zh else "🔄 Refresh"), "callback_data": f"cfgc:field:{section_id}:{f}:{int(section_page or 0)}"},
                ]
            )
            return ("\n".join(lines).strip(), {"inline_keyboard": kb_rows})

        def _llm_menu() -> tuple[str, dict]:
            def _as_bool(raw: str, fallback: bool) -> bool:
                low = (raw or "").strip().lower()
                if low in {"true", "1", "yes", "y", "on"}:
                    return True
                if low in {"false", "0", "no", "n", "off"}:
                    return False
                return bool(fallback)

            out_lang = _out_lang()
            curation_enabled = _as_bool(
                (repo.get_app_config("llm_curation_enabled") or ""),
                bool(getattr(settings, "llm_curation_enabled", False)),
            )
            triage_enabled = _as_bool(
                (repo.get_app_config("llm_curation_triage_enabled") or ""),
                bool(getattr(settings, "llm_curation_triage_enabled", False)),
            )
            prio_enabled = _as_bool(
                (repo.get_app_config("priority_lane_enabled") or ""),
                bool(getattr(settings, "priority_lane_enabled", False)),
            )
            base_url = (repo.get_app_config("llm_base_url") or getattr(settings, "llm_base_url", "") or "").strip()
            model = (repo.get_app_config("llm_model") or getattr(settings, "llm_model", "") or "").strip()
            model_reasoning = (
                (repo.get_app_config("llm_model_reasoning") or getattr(settings, "llm_model_reasoning", "") or "")
                .strip()
            )
            model_mini = (
                (repo.get_app_config("llm_model_mini") or getattr(settings, "llm_model_mini", "") or "").strip()
            )
            has_key = bool((getattr(settings, "llm_api_key", None) or "").strip())
            proxy = (getattr(settings, "llm_proxy", "") or "").strip()

            mini_base_url = (
                (repo.get_app_config("llm_mini_base_url") or getattr(settings, "llm_mini_base_url", "") or "")
                .strip()
            )
            has_mini_key = bool((getattr(settings, "llm_mini_api_key", None) or "").strip())
            mini_proxy = (getattr(settings, "llm_mini_proxy", "") or "").strip()

            if out_lang == "zh":
                text = (
                    "LLM 配置\n"
                    f"- LLM_CURATION_ENABLED: {str(bool(curation_enabled)).lower()}\n"
                    f"- LLM_CURATION_TRIAGE_ENABLED: {str(bool(triage_enabled)).lower()}\n"
                    f"- PRIORITY_LANE_ENABLED: {str(bool(prio_enabled)).lower()}\n"
                    f"- reasoning.base_url: {base_url or '-'}\n"
                    f"- reasoning.model: {model or '-'}\n"
                    f"- reasoning.model_reasoning: {model_reasoning or '-'}\n"
                    f"- reasoning.api_key: {'set' if has_key else 'unset'}\n"
                    f"- reasoning.proxy: {proxy or '-'}\n"
                    f"- mini.base_url: {mini_base_url or '-'}\n"
                    f"- mini.model_mini: {model_mini or '-'}\n"
                    f"- mini.api_key: {'set' if has_mini_key else 'unset'}\n"
                    f"- mini.proxy: {mini_proxy or '-'}\n\n"
                    "路由：triage/digest_summary 优先走 mini（未配置则回退 reasoning）。"
                )
            else:
                text = (
                    "LLM Config\n"
                    f"- LLM_CURATION_ENABLED: {str(bool(curation_enabled)).lower()}\n"
                    f"- LLM_CURATION_TRIAGE_ENABLED: {str(bool(triage_enabled)).lower()}\n"
                    f"- PRIORITY_LANE_ENABLED: {str(bool(prio_enabled)).lower()}\n"
                    f"- reasoning.base_url: {base_url or '-'}\n"
                    f"- reasoning.model: {model or '-'}\n"
                    f"- reasoning.model_reasoning: {model_reasoning or '-'}\n"
                    f"- reasoning.api_key: {'set' if has_key else 'unset'}\n"
                    f"- reasoning.proxy: {proxy or '-'}\n"
                    f"- mini.base_url: {mini_base_url or '-'}\n"
                    f"- mini.model_mini: {model_mini or '-'}\n"
                    f"- mini.api_key: {'set' if has_mini_key else 'unset'}\n"
                    f"- mini.proxy: {mini_proxy or '-'}\n\n"
                    "Routing: triage/digest_summary prefers mini (falls back to reasoning)."
                )

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("Curation：开" if out_lang == "zh" else "Curation: ON"), "callback_data": "llm:cur:true"},
                        {"text": ("Curation：关" if out_lang == "zh" else "Curation: OFF"), "callback_data": "llm:cur:false"},
                    ],
                    [
                        {"text": ("Triage：开" if out_lang == "zh" else "Triage: ON"), "callback_data": "llm:tri:true"},
                        {"text": ("Triage：关" if out_lang == "zh" else "Triage: OFF"), "callback_data": "llm:tri:false"},
                    ],
                    [
                        {"text": ("Priority：开" if out_lang == "zh" else "Priority: ON"), "callback_data": "llm:prio:true"},
                        {"text": ("Priority：关" if out_lang == "zh" else "Priority: OFF"), "callback_data": "llm:prio:false"},
                    ],
                    [
                        {"text": ("设置 base_url" if out_lang == "zh" else "Set base_url"), "callback_data": "llm:set:base_url"},
                        {"text": ("设置 model" if out_lang == "zh" else "Set model"), "callback_data": "llm:set:model"},
                    ],
                    [
                        {"text": ("设置 model_reasoning" if out_lang == "zh" else "Set model_reasoning"), "callback_data": "llm:set:model_reasoning"},
                        {"text": ("设置 model_mini" if out_lang == "zh" else "Set model_mini"), "callback_data": "llm:set:model_mini"},
                    ],
                    [
                        {"text": ("设置 api_key" if out_lang == "zh" else "Set api_key"), "callback_data": "llm:set:api_key"},
                        {"text": ("设置 proxy" if out_lang == "zh" else "Set proxy"), "callback_data": "llm:set:proxy"},
                    ],
                    [
                        {"text": ("设置 mini_base_url" if out_lang == "zh" else "Set mini_base_url"), "callback_data": "llm:set:mini_base_url"},
                        {"text": ("设置 mini_api_key" if out_lang == "zh" else "Set mini_api_key"), "callback_data": "llm:set:mini_api_key"},
                    ],
                    [
                        {"text": ("设置 mini_proxy" if out_lang == "zh" else "Set mini_proxy"), "callback_data": "llm:set:mini_proxy"},
                        {"text": ("🔄 刷新" if out_lang == "zh" else "🔄 Refresh"), "callback_data": "llm:menu"},
                    ],
                ]
            }
            return (text, kb)

        def _read_env_assignments() -> dict[str, str]:
            try:
                from pathlib import Path

                from tracker.envfile import parse_env_assignments

                p = Path(_env_path())
                if not p.exists():
                    return {}
                return parse_env_assignments(p.read_text(encoding="utf-8"))
            except Exception:
                return {}

        def _api_menu() -> tuple[str, dict]:
            out_lang = _out_lang()
            host = (repo.get_app_config("api_host") or str(getattr(settings, "api_host", "") or "")).strip() or "127.0.0.1"
            port = (repo.get_app_config("api_port") or str(getattr(settings, "api_port", "") or "")).strip() or "8080"

            env = _read_env_assignments()
            token_set = bool((env.get("TRACKER_API_TOKEN") or str(getattr(settings, "api_token", "") or "")).strip())
            pw_set = bool((env.get("TRACKER_ADMIN_PASSWORD") or str(getattr(settings, "admin_password", "") or "")).strip())

            if out_lang == "zh":
                text = (
                    "API / Admin 绑定\n"
                    f"- host: {host}\n"
                    f"- port: {port}\n"
                    f"- auth: api_token={'set' if token_set else 'unset'} / admin_password={'set' if pw_set else 'unset'}\n\n"
                    "提示：绑定到 0.0.0.0 前需要先配置 TRACKER_API_TOKEN 或 TRACKER_ADMIN_PASSWORD（可在本菜单设置；否则 tracker-api 会拒绝启动）。"
                )
            else:
                text = (
                    "API / Admin bind\n"
                    f"- host: {host}\n"
                    f"- port: {port}\n"
                    f"- auth: api_token={'set' if token_set else 'unset'} / admin_password={'set' if pw_set else 'unset'}\n\n"
                    "Note: binding to 0.0.0.0 requires TRACKER_API_TOKEN or TRACKER_ADMIN_PASSWORD (you can set them here; otherwise tracker-api refuses to start)."
                )

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("Host: 本机" if out_lang == "zh" else "Host: localhost"), "callback_data": "api:host:127.0.0.1"},
                        {"text": ("Host: 0.0.0.0" if out_lang == "zh" else "Host: 0.0.0.0"), "callback_data": "api:host:0.0.0.0"},
                    ],
                    [
                        {"text": ("Port: 8080" if out_lang == "zh" else "Port: 8080"), "callback_data": "api:port:8080"},
                        {"text": ("Port: 8899" if out_lang == "zh" else "Port: 8899"), "callback_data": "api:port:8899"},
                    ],
                    [
                        {"text": ("自定义 Host…" if out_lang == "zh" else "Custom host…"), "callback_data": "api:host:custom"},
                        {"text": ("自定义 Port…" if out_lang == "zh" else "Custom port…"), "callback_data": "api:port:custom"},
                    ],
                    [
                        {"text": ("设置 API token" if out_lang == "zh" else "Set API token"), "callback_data": "api:auth:token"},
                        {"text": ("设置 Admin 密码" if out_lang == "zh" else "Set Admin password"), "callback_data": "api:auth:password"},
                    ],
                    [
                        {"text": ("🔄 刷新" if out_lang == "zh" else "🔄 Refresh"), "callback_data": "api:menu"},
                    ],
                ]
            }
            return (text, kb)

        def _push_menu() -> tuple[str, dict]:
            def _as_bool(raw: str, fallback: bool) -> bool:
                low = (raw or "").strip().lower()
                if low in {"true", "1", "yes", "y", "on"}:
                    return True
                if low in {"false", "0", "no", "n", "off"}:
                    return False
                return bool(fallback)

            out_lang = _out_lang()
            env = _read_env_assignments()

            # Telegram (already connected if commands are available)
            telegram_chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
            telegram_enabled = _as_bool(
                (repo.get_app_config("push_telegram_enabled") or "").strip(),
                bool(getattr(settings, "push_telegram_enabled", True)),
            )

            # DingTalk
            dingtalk_enabled = _as_bool(
                (repo.get_app_config("push_dingtalk_enabled") or "").strip(),
                bool(getattr(settings, "push_dingtalk_enabled", True)),
            )
            dingtalk_url_set = bool((env.get("TRACKER_DINGTALK_WEBHOOK_URL") or str(getattr(settings, "dingtalk_webhook_url", "") or "")).strip())
            dingtalk_secret_set = bool((env.get("TRACKER_DINGTALK_SECRET") or str(getattr(settings, "dingtalk_secret", "") or "")).strip())

            # Email
            smtp_host = (repo.get_app_config("smtp_host") or str(getattr(settings, "smtp_host", "") or "")).strip()
            smtp_port = (repo.get_app_config("smtp_port") or str(getattr(settings, "smtp_port", "") or "")).strip()
            smtp_user = (repo.get_app_config("smtp_user") or str(getattr(settings, "smtp_user", "") or "")).strip()
            smtp_password_set = bool((env.get("TRACKER_SMTP_PASSWORD") or str(getattr(settings, "smtp_password", "") or "")).strip())
            smtp_starttls = _as_bool((repo.get_app_config("smtp_starttls") or "").strip(), bool(getattr(settings, "smtp_starttls", True)))
            smtp_use_ssl = _as_bool((repo.get_app_config("smtp_use_ssl") or "").strip(), bool(getattr(settings, "smtp_use_ssl", False)))
            email_from = (repo.get_app_config("email_from") or str(getattr(settings, "email_from", "") or "")).strip()
            email_to = (repo.get_app_config("email_to") or str(getattr(settings, "email_to", "") or "")).strip()

            # Generic webhook
            webhook_set = bool((env.get("TRACKER_WEBHOOK_URL") or str(getattr(settings, "webhook_url", "") or "")).strip())

            if out_lang == "zh":
                text = (
                    "Push 配置\n"
                    f"- Telegram: enabled={str(bool(telegram_enabled)).lower()} {'connected' if telegram_chat_id else 'not connected'}\n"
                    f"- DingTalk: enabled={str(bool(dingtalk_enabled)).lower()} webhook={'set' if dingtalk_url_set else 'unset'} secret={'set' if dingtalk_secret_set else 'unset'}\n"
                    f"- Email: smtp_host={smtp_host or '-'} smtp_port={smtp_port or '-'} smtp_user={smtp_user or '-'} smtp_password={'set' if smtp_password_set else 'unset'} starttls={str(bool(smtp_starttls)).lower()} ssl={str(bool(smtp_use_ssl)).lower()}\n"
                    f"- Email: from={email_from or '-'} to={email_to or '-'}\n"
                    f"- Webhook: {'set' if webhook_set else 'unset'}\n\n"
                    "提示：密钥类配置写入 .env 后通常需要 /restart 才会生效（bot 不会回显密钥）。"
                )
            else:
                text = (
                    "Push Config\n"
                    f"- Telegram: enabled={str(bool(telegram_enabled)).lower()} {'connected' if telegram_chat_id else 'not connected'}\n"
                    f"- DingTalk: enabled={str(bool(dingtalk_enabled)).lower()} webhook={'set' if dingtalk_url_set else 'unset'} secret={'set' if dingtalk_secret_set else 'unset'}\n"
                    f"- Email: smtp_host={smtp_host or '-'} smtp_port={smtp_port or '-'} smtp_user={smtp_user or '-'} smtp_password={'set' if smtp_password_set else 'unset'} starttls={str(bool(smtp_starttls)).lower()} ssl={str(bool(smtp_use_ssl)).lower()}\n"
                    f"- Email: from={email_from or '-'} to={email_to or '-'}\n"
                    f"- Webhook: {'set' if webhook_set else 'unset'}\n\n"
                    "Note: secret keys are written to .env and usually require /restart to take effect (the bot won't echo secrets)."
                )

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("Telegram：开" if out_lang == "zh" else "Telegram: ON"), "callback_data": "push:bool:TRACKER_PUSH_TELEGRAM_ENABLED:true"},
                        {"text": ("Telegram：关" if out_lang == "zh" else "Telegram: OFF"), "callback_data": "push:bool:TRACKER_PUSH_TELEGRAM_ENABLED:false"},
                    ],
                    [
                        {"text": ("DingTalk：开" if out_lang == "zh" else "DingTalk: ON"), "callback_data": "push:bool:TRACKER_PUSH_DINGTALK_ENABLED:true"},
                        {"text": ("DingTalk：关" if out_lang == "zh" else "DingTalk: OFF"), "callback_data": "push:bool:TRACKER_PUSH_DINGTALK_ENABLED:false"},
                    ],
                    [
                        {"text": ("设置 钉钉Webhook" if out_lang == "zh" else "Set DingTalk webhook"), "callback_data": "push:set:TRACKER_DINGTALK_WEBHOOK_URL"},
                        {"text": ("设置 钉钉Secret" if out_lang == "zh" else "Set DingTalk secret"), "callback_data": "push:set:TRACKER_DINGTALK_SECRET"},
                    ],
                    [
                        {"text": ("设置 SMTP_HOST" if out_lang == "zh" else "Set SMTP host"), "callback_data": "push:set:TRACKER_SMTP_HOST"},
                        {"text": ("设置 SMTP_PORT" if out_lang == "zh" else "Set SMTP port"), "callback_data": "push:set:TRACKER_SMTP_PORT"},
                    ],
                    [
                        {"text": ("设置 SMTP_USER" if out_lang == "zh" else "Set SMTP user"), "callback_data": "push:set:TRACKER_SMTP_USER"},
                        {"text": ("设置 SMTP_PASSWORD" if out_lang == "zh" else "Set SMTP password"), "callback_data": "push:set:TRACKER_SMTP_PASSWORD"},
                    ],
                    [
                        {"text": ("设置 Email from" if out_lang == "zh" else "Set Email from"), "callback_data": "push:set:TRACKER_EMAIL_FROM"},
                        {"text": ("设置 Email to" if out_lang == "zh" else "Set Email to"), "callback_data": "push:set:TRACKER_EMAIL_TO"},
                    ],
                    [
                        {"text": ("STARTTLS: true" if out_lang == "zh" else "STARTTLS: true"), "callback_data": "push:bool:TRACKER_SMTP_STARTTLS:true"},
                        {"text": ("STARTTLS: false" if out_lang == "zh" else "STARTTLS: false"), "callback_data": "push:bool:TRACKER_SMTP_STARTTLS:false"},
                    ],
                    [
                        {"text": ("SSL(465): true" if out_lang == "zh" else "SSL(465): true"), "callback_data": "push:bool:TRACKER_SMTP_USE_SSL:true"},
                        {"text": ("SSL(465): false" if out_lang == "zh" else "SSL(465): false"), "callback_data": "push:bool:TRACKER_SMTP_USE_SSL:false"},
                    ],
                    [
                        {"text": ("设置 Webhook URL" if out_lang == "zh" else "Set Webhook URL"), "callback_data": "push:set:TRACKER_WEBHOOK_URL"},
                        {"text": ("🔄 刷新" if out_lang == "zh" else "🔄 Refresh"), "callback_data": "push:menu"},
                    ],
                ]
            }
            return (text, kb)

        def _auth_menu() -> tuple[str, dict]:
            out_lang = _out_lang()
            env = _read_env_assignments()

            discourse_cookie_set = bool((env.get("TRACKER_DISCOURSE_COOKIE") or str(getattr(settings, "discourse_cookie", "") or "")).strip())
            cookie_jar_set = bool((env.get("TRACKER_COOKIE_JAR_JSON") or str(getattr(settings, "cookie_jar_json", "") or "")).strip())

            if out_lang == "zh":
                text = (
                    "Auth（登录/Cookie）\n"
                    f"- discourse_cookie: {'set' if discourse_cookie_set else 'unset'}\n"
                    f"- cookie_jar_json: {'set' if cookie_jar_set else 'unset'}\n\n"
                    "说明：\n"
                    "- 站点可能会过期/踢下线；OpenInfoMate 检测到需要登录会推送告警（auth_required）。\n"
                    "- secrets 不会被 bot 回显（但你的回复仍会出现在聊天记录里）。"
                )
            else:
                text = (
                    "Auth (login/cookies)\n"
                    f"- discourse_cookie: {'set' if discourse_cookie_set else 'unset'}\n"
                    f"- cookie_jar_json: {'set' if cookie_jar_set else 'unset'}\n\n"
                    "Notes:\n"
                    "- Sites may expire sessions; OpenInfoMate will alert 'auth_required'.\n"
                    "- Secrets are not echoed (but your reply still appears in chat history)."
                )

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("设置 discourse_cookie" if out_lang == "zh" else "Set discourse_cookie"), "callback_data": "auth:set:TRACKER_DISCOURSE_COOKIE"},
                        {"text": ("设置 cookie_jar_json" if out_lang == "zh" else "Set cookie_jar_json"), "callback_data": "auth:set:TRACKER_COOKIE_JAR_JSON"},
                    ],
                    [
                        {"text": ("🔄 刷新" if out_lang == "zh" else "🔄 Refresh"), "callback_data": "auth:menu"},
                    ],
                ]
            }
            return (text, kb)

        def _profile_menu() -> tuple[str, dict]:
            topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
            has_profile_text = bool((repo.get_app_config("profile_text") or "").strip())
            has_draft = bool((repo.get_app_config("profile_onboarding_draft_json") or "").strip())
            has_prompt = False
            bound_sources = 0
            try:
                topic = repo.get_topic_by_name(topic_name)
                if topic:
                    pol = repo.get_topic_policy(topic_id=int(topic.id))
                    has_prompt = bool((pol.llm_curation_prompt if pol else "") or "")
                    bound_sources = len(repo.list_topic_sources(topic=topic))
            except Exception:
                has_prompt = False
                bound_sources = 0

            out_lang = _out_lang()
            if out_lang == "zh":
                text = (
                    "Profile（画像）\n"
                    f"- topic: {topic_name}\n"
                    f"- profile_text: {'set' if has_profile_text else 'unset'}\n"
                    f"- ai_prompt: {'set' if has_prompt else 'unset'}\n"
                    f"- sources_bound: {bound_sources}\n"
                    f"- draft: {'ready' if has_draft else 'none'}\n\n"
                    "用法：先 Start 粘贴你的书签/笔记，然后选择一个预设 Apply。"
                )
            else:
                text = (
                    "Profile\n"
                    f"- topic: {topic_name}\n"
                    f"- profile_text: {'set' if has_profile_text else 'unset'}\n"
                    f"- ai_prompt: {'set' if has_prompt else 'unset'}\n"
                    f"- sources_bound: {bound_sources}\n"
                    f"- draft: {'ready' if has_draft else 'none'}\n\n"
                    "Usage: Start -> paste bookmarks/notes -> choose a preset Apply."
                )

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("开始/更新" if out_lang == "zh" else "Start / Update"), "callback_data": "profile:start"},
                        {"text": ("应用（完整）" if out_lang == "zh" else "Apply (full)"), "callback_data": "profile:apply:full"},
                    ],
                    [
                        {"text": ("应用（轻量）" if out_lang == "zh" else "Apply (light)"), "callback_data": "profile:apply:light"},
                        {"text": ("🔄 刷新" if out_lang == "zh" else "🔄 Refresh"), "callback_data": "profile:menu"},
                    ],
                ]
            }
            return (text, kb)

        pending_feedback_for_profile: list[int] = []

        max_update_id: int | None = offset - 1 if offset is not None else None
        for upd in updates:
            if not isinstance(upd, dict):
                continue
            try:
                update_id = int(upd.get("update_id"))
            except Exception:
                update_id = None
            if update_id is not None:
                max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)

            # 0) Inline button clicks (callback_query).
            cq = upd.get("callback_query")
            if isinstance(cq, dict):
                cq_id = str(cq.get("id") or "").strip()
                data = str(cq.get("data") or "").strip()
                from_obj = cq.get("from")
                uid = str(from_obj.get("id") or "").strip() if isinstance(from_obj, dict) else ""

                msg_obj = cq.get("message")
                if not isinstance(msg_obj, dict):
                    continue
                chat = msg_obj.get("chat")
                if not isinstance(chat, dict) or str(chat.get("id") or "").strip() != existing_chat_id:
                    continue

                # Private-bot gating.
                if not owner_user_id and uid:
                    owner_user_id = uid
                    repo.set_app_config("telegram_owner_user_id", uid)
                if owner_user_id and uid and uid != owner_user_id:
                    continue

                try:
                    mid = int(msg_obj.get("message_id") or 0)
                except Exception:
                    mid = 0
                if mid <= 0:
                    continue

                # Best-effort: stop the client spinner early.
                try:
                    ack_text = "OK"
                    if data.startswith("br:rerun"):
                        ack_text = "正在生成新一份参考消息…" if _out_lang() == "zh" else "Generating a new batch…"
                    elif data.startswith("cfgag:apply"):
                        ack_text = "正在应用智能配置…" if _out_lang() == "zh" else "Applying config…"
                    elif data.startswith("cfgag:cancel"):
                        ack_text = "已取消" if _out_lang() == "zh" else "Canceled"
                    await telegram_answer_callback_query(
                        bot_token=token,
                        callback_query_id=cq_id,
                        text=ack_text,
                        show_alert=False,
                        client_timeout_seconds=settings.http_timeout_seconds,
                    )
                except Exception:
                    pass

                if data.startswith("cfgag:"):
                    parts = [p for p in data.split(":") if p]
                    action = (parts[1] if len(parts) >= 2 else "").strip().lower()
                    try:
                        task_id = int((parts[2] if len(parts) >= 3 else "0") or 0)
                    except Exception:
                        task_id = 0
                    if task_id <= 0:
                        continue
                    try:
                        from tracker.models import TelegramTask

                        row = repo.session.get(TelegramTask, int(task_id))
                    except Exception:
                        row = None
                    if not row or (row.kind or "").strip() != "config_agent":
                        continue

                    if action == "apply":
                        status = (row.status or "").strip()
                        if status not in {"awaiting", "failed"}:
                            continue
                        row.status = "pending_apply"
                        row.provider = "apply"
                        row.started_at = None
                        row.finished_at = None
                        row.error = ""
                        repo.session.commit()
                        continue

                    if action == "cancel":
                        repo.mark_telegram_task_canceled(int(task_id), reason="user_canceled")
                        continue

                    continue

                # --- Source expansion (candidates): batch actions + discovery toggle
                if data.startswith("cands:"):
                    parts = [p for p in data.split(":") if p]
                    action = (parts[1] if len(parts) >= 2 else "").strip().lower()

                    # Toggle discovery (pause/resume) without restart.
                    if action in {"discover", "discovery"}:
                        desired = (parts[2] if len(parts) >= 3 else "toggle").strip().lower()
                        cur_raw = (repo.get_app_config("discover_sources_enabled") or "").strip().lower()
                        cur_on = False if cur_raw in {"0", "false", "off", "no"} else True
                        if desired in {"off", "pause", "stop", "0", "false"}:
                            next_on = False
                        elif desired in {"on", "resume", "start", "1", "true"}:
                            next_on = True
                        else:
                            next_on = not cur_on
                        try:
                            repo.set_app_config("discover_sources_enabled", "true" if next_on else "false")
                        except Exception:
                            pass
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已恢复扩源。" if next_on else "⏸️ 已停止扩源。")
                        else:
                            await _send_ack("✅ Source discovery resumed." if next_on else "⏸️ Source discovery paused.")
                        continue

                    if action in {"accept", "ignore"}:
                        cutoff_id = 0
                        try:
                            cutoff_id = int((parts[2] if len(parts) >= 3 else "0") or 0)
                        except Exception:
                            cutoff_id = 0
                        if cutoff_id <= 0:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 参数错误：cutoff_id 无效。")
                            else:
                                await _send_ack("⚠️ Invalid cutoff_id.")
                            continue

                        try:
                            from sqlalchemy import and_, func, select
                            from tracker.actions import (
                                accept_source_candidate as accept_source_candidate_action,
                                ignore_source_candidate as ignore_source_candidate_action,
                            )
                            from tracker.models import SourceCandidate
                        except Exception:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 服务器缺少依赖，无法处理候选源。")
                            else:
                                await _send_ack("⚠️ Server missing deps; can't process candidates.")
                            continue

                        # Bounded: keep Telegram polling responsive.
                        limit = 50
                        cand_ids = []
                        try:
                            cand_ids = list(
                                repo.session.scalars(
                                    select(SourceCandidate.id)
                                    .where(
                                        and_(
                                            SourceCandidate.status == "new",
                                            SourceCandidate.id <= int(cutoff_id),
                                        )
                                    )
                                    .order_by(SourceCandidate.id.asc())
                                    .limit(limit)
                                )
                                or []
                            )
                        except Exception:
                            cand_ids = []

                        if not cand_ids:
                            if _out_lang() == "zh":
                                await _send_ack("✅ 没有需要处理的候选源（可能已在网页端处理）。")
                            else:
                                await _send_ack("✅ No candidates to process (maybe already handled in Web Admin).")
                            continue

                        ok_n = 0
                        err_n = 0
                        for cid in [int(x or 0) for x in cand_ids if int(x or 0) > 0]:
                            try:
                                if action == "accept":
                                    accept_source_candidate_action(session=repo.session, candidate_id=int(cid), enabled=True)
                                else:
                                    ignore_source_candidate_action(session=repo.session, candidate_id=int(cid))
                                ok_n += 1
                            except Exception:
                                err_n += 1

                        remain_new = 0
                        try:
                            remain_new = int(
                                repo.session.scalar(
                                    select(func.count())
                                    .select_from(SourceCandidate)
                                    .where(SourceCandidate.status == "new")
                                )
                                or 0
                            )
                        except Exception:
                            remain_new = 0

                        truncated = len(cand_ids) >= limit
                        if _out_lang() == "zh":
                            verb = "接受" if action == "accept" else "忽略"
                            extra = "（已到上限，点一次继续处理）" if truncated else ""
                            await _send_ack(f"✅ 已{verb} {ok_n} 条候选源，错误 {err_n}。剩余候选源：{remain_new}。{extra}")
                        else:
                            verb = "accepted" if action == "accept" else "ignored"
                            extra = " (limit reached; click again to continue)" if truncated else ""
                            await _send_ack(
                                f"✅ {ok_n} candidates {verb}, {err_n} errors. Remaining new: {remain_new}.{extra}"
                            )
                        continue

                # --- Report Reader (Curated Info)
                if data.startswith("br:"):
                    try:
                        msg_map = repo.get_telegram_message(chat_id=existing_chat_id, message_id=mid)
                    except Exception:
                        msg_map = None
                    report_key = (getattr(msg_map, "idempotency_key", "") or "").strip() if msg_map else ""
                    if not report_key:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 这条 Reader 已过期（找不到关联报告）。请等下一轮推送。")
                        else:
                            await _send_ack("⚠️ Reader expired (missing report mapping). Please wait for the next push.")
                        continue

                    if not report_key.startswith("digest:"):
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 这条 Reader 不再支持（仅 Curated Info）。请等下一轮推送。")
                        else:
                            await _send_ack("⚠️ This reader is no longer supported (Curated Info only). Please wait for the next push.")
                        continue

                    report_kind = "digest"
                    rep = None

                    # Anchor Curated Info readers to the message timestamp when the mapping key is a
                    # collapsed digest prefix (e.g. `digest:<topic_id>:<day>`). Those keys used to be
                    # overwritten by later runs, which made old reader messages "mutate" when navigating.
                    try:
                        msg_at = getattr(msg_map, "created_at", None) if msg_map else None
                    except Exception:
                        msg_at = None
                    try:
                        parts0 = [p for p in report_key.split(":") if p]
                        is_digest_prefix_key = bool(
                            report_key.startswith("digest:")
                            and len(parts0) == 3
                            and (parts0[1].isdigit() if len(parts0) >= 2 else False)
                        )
                    except Exception:
                        is_digest_prefix_key = False

                    if is_digest_prefix_key and msg_map:
                        try:
                            best = None
                            best_ts = None
                            for r, _t in repo.list_reports(kind=report_kind, limit=400):
                                k2 = (getattr(r, "idempotency_key", "") or "").strip()
                                if k2 != report_key and (not k2.startswith(report_key + ":")):
                                    continue
                                rts = getattr(r, "created_at", None)
                                if msg_at and isinstance(rts, dt.datetime):
                                    if rts > msg_at:
                                        continue
                                    if (best_ts is None) or (rts > best_ts):
                                        best = r
                                        best_ts = rts
                                else:
                                    best = r
                                    break
                            rep = best
                        except Exception:
                            rep = None
                    if rep is None:
                        try:
                            rep = repo.get_report_by_key(kind=report_kind, idempotency_key=report_key)
                        except Exception:
                            rep = None
                    if not rep and msg_map:
                        # Backward-compatible recovery: old Telegram mappings used collapsed keys
                        # (e.g. `digest:<topic_id>:<day>`). Anchor to the message time to keep the reader stable.
                        try:
                            msg_at = getattr(msg_map, "created_at", None)
                        except Exception:
                            msg_at = None
                        try:
                            for r, _t in repo.list_reports(kind=report_kind, limit=200):
                                k2 = (getattr(r, "idempotency_key", "") or "").strip()
                                if k2 == report_key or k2.startswith(report_key + ":"):
                                    if msg_at and isinstance(getattr(r, "created_at", None), dt.datetime):
                                        if r.created_at > msg_at:
                                            continue
                                    rep = r
                                    break
                        except Exception:
                            rep = None
                    md = (rep.markdown if rep else "").strip()
                    if not md:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 找不到对应报告（可能已被清理）。")
                        else:
                            await _send_ack("⚠️ Report not found (may have been pruned).")
                        continue

                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else "toc"
                    out_lang = _out_lang()
                    show_feedback = bool(
                        report_key.startswith("digest:")
                        and bool(getattr(settings, "telegram_digest_item_feedback_enabled", True))
                    )

                    # Curated Info is a de-dupe-only batch surface: don't show section navigation
                    # (old keyboards may still have section buttons).
                    if report_key.startswith("digest:") and action == "sec":
                        action = "toc"

                    # Curated Info: "rerun" sends a NEW batch message (does not edit the current one).
                    if action in {"rerun"}:
                        if not report_key.startswith("digest:"):
                            continue
                        try:
                            key_parts = [p for p in report_key.split(":") if p]
                            topic_id = int(key_parts[1]) if len(key_parts) >= 2 else 0
                        except Exception:
                            topic_id = 0
                        sent2 = False
                        try:
                            from tracker.runner import run_curated_info, run_digest

                            try:
                                hours2 = int(getattr(settings, "digest_hours", 24) or 24)
                            except Exception:
                                hours2 = 24
                            if hours2 <= 0:
                                hours2 = 24
                            from tracker.push_ops import make_manual_key_suffix

                            suffix = make_manual_key_suffix()
                            if topic_id > 0:
                                res = await run_digest(
                                    session=repo.session,
                                    settings=settings,
                                    hours=hours2,
                                    push=False,
                                    topic_ids=[topic_id],
                                    key_suffix=suffix,
                                )
                                per = res.per_topic[0] if (res.per_topic or []) else None
                                if per and (per.idempotency_key and per.markdown):
                                    from tracker.push_dispatch import push_telegram_report_reader

                                    sent2 = await push_telegram_report_reader(
                                        repo=repo,
                                        settings=settings,
                                        idempotency_key=str(per.idempotency_key),
                                        markdown=str(per.markdown),
                                    )
                                    if sent2:
                                        logger.info("telegram reader rerun sent: key=%s topic_id=%s", per.idempotency_key, topic_id)
                            else:
                                res2 = await run_curated_info(
                                    session=repo.session,
                                    settings=settings,
                                    hours=hours2,
                                    push=False,
                                    key_suffix=suffix,
                                )
                                if res2 and (res2.idempotency_key and res2.markdown):
                                    from tracker.push_dispatch import push_telegram_report_reader

                                    sent2 = await push_telegram_report_reader(
                                        repo=repo,
                                        settings=settings,
                                        idempotency_key=str(res2.idempotency_key),
                                        markdown=str(res2.markdown),
                                    )
                                    if sent2:
                                        logger.info("telegram reader rerun sent: key=%s topic_id=0", res2.idempotency_key)
                        except Exception as exc:
                            sent2 = False
                            err = (str(exc) or exc.__class__.__name__).strip()
                            if out_lang == "zh":
                                await _send_ack(f"⚠️ 再发一份失败：{err[:180] + ('…' if len(err) > 180 else '')}")
                            else:
                                await _send_ack(f"⚠️ New batch failed: {err[:180] + ('…' if len(err) > 180 else '')}")
                            continue
                        if not sent2:
                            if out_lang == "zh":
                                await _send_ack("⚠️ 再发一份失败：推送被跳过或结果为空，请稍后重试。")
                            else:
                                await _send_ack("⚠️ New batch failed: push skipped or empty result; retry later.")
                        continue

                    try:
                        from tracker.push.telegram import TelegramPusher

                        p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))

                        text_html = ""
                        kb: dict | None = None

                        if action in {"toc", "menu"}:
                            try:
                                toc_page = int(parts[2]) if len(parts) >= 3 else 0
                            except Exception:
                                toc_page = 0
                            from tracker.telegram_report_reader import render_cover_html

                            text_html, kb = render_cover_html(
                                markdown=md,
                                idempotency_key=report_key,
                                lang=out_lang,
                                toc_page=toc_page,
                                show_feedback=show_feedback,
                            )
                        elif action == "sec":
                            try:
                                sec_idx = int(parts[2]) if len(parts) >= 3 else 0
                            except Exception:
                                sec_idx = 0
                            try:
                                page_i = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                page_i = 0
                            from tracker.telegram_report_reader import render_section_html

                            text_html, kb = render_section_html(
                                markdown=md,
                                section_index=sec_idx,
                                page=page_i,
                                lang=out_lang,
                                show_feedback=show_feedback,
                            )
                        elif action in {"refs", "ref"}:
                            try:
                                page_i = int(parts[2]) if len(parts) >= 3 else 0
                            except Exception:
                                page_i = 0
                            from tracker.telegram_report_reader import render_references_html

                            text_html, kb = render_references_html(
                                markdown=md, page=page_i, lang=out_lang, show_feedback=show_feedback
                            )
                        elif action in {"full"}:
                            try:
                                page_i = int(parts[2]) if len(parts) >= 3 else 0
                            except Exception:
                                page_i = 0
                            if report_key.startswith("digest:"):
                                from tracker.telegram_report_reader import render_digest_full_html

                                text_html, kb = render_digest_full_html(
                                    markdown=md,
                                    page=page_i,
                                    lang=out_lang,
                                    show_feedback=show_feedback,
                                )
                            else:
                                from tracker.telegram_report_reader import render_full_html

                                text_html, kb = render_full_html(
                                    markdown=md,
                                    page=page_i,
                                    lang=out_lang,
                                    show_feedback=show_feedback,
                                )
                        elif action == "fb":
                            if not show_feedback:
                                msg = "⚠️ 已关闭：digest 条目反馈" if out_lang == "zh" else "⚠️ Disabled: digest item feedback"
                                text_html = f"🗳️ <b>{'反馈' if out_lang == 'zh' else 'Feedback'}</b>\n\n{msg}"
                                kb = {
                                    "inline_keyboard": [
                                        [
                                            {
                                                "text": ("⬅️ 目录" if out_lang == "zh" else "⬅️ TOC"),
                                                "callback_data": "br:toc:0",
                                            }
                                        ]
                                    ]
                                }
                            else:
                                # br:fb:<page> | br:fb:<like|dislike|mute>:<n>:<page>
                                sub = parts[2] if len(parts) >= 3 else "0"
                                status = ""
                                page_i = 0
                                if sub.isdigit():
                                    try:
                                        page_i = int(sub)
                                    except Exception:
                                        page_i = 0
                                else:
                                    kind = sub.strip().lower()
                                    try:
                                        ref_n = int(parts[3]) if len(parts) >= 4 else 0
                                    except Exception:
                                        ref_n = 0
                                    try:
                                        page_i = int(parts[4]) if len(parts) >= 5 else 0
                                    except Exception:
                                        page_i = 0

                                    from tracker.telegram_report_reader import parse_reference_entries, parse_report_markdown

                                    doc = parse_report_markdown(md)
                                    ref_entries = parse_reference_entries(doc.references)
                                    ref_map = {int(n): (t, u) for n, t, u in ref_entries}
                                    title2, url2 = ref_map.get(int(ref_n), ("", ""))
                                    domain2 = _domain_from_url(url2)

                                    item_id = 0
                                    try:
                                        item = repo.get_item_by_canonical_url(url2) if url2 else None
                                        item_id = int(getattr(item, "id", 0) or 0) if item else 0
                                    except Exception:
                                        item_id = 0

                                    if kind in {"like", "dislike"} and url2:
                                        ev = repo.add_feedback_event(
                                            channel="telegram",
                                            user_id=uid,
                                            chat_id=existing_chat_id,
                                            message_id=mid,
                                            kind=kind,
                                            value_int=0,
                                            item_id=(item_id if item_id > 0 else None),
                                            url=url2,
                                            domain=domain2,
                                            note="digest_reader",
                                            raw=json.dumps(
                                                {"action": kind, "ref_n": int(ref_n), "title": title2[:200]},
                                                ensure_ascii=False,
                                            ),
                                        )
                                        try:
                                            _apply_source_score_feedback(item_id=(item_id if item_id > 0 else None), feedback_event_id=int(ev.id), kind=kind)
                                        except Exception:
                                            pass
                                        pending_feedback_for_profile.append(int(ev.id))
                                        if out_lang == "zh":
                                            status = (f"{'👍' if kind == 'like' else '👎'} 已记录：#{int(ref_n)}").strip()
                                        else:
                                            status = (f"{'👍' if kind == 'like' else '👎'} recorded: #{int(ref_n)}").strip()
                                    elif kind == "mute" and url2 and domain2:
                                        days = _default_mute_days()
                                        until = dt.datetime.utcnow() + dt.timedelta(days=int(days))
                                        repo.upsert_mute_rule(
                                            scope="domain",
                                            key=domain2,
                                            muted_until=until,
                                            reason=f"telegram digest reader mute #{int(ref_n)}",
                                        )
                                        evm = repo.add_feedback_event(
                                            channel="telegram",
                                            user_id=uid,
                                            chat_id=existing_chat_id,
                                            message_id=mid,
                                            kind="mute",
                                            value_int=int(days),
                                            item_id=(item_id if item_id > 0 else None),
                                            url=url2,
                                            domain=domain2,
                                            note="digest_reader",
                                            raw=json.dumps(
                                                {"action": "mute", "ref_n": int(ref_n), "title": title2[:200]},
                                                ensure_ascii=False,
                                            ),
                                        )
                                        try:
                                            _apply_source_score_feedback(item_id=(item_id if item_id > 0 else None), feedback_event_id=int(getattr(evm, "id", 0) or 0), kind="mute")
                                        except Exception:
                                            pass
                                        status = f"🔕 已静音：{domain2}（{int(days)} 天）" if out_lang == "zh" else f"🔕 muted: {domain2} ({int(days)}d)"
                                    else:
                                        status = f"⚠️ 找不到条目：#{int(ref_n)}" if out_lang == "zh" else f"⚠️ item not found: #{int(ref_n)}"

                                from tracker.telegram_report_reader import render_feedback_html

                                text_html, kb = render_feedback_html(
                                    markdown=md,
                                    page=page_i,
                                    lang=out_lang,
                                    mute_days=_default_mute_days(),
                                    status=status,
                                )
                        else:
                            from tracker.telegram_report_reader import render_cover_html

                            text_html, kb = render_cover_html(
                                markdown=md,
                                idempotency_key=report_key,
                                lang=out_lang,
                                toc_page=0,
                                show_feedback=show_feedback,
                            )

                        if not (text_html and isinstance(kb, dict)):
                            continue

                        try:
                            await p.edit_text(
                                chat_id=existing_chat_id,
                                message_id=mid,
                                text=text_html,
                                parse_mode="HTML",
                                disable_preview=True,
                                reply_markup=kb,
                            )
                        except Exception as exc:
                            from tracker.push.telegram import is_stale_telegram_edit_error

                            if not is_stale_telegram_edit_error(exc):
                                raise
                            # Fallback only when the original reader message is genuinely stale/non-editable.
                            mid2 = await p.send_raw_text(
                                chat_id=existing_chat_id,
                                text=text_html,
                                parse_mode="HTML",
                                disable_preview=True,
                                reply_markup=kb,
                            )
                            if int(mid2 or 0) > 0:
                                try:
                                    repo.ensure_telegram_messages_recorded(
                                        chat_id=existing_chat_id,
                                        idempotency_key=report_key,
                                        message_ids=[int(mid2)],
                                        kind="digest",
                                        item_id=None,
                                    )
                                except Exception as map_exc:
                                    logger.warning(
                                        "telegram reader fallback mapping persist failed: key=%s chat_id=%s mid=%s err=%s",
                                        report_key,
                                        existing_chat_id,
                                        int(mid2),
                                        map_exc,
                                    )
                        continue
                    except Exception as exc:
                        # Best-effort fallback: send a new message when edit fails (e.g. not editable).
                        msg = (str(exc) or "").strip()
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ Reader 操作失败：{msg[:180] + ('…' if len(msg) > 180 else '')}")
                        else:
                            await _send_ack(f"⚠️ Reader failed: {msg[:180] + ('…' if len(msg) > 180 else '')}")
                        continue

                    continue

                # --- Topics menu/actions (website-free)
                if data.startswith("t:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "page":
                        try:
                            page_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _topic_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "toggle":
                        try:
                            topic_id = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            topic_id = 0
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0
                        if topic_id > 0:
                            try:
                                from tracker.models import Topic

                                topic = repo.session.get(Topic, int(topic_id))
                                if topic:
                                    topic.enabled = not bool(getattr(topic, "enabled", False))
                                    repo.session.commit()
                                    if _out_lang() == "zh":
                                        await _send_ack(f"✅ 已更新：{topic.name} enabled={topic.enabled}")
                                    else:
                                        await _send_ack(f"✅ updated: {topic.name} enabled={topic.enabled}")
                            except Exception:
                                pass
                        text, kb = _topic_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "add":
                        # Only keep one pending prompt to avoid confusion.
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="topic_add",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass
                        is_zh = _out_lang() == "zh"
                        prompt = (
                            "添加 Topic：请直接回复这条消息（建议一行一个），格式：\n"
                            "`name | query | digest_cron(可选)`\n\n"
                            "示例：\n"
                            "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                            "取消：回复 0 或 cancel"
                            if is_zh
                            else (
                                "Add Topic: reply to this message with:\n"
                                "`name | query | digest_cron(optional)`\n\n"
                                "Example:\n"
                                "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                                "Cancel: reply 0 or cancel"
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="topic_add",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query="topic_add",
                                )
                            except Exception:
                                pass
                        continue

                    if action == "edit":
                        # Reply-based edit (keeps callback payload compact).
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="topic_edit",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass
                        is_zh = _out_lang() == "zh"
                        prompt = (
                            "编辑 Topic：请直接回复这条消息（建议一行一个），格式：\n"
                            "`name | query | digest_cron(可选)`\n\n"
                            "示例：\n"
                            "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                            "取消：回复 0 或 cancel"
                            if is_zh
                            else (
                                "Edit Topic: reply to this message with:\n"
                                "`name | query | digest_cron(optional)`\n\n"
                                "Example:\n"
                                "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                                "Cancel: reply 0 or cancel"
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="topic_edit",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query="topic_edit",
                                )
                            except Exception:
                                pass
                        continue

                    # Unknown topic action.
                    continue

                # --- Sources / Bindings menu/actions (website-free)
                if data.startswith("s:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "page":
                        try:
                            page_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _sources_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "detail":
                        try:
                            source_id = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            source_id = 0
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _source_details(source_id=source_id, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "toggle":
                        try:
                            source_id = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            source_id = 0
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0

                        if source_id > 0:
                            try:
                                from tracker.models import Source

                                src = repo.session.get(Source, int(source_id))
                                if src:
                                    src.enabled = not bool(getattr(src, "enabled", False))
                                    repo.session.commit()
                                    if _out_lang() == "zh":
                                        await _send_ack(f"✅ 已更新：Source #{int(src.id)} enabled={src.enabled}")
                                    else:
                                        await _send_ack(f"✅ updated: Source #{int(src.id)} enabled={src.enabled}")
                            except Exception:
                                pass

                        text, kb = _source_details(source_id=source_id, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "add":
                        # Reply-based bulk import (URLs).
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="source_import",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass
                        is_zh = _out_lang() == "zh"
                        prompt = (
                            "添加 Sources（RSS/Atom）：请直接回复这条消息粘贴 URL（可多行）。\n"
                            "我会先给出导入预览，再让你确认。\n\n"
                            "取消：回复 0 或 cancel"
                            if is_zh
                            else "Add sources (RSS/Atom): reply with URLs (multi-line ok). You'll get a preview before applying. Cancel: reply 0 or cancel"
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="source_import",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query="source_import",
                                )
                            except Exception:
                                pass
                        continue

                    if action == "bind":
                        sub = parts[2] if len(parts) >= 3 else ""
                        if sub in {"menu", "page"}:
                            try:
                                source_id = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                source_id = 0
                            try:
                                src_page = int(parts[4]) if len(parts) >= 5 else 0
                            except Exception:
                                src_page = 0
                            try:
                                page_i = int(parts[5]) if len(parts) >= 6 else 0
                            except Exception:
                                page_i = 0
                            text, kb = _source_bind_menu(source_id=source_id, src_page=src_page, page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        if sub == "toggle":
                            try:
                                source_id = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                source_id = 0
                            try:
                                topic_id = int(parts[4]) if len(parts) >= 5 else 0
                            except Exception:
                                topic_id = 0
                            try:
                                src_page = int(parts[5]) if len(parts) >= 6 else 0
                            except Exception:
                                src_page = 0
                            try:
                                page_i = int(parts[6]) if len(parts) >= 7 else 0
                            except Exception:
                                page_i = 0

                            if source_id > 0 and topic_id > 0:
                                try:
                                    from tracker.models import Source, Topic

                                    src = repo.session.get(Source, int(source_id))
                                    topic = repo.session.get(Topic, int(topic_id))
                                    if src and topic:
                                        ts = repo.get_topic_source(topic_id=int(topic.id), source_id=int(src.id))
                                        if ts:
                                            repo.unbind_topic_source(topic=topic, source=src)
                                            if _out_lang() == "zh":
                                                await _send_ack(f"✅ 已解绑：{topic.name} ← Source #{int(src.id)}")
                                            else:
                                                await _send_ack(f"✅ unbound: {topic.name} <- Source #{int(src.id)}")
                                        else:
                                            repo.bind_topic_source(topic=topic, source=src)
                                            if _out_lang() == "zh":
                                                await _send_ack(f"✅ 已绑定：{topic.name} ← Source #{int(src.id)}")
                                            else:
                                                await _send_ack(f"✅ bound: {topic.name} <- Source #{int(src.id)}")
                                except Exception:
                                    pass

                            text, kb = _source_bind_menu(source_id=source_id, src_page=src_page, page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        continue

                    if action == "imp":
                        # Apply a prepared import draft stored in a TelegramTask.
                        sub = parts[2] if len(parts) >= 3 else ""
                        t_imp = repo.get_telegram_task_by_prompt_message(
                            chat_id=existing_chat_id,
                            prompt_message_id=mid,
                            kind="source_import_confirm",
                        )
                        if not t_imp or (t_imp.status or "").strip() != "awaiting":
                            continue

                        draft: dict[str, object] = {}
                        try:
                            draft = json.loads(str(t_imp.query or "") or "{}") if (t_imp.query or "").strip() else {}
                        except Exception:
                            draft = {}

                        urls = [str(u).strip() for u in (draft.get("urls") if isinstance(draft, dict) else []) or []]
                        urls = [u for u in urls if u]
                        if not urls:
                            repo.mark_telegram_task_failed(int(t_imp.id), error="empty import urls")
                            await _send_ack("⚠️ empty import" if _out_lang() != "zh" else "⚠️ 导入为空")
                            continue

                        if sub in {"cancel", "c"}:
                            repo.mark_telegram_task_canceled(int(t_imp.id), reason="user_canceled")
                            if _out_lang() == "zh":
                                await _send_ack("✅ 已取消导入")
                            else:
                                await _send_ack("✅ import canceled")
                            continue

                        if sub == "pick_topic":
                            try:
                                page_i = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                page_i = 0

                            # Supersede any existing picker task.
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="source_import_topic",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass

                            is_zh = _out_lang() == "zh"
                            topics = repo.list_topics()
                            page_size = 8
                            total = len(topics)
                            max_page = max(0, ((total - 1) // page_size) if total else 0)
                            page_i2 = max(0, min(page_i, max_page))
                            start = page_i2 * page_size
                            chunk = topics[start : start + page_size]
                            header = (
                                f"选择要绑定的 Topic（{start + 1}-{start + len(chunk)} / {total}）"
                                if is_zh
                                else f"Pick a Topic to bind ({start + 1}-{start + len(chunk)} / {total})"
                            )
                            lines2: list[str] = [header if total else (("选择要绑定的 Topic（0）" if is_zh else "Pick a Topic to bind (0)")), ""]
                            for t in chunk:
                                lines2.append(f"- {t.name}")
                            kb_rows: list[list[dict[str, str]]] = []
                            row: list[dict[str, str]] = []

                            def _short(s: str, n: int) -> str:
                                s2 = (s or "").strip()
                                if len(s2) <= n:
                                    return s2
                                return s2[: max(0, n - 1)] + "…"

                            for t in chunk:
                                label = _short(str(getattr(t, "name", "") or ""), 28) or "topic"
                                row.append({"text": label, "callback_data": f"s:imp_topic:select:{int(t.id)}:{page_i2}"})
                                if len(row) >= 2:
                                    kb_rows.append(row)
                                    row = []
                            if row:
                                kb_rows.append(row)

                            nav: list[dict[str, str]] = []
                            if page_i2 > 0:
                                nav.append(
                                    {
                                        "text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"),
                                        "callback_data": f"s:imp_topic:page:{page_i2 - 1}",
                                    }
                                )
                            if page_i2 < max_page:
                                nav.append(
                                    {
                                        "text": ("下一页 ➡️" if is_zh else "Next ➡️"),
                                        "callback_data": f"s:imp_topic:page:{page_i2 + 1}",
                                    }
                                )
                            if nav:
                                kb_rows.append(nav)
                            kb_rows.append(
                                [
                                    {"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": "s:imp_topic:cancel"},
                                    {"text": ("Cancel" if not is_zh else "取消"), "callback_data": "s:imp_topic:cancel"},
                                ]
                            )
                            mid2 = await _send_with_markup(text="\n".join(lines2).strip(), reply_markup={"inline_keyboard": kb_rows})
                            if mid2 > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="source_import_topic",
                                        status="awaiting",
                                        prompt_message_id=mid2,
                                        request_message_id=mid,
                                        query=json.dumps(draft, ensure_ascii=False),
                                    )
                                    repo.mark_telegram_task_done(int(t_imp.id), result_key="pick_topic")
                                except Exception:
                                    pass
                            continue

                        if sub == "apply":
                            mode = (parts[3] if len(parts) >= 4 else "").strip().lower()
                            topic_name = ""
                            if mode == "profile":
                                topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"

                            try:
                                from tracker.actions import SourceBindingSpec, create_rss_sources_bulk

                                bind = SourceBindingSpec(topic=topic_name) if topic_name else None
                                created, bound = create_rss_sources_bulk(
                                    session=repo.session,
                                    urls=urls,
                                    bind=bind,
                                    notes="imported via telegram",
                                )
                                repo.mark_telegram_task_done(int(t_imp.id), result_key="imported")
                                if _out_lang() == "zh":
                                    tail = f"，已绑定 {bound}" if topic_name else ""
                                    await _send_ack(f"✅ 已导入 sources：新建 {created}{tail}")
                                else:
                                    tail = f", bound {bound}" if topic_name else ""
                                    await _send_ack(f"✅ imported sources: created {created}{tail}")
                            except Exception as exc:
                                repo.mark_telegram_task_failed(int(t_imp.id), error=str(exc)[:4000])
                                if _out_lang() == "zh":
                                    await _send_ack(f"⚠️ 导入失败：{exc}")
                                else:
                                    await _send_ack(f"⚠️ import failed: {exc}")
                            continue

                        continue

                    if action == "imp_topic":
                        sub = parts[2] if len(parts) >= 3 else ""
                        t_pick = repo.get_telegram_task_by_prompt_message(
                            chat_id=existing_chat_id,
                            prompt_message_id=mid,
                            kind="source_import_topic",
                        )
                        if not t_pick or (t_pick.status or "").strip() != "awaiting":
                            continue

                        draft: dict[str, object] = {}
                        try:
                            draft = json.loads(str(t_pick.query or "") or "{}") if (t_pick.query or "").strip() else {}
                        except Exception:
                            draft = {}
                        urls = [str(u).strip() for u in (draft.get("urls") if isinstance(draft, dict) else []) or []]
                        urls = [u for u in urls if u]

                        if sub in {"cancel", "c"}:
                            repo.mark_telegram_task_canceled(int(t_pick.id), reason="user_canceled")
                            if _out_lang() == "zh":
                                await _send_ack("✅ 已取消选择 Topic")
                            else:
                                await _send_ack("✅ topic pick canceled")
                            text, kb = _sources_menu(page=0)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        if sub == "page":
                            try:
                                page_i = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                page_i = 0

                            # Supersede any existing picker task (including this one).
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="source_import_topic",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass

                            is_zh = _out_lang() == "zh"
                            topics = repo.list_topics()
                            page_size = 8
                            total = len(topics)
                            max_page = max(0, ((total - 1) // page_size) if total else 0)
                            page_i2 = max(0, min(page_i, max_page))
                            start = page_i2 * page_size
                            chunk = topics[start : start + page_size]
                            header = (
                                f"选择要绑定的 Topic（{start + 1}-{start + len(chunk)} / {total}）"
                                if is_zh
                                else f"Pick a Topic to bind ({start + 1}-{start + len(chunk)} / {total})"
                            )
                            lines2: list[str] = [
                                header if total else (("选择要绑定的 Topic（0）" if is_zh else "Pick a Topic to bind (0)")),
                                "",
                            ]
                            for t in chunk:
                                lines2.append(f"- {t.name}")

                            def _short(s: str, n: int) -> str:
                                s2 = (s or "").strip()
                                if len(s2) <= n:
                                    return s2
                                return s2[: max(0, n - 1)] + "…"

                            kb_rows: list[list[dict[str, str]]] = []
                            row: list[dict[str, str]] = []
                            for t in chunk:
                                label = _short(str(getattr(t, "name", "") or ""), 28) or "topic"
                                row.append({"text": label, "callback_data": f"s:imp_topic:select:{int(t.id)}:{page_i2}"})
                                if len(row) >= 2:
                                    kb_rows.append(row)
                                    row = []
                            if row:
                                kb_rows.append(row)

                            nav: list[dict[str, str]] = []
                            if page_i2 > 0:
                                nav.append(
                                    {
                                        "text": ("⬅️ 上一页" if is_zh else "⬅️ Prev"),
                                        "callback_data": f"s:imp_topic:page:{page_i2 - 1}",
                                    }
                                )
                            if page_i2 < max_page:
                                nav.append(
                                    {
                                        "text": ("下一页 ➡️" if is_zh else "Next ➡️"),
                                        "callback_data": f"s:imp_topic:page:{page_i2 + 1}",
                                    }
                                )
                            if nav:
                                kb_rows.append(nav)
                            kb_rows.append(
                                [
                                    {"text": ("⬅️ 返回" if is_zh else "⬅️ Back"), "callback_data": "s:imp_topic:cancel"},
                                    {"text": ("Cancel" if not is_zh else "取消"), "callback_data": "s:imp_topic:cancel"},
                                ]
                            )
                            mid2 = await _send_with_markup(
                                text="\n".join(lines2).strip(),
                                reply_markup={"inline_keyboard": kb_rows},
                            )
                            if mid2 > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="source_import_topic",
                                        status="awaiting",
                                        prompt_message_id=mid2,
                                        request_message_id=mid,
                                        query=json.dumps(draft, ensure_ascii=False),
                                    )
                                except Exception:
                                    pass
                            continue

                        if sub == "select":
                            try:
                                topic_id = int(parts[3]) if len(parts) >= 4 else 0
                            except Exception:
                                topic_id = 0
                            try:
                                from tracker.models import Topic

                                topic = repo.session.get(Topic, int(topic_id))
                            except Exception:
                                topic = None
                            if not topic:
                                await _send_ack("⚠️ topic not found" if _out_lang() != "zh" else "⚠️ topic 不存在")
                                continue

                            try:
                                from tracker.actions import SourceBindingSpec, create_rss_sources_bulk

                                created, bound = create_rss_sources_bulk(
                                    session=repo.session,
                                    urls=urls,
                                    bind=SourceBindingSpec(topic=str(topic.name)),
                                    notes="imported via telegram",
                                )
                                repo.mark_telegram_task_done(int(t_pick.id), result_key="imported")
                                if _out_lang() == "zh":
                                    await _send_ack(f"✅ 已导入 sources：新建 {created}，已绑定 {bound}（topic={topic.name}）")
                                else:
                                    await _send_ack(f"✅ imported sources: created {created}, bound {bound} (topic={topic.name})")
                            except Exception as exc:
                                repo.mark_telegram_task_failed(int(t_pick.id), error=str(exc)[:4000])
                                if _out_lang() == "zh":
                                    await _send_ack(f"⚠️ 导入失败：{exc}")
                                else:
                                    await _send_ack(f"⚠️ import failed: {exc}")
                            continue

                        continue

                    # Unknown sources action.
                    continue

                if data.startswith("b:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "page":
                        try:
                            page_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _bindings_topic_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "topic":
                        try:
                            topic_id = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            topic_id = 0
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _topic_bindings_menu(topic_id=topic_id, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "unbind":
                        try:
                            topic_id = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            topic_id = 0
                        try:
                            source_id = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            source_id = 0
                        try:
                            page_i = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            page_i = 0

                        if topic_id > 0 and source_id > 0:
                            try:
                                from tracker.models import Source, Topic

                                topic = repo.session.get(Topic, int(topic_id))
                                src = repo.session.get(Source, int(source_id))
                                if topic and src:
                                    repo.unbind_topic_source(topic=topic, source=src)
                                    if _out_lang() == "zh":
                                        await _send_ack(f"✅ 已解绑：{topic.name} ← Source #{int(src.id)}")
                                    else:
                                        await _send_ack(f"✅ unbound: {topic.name} <- Source #{int(src.id)}")
                            except Exception:
                                pass

                        text, kb = _topic_bindings_menu(topic_id=topic_id, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    continue

                # --- Config Center (v2, registry-driven; same registry as Web Admin)
                if data.startswith("cfgc:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action in {"menu", "home"}:
                        text, kb = _cfgc_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "restart":
                        from tracker.service_control import queue_restart_systemd_user, restart_hint_text

                        res = queue_restart_systemd_user(units=["tracker", "tracker-api"], delay_seconds=0.8)
                        if res.ok:
                            await _send_ack("♻️ 已排队重启：tracker + tracker-api" if _out_lang() == "zh" else "♻️ Restart queued: tracker + tracker-api")
                        else:
                            await _send_ack(
                                (f"⚠️ 自动重启失败：{res.message}\n{restart_hint_text(lang='zh', units=res.units)}")
                                if _out_lang() == "zh"
                                else f"⚠️ Auto restart failed: {res.message}\n{restart_hint_text(lang='en', units=res.units)}"
                            )
                        continue

                    if action == "sec":
                        sid = (parts[2] if len(parts) >= 3 else "").strip()
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _cfgc_section_menu(section_id=sid, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "field":
                        sid = (parts[2] if len(parts) >= 3 else "").strip()
                        field = (parts[3] if len(parts) >= 4 else "").strip()
                        try:
                            page_i = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _cfgc_field_menu(section_id=sid, field=field, section_page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "set":
                        sid = (parts[2] if len(parts) >= 3 else "").strip()
                        field = (parts[3] if len(parts) >= 4 else "").strip()
                        value = (parts[4] if len(parts) >= 5 else "").strip()
                        try:
                            page_i = int(parts[5]) if len(parts) >= 6 else 0
                        except Exception:
                            page_i = 0

                        if not field:
                            text, kb = _cfgc_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        if field in _CFG_C_DANGEROUS_FIELDS:
                            await _send_ack("⚠️ 该字段为危险项，请用 /api 或 SSH/CLI。" if _out_lang() == "zh" else "⚠️ Dangerous key: use /api or SSH/CLI.")
                            text, kb = _cfgc_field_menu(section_id=sid, field=field, section_page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            from pathlib import Path

                            from tracker.admin_settings import parse_settings_patch_form
                            from tracker.dynamic_config import apply_env_block_updates

                            updates, errors = parse_settings_patch_form(form={field: value}, repo=repo, settings=settings)
                            if errors:
                                await _send_ack("⚠️ 配置不合法" if _out_lang() == "zh" else "⚠️ invalid config")
                            elif not updates:
                                await _send_ack("✅ 无变化" if _out_lang() == "zh" else "✅ no changes")
                            else:
                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates=updates,
                                )
                                try:
                                    repo.add_settings_change(
                                        source="tg_cfgc_set",
                                        fields=[field],
                                        env_keys=list(res.updated_env_keys),
                                        restart_required=bool(res.restart_required),
                                        actor=f"tg:{uid}",
                                        client_host="telegram",
                                    )
                                except Exception:
                                    pass
                                keys = ", ".join(sorted(res.updated_env_keys))
                                if _out_lang() == "zh":
                                    tail = "（重启服务后生效：/restart）" if res.restart_required else "（无需重启）"
                                    await _send_ack(f"✅ 已更新：{keys}{tail}")
                                else:
                                    tail = " (/restart to apply)" if res.restart_required else " (no restart needed)"
                                    await _send_ack(f"✅ updated: {keys}{tail}")
                        except Exception as exc:
                            await _send_ack(f"⚠️ 写入失败：{exc}" if _out_lang() == "zh" else f"⚠️ apply failed: {exc}")

                        text, kb = _cfgc_field_menu(section_id=sid, field=field, section_page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "edit":
                        sid = (parts[2] if len(parts) >= 3 else "").strip()
                        field = (parts[3] if len(parts) >= 4 else "").strip()
                        try:
                            page_i = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            page_i = 0

                        if not field:
                            continue
                        if field in _CFG_C_DANGEROUS_FIELDS:
                            await _send_ack("⚠️ 该字段为危险项，请用 /api 或 SSH/CLI。" if _out_lang() == "zh" else "⚠️ Dangerous key: use /api or SSH/CLI.")
                            text, kb = _cfgc_field_menu(section_id=sid, field=field, section_page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="cfgc_set",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        label = field
                        try:
                            ui = _cfgc_build_ui()
                            v = ((ui or {}).get("views") or {}).get(field) if isinstance(ui, dict) else None
                            if isinstance(v, dict):
                                label = ui_t(_out_lang(), str(v.get("label") or field))
                        except Exception:
                            label = field

                        prompt = (
                            f"设置：{label}\n"
                            f"- 字段：{field}\n"
                            "请回复这条消息填写新值。\n"
                            "清空/禁用：回复 off / disable\n"
                            "取消：回复 0 或 cancel\n"
                            "（密钥推荐用 /env 粘贴 TRACKER_*，bot 不会回显密钥）"
                            if _out_lang() == "zh"
                            else (
                                f"Set: {label}\n"
                                f"- field: {field}\n"
                                "Reply to this message with the new value.\n"
                                "Clear/disable: reply off / disable\n"
                                "Cancel: reply 0 or cancel\n"
                                "(For secrets, prefer /env with TRACKER_*; the bot won't echo secrets.)"
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="cfgc_set",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=json.dumps({"section_id": sid, "field": field, "page": int(page_i or 0)}),
                                )
                            except Exception:
                                pass
                        continue

                    # Unknown cfgc action -> refresh menu.
                    text, kb = _cfgc_menu()
                    await _send_with_markup(text=text, reply_markup=kb)
                    continue

                # --- Config menu/actions (website-free)
                if data.startswith("cfg:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _config_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "lang":
                        lang = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if lang in {"zh", "en"}:
                            res = None
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={"TRACKER_OUTPUT_LANGUAGE": lang},
                                )
                            except Exception:
                                pass
                            if _out_lang() == "zh":
                                tail = "（重启服务后生效）" if (res and res.restart_required) else "（无需重启）"
                                await _send_ack(f"✅ 已设置 output_language={lang}{tail}")
                            else:
                                tail = " (restart services to apply)" if (res and res.restart_required) else " (no restart needed)"
                                await _send_ack(f"✅ set output_language={lang}{tail}")
                        text, kb = _config_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "tz":
                        tz = (parts[2] if len(parts) >= 3 else "").strip()
                        if tz.lower() == "custom":
                            # Prompt for a reply (e.g. "Asia/Shanghai").
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="config_set_tz",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass
                            prompt = (
                                "设置时区：请回复这条消息，填 IANA 时区名（如 Asia/Shanghai）。\n"
                                "取消：回复 0 或 cancel"
                                if _out_lang() == "zh"
                                else "Set timezone: reply with an IANA timezone name (e.g. Asia/Shanghai). Cancel: reply 0 or cancel"
                            )
                            prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                            if prompt_mid > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="config_set_tz",
                                        status="awaiting",
                                        prompt_message_id=prompt_mid,
                                        request_message_id=mid,
                                        query="cron_timezone",
                                    )
                                except Exception:
                                    pass
                            continue

                        if tz:
                            res = None
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={"TRACKER_CRON_TIMEZONE": tz},
                                )
                                if _out_lang() == "zh":
                                    tail = "（重启服务后生效）" if (res and res.restart_required) else ""
                                    await _send_ack(f"✅ 已更新：TRACKER_CRON_TIMEZONE{tail}")
                                else:
                                    tail = " (restart services to apply)" if (res and res.restart_required) else ""
                                    await _send_ack(f"✅ updated: TRACKER_CRON_TIMEZONE{tail}")
                            except Exception:
                                if _out_lang() == "zh":
                                    await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                                else:
                                    await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")

                        text, kb = _config_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "mute":
                        sub = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if sub == "custom":
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="config_set_mute_days",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass
                            prompt = (
                                "设置默认静音天数：请回复这条消息，填整数（1-365）。\n"
                                "取消：回复 0 或 cancel"
                                if _out_lang() == "zh"
                                else "Set default mute days: reply with an integer (1-365). Cancel: reply 0 or cancel"
                            )
                            prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                            if prompt_mid > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="config_set_mute_days",
                                        status="awaiting",
                                        prompt_message_id=prompt_mid,
                                        request_message_id=mid,
                                        query="telegram_feedback_mute_days_default",
                                    )
                                except Exception:
                                    pass
                            continue

                        # Fixed options: cfg:mute:7 / cfg:mute:14
                        try:
                            n = int(sub)
                        except Exception:
                            n = 0
                        if n > 0:
                            n = max(1, min(365, int(n)))
                            try:
                                repo.set_app_config("telegram_feedback_mute_days_default", str(n))
                            except Exception:
                                pass
                            if _out_lang() == "zh":
                                await _send_ack(f"✅ 已设置默认静音天数：{n} 天")
                            else:
                                await _send_ack(f"✅ set default mute days: {n}d")
                        text, kb = _config_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    continue

                # --- API bind menu/actions (website-free)
                if data.startswith("api:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _api_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "auth":
                        sub = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if sub not in {"token", "password"}:
                            text, kb = _api_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        env_key = "TRACKER_API_TOKEN" if sub == "token" else "TRACKER_ADMIN_PASSWORD"
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="api_set_auth",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"API/Auth：请回复这条消息，填 `{env_key}` 的值。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：密钥不会被 bot 回显；但你的回复仍会出现在聊天记录里（谨慎）。"
                            if is_zh
                            else (
                                f"API/Auth: reply with the value for `{env_key}`.\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: the bot won't echo secrets, but your reply is still in chat history."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="api_set_auth",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=env_key,
                                )
                            except Exception:
                                pass
                        continue

                    if action == "host":
                        host = (parts[2] if len(parts) >= 3 else "").strip()
                        if host.lower() == "custom":
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="api_set_host",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass
                            prompt = (
                                "设置 API host：请回复这条消息（建议用 127.0.0.1 或 0.0.0.0）。\n"
                                "取消：回复 0 或 cancel"
                                if _out_lang() == "zh"
                                else "Set API host: reply with a bind host (e.g. 127.0.0.1 or 0.0.0.0). Cancel: reply 0 or cancel"
                            )
                            prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                            if prompt_mid > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="api_set_host",
                                        status="awaiting",
                                        prompt_message_id=prompt_mid,
                                        request_message_id=mid,
                                        query="api_host",
                                    )
                                except Exception:
                                    pass
                            continue

                        host2 = host.strip()
                        low = host2.lower()
                        if not host2 or any(ch.isspace() for ch in host2) or len(host2) > 128:
                            await _send_ack("⚠️ host 不合法" if _out_lang() == "zh" else "⚠️ invalid host")
                            text, kb = _api_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        is_loopback = low in {"127.0.0.1", "::1", "localhost"}
                        if not is_loopback:
                            env = _read_env_assignments()
                            token_set = bool((env.get("TRACKER_API_TOKEN") or "").strip())
                            pw_set = bool((env.get("TRACKER_ADMIN_PASSWORD") or "").strip())
                            if not (token_set or pw_set):
                                await _send_ack(
                                    "⚠️ 绑定到 0.0.0.0 前，需要先配置 TRACKER_API_TOKEN 或 TRACKER_ADMIN_PASSWORD（在 /api 里点“设置 API token / 设置 Admin 密码”，或用 /env；否则 tracker-api 会拒绝启动）"
                                    if _out_lang() == "zh"
                                    else (
                                        "⚠️ Before binding to 0.0.0.0, set TRACKER_API_TOKEN or TRACKER_ADMIN_PASSWORD "
                                        "(use 'Set API token / Set Admin password' in /api, or /env; otherwise tracker-api refuses to start)"
                                    )
                                )
                                text, kb = _api_menu()
                                await _send_with_markup(text=text, reply_markup=kb)
                                continue

                        try:
                            from pathlib import Path

                            from tracker.dynamic_config import apply_env_block_updates

                            res = apply_env_block_updates(
                                repo=repo,
                                settings=settings,
                                env_path=Path(_env_path()),
                                env_updates={"TRACKER_API_HOST": host2},
                            )
                            if _out_lang() == "zh":
                                tail = "（重启 tracker-api 后生效：/restart）" if res.restart_required else ""
                                await _send_ack(f"✅ 已更新：TRACKER_API_HOST={host2}{tail}")
                            else:
                                tail = " (restart tracker-api to apply: /restart)" if res.restart_required else ""
                                await _send_ack(f"✅ updated: TRACKER_API_HOST={host2}{tail}")
                        except Exception:
                            await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")

                        text, kb = _api_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "port":
                        raw = (parts[2] if len(parts) >= 3 else "").strip()
                        if raw.lower() == "custom":
                            try:
                                repo.cancel_telegram_tasks(
                                    chat_id=existing_chat_id,
                                    kind="api_set_port",
                                    status="awaiting",
                                    reason="superseded",
                                )
                            except Exception:
                                pass
                            prompt = (
                                "设置 API port：请回复这条消息（如 8080 或 8899）。\n"
                                "取消：回复 0 或 cancel"
                                if _out_lang() == "zh"
                                else "Set API port: reply with a port number (e.g. 8080 or 8899). Cancel: reply 0 or cancel"
                            )
                            prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                            if prompt_mid > 0:
                                try:
                                    repo.create_telegram_task(
                                        chat_id=existing_chat_id,
                                        user_id=uid,
                                        kind="api_set_port",
                                        status="awaiting",
                                        prompt_message_id=prompt_mid,
                                        request_message_id=mid,
                                        query="api_port",
                                    )
                                except Exception:
                                    pass
                            continue

                        try:
                            port = int(raw)
                        except Exception:
                            port = 0
                        if port < 1 or port > 65535:
                            await _send_ack("⚠️ port 不合法" if _out_lang() == "zh" else "⚠️ invalid port")
                            text, kb = _api_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            from pathlib import Path

                            from tracker.dynamic_config import apply_env_block_updates

                            res = apply_env_block_updates(
                                repo=repo,
                                settings=settings,
                                env_path=Path(_env_path()),
                                env_updates={"TRACKER_API_PORT": str(port)},
                            )
                            if _out_lang() == "zh":
                                tail = "（重启 tracker-api 后生效：/restart）" if res.restart_required else ""
                                await _send_ack(f"✅ 已更新：TRACKER_API_PORT={port}{tail}")
                            else:
                                tail = " (restart tracker-api to apply: /restart)" if res.restart_required else ""
                                await _send_ack(f"✅ updated: TRACKER_API_PORT={port}{tail}")
                        except Exception:
                            await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")

                        text, kb = _api_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    continue

                # --- Push menu/actions (website-free)
                if data.startswith("push:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _push_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "bool":
                        env_key = (parts[2] if len(parts) >= 3 else "").strip()
                        v = (parts[3] if len(parts) >= 4 else "").strip().lower()
                        if not (env_key.startswith("TRACKER_") and v in {"true", "false"}):
                            text, kb = _push_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        try:
                            from pathlib import Path

                            from tracker.dynamic_config import apply_env_block_updates

                            res = apply_env_block_updates(
                                repo=repo,
                                settings=settings,
                                env_path=Path(_env_path()),
                                env_updates={env_key: v},
                            )
                            if _out_lang() == "zh":
                                tail = "（重启服务后生效：/restart）" if res.restart_required else "（无需重启）"
                                await _send_ack(f"✅ 已设置 {env_key}={v}{tail}")
                            else:
                                tail = " (restart services to apply: /restart)" if res.restart_required else " (no restart needed)"
                                await _send_ack(f"✅ set {env_key}={v}{tail}")
                        except Exception:
                            await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")
                        text, kb = _push_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "set":
                        env_key = (parts[2] if len(parts) >= 3 else "").strip()
                        if not env_key.startswith("TRACKER_"):
                            text, kb = _push_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="push_set",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"Push 配置：请回复这条消息，填 `{env_key}` 的值。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：密钥不会被 bot 回显；但你的回复仍会出现在聊天记录里（谨慎）。"
                            if is_zh
                            else (
                                f"Push config: reply with the value for `{env_key}`.\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: the bot won't echo secrets, but your reply is still in chat history."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="push_set",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=env_key,
                                )
                            except Exception:
                                pass
                        continue

                    text, kb = _push_menu()
                    await _send_with_markup(text=text, reply_markup=kb)
                    continue

                # --- Auth menu/actions (website-free)
                if data.startswith("auth:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _auth_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "set":
                        env_key = (parts[2] if len(parts) >= 3 else "").strip()
                        if env_key not in _TG_AUTH_ALLOWED_KEYS:
                            text, kb = _auth_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="auth_set",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"Auth 配置：请回复这条消息，填 `{env_key}` 的值。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：密钥不会被 bot 回显；但你的回复仍会出现在聊天记录里（谨慎）。"
                            if is_zh
                            else (
                                f"Auth config: reply with the value for `{env_key}`.\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: the bot won't echo secrets, but your reply is still in chat history."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="auth_set",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=env_key,
                                )
                            except Exception:
                                pass
                        continue

                    text, kb = _auth_menu()
                    await _send_with_markup(text=text, reply_markup=kb)
                    continue

                # --- Research menu/actions (website-free)
                # --- Prompts (slot bindings + template overrides)
                if data.startswith("pr:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action in {"menu", "home"}:
                        text, kb = _prompts_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "slots":
                        try:
                            page_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _prompts_slots_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "slot":
                        try:
                            slot_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            slot_i = 0
                        try:
                            slots_page = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            slots_page = 0
                        try:
                            tpl_page = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            tpl_page = 0
                        text, kb = _prompts_slot_detail(slot_index=slot_i, slots_page=slots_page, tpl_page=tpl_page)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "bind":
                        try:
                            slot_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            slot_i = 0
                        try:
                            tpl_i = int(parts[3]) if len(parts) >= 4 else -1
                        except Exception:
                            tpl_i = -1
                        try:
                            slots_page = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            slots_page = 0
                        try:
                            tpl_page = int(parts[5]) if len(parts) >= 6 else 0
                        except Exception:
                            tpl_page = 0

                        slots = _prompt_slots_all()
                        templates = _prompt_templates_all()
                        if slot_i < 0 or slot_i >= len(slots):
                            text, kb = _prompts_slots_menu(page=slots_page)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        slot_id = str(slots[slot_i].get("id") or "").strip()
                        template_id = ""
                        if tpl_i >= 0 and tpl_i < len(templates):
                            template_id = str(templates[tpl_i].get("id") or "").strip()

                        try:
                            from tracker.prompt_templates import load_bindings, save_bindings

                            bindings = load_bindings(repo)
                            if not template_id:
                                bindings.pop(slot_id, None)
                            else:
                                bindings[slot_id] = template_id
                            save_bindings(repo, bindings)
                            if _out_lang() == "zh":
                                await _send_ack(f"✅ 已更新绑定：{slot_id} → {template_id or 'Default'}")
                            else:
                                await _send_ack(f"✅ binding updated: {slot_id} -> {template_id or 'Default'}")
                        except Exception as exc:
                            err = str(exc) or exc.__class__.__name__
                            if _out_lang() == "zh":
                                await _send_ack(f"⚠️ 绑定保存失败：{err}")
                            else:
                                await _send_ack(f"⚠️ binding save failed: {err}")

                        text, kb = _prompts_slot_detail(slot_index=slot_i, slots_page=slots_page, tpl_page=tpl_page)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "edit":
                        try:
                            tpl_i = int(parts[2]) if len(parts) >= 3 else -1
                        except Exception:
                            tpl_i = -1
                        lang_code = (parts[3] if len(parts) >= 4 else "zh").strip().lower()
                        if lang_code not in {"zh", "en"}:
                            lang_code = "zh"
                        try:
                            slot_i = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            slot_i = 0
                        try:
                            slots_page = int(parts[5]) if len(parts) >= 6 else 0
                        except Exception:
                            slots_page = 0
                        try:
                            tpl_page = int(parts[6]) if len(parts) >= 7 else 0
                        except Exception:
                            tpl_page = 0

                        templates = _prompt_templates_all()
                        if tpl_i < 0 or tpl_i >= len(templates):
                            text, kb = _prompts_slot_detail(slot_index=slot_i, slots_page=slots_page, tpl_page=tpl_page)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        template_id = str(templates[tpl_i].get("id") or "").strip()
                        if not template_id:
                            text, kb = _prompts_slot_detail(slot_index=slot_i, slots_page=slots_page, tpl_page=tpl_page)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="prompt_template_edit",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"模板编辑：请回复这条消息，粘贴 `{template_id}` 的新内容（{lang_code}）。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：保存后对下一轮任务生效。"
                            if is_zh
                            else (
                                f"Template edit: reply with new content for `{template_id}` ({lang_code}).\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: changes apply to the next run."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="prompt_template_edit",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=json.dumps({"template_id": template_id, "lang": lang_code}, ensure_ascii=False),
                                )
                            except Exception:
                                pass
                        continue

                    if action == "tpl":
                        try:
                            page_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _prompts_templates_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "tplv":
                        try:
                            tpl_i = int(parts[2]) if len(parts) >= 3 else 0
                        except Exception:
                            tpl_i = 0
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0
                        text, kb = _prompts_template_detail(template_index=tpl_i, page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "edit2":
                        try:
                            tpl_i = int(parts[2]) if len(parts) >= 3 else -1
                        except Exception:
                            tpl_i = -1
                        lang_code = (parts[3] if len(parts) >= 4 else "zh").strip().lower()
                        if lang_code not in {"zh", "en"}:
                            lang_code = "zh"
                        try:
                            page_i = int(parts[4]) if len(parts) >= 5 else 0
                        except Exception:
                            page_i = 0

                        templates = _prompt_templates_all()
                        if tpl_i < 0 or tpl_i >= len(templates):
                            text, kb = _prompts_templates_menu(page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        template_id = str(templates[tpl_i].get("id") or "").strip()
                        if not template_id:
                            text, kb = _prompts_templates_menu(page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="prompt_template_edit",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"模板编辑：请回复这条消息，粘贴 `{template_id}` 的新内容（{lang_code}）。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：保存后对下一轮任务生效。"
                            if is_zh
                            else (
                                f"Template edit: reply with new content for `{template_id}` ({lang_code}).\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: changes apply to the next run."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="prompt_template_edit",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=json.dumps({"template_id": template_id, "lang": lang_code}, ensure_ascii=False),
                                )
                            except Exception:
                                pass
                        continue

                    if action == "del":
                        try:
                            tpl_i = int(parts[2]) if len(parts) >= 3 else -1
                        except Exception:
                            tpl_i = -1
                        try:
                            page_i = int(parts[3]) if len(parts) >= 4 else 0
                        except Exception:
                            page_i = 0

                        templates = _prompt_templates_all()
                        if tpl_i < 0 or tpl_i >= len(templates):
                            text, kb = _prompts_templates_menu(page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue
                        template_id = str(templates[tpl_i].get("id") or "").strip()
                        if not template_id:
                            text, kb = _prompts_templates_menu(page=page_i)
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            from tracker.prompt_templates import builtin_templates, load_bindings, load_custom_templates, save_bindings, save_custom_templates

                            custom = load_custom_templates(repo)
                            existed = bool(template_id in custom)
                            if existed:
                                custom.pop(template_id, None)
                                save_custom_templates(repo, custom)

                            still_exists = bool(template_id in builtin_templates() or template_id in custom)
                            if not still_exists:
                                bindings = load_bindings(repo)
                                for slot, tid in list(bindings.items()):
                                    if str(tid or "").strip() == template_id:
                                        bindings.pop(slot, None)
                                save_bindings(repo, bindings)

                            if _out_lang() == "zh":
                                await _send_ack(f"✅ 已删除覆盖：{template_id}" if existed else f"⚠️ 非自定义模板：{template_id}")
                            else:
                                await _send_ack(f"✅ override deleted: {template_id}" if existed else f"⚠️ not a custom template: {template_id}")
                        except Exception as exc:
                            err = str(exc) or exc.__class__.__name__
                            if _out_lang() == "zh":
                                await _send_ack(f"⚠️ 删除失败：{err}")
                            else:
                                await _send_ack(f"⚠️ delete failed: {err}")

                        text, kb = _prompts_templates_menu(page=page_i)
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    text, kb = _prompts_menu()
                    await _send_with_markup(text=text, reply_markup=kb)
                    continue

                # --- LLM menu/actions (website-free)
                if data.startswith("llm:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _llm_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "cur":
                        v = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if v in {"true", "false"}:
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={"TRACKER_LLM_CURATION_ENABLED": v},
                                )
                                if _out_lang() == "zh":
                                    tail = "（重启服务后生效）" if res.restart_required else "（无需重启）"
                                    await _send_ack(f"✅ 已设置 TRACKER_LLM_CURATION_ENABLED={v}{tail}")
                                else:
                                    tail = " (restart services to apply)" if res.restart_required else " (no restart needed)"
                                    await _send_ack(f"✅ set TRACKER_LLM_CURATION_ENABLED={v}{tail}")
                            except Exception:
                                if _out_lang() == "zh":
                                    await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                                else:
                                    await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")
                        text, kb = _llm_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "tri":
                        v = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if v in {"true", "false"}:
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={"TRACKER_LLM_CURATION_TRIAGE_ENABLED": v},
                                )
                                if _out_lang() == "zh":
                                    tail = "（重启服务后生效）" if res.restart_required else "（无需重启）"
                                    await _send_ack(f"✅ 已设置 TRACKER_LLM_CURATION_TRIAGE_ENABLED={v}{tail}")
                                else:
                                    tail = " (restart services to apply)" if res.restart_required else " (no restart needed)"
                                    await _send_ack(f"✅ set TRACKER_LLM_CURATION_TRIAGE_ENABLED={v}{tail}")
                            except Exception:
                                if _out_lang() == "zh":
                                    await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                                else:
                                    await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")
                        text, kb = _llm_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "prio":
                        v = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        if v in {"true", "false"}:
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                res = apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={"TRACKER_PRIORITY_LANE_ENABLED": v},
                                )
                                if _out_lang() == "zh":
                                    tail = "（重启服务后生效）" if res.restart_required else "（无需重启）"
                                    await _send_ack(f"✅ 已设置 TRACKER_PRIORITY_LANE_ENABLED={v}{tail}")
                                else:
                                    tail = " (restart services to apply)" if res.restart_required else " (no restart needed)"
                                    await _send_ack(f"✅ set TRACKER_PRIORITY_LANE_ENABLED={v}{tail}")
                            except Exception:
                                if _out_lang() == "zh":
                                    await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                                else:
                                    await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")
                        text, kb = _llm_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "set":
                        field = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        field_to_env = {
                            "base_url": "TRACKER_LLM_BASE_URL",
                            "model": "TRACKER_LLM_MODEL",
                            "model_reasoning": "TRACKER_LLM_MODEL_REASONING",
                            "model_mini": "TRACKER_LLM_MODEL_MINI",
                            "api_key": "TRACKER_LLM_API_KEY",
                            "proxy": "TRACKER_LLM_PROXY",
                            "mini_base_url": "TRACKER_LLM_MINI_BASE_URL",
                            "mini_api_key": "TRACKER_LLM_MINI_API_KEY",
                            "mini_proxy": "TRACKER_LLM_MINI_PROXY",
                        }
                        env_key = field_to_env.get(field, "")
                        if not env_key:
                            continue

                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="llm_set",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        prompt = (
                            f"设置 LLM：请回复这条消息，填 `{env_key}` 的值。\n"
                            "取消：回复 0 或 cancel\n\n"
                            "提示：密钥不会被 bot 回显；但你的回复仍会出现在聊天记录里（谨慎）。"
                            if is_zh
                            else (
                                f"Set LLM: reply with the value for `{env_key}`.\n"
                                "Cancel: reply 0 or cancel\n\n"
                                "Note: the bot won't echo secrets, but your reply is still in chat history."
                            )
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="llm_set",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query=env_key,
                                )
                            except Exception:
                                pass
                        continue

                    continue

                # --- Profile onboarding (website-free)
                if data.startswith("profile:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""

                    if action == "menu":
                        text, kb = _profile_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    if action == "start":
                        try:
                            repo.cancel_telegram_tasks(
                                chat_id=existing_chat_id,
                                kind="profile_text",
                                status="awaiting",
                                reason="superseded",
                            )
                        except Exception:
                            pass
                        is_zh = _out_lang() == "zh"
                        prompt = (
                            "Profile 设置：请直接回复这条消息粘贴你的 PROFILE_TEXT（书签/笔记/兴趣描述都可以）。\n"
                            "取消：回复 0 或 cancel"
                            if is_zh
                            else "Profile setup: reply with PROFILE_TEXT (bookmarks/notes/interests). Cancel: reply 0 or cancel"
                        )
                        prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                        if prompt_mid > 0:
                            try:
                                repo.create_telegram_task(
                                    chat_id=existing_chat_id,
                                    user_id=uid,
                                    kind="profile_text",
                                    status="awaiting",
                                    prompt_message_id=prompt_mid,
                                    request_message_id=mid,
                                    query="profile_text",
                                )
                            except Exception:
                                pass
                        continue

                    if action == "apply":
                        preset = (parts[2] if len(parts) >= 3 else "").strip().lower()
                        raw_draft = (repo.get_app_config("profile_onboarding_draft_json") or "").strip()
                        if not raw_draft:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 没有可用的 draft。请先点 Start 粘贴 PROFILE_TEXT。")
                            else:
                                await _send_ack("⚠️ No draft found. Tap Start and paste PROFILE_TEXT first.")
                            text, kb = _profile_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        try:
                            draft = json.loads(raw_draft)
                        except Exception:
                            draft = None
                        if not isinstance(draft, dict):
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ draft 解析失败，请重新 Start。")
                            else:
                                await _send_ack("⚠️ draft parse failed; please Start again.")
                            text, kb = _profile_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        topic_name = str(draft.get("topic_name") or "Profile").strip() or "Profile"
                        profile_text = str(draft.get("profile_text") or "").strip()
                        understanding = str(draft.get("understanding") or "").strip()
                        interest_axes = draft.get("interest_axes") if isinstance(draft.get("interest_axes"), list) else []
                        interest_keywords = (
                            draft.get("interest_keywords") if isinstance(draft.get("interest_keywords"), list) else []
                        )
                        retrieval_queries = (
                            draft.get("retrieval_queries") if isinstance(draft.get("retrieval_queries"), list) else []
                        )
                        ai_prompt = str(draft.get("ai_prompt") or "").strip()

                        if not (profile_text and ai_prompt):
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ draft 缺少 profile_text/ai_prompt，请重新 Start。")
                            else:
                                await _send_ack("⚠️ draft missing profile_text/ai_prompt; please Start again.")
                            text, kb = _profile_menu()
                            await _send_with_markup(text=text, reply_markup=kb)
                            continue

                        # Apply: create/update topic, seed sources, and set the topic AI policy prompt.
                        try:
                            from tracker.actions import (
                                SourceBindingSpec,
                                TopicAiPolicySpec,
                                TopicSpec,
                                create_html_list_source as create_html_list_source_action,
                                create_rss_source as create_rss_source_action,
                                create_rss_sources_bulk as create_rss_sources_bulk_action,
                                create_searxng_search_source as create_searxng_search_source_action,
                                create_topic as create_topic_action,
                                upsert_topic_ai_policy as upsert_topic_ai_policy_action,
                            )
                            from tracker.source_packs import get_rss_pack

                            # Ensure global AI curation is enabled for the runner.
                            try:
                                from pathlib import Path

                                from tracker.dynamic_config import apply_env_block_updates

                                apply_env_block_updates(
                                    repo=repo,
                                    settings=settings,
                                    env_path=Path(_env_path()),
                                    env_updates={
                                        "TRACKER_LLM_CURATION_ENABLED": "true",
                                        "TRACKER_LLM_CURATION_TRIAGE_ENABLED": "true",
                                        "TRACKER_PRIORITY_LANE_ENABLED": "true",
                                    },
                                )
                            except Exception:
                                pass

                            topic = repo.get_topic_by_name(topic_name)
                            if not topic:
                                topic = create_topic_action(
                                    session=repo.session,
                                    spec=TopicSpec(
                                        name=topic_name,
                                        query="",
                                        digest_cron="0 9 * * *",
                                        alert_keywords="",
                                    ),
                                )
                            else:
                                topic.query = ""
                                repo.session.commit()

                            # Persist profile config (single profile).
                            repo.set_app_config("profile_topic_name", topic_name)
                            repo.set_app_config("profile_text", profile_text)
                            repo.set_app_config("profile_understanding", understanding)
                            repo.set_app_config("profile_interest_axes", "\n".join([str(x).strip() for x in interest_axes if str(x).strip()]))
                            repo.set_app_config(
                                "profile_interest_keywords",
                                ", ".join([str(x).strip() for x in interest_keywords if str(x).strip()]),
                            )
                            repo.set_app_config(
                                "profile_retrieval_queries",
                                "\n".join([str(x).strip() for x in retrieval_queries if str(x).strip()]),
                            )

                            # Seed stream sources (broad recall; AI filters hard).
                            if preset in {"full", "light", ""}:
                                create_rss_source_action(
                                    session=repo.session,
                                    url="https://news.ycombinator.com/rss",
                                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                                )
                                pack = get_rss_pack("hn_popularity_karpathy")
                                create_rss_sources_bulk_action(
                                    session=repo.session,
                                    urls=pack.urls,
                                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                                    tags="hn-popularity,karpathy",
                                )

                            if preset == "full":
                                # GitHub trending (daily)
                                create_html_list_source_action(
                                    session=repo.session,
                                    page_url="https://github.com/trending?since=daily",
                                    item_selector="article.Box-row",
                                    title_selector="h2 a",
                                    summary_selector="p",
                                    max_items=25,
                                    bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                                )
                                # arXiv (default categories)
                                for c in ["cs.AI", "cs.LG", "cs.CL", "cs.CV", "stat.ML"]:
                                    create_rss_source_action(
                                        session=repo.session,
                                        url=f"https://export.arxiv.org/rss/{c}",
                                        bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                                    )
                                # Optional forum sources are intentionally NOT hard-coded here.
                                # SearxNG recall (optional)
                                base = "http://127.0.0.1:8888"
                                for q in [str(x).strip() for x in (retrieval_queries or []) if str(x).strip()][:6]:
                                    create_searxng_search_source_action(
                                        session=repo.session,
                                        base_url=base,
                                        query=q,
                                        time_range="day",
                                        results=20,
                                        bind=SourceBindingSpec(topic=topic_name, include_keywords=""),
                                    )

                            upsert_topic_ai_policy_action(
                                session=repo.session,
                                spec=TopicAiPolicySpec(topic=topic_name, enabled=True, prompt=ai_prompt),
                            )

                            # Clear draft after apply.
                            repo.delete_app_config("profile_onboarding_draft_json")

                            if _out_lang() == "zh":
                                await _send_ack(f"✅ Profile 已应用：{topic_name}（preset={preset or 'full'}）")
                            else:
                                await _send_ack(f"✅ Profile applied: {topic_name} (preset={preset or 'full'})")
                        except Exception as exc:
                            if _out_lang() == "zh":
                                await _send_ack(f"⚠️ 应用失败：{exc}")
                            else:
                                await _send_ack(f"⚠️ apply failed: {exc}")

                        text, kb = _profile_menu()
                        await _send_with_markup(text=text, reply_markup=kb)
                        continue

                    continue

                # --- Feedback actions (free-form reply comments)
                if data.startswith("fb:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""
                    raw_id = parts[2] if len(parts) >= 3 else ""
                    try:
                        ev_id = int(raw_id)
                    except Exception:
                        ev_id = 0
                    if ev_id <= 0:
                        continue

                    try:
                        from tracker.models import FeedbackEvent

                        ev0 = repo.session.get(FeedbackEvent, int(ev_id))
                    except Exception:
                        ev0 = None
                    if not ev0:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 找不到对应的反馈事件（可能已过期）")
                        else:
                            await _send_ack("⚠️ feedback event not found (maybe expired)")
                        continue

                    def _extract_text(raw: str) -> str:
                        sraw = (raw or "").strip()
                        if not sraw:
                            return ""
                        try:
                            obj = json.loads(sraw)
                        except Exception:
                            return ""
                        if not isinstance(obj, dict):
                            return ""
                        t = obj.get("text")
                        return str(t or "").strip() if isinstance(t, str) else ""

                    comment_text = _extract_text(str(getattr(ev0, "raw", "") or ""))
                    item_id = int(getattr(ev0, "item_id", 0) or 0)
                    url = str(getattr(ev0, "url", "") or "").strip()
                    domain = str(getattr(ev0, "domain", "") or "").strip()

                    if action in {"like", "dislike"}:
                        ev2 = repo.add_feedback_event(
                            channel="telegram",
                            user_id=uid,
                            chat_id=existing_chat_id,
                            message_id=mid,
                            kind=action,
                            value_int=0,
                            item_id=(item_id if item_id > 0 else None),
                            url=url,
                            domain=domain,
                            note=f"from_comment:{ev_id}",
                            raw=json.dumps({"comment_id": ev_id}, ensure_ascii=False),
                        )
                        try:
                            _apply_source_score_feedback(item_id=(item_id if item_id > 0 else None), feedback_event_id=int(ev2.id), kind=action)
                        except Exception:
                            pass
                        pending_feedback_for_profile.append(int(ev2.id))
                        repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack(f"✅ 已记录：{action}（用于更新 Profile/域名质量）")
                        else:
                            await _send_ack(f"✅ recorded: {action} (for profile/domain quality)")
                        continue

                    if action in {"note", "profile_note"}:
                        ev2 = repo.add_feedback_event(
                            channel="telegram",
                            user_id=uid,
                            chat_id=existing_chat_id,
                            message_id=mid,
                            kind="profile_note",
                            value_int=0,
                            item_id=(item_id if item_id > 0 else None),
                            url=url,
                            domain=domain,
                            note=f"from_comment:{ev_id}",
                            raw=json.dumps({"comment_id": ev_id, "text": comment_text[:2000]}, ensure_ascii=False),
                        )
                        pending_feedback_for_profile.append(int(ev2.id))
                        repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已加入 Profile 更新队列（profile_note）")
                        else:
                            await _send_ack("✅ queued for profile update (profile_note)")
                        continue

                    if action in {"prompt_note", "prompt"}:
                        if not bool(getattr(settings, "telegram_prompt_delta_enabled", True)):
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 已关闭“提示词修正”功能（telegram_prompt_delta_enabled=false）")
                            else:
                                await _send_ack("⚠️ Prompt delta is disabled (telegram_prompt_delta_enabled=false)")
                            continue
                        if not (
                            getattr(settings, "llm_base_url", None)
                            and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
                        ):
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 未配置 LLM（TRACKER_LLM_BASE_URL/MODEL）；无法生成提示词修正提案")
                            else:
                                await _send_ack("⚠️ LLM is not configured; cannot propose prompt deltas")
                            continue

                        target_slot_id = (
                            (repo.get_app_config("telegram_prompt_delta_target_slot_id") or "").strip()
                            or (getattr(settings, "telegram_prompt_delta_target_slot_id", "") or "").strip()
                            or "research.engine.synth.operator_delta"
                        )

                        ev2 = repo.add_feedback_event(
                            channel="telegram",
                            user_id=uid,
                            chat_id=existing_chat_id,
                            message_id=mid,
                            kind="prompt_note",
                            value_int=0,
                            item_id=(item_id if item_id > 0 else None),
                            url=url,
                            domain=domain,
                            note=f"from_comment:{ev_id}",
                            raw=json.dumps(
                                {"comment_id": ev_id, "text": comment_text[:2000], "target_slot_id": target_slot_id},
                                ensure_ascii=False,
                            ),
                        )
                        # Queue at most ONE pending prompt_delta worker task. It will pick up all pending prompt_note events.
                        try:
                            existing_tasks = repo.list_telegram_tasks(chat_id=existing_chat_id, kind="prompt_delta", limit=10)
                        except Exception:
                            existing_tasks = []
                        has_pending = any(str(getattr(t, "status", "") or "") == "pending" for t in existing_tasks)
                        if not has_pending:
                            placeholder_mid = -int(dt.datetime.utcnow().timestamp() * 1000)
                            repo.create_telegram_task(
                                chat_id=existing_chat_id,
                                user_id=uid,
                                kind="prompt_delta",
                                status="pending",
                                prompt_message_id=placeholder_mid,
                                request_message_id=0,
                                item_id=(item_id if item_id > 0 else None),
                                url=url,
                                query=json.dumps({"target_slot_id": target_slot_id}, ensure_ascii=False),
                            )

                        repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已加入提示词修正队列（会发“提案”，需要你点 Apply 才会生效）")
                        else:
                            await _send_ack("✅ queued for prompt delta proposal (requires Apply to take effect)")
                        continue

                    if action in {"exclude_domain", "exclude"}:
                        dom = domain or _domain_from_url(url)
                        if not dom:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 未识别域名（请对单条 Alert 回复，或发送 /why 查看来源）")
                            else:
                                await _send_ack("⚠️ could not determine domain")
                            continue
                        try:
                            from tracker.http_auth import parse_domains_csv

                            cur = (repo.get_app_config("exclude_domains") or getattr(settings, "exclude_domains", "") or "").strip()
                            parts2 = parse_domains_csv(cur)
                            if dom not in parts2:
                                parts2.append(dom)
                            repo.set_app_config("exclude_domains", ", ".join(parts2))
                        except Exception:
                            # Best-effort; avoid crashing the poll loop.
                            pass
                        if str(getattr(ev0, "kind", "") or "").strip() == "comment":
                            repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack(f"🚫 已屏蔽域名：{dom}\n（已写入 exclude_domains；约 60s 内会同步回 .env）")
                        else:
                            await _send_ack(
                                f"🚫 excluded domain: {dom}\n(wrote exclude_domains; will sync back to .env within ~60s)"
                            )
                        continue

                    if action in {"downrank_domain", "downrank"}:
                        dom = domain or _domain_from_url(url)
                        if not dom:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 未识别域名（请对单条 Alert 回复，或发送 /why 查看来源）")
                            else:
                                await _send_ack("⚠️ could not determine domain")
                            continue
                        try:
                            from tracker.http_auth import parse_domains_csv

                            cur = (
                                repo.get_app_config("domain_quality_low_domains")
                                or getattr(settings, "domain_quality_low_domains", "")
                                or ""
                            ).strip()
                            parts2 = parse_domains_csv(cur)
                            if dom not in parts2:
                                parts2.append(dom)
                            repo.set_app_config("domain_quality_low_domains", ", ".join(parts2))
                        except Exception:
                            pass
                        if str(getattr(ev0, "kind", "") or "").strip() == "comment":
                            repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack(f"⬇️ 已降级域名：{dom}\n（写入 domain_quality_low_domains；用于减少推送噪音）")
                        else:
                            await _send_ack(
                                f"⬇️ downranked domain: {dom}\n(wrote domain_quality_low_domains; reduces push noise)"
                            )
                        continue

                    if action == "mute":
                        if not domain:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 未识别域名（请对单条 Alert 回复，或发送 /mute <domain>）")
                            else:
                                await _send_ack("⚠️ could not determine domain (reply to an Alert, or /mute <domain>)")
                            continue
                        days = _default_mute_days()
                        until = dt.datetime.utcnow() + dt.timedelta(days=days)
                        repo.upsert_mute_rule(
                            scope="domain",
                            key=domain,
                            muted_until=until,
                            reason=f"telegram feedback mute (comment {ev_id})",
                        )
                        # Only mark "comment" containers as handled. For direct signals like
                        # reaction-based dislike, keep the event pending so profile updates can
                        # still consume it later.
                        if str(getattr(ev0, "kind", "") or "").strip() == "comment":
                            repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack(f"🔕 已静音：{domain}（{days} 天）")
                        else:
                            await _send_ack(f"🔕 muted: {domain} ({days} days)")
                        continue

                    if action == "ignore":
                        if str(getattr(ev0, "kind", "") or "").strip() == "comment":
                            repo.mark_feedback_events_applied(ids=[int(ev_id)])
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已忽略本条反馈")
                        else:
                            await _send_ack("✅ feedback ignored")
                        continue

                    continue

                # --- Profile delta proposals (feedback-driven)
                if data.startswith("pd:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""
                    raw_id = parts[2] if len(parts) >= 3 else ""
                    try:
                        task_id = int(raw_id)
                    except Exception:
                        task_id = 0
                    if task_id <= 0:
                        continue

                    try:
                        from tracker.models import TelegramTask

                        t_pd = repo.session.get(TelegramTask, int(task_id))
                    except Exception:
                        t_pd = None
                    if not t_pd or str(getattr(t_pd, "kind", "") or "").strip() != "profile_delta":
                        continue

                    # Parse proposal payload.
                    try:
                        payload = json.loads((getattr(t_pd, "intent", "") or "").strip() or "{}")
                    except Exception:
                        payload = {}
                    delta_new = ""
                    note = ""
                    fb_ids: list[int] = []
                    if isinstance(payload, dict):
                        delta_new = str(payload.get("delta_prompt") or "").strip()
                        note = str(payload.get("note") or "").strip()
                        raw_ids = payload.get("feedback_ids")
                        if isinstance(raw_ids, list):
                            for x in raw_ids:
                                try:
                                    n = int(x)
                                except Exception:
                                    n = 0
                                if n > 0:
                                    fb_ids.append(n)
                    if not fb_ids:
                        try:
                            obj2 = json.loads((getattr(t_pd, "query", "") or "").strip() or "{}")
                        except Exception:
                            obj2 = {}
                        if isinstance(obj2, dict):
                            raw_ids2 = obj2.get("feedback_ids")
                            if isinstance(raw_ids2, list):
                                for x in raw_ids2:
                                    try:
                                        n = int(x)
                                    except Exception:
                                        n = 0
                                    if n > 0:
                                        fb_ids.append(n)

                    is_zh = _out_lang() == "zh"

                    # Resolve profile topic/policy and current core.
                    profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
                    topic = repo.get_topic_by_name(profile_topic_name)
                    pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
                    core = (repo.get_app_config("profile_prompt_core") or "").strip()
                    if not core and pol and (pol.llm_curation_prompt or "").strip():
                        core = (pol.llm_curation_prompt or "").strip()
                        if core:
                            repo.set_app_config("profile_prompt_core", core)

                    now_iso = dt.datetime.utcnow().isoformat() + "Z"

                if action == "edit":
                    try:
                        t_pd.option = 1
                        t_pd.status = "awaiting"
                        repo.session.commit()
                    except Exception:
                        pass
                    prompt = (
                        "✏️ 请直接回复这条消息，粘贴新的 delta_prompt（回复 0 取消）。"
                        if is_zh
                        else "✏️ Reply to this message with a replacement delta_prompt (reply 0 to cancel)."
                    )
                    req_mid = await _send_with_markup(text=prompt, reply_markup=None)
                    if req_mid > 0:
                        try:
                            t_pd.request_message_id = int(req_mid)
                            repo.session.commit()
                        except Exception:
                            pass
                    continue

                    if action == "reject":
                        if fb_ids:
                            try:
                                repo.mark_feedback_events_applied(ids=fb_ids)
                            except Exception:
                                pass
                        try:
                            repo.set_app_config("profile_feedback_last_update_at_utc", now_iso)
                        except Exception:
                            pass
                        try:
                            repo.mark_telegram_task_canceled(task_id, reason="rejected")
                        except Exception:
                            pass
                        if is_zh:
                            await _send_ack("✅ 已拒绝本轮 Profile 更新（未修改画像）。")
                        else:
                            await _send_ack("✅ Rejected this profile update (profile unchanged).")
                        continue

                    if action == "apply":
                        if not (topic and pol and core and delta_new):
                            if is_zh:
                                await _send_ack("⚠️ Profile 未就绪或缺少 delta（请先完成 /profile 初始化）")
                            else:
                                await _send_ack("⚠️ Profile not ready or missing delta (run /profile first)")
                            continue
                        effective = (core + ("\n\n" + delta_new if delta_new else "")).strip()
                        try:
                            repo.set_app_config("profile_prompt_delta", delta_new)
                            repo.set_app_config("profile_feedback_last_update_at_utc", now_iso)
                            repo.upsert_topic_policy(topic_id=int(topic.id), llm_curation_prompt=effective)
                            rev = repo.add_profile_revision(
                                kind="delta",
                                core_prompt=core,
                                delta_prompt=delta_new,
                                effective_prompt=effective,
                                note=note,
                                applied_feedback_ids=fb_ids,
                            )
                            if fb_ids:
                                repo.mark_feedback_events_applied(ids=fb_ids)
                            try:
                                t_pd.status = "done"
                                t_pd.result_key = "profile_delta_applied"
                                t_pd.error = ""
                                t_pd.finished_at = dt.datetime.utcnow()
                                repo.session.commit()
                            except Exception:
                                pass
                            if is_zh:
                                await _send_ack(f"✅ Profile 已更新（delta）（rev={int(rev.id)})")
                            else:
                                await _send_ack(f"✅ Profile updated (delta) (rev={int(rev.id)})")
                        except Exception as exc:
                            if is_zh:
                                await _send_ack(f"⚠️ 应用失败：{exc}")
                            else:
                                await _send_ack(f"⚠️ apply failed: {exc}")
                        continue

                    continue

                # --- Prompt delta proposals (feedback-driven)
                if data.startswith("td:"):
                    parts = [p for p in data.split(":") if p]
                    action = parts[1] if len(parts) >= 2 else ""
                    raw_id = parts[2] if len(parts) >= 3 else ""
                    try:
                        task_id = int(raw_id)
                    except Exception:
                        task_id = 0
                    if task_id <= 0:
                        continue

                    try:
                        from tracker.models import TelegramTask

                        t_td = repo.session.get(TelegramTask, int(task_id))
                    except Exception:
                        t_td = None
                    if not t_td or str(getattr(t_td, "kind", "") or "").strip() != "prompt_delta":
                        continue

                    # Parse proposal payload.
                    try:
                        payload = json.loads((getattr(t_td, "intent", "") or "").strip() or "{}")
                    except Exception:
                        payload = {}
                    target_slot_id = "research.engine.synth.operator_delta"
                    target_template_id = ""
                    out_lang = _out_lang()
                    delta_new = ""
                    note = ""
                    fb_ids: list[int] = []
                    if isinstance(payload, dict):
                        ts = str(payload.get("target_slot_id") or "").strip()
                        if ts:
                            target_slot_id = ts
                        target_template_id = str(payload.get("target_template_id") or "").strip()
                        out_lang = str(payload.get("lang") or "").strip() or out_lang
                        delta_new = str(payload.get("delta_prompt") or "").strip()
                        note = str(payload.get("note") or "").strip()
                        raw_ids = payload.get("feedback_ids")
                        if isinstance(raw_ids, list):
                            for x in raw_ids:
                                try:
                                    n = int(x)
                                except Exception:
                                    n = 0
                                if n > 0:
                                    fb_ids.append(n)

                    is_zh = (out_lang or "").strip().lower().startswith("zh") or out_lang in {"中文", "简体中文", "繁體中文", "繁体中文"}

                    if action == "edit":
                        try:
                            t_td.option = 1
                            t_td.status = "awaiting"
                            repo.session.commit()
                        except Exception:
                            pass
                        if is_zh:
                            await _send_ack("✏️ 请直接回复这条消息，粘贴新的 delta（回复 0 取消）。")
                        else:
                            await _send_ack("✏️ Reply to this message with a replacement delta (reply 0 to cancel).")
                        continue

                    now_iso = dt.datetime.utcnow().isoformat() + "Z"

                    if action == "reject":
                        if fb_ids:
                            try:
                                repo.mark_feedback_events_applied(ids=fb_ids)
                            except Exception:
                                pass
                        try:
                            repo.mark_telegram_task_canceled(task_id, reason="rejected")
                        except Exception:
                            pass
                        if is_zh:
                            await _send_ack("✅ 已拒绝本轮提示词更新（未修改提示词）。")
                        else:
                            await _send_ack("✅ Rejected this prompt update (prompts unchanged).")
                        continue

                    if action == "apply":
                        if not delta_new:
                            if is_zh:
                                await _send_ack("⚠️ 缺少 delta（可能任务已过期/失败）")
                            else:
                                await _send_ack("⚠️ missing delta (maybe expired/failed)")
                            continue
                        try:
                            from tracker.prompt_templates import builtin_templates, load_custom_templates, save_custom_templates
                            from tracker.prompt_templates import resolve_prompt_best_effort

                            tpl_id = (target_template_id or "").strip()
                            if not tpl_id:
                                try:
                                    tpl_id = resolve_prompt_best_effort(
                                        repo=repo,
                                        settings=settings,
                                        slot_id=target_slot_id,
                                        language=("zh" if is_zh else "en"),  # type: ignore[arg-type]
                                    ).template_id
                                except Exception:
                                    tpl_id = target_slot_id

                            custom = load_custom_templates(repo)
                            obj = custom.get(tpl_id)
                            if not isinstance(obj, dict):
                                obj = {}

                            built = builtin_templates().get(tpl_id)
                            title = str(obj.get("title") or (built.title if built else tpl_id) or tpl_id).strip()
                            desc = str(obj.get("description") or (built.description if built else "") or "").strip()
                            text_obj = obj.get("text")
                            if not isinstance(text_obj, dict):
                                text_obj = {}
                                if "text_zh" in obj or "text_en" in obj:
                                    text_obj["zh"] = str(obj.get("text_zh") or "").strip()
                                    text_obj["en"] = str(obj.get("text_en") or "").strip()

                            if is_zh:
                                text_obj["zh"] = delta_new
                            else:
                                text_obj["en"] = delta_new

                            custom[tpl_id] = {"title": title, "description": desc, "text": text_obj}
                            save_custom_templates(repo, custom)
                            repo.add_settings_change(
                                source="tg_prompt_delta_apply",
                                actor=str(uid or ""),
                                fields=["prompt_templates_custom_json"],
                                env_keys=["TRACKER_PROMPT_TEMPLATES_CUSTOM_JSON"],
                                restart_required=False,
                            )

                            if fb_ids:
                                try:
                                    repo.mark_feedback_events_applied(ids=fb_ids)
                                except Exception:
                                    pass
                            try:
                                t_td.status = "done"
                                t_td.result_key = "prompt_delta_applied"
                                t_td.error = ""
                                t_td.finished_at = dt.datetime.utcnow()
                                repo.session.commit()
                            except Exception:
                                pass

                            if is_zh:
                                extra = f"\nnote: {note}" if note else ""
                                await _send_ack(f"✅ 已应用提示词 delta\n- target: {target_slot_id}{extra}")
                            else:
                                extra = f"\nnote: {note}" if note else ""
                                await _send_ack(f"✅ Applied prompt delta\n- target: {target_slot_id}{extra}")
                        except Exception as exc:
                            if is_zh:
                                await _send_ack(f"⚠️ 应用失败：{exc}")
                            else:
                                await _send_ack(f"⚠️ apply failed: {exc}")
                        continue

                    continue

            # 1) Reactions (no message text in payload; must map via saved message ids).
            react = upd.get("message_reaction")
            if isinstance(react, dict):
                chat = react.get("chat")
                if isinstance(chat, dict) and str(chat.get("id") or "").strip() == existing_chat_id:
                    try:
                        mid = int(react.get("message_id") or 0)
                    except Exception:
                        mid = 0
                    user_obj = react.get("user")
                    uid = str(user_obj.get("id") or "").strip() if isinstance(user_obj, dict) else ""

                    if not owner_user_id and uid:
                        owner_user_id = uid
                        repo.set_app_config("telegram_owner_user_id", uid)
                    if owner_user_id and uid and uid != owner_user_id:
                        continue

                    emoji = ""
                    new_reaction = react.get("new_reaction")
                    if isinstance(new_reaction, list) and new_reaction:
                        first = new_reaction[0]
                        if isinstance(first, dict):
                            emoji = str(first.get("emoji") or "").strip()
                        elif isinstance(first, str):
                            emoji = first.strip()
                    elif isinstance(new_reaction, dict):
                        emoji = str(new_reaction.get("emoji") or "").strip()
                    elif isinstance(new_reaction, str):
                        emoji = new_reaction.strip()

                    if not reactions_enabled:
                        continue

                    if mid > 0 and emoji:
                        tm = repo.get_telegram_message(chat_id=existing_chat_id, message_id=mid)
                        item_id = int(getattr(tm, "item_id", 0) or 0) if tm else 0
                        if item_id > 0:
                            url, domain = _item_from_id(item_id)
                            kind = ""
                            value_int = 0
                            if emoji in like_emojis:
                                kind = "like"
                            elif emoji in dislike_emojis:
                                kind = "dislike"
                            elif emoji in mute_emojis:
                                kind = "mute"
                                value_int = _default_mute_days()
                            if kind:
                                ev = repo.add_feedback_event(
                                    channel="telegram",
                                    user_id=uid,
                                    chat_id=existing_chat_id,
                                    message_id=mid,
                                    kind=kind,
                                    value_int=value_int,
                                    item_id=item_id,
                                    url=url,
                                    domain=domain,
                                    note=f"reaction:{emoji}",
                                    raw=json.dumps({"emoji": emoji}),
                                )
                                try:
                                    _apply_source_score_feedback(item_id=item_id, feedback_event_id=int(ev.id), kind=kind)
                                except Exception:
                                    pass
                                if kind in {"like", "dislike", "rate"}:
                                    pending_feedback_for_profile.append(int(ev.id))
                                if kind == "mute" and domain:
                                    until = dt.datetime.utcnow() + dt.timedelta(days=value_int)
                                    repo.upsert_mute_rule(scope="domain", key=domain, muted_until=until, reason=f"telegram reaction {emoji}")
                                    # Acknowledge mutes only (avoid spam).
                                    if _out_lang() == "zh":
                                        await _send_ack(f"🔕 已静音：{domain}（{value_int} 天）")
                                    else:
                                        await _send_ack(f"🔕 muted: {domain} ({value_int} days)")
                                elif kind == "dislike" and domain:
                                    # Offer a one-tap "mute domain" suggestion without spamming.
                                    try:
                                        muted = repo.is_muted(scope="domain", key=domain, when=dt.datetime.utcnow())
                                    except Exception:
                                        muted = False
                                    if not muted:
                                        days2 = _default_mute_days()
                                        is_zh = _out_lang() == "zh"
                                        text2 = (
                                            f"已记录：👎\n要静音域名 `{domain}` {days2} 天吗？"
                                            if is_zh
                                            else f"Recorded: 👎\nMute `{domain}` for {days2} days?"
                                        )
                                        kb2 = {
                                            "inline_keyboard": [
                                                [
                                                    {"text": (f"🔕 静音 {days2} 天" if is_zh else f"🔕 Mute {days2}d"), "callback_data": f"fb:mute:{int(ev.id)}"},
                                                    {"text": ("🚫 屏蔽域名" if is_zh else "🚫 Exclude domain"), "callback_data": f"fb:exclude_domain:{int(ev.id)}"},
                                                ],
                                                [
                                                    {"text": ("忽略" if is_zh else "Ignore"), "callback_data": f"fb:ignore:{int(ev.id)}"},
                                                ],
                                            ]
                                        }
                                        await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            # 2) Messages (commands / reply feedback).
            msg = upd.get("message")
            if not isinstance(msg, dict):
                continue
            chat = msg.get("chat")
            if not isinstance(chat, dict) or "id" not in chat:
                continue
            chat_id = str(chat.get("id")).strip()
            if chat_id != existing_chat_id:
                continue

            from_obj = msg.get("from")
            uid = str(from_obj.get("id") or "").strip() if isinstance(from_obj, dict) else ""
            if not owner_user_id and uid:
                owner_user_id = uid
                repo.set_app_config("telegram_owner_user_id", uid)
            if owner_user_id and uid and uid != owner_user_id:
                continue

            text = msg.get("text")
            if not isinstance(text, str):
                continue
            s = text.strip()
            if not s:
                continue

            # Status/help commands.
            if s.startswith("/status") or s.startswith("/start"):
                try:
                    from tracker.push.telegram import TelegramPusher

                    p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))
                    await p.send_text(
                        chat_id=existing_chat_id,
                        text=_telegram_status_text(repo=repo, settings=settings),
                        disable_preview=True,
                    )
                except Exception:
                    pass
                continue

            # Feedback: prefer reply-to context (maps to an item via saved message ids).
            try:
                msg_id = int(msg.get("message_id") or 0)
            except Exception:
                msg_id = 0
            reply = msg.get("reply_to_message")
            reply_mid = 0
            if isinstance(reply, dict):
                try:
                    reply_mid = int(reply.get("message_id") or 0)
                except Exception:
                    reply_mid = 0

            # Interactive workflows: replies to a bot prompt should not be misinterpreted as feedback.
            if reply_mid > 0:
                # 0) /env (paste an env block)
                t_env = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="env_import",
                )
                if t_env:
                    if (t_env.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_env.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消 env 导入")
                        else:
                            await _send_ack("✅ env import canceled")
                        continue

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

                        updates = parse_settings_env_block(
                            raw,
                            allow_remote_updates=True,
                            blank_values_mean_no_change=True,
                        )
                        if not updates:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ env 导入：没有变化")
                            else:
                                await _send_ack("⚠️ env import: no changes")
                            continue
                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates=updates,
                        )
                        repo.mark_telegram_task_done(int(t_env.id), result_key="env_updated")
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except ValueError as exc:
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ env block 不合法：{exc}")
                        else:
                            await _send_ack(f"⚠️ invalid env block: {exc}")
                    except Exception:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ env 导入失败（请重试或用 Web Admin）")
                        else:
                            await _send_ack("⚠️ env import failed (retry or use Web Admin)")
                    continue

                # 0.2) /prompts edit template (reply-based)
                t_pt = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="prompt_template_edit",
                )
                if t_pt:
                    if (t_pt.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").rstrip()
                    low = raw.lower().strip()
                    if raw.strip() in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_pt.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消模板编辑")
                        else:
                            await _send_ack("✅ template edit canceled")
                        continue

                    template_id = ""
                    lang_code = "zh"
                    try:
                        obj = json.loads((t_pt.query or "").strip() or "{}")
                    except Exception:
                        obj = {}
                    if isinstance(obj, dict):
                        template_id = str(obj.get("template_id") or "").strip()
                        lang_code = str(obj.get("lang") or "").strip().lower() or "zh"
                    if lang_code not in {"zh", "en"}:
                        lang_code = "zh"
                    if not template_id:
                        repo.mark_telegram_task_failed(int(t_pt.id), error="missing template_id")
                        continue
                    if not raw.strip():
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 模板内容为空（请重试）")
                        else:
                            await _send_ack("⚠️ empty template text (retry)")
                        continue

                    try:
                        from tracker.prompt_templates import builtin_templates, load_custom_templates, save_custom_templates

                        templates = load_custom_templates(repo)
                        cur = templates.get(template_id)
                        if not isinstance(cur, dict):
                            cur = {}
                        title = str(cur.get("title") or "").strip()
                        desc = str(cur.get("description") or "").strip()
                        text_obj = cur.get("text")
                        if not isinstance(text_obj, dict):
                            text_obj = {}
                        # Seed from builtin metadata if needed.
                        if not title:
                            b = builtin_templates().get(template_id)
                            if b:
                                title = str(getattr(b, "title", "") or "").strip() or template_id
                                desc = str(getattr(b, "description", "") or "").strip()
                            else:
                                title = template_id

                        text_obj[lang_code] = raw
                        templates[template_id] = {"title": title or template_id, "description": desc, "text": {"zh": str(text_obj.get("zh") or ""), "en": str(text_obj.get("en") or "")}}
                        save_custom_templates(repo, templates)
                        repo.mark_telegram_task_done(int(t_pt.id), result_key="prompt_template_saved")
                        if _out_lang() == "zh":
                            await _send_ack(f"✅ 已保存模板：{template_id}（{lang_code}）")
                        else:
                            await _send_ack(f"✅ saved template: {template_id} ({lang_code})")
                    except Exception as exc:
                        err = str(exc) or exc.__class__.__name__
                        repo.mark_telegram_task_failed(int(t_pt.id), error=err[:4000])
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ 模板保存失败：{err}")
                        else:
                            await _send_ack(f"⚠️ template save failed: {err}")
                    continue

                # 0.45) Config Center v2: set a single Settings field (reply-based)
                t_cfgc = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="cfgc_set",
                )
                if t_cfgc:
                    if (t_cfgc.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_cfgc.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消配置修改")
                        else:
                            await _send_ack("✅ config change canceled")
                        text2, kb2 = _cfgc_menu()
                        await _send_with_markup(text=text2, reply_markup=kb2)
                        continue

                    # Default: allow multi-line (textarea). For simple inputs, users can just send one line.
                    value = raw
                    if low in {"off", "disable"}:
                        value = ""

                    # Payload carries {section_id, field, page}.
                    field = ""
                    sid = ""
                    page_i = 0
                    try:
                        obj = json.loads((t_cfgc.query or "").strip() or "{}")
                    except Exception:
                        obj = {}
                    if isinstance(obj, dict):
                        field = str(obj.get("field") or "").strip()
                        sid = str(obj.get("section_id") or "").strip()
                        try:
                            page_i = int(obj.get("page") or 0)
                        except Exception:
                            page_i = 0
                    if not field:
                        field = str(t_cfgc.query or "").strip()

                    if not field:
                        repo.mark_telegram_task_failed(int(t_cfgc.id), error="missing field")
                        continue
                    if field in _CFG_C_DANGEROUS_FIELDS:
                        repo.mark_telegram_task_failed(int(t_cfgc.id), error="dangerous field")
                        await _send_ack(
                            "⚠️ 该字段为危险项，请用 /api 或 SSH/CLI。"
                            if _out_lang() == "zh"
                            else "⚠️ Dangerous key: use /api or SSH/CLI."
                        )
                        text2, kb2 = _cfgc_menu()
                        await _send_with_markup(text=text2, reply_markup=kb2)
                        continue

                    try:
                        from pathlib import Path

                        from tracker.admin_settings import parse_settings_patch_form
                        from tracker.dynamic_config import apply_env_block_updates

                        updates, errors = parse_settings_patch_form(form={field: value}, repo=repo, settings=settings)
                        if errors:
                            repo.mark_telegram_task_failed(int(t_cfgc.id), error="invalid field value")
                            await _send_ack("⚠️ 配置不合法" if _out_lang() == "zh" else "⚠️ invalid config")
                        elif not updates:
                            repo.mark_telegram_task_done(int(t_cfgc.id), result_key="no_changes")
                            await _send_ack("✅ 无变化" if _out_lang() == "zh" else "✅ no changes")
                        else:
                            res = apply_env_block_updates(
                                repo=repo,
                                settings=settings,
                                env_path=Path(_env_path()),
                                env_updates=updates,
                            )
                            repo.mark_telegram_task_done(int(t_cfgc.id), result_key="cfgc_updated")
                            try:
                                repo.add_settings_change(
                                    source="tg_cfgc_reply",
                                    fields=[field],
                                    env_keys=list(res.updated_env_keys),
                                    restart_required=bool(res.restart_required),
                                    actor=f"tg:{uid}",
                                    client_host="telegram",
                                )
                            except Exception:
                                pass
                            keys = ", ".join(sorted(res.updated_env_keys))
                            if _out_lang() == "zh":
                                tail = "（重启服务后生效：/restart）" if res.restart_required else "（无需重启）"
                                await _send_ack(f"✅ 已更新：{keys}{tail}")
                            else:
                                tail = " (/restart to apply)" if res.restart_required else " (no restart needed)"
                                await _send_ack(f"✅ updated: {keys}{tail}")
                    except Exception as exc:
                        repo.mark_telegram_task_failed(int(t_cfgc.id), error=str(exc))
                        await _send_ack(
                            f"⚠️ 写入失败：{exc}" if _out_lang() == "zh" else f"⚠️ apply failed: {exc}"
                        )

                    if sid:
                        text2, kb2 = _cfgc_field_menu(section_id=sid, field=field, section_page=page_i)
                    else:
                        text2, kb2 = _cfgc_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                # 0.5) /llm set (single key)
                t_llm = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="llm_set",
                )
                if t_llm:
                    if (t_llm.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_llm.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消 LLM 配置")
                        else:
                            await _send_ack("✅ LLM config canceled")
                        continue

                    env_key = (t_llm.query or "").strip()
                    if not env_key.startswith("TRACKER_"):
                        repo.mark_telegram_task_failed(int(t_llm.id), error="invalid env key")
                        continue

                    # Keep it simple: take the first non-empty line as the value.
                    value = ""
                    for line in raw.splitlines():
                        sline = (line or "").strip()
                        if sline:
                            value = sline
                            break
                    if not value:
                        continue

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

                        updates = parse_settings_env_block(
                            f"{env_key}={value}\n",
                            allow_remote_updates=True,
                            blank_values_mean_no_change=False,
                        )
                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates=updates,
                        )
                        repo.mark_telegram_task_done(int(t_llm.id), result_key="llm_updated")
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except ValueError as exc:
                        repo.mark_telegram_task_failed(int(t_llm.id), error=str(exc))
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ 配置不合法：{exc}")
                        else:
                            await _send_ack(f"⚠️ invalid config: {exc}")
                    except Exception:
                        repo.mark_telegram_task_failed(int(t_llm.id), error="apply failed")
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                        else:
                            await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")

                    # Always re-show the menu for faster iteration.
                    text2, kb2 = _llm_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                # 0.6) /push set (single key)
                t_push = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="push_set",
                )
                if t_push:
                    if (t_push.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_push.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消 Push 配置")
                        else:
                            await _send_ack("✅ Push config canceled")
                        continue

                    env_key = (t_push.query or "").strip()
                    if not env_key.startswith("TRACKER_"):
                        repo.mark_telegram_task_failed(int(t_push.id), error="invalid env key")
                        continue

                    # Keep it simple: take the first non-empty line as the value.
                    value = ""
                    for line in raw.splitlines():
                        sline = (line or "").strip()
                        if sline:
                            value = sline
                            break
                    if not value:
                        continue

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

                        updates = parse_settings_env_block(
                            f"{env_key}={value}\n",
                            allow_remote_updates=True,
                            blank_values_mean_no_change=False,
                        )
                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates=updates,
                        )
                        repo.mark_telegram_task_done(int(t_push.id), result_key="push_updated")
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except ValueError as exc:
                        repo.mark_telegram_task_failed(int(t_push.id), error=str(exc))
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ 配置不合法：{exc}")
                        else:
                            await _send_ack(f"⚠️ invalid config: {exc}")
                    except Exception:
                        repo.mark_telegram_task_failed(int(t_push.id), error="apply failed")
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                        else:
                            await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")

                    # Always re-show the menu for faster iteration.
                    text2, kb2 = _push_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                # 0.65) /auth set (single key)
                t_auth = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="auth_set",
                )
                if t_auth:
                    if (t_auth.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_auth.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消 Auth 配置")
                        else:
                            await _send_ack("✅ Auth config canceled")
                        continue

                    env_key = (t_auth.query or "").strip()
                    if env_key not in _TG_AUTH_ALLOWED_KEYS:
                        repo.mark_telegram_task_failed(int(t_auth.id), error="invalid env key")
                        continue

                    # For auth secrets (cookie headers / JSON), allow multi-line payloads.
                    # For simple fields, use the first non-empty line.
                    value = ""
                    if env_key in {"TRACKER_DISCOURSE_COOKIE", "TRACKER_COOKIE_JAR_JSON"}:
                        value = raw.strip()
                    else:
                        for line in raw.splitlines():
                            sline = (line or "").strip()
                            if sline:
                                value = sline
                                break
                    if not value:
                        continue
                    if len(value) > 16000:
                        value = value[:16000] + "…"

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates

                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates={env_key: value},
                        )
                        repo.mark_telegram_task_done(int(t_auth.id), result_key="auth_updated")
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except Exception:
                        repo.mark_telegram_task_failed(int(t_auth.id), error="apply failed")
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                        else:
                            await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")

                    text2, kb2 = _auth_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                # 0.7) Profile delta proposal edit (reply with replacement delta_prompt)
                t_pd = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="profile_delta",
                )
                if not t_pd:
                    t_pd = repo.get_telegram_task_by_request_message(
                        chat_id=existing_chat_id,
                        request_message_id=reply_mid,
                        kind="profile_delta",
                    )
                if t_pd:
                    if (t_pd.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        try:
                            t_pd.option = 0
                            repo.session.commit()
                        except Exception:
                            pass
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消编辑（可继续点 Apply/Reject）")
                        else:
                            await _send_ack("✅ edit canceled (you can still tap Apply/Reject)")
                        continue

                    # Ack immediately so operators don't think it's swallowed.
                    if _out_lang() == "zh":
                        await _send_ack("⏳ 已收到，正在处理…")
                    else:
                        await _send_ack("⏳ Received. Processing…")

                    new_delta = raw.strip()
                    if len(new_delta) > 2000:
                        new_delta = new_delta[:2000] + "…"

                    # Load feedback ids from task payload (best-effort).
                    fb_ids: list[int] = []
                    try:
                        payload = json.loads((getattr(t_pd, "intent", "") or "").strip() or "{}")
                    except Exception:
                        payload = {}
                    if isinstance(payload, dict):
                        raw_ids = payload.get("feedback_ids")
                        if isinstance(raw_ids, list):
                            for x in raw_ids:
                                try:
                                    n = int(x)
                                except Exception:
                                    n = 0
                                if n > 0:
                                    fb_ids.append(n)
                    if not fb_ids:
                        try:
                            obj2 = json.loads((getattr(t_pd, "query", "") or "").strip() or "{}")
                        except Exception:
                            obj2 = {}
                        if isinstance(obj2, dict):
                            raw_ids2 = obj2.get("feedback_ids")
                            if isinstance(raw_ids2, list):
                                for x in raw_ids2:
                                    try:
                                        n = int(x)
                                    except Exception:
                                        n = 0
                                    if n > 0:
                                        fb_ids.append(n)

                    is_zh = _out_lang() == "zh"
                    profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
                    topic = repo.get_topic_by_name(profile_topic_name)
                    pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
                    core = (repo.get_app_config("profile_prompt_core") or "").strip()
                    if not core and pol and (pol.llm_curation_prompt or "").strip():
                        core = (pol.llm_curation_prompt or "").strip()
                        if core:
                            repo.set_app_config("profile_prompt_core", core)

                    if not (topic and pol and core):
                        if is_zh:
                            await _send_ack("⚠️ Profile 未就绪（请先用 /profile 初始化）")
                        else:
                            await _send_ack("⚠️ Profile not ready (run /profile first)")
                        continue

                    effective = (core + ("\n\n" + new_delta if new_delta else "")).strip()
                    now_iso = dt.datetime.utcnow().isoformat() + "Z"
                    try:
                        repo.set_app_config("profile_prompt_delta", new_delta)
                        repo.set_app_config("profile_feedback_last_update_at_utc", now_iso)
                        repo.upsert_topic_policy(topic_id=int(topic.id), llm_curation_prompt=effective)
                        rev = repo.add_profile_revision(
                            kind="manual",
                            core_prompt=core,
                            delta_prompt=new_delta,
                            effective_prompt=effective,
                            note="manual edit via telegram",
                            applied_feedback_ids=fb_ids,
                        )
                        if fb_ids:
                            repo.mark_feedback_events_applied(ids=fb_ids)
                        try:
                            t_pd.status = "done"
                            t_pd.result_key = "profile_delta_manual"
                            t_pd.error = ""
                            t_pd.finished_at = dt.datetime.utcnow()
                            repo.session.commit()
                        except Exception:
                            pass
                        if is_zh:
                            await _send_ack(f"✅ 已应用手工 delta（rev={int(rev.id)})")
                        else:
                            await _send_ack(f"✅ applied manual delta (rev={int(rev.id)})")
                    except Exception as exc:
                        if is_zh:
                            await _send_ack(f"⚠️ 应用失败：{exc}")
                        else:
                            await _send_ack(f"⚠️ apply failed: {exc}")
                    continue

                # 0.7b) Prompt delta proposal edit (reply with replacement delta)
                t_td = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="prompt_delta",
                )
                if t_td:
                    if (t_td.status or "").strip() != "awaiting" or int(getattr(t_td, "option", 0) or 0) != 1:
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        try:
                            t_td.option = 0
                            repo.session.commit()
                        except Exception:
                            pass
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消编辑（可继续点 Apply/Reject）")
                        else:
                            await _send_ack("✅ edit canceled (you can still tap Apply/Reject)")
                        continue

                    new_delta = raw.strip()
                    if len(new_delta) > 2000:
                        new_delta = new_delta[:2000] + "…"

                    # Load payload from task intent (best-effort).
                    try:
                        payload = json.loads((getattr(t_td, "intent", "") or "").strip() or "{}")
                    except Exception:
                        payload = {}
                    target_slot_id = "research.engine.synth.operator_delta"
                    target_template_id = ""
                    out_lang = _out_lang()
                    fb_ids: list[int] = []
                    if isinstance(payload, dict):
                        ts = str(payload.get("target_slot_id") or "").strip()
                        if ts:
                            target_slot_id = ts
                        target_template_id = str(payload.get("target_template_id") or "").strip()
                        out_lang = str(payload.get("lang") or "").strip() or out_lang
                        raw_ids = payload.get("feedback_ids")
                        if isinstance(raw_ids, list):
                            for x in raw_ids:
                                try:
                                    n = int(x)
                                except Exception:
                                    n = 0
                                if n > 0:
                                    fb_ids.append(n)

                    is_zh = (out_lang or "").strip().lower().startswith("zh") or out_lang in {"中文", "简体中文", "繁體中文", "繁体中文"}
                    try:
                        from tracker.prompt_templates import builtin_templates, load_custom_templates, save_custom_templates
                        from tracker.prompt_templates import resolve_prompt_best_effort

                        tpl_id = (target_template_id or "").strip()
                        if not tpl_id:
                            try:
                                tpl_id = resolve_prompt_best_effort(
                                    repo=repo,
                                    settings=settings,
                                    slot_id=target_slot_id,
                                    language=("zh" if is_zh else "en"),  # type: ignore[arg-type]
                                ).template_id
                            except Exception:
                                tpl_id = target_slot_id

                        custom = load_custom_templates(repo)
                        obj = custom.get(tpl_id)
                        if not isinstance(obj, dict):
                            obj = {}

                        built = builtin_templates().get(tpl_id)
                        title = str(obj.get("title") or (built.title if built else tpl_id) or tpl_id).strip()
                        desc = str(obj.get("description") or (built.description if built else "") or "").strip()
                        text_obj = obj.get("text")
                        if not isinstance(text_obj, dict):
                            text_obj = {}
                            if "text_zh" in obj or "text_en" in obj:
                                text_obj["zh"] = str(obj.get("text_zh") or "").strip()
                                text_obj["en"] = str(obj.get("text_en") or "").strip()
                        if is_zh:
                            text_obj["zh"] = new_delta
                        else:
                            text_obj["en"] = new_delta
                        custom[tpl_id] = {"title": title, "description": desc, "text": text_obj}
                        save_custom_templates(repo, custom)
                        repo.add_settings_change(
                            source="tg_prompt_delta_manual",
                            actor=str(uid or ""),
                            fields=["prompt_templates_custom_json"],
                            env_keys=["TRACKER_PROMPT_TEMPLATES_CUSTOM_JSON"],
                            restart_required=False,
                        )
                        if fb_ids:
                            try:
                                repo.mark_feedback_events_applied(ids=fb_ids)
                            except Exception:
                                pass
                        try:
                            t_td.status = "done"
                            t_td.result_key = "prompt_delta_manual"
                            t_td.error = ""
                            t_td.finished_at = dt.datetime.utcnow()
                            repo.session.commit()
                        except Exception:
                            pass
                        if is_zh:
                            await _send_ack("✅ 已应用手工 delta（提示词已更新）")
                        else:
                            await _send_ack("✅ applied manual delta (prompt updated)")
                    except Exception as exc:
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ 应用失败：{exc}")
                        else:
                            await _send_ack(f"⚠️ apply failed: {exc}")
                    continue

                # 0.8) /profile start (paste profile text -> LLM propose -> store draft)
                t_profile = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="profile_text",
                )
                if t_profile:
                    if (t_profile.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_profile.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消 Profile 设置")
                        else:
                            await _send_ack("✅ Profile setup canceled")
                        continue

                    try:
                        from tracker.llm import llm_propose_profile_setup
                        from tracker.llm_usage import make_llm_usage_recorder
                        from tracker.profile_input import normalize_profile_text

                        txt = normalize_profile_text(text=raw)
                        if not txt:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ 空的 PROFILE_TEXT（请重试）")
                            else:
                                await _send_ack("⚠️ empty PROFILE_TEXT (retry)")
                            continue

                        usage_cb = make_llm_usage_recorder(session=repo.session)
                        try:
                            settings_out = settings.model_copy(update={"output_language": _out_lang()})  # type: ignore[attr-defined]
                        except Exception:
                            settings_out = settings
                        proposal = await llm_propose_profile_setup(
                            repo=repo,
                            settings=settings_out,
                            profile_text=txt,
                            usage_cb=usage_cb,
                        )
                        if proposal is None or not (proposal.ai_prompt or "").strip():
                            repo.mark_telegram_task_failed(int(t_profile.id), error="llm not configured")
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ LLM 未配置（请先 /llm 设置 base_url/model 并启用 curation）")
                            else:
                                await _send_ack("⚠️ LLM not configured (set via /llm first)")
                            continue

                        draft = {
                            "topic_name": "Profile",
                            "profile_text": txt,
                            "understanding": proposal.understanding,
                            "interest_axes": list(proposal.interest_axes or []),
                            "interest_keywords": list(proposal.interest_keywords or []),
                            "retrieval_queries": list(proposal.retrieval_queries or []),
                            "ai_prompt": proposal.ai_prompt,
                        }
                        repo.set_app_config("profile_onboarding_draft_json", json.dumps(draft, ensure_ascii=False))
                        repo.mark_telegram_task_done(int(t_profile.id), result_key="profile_draft_ready")

                        is_zh = _out_lang() == "zh"
                        axes = [str(x).strip() for x in (proposal.interest_axes or []) if str(x).strip()]
                        axes_short = axes[:6]
                        axes_tail = f"\n…(+{len(axes) - len(axes_short)} more)" if len(axes) > len(axes_short) else ""
                        msg2 = (
                            "✅ Profile draft 已生成\n"
                            f"- understanding: {proposal.understanding}\n"
                            + ("- interest_axes:\n  - " + "\n  - ".join(axes_short) + axes_tail + "\n" if axes_short else "")
                            + "\n请选择 Apply 预设：\n"
                            "- full: HN RSS + Karpathy 90+ + GitHub Trending + arXiv + SearxNG\n"
                            "- light: 仅 RSS（HN RSS + Karpathy 90+）"
                            if is_zh
                            else (
                                "✅ Profile draft generated\n"
                                f"- understanding: {proposal.understanding}\n"
                                + ("- interest_axes:\n  - " + "\n  - ".join(axes_short) + axes_tail + "\n" if axes_short else "")
                                + "\nChoose a preset to Apply:\n"
                                "- full: HN RSS + Karpathy 90+ + GitHub Trending + arXiv + SearxNG\n"
                                "- light: RSS only (HN RSS + Karpathy 90+)"
                            )
                        )
                        kb2 = {
                            "inline_keyboard": [
                                [
                                    {"text": "Apply (full)", "callback_data": "profile:apply:full"},
                                    {"text": "Apply (light)", "callback_data": "profile:apply:light"},
                                ],
                                [
                                    {"text": "Profile menu", "callback_data": "profile:menu"},
                                ],
                            ]
                        }
                        await _send_with_markup(text=msg2, reply_markup=kb2)
                    except Exception as exc:
                        repo.mark_telegram_task_failed(int(t_profile.id), error=str(exc)[:4000])
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ Profile draft 生成失败：{exc}")
                        else:
                            await _send_ack(f"⚠️ Profile draft failed: {exc}")
                    continue

                # 1) /t add (topic add)
                t_topic = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="topic_add",
                )
                if t_topic:
                    if (t_topic.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_topic.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消添加 Topic")
                        else:
                            await _send_ack("✅ add topic canceled")
                        continue

                    created = 0
                    errors: list[str] = []
                    for line in raw.splitlines():
                        sline = (line or "").strip()
                        if not sline:
                            continue
                        parts = [p.strip() for p in sline.split("|")]
                        parts = [p for p in parts if p]
                        if len(parts) < 2:
                            errors.append(sline)
                            continue
                        name = parts[0]
                        query = parts[1]
                        digest_cron = parts[2] if len(parts) >= 3 else "0 9 * * *"
                        try:
                            repo.add_topic(name=name, query=query, digest_cron=digest_cron)
                            created += 1
                        except Exception as exc:
                            errors.append(f"{name}: {exc}")
                    if created > 0 and not errors:
                        repo.mark_telegram_task_done(int(t_topic.id), result_key="topic_add")
                    elif created > 0:
                        repo.mark_telegram_task_done(int(t_topic.id), result_key="topic_add_partial")
                    # Keep awaiting when all lines failed, so user can retry.

                    if _out_lang() == "zh":
                        msg2 = f"✅ 已创建 Topic: {created}"
                        if errors:
                            msg2 += "\n⚠️ 失败：\n" + "\n".join(errors[:5])
                        await _send_ack(msg2)
                    else:
                        msg2 = f"✅ topics created: {created}"
                        if errors:
                            msg2 += "\n⚠️ failures:\n" + "\n".join(errors[:5])
                        await _send_ack(msg2)
                    continue

                # 1.5) /t edit (topic edit)
                t_edit = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="topic_edit",
                )
                if t_edit:
                    if (t_edit.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_edit.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消编辑 Topic")
                        else:
                            await _send_ack("✅ edit topic canceled")
                        continue

                    updated = 0
                    errors: list[str] = []
                    for line in raw.splitlines():
                        sline = (line or "").strip()
                        if not sline:
                            continue
                        parts = [p.strip() for p in sline.split("|")]
                        parts = [p for p in parts if p]
                        if len(parts) < 2:
                            errors.append(sline)
                            continue
                        name = parts[0]
                        query = parts[1]
                        digest_cron = parts[2] if len(parts) >= 3 else None
                        try:
                            topic = repo.get_topic_by_name(name)
                            if not topic:
                                errors.append(f"{name}: not found")
                                continue
                            topic.query = query
                            if digest_cron:
                                topic.digest_cron = digest_cron
                            repo.session.commit()
                            updated += 1
                        except Exception as exc:
                            errors.append(f"{name}: {exc}")

                    if updated > 0 and not errors:
                        repo.mark_telegram_task_done(int(t_edit.id), result_key="topic_edit")
                    elif updated > 0:
                        repo.mark_telegram_task_done(int(t_edit.id), result_key="topic_edit_partial")
                    # Keep awaiting when all lines failed, so user can retry.

                    if _out_lang() == "zh":
                        msg2 = f"✅ 已更新 Topic: {updated}"
                        if errors:
                            msg2 += "\n⚠️ 失败：\n" + "\n".join(errors[:5])
                        await _send_ack(msg2)
                    else:
                        msg2 = f"✅ topics updated: {updated}"
                        if errors:
                            msg2 += "\n⚠️ failures:\n" + "\n".join(errors[:5])
                    await _send_ack(msg2)
                    continue

                # 1.8) /s add (sources import)
                t_src = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="source_import",
                )
                if t_src:
                    if (t_src.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_src.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消添加 Sources")
                        else:
                            await _send_ack("✅ add sources canceled")
                        continue

                    # Extract URLs (best-effort) and dedupe.
                    raw_urls = [m.strip() for m in _URL_RE.findall(raw or "")]
                    cleaned: list[str] = []
                    for u in raw_urls:
                        s2 = (u or "").strip().rstrip(").,;]")
                        if s2:
                            cleaned.append(s2)
                    seen: set[str] = set()
                    valid: list[str] = []
                    invalid: list[str] = []
                    for u in cleaned:
                        if u in seen:
                            continue
                        seen.add(u)
                        try:
                            sp = urlsplit(u)
                            if sp.scheme not in {"http", "https"} or not sp.netloc:
                                invalid.append(u)
                                continue
                        except Exception:
                            invalid.append(u)
                            continue
                        valid.append(u)

                    duplicates = max(0, len(cleaned) - len(seen))
                    if not valid:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 未识别到可用 URL（请直接粘贴 http(s)://... 链接；可多行）")
                        else:
                            await _send_ack("⚠️ no valid URLs found (paste http(s)://... links; multi-line ok)")
                        continue

                    # Mark input handled, and create a confirm prompt with an inline keyboard.
                    repo.mark_telegram_task_done(int(t_src.id), result_key="source_import_parsed")
                    try:
                        repo.cancel_telegram_tasks(
                            chat_id=existing_chat_id,
                            kind="source_import_confirm",
                            status="awaiting",
                            reason="superseded",
                        )
                    except Exception:
                        pass

                    is_zh = _out_lang() == "zh"
                    profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
                    profile_ok = bool(repo.get_topic_by_name(profile_topic_name))

                    doms: list[str] = []
                    for u in valid[:12]:
                        d = _domain_from_url(u)
                        if d:
                            doms.append(d)
                    uniq_doms: list[str] = []
                    for d in doms:
                        if d in uniq_doms:
                            continue
                        uniq_doms.append(d)

                    lines2: list[str] = []
                    if is_zh:
                        lines2.append("Sources 导入预览")
                        lines2.append(f"- valid: {len(valid)}")
                        lines2.append(f"- duplicates: {duplicates}")
                        lines2.append(f"- invalid: {len(invalid)}")
                        if uniq_doms:
                            lines2.append(f"- domains: {', '.join(uniq_doms[:8])}")
                        lines2.append("")
                        lines2.append("将按 RSS 形式创建 sources（type=rss）。请选择是否绑定到某个 Topic：")
                    else:
                        lines2.append("Sources import preview")
                        lines2.append(f"- valid: {len(valid)}")
                        lines2.append(f"- duplicates: {duplicates}")
                        lines2.append(f"- invalid: {len(invalid)}")
                        if uniq_doms:
                            lines2.append(f"- domains: {', '.join(uniq_doms[:8])}")
                        lines2.append("")
                        lines2.append("We'll create RSS sources (type=rss). Choose whether to bind to a Topic:")

                    kb_rows: list[list[dict[str, str]]] = []
                    row: list[dict[str, str]] = [{"text": ("✅ 仅导入" if is_zh else "✅ Import only"), "callback_data": "s:imp:apply:none"}]
                    if profile_ok:
                        row.append(
                            {
                                "text": (f"🔗 绑定 {profile_topic_name}" if is_zh else f"🔗 Bind {profile_topic_name}"),
                                "callback_data": "s:imp:apply:profile",
                            }
                        )
                    kb_rows.append(row)
                    kb_rows.append(
                        [
                            {"text": ("🔗 选择 Topic…" if is_zh else "🔗 Pick Topic…"), "callback_data": "s:imp:pick_topic:0"},
                            {"text": ("取消" if is_zh else "Cancel"), "callback_data": "s:imp:cancel"},
                        ]
                    )

                    prompt_mid = await _send_with_markup(text="\n".join(lines2).strip(), reply_markup={"inline_keyboard": kb_rows})
                    if prompt_mid > 0:
                        try:
                            repo.create_telegram_task(
                                chat_id=existing_chat_id,
                                user_id=uid,
                                kind="source_import_confirm",
                                status="awaiting",
                                prompt_message_id=prompt_mid,
                                request_message_id=msg_id,
                                query=json.dumps({"urls": valid}, ensure_ascii=False),
                            )
                        except Exception:
                            pass
                    continue

                # 2) /config tz custom
                t_tz = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="config_set_tz",
                )
                if t_tz:
                    if (t_tz.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_tz.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消设置时区")
                        else:
                            await _send_ack("✅ timezone update canceled")
                        continue
                    tz = raw.splitlines()[0].strip()
                    if not tz:
                        continue
                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates

                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates={"TRACKER_CRON_TIMEZONE": tz},
                        )
                        repo.mark_telegram_task_done(int(t_tz.id), result_key="cron_timezone")
                        if _out_lang() == "zh":
                            tail = "（重启服务后生效）" if res.restart_required else ""
                            await _send_ack(f"✅ 已更新：TRACKER_CRON_TIMEZONE{tail}")
                        else:
                            tail = " (restart services to apply)" if res.restart_required else ""
                            await _send_ack(f"✅ updated: TRACKER_CRON_TIMEZONE{tail}")
                    except Exception:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ 写入 .env 失败（请用 /env 或 Web Admin）")
                        else:
                            await _send_ack("⚠️ failed to write .env (use /env or Web Admin)")
                    continue

                # 2.05) /config mute days custom
                t_mute = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="config_set_mute_days",
                )
                if t_mute:
                    if (t_mute.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_mute.id), reason="user_canceled")
                        await _send_ack("✅ 已取消设置默认静音天数" if _out_lang() == "zh" else "✅ mute days update canceled")
                        continue
                    first = raw.splitlines()[0].strip()
                    if not first:
                        continue
                    try:
                        n = int(first)
                    except Exception:
                        n = 0
                    if n <= 0:
                        await _send_ack("⚠️ 请输入整数（1-365）" if _out_lang() == "zh" else "⚠️ please enter an integer (1-365)")
                        continue
                    n = max(1, min(365, int(n)))
                    try:
                        repo.set_app_config("telegram_feedback_mute_days_default", str(n))
                        repo.mark_telegram_task_done(int(t_mute.id), result_key="telegram_feedback_mute_days_default")
                        if _out_lang() == "zh":
                            await _send_ack(f"✅ 已设置默认静音天数：{n} 天")
                        else:
                            await _send_ack(f"✅ set default mute days: {n}d")
                    except Exception:
                        await _send_ack("⚠️ 写入失败（请用 Web Admin）" if _out_lang() == "zh" else "⚠️ failed to update (use Web Admin)")

                    text2, kb2 = _config_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                # 2.5) /api host custom
                t_api_host = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="api_set_host",
                )
                if t_api_host:
                    if (t_api_host.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_api_host.id), reason="user_canceled")
                        await _send_ack("✅ 已取消设置 API host" if _out_lang() == "zh" else "✅ API host update canceled")
                        continue
                    host2 = raw.splitlines()[0].strip()
                    low2 = host2.lower()
                    if not host2 or any(ch.isspace() for ch in host2) or len(host2) > 128:
                        await _send_ack("⚠️ host 不合法（请重试）" if _out_lang() == "zh" else "⚠️ invalid host (retry)")
                        continue

                    is_loopback = low2 in {"127.0.0.1", "::1", "localhost"}
                    if not is_loopback:
                        env = _read_env_assignments()
                        token_set = bool((env.get("TRACKER_API_TOKEN") or "").strip())
                        pw_set = bool((env.get("TRACKER_ADMIN_PASSWORD") or "").strip())
                        if not (token_set or pw_set):
                            await _send_ack(
                                "⚠️ 绑定到 0.0.0.0 前，需要先通过 /env 配置 TRACKER_API_TOKEN 或 TRACKER_ADMIN_PASSWORD（否则 tracker-api 会拒绝启动）"
                                if _out_lang() == "zh"
                                else "⚠️ Before binding to 0.0.0.0, set TRACKER_API_TOKEN or TRACKER_ADMIN_PASSWORD via /env"
                            )
                            continue

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates

                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates={"TRACKER_API_HOST": host2},
                        )
                        repo.mark_telegram_task_done(int(t_api_host.id), result_key="api_host")
                        if _out_lang() == "zh":
                            tail = "（重启 tracker-api 后生效：/restart）" if res.restart_required else ""
                            await _send_ack(f"✅ 已更新：TRACKER_API_HOST={host2}{tail}")
                        else:
                            tail = " (restart tracker-api to apply: /restart)" if res.restart_required else ""
                            await _send_ack(f"✅ updated: TRACKER_API_HOST={host2}{tail}")
                    except Exception:
                        await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")
                    continue

                # 2.6) /api port custom
                t_api_port = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="api_set_port",
                )
                if t_api_port:
                    if (t_api_port.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_api_port.id), reason="user_canceled")
                        await _send_ack("✅ 已取消设置 API port" if _out_lang() == "zh" else "✅ API port update canceled")
                        continue
                    first = raw.splitlines()[0].strip()
                    try:
                        port = int(first)
                    except Exception:
                        port = 0
                    if port < 1 or port > 65535:
                        await _send_ack("⚠️ port 不合法（请重试）" if _out_lang() == "zh" else "⚠️ invalid port (retry)")
                        continue
                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates

                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates={"TRACKER_API_PORT": str(port)},
                        )
                        repo.mark_telegram_task_done(int(t_api_port.id), result_key="api_port")
                        if _out_lang() == "zh":
                            tail = "（重启 tracker-api 后生效：/restart）" if res.restart_required else ""
                            await _send_ack(f"✅ 已更新：TRACKER_API_PORT={port}{tail}")
                        else:
                            tail = " (restart tracker-api to apply: /restart)" if res.restart_required else ""
                            await _send_ack(f"✅ updated: TRACKER_API_PORT={port}{tail}")
                    except Exception:
                        await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")
                    continue

                # 2.7) /api auth (token/password) via reply
                t_api_auth = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="api_set_auth",
                )
                if t_api_auth:
                    if (t_api_auth.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_api_auth.id), reason="user_canceled")
                        await _send_ack("✅ 已取消 API/Auth 配置" if _out_lang() == "zh" else "✅ API/Auth config canceled")
                        continue

                    env_key = (t_api_auth.query or "").strip()
                    if env_key not in {"TRACKER_API_TOKEN", "TRACKER_ADMIN_PASSWORD"}:
                        repo.mark_telegram_task_failed(int(t_api_auth.id), error="invalid env key")
                        continue

                    value = ""
                    for line in raw.splitlines():
                        sline = (line or "").strip()
                        if sline:
                            value = sline
                            break
                    if not value:
                        continue

                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

                        updates = parse_settings_env_block(
                            f"{env_key}={value}\n",
                            allow_remote_updates=True,
                            blank_values_mean_no_change=False,
                        )
                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates=updates,
                        )
                        repo.mark_telegram_task_done(int(t_api_auth.id), result_key="api_auth_updated")
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except ValueError as exc:
                        repo.mark_telegram_task_failed(int(t_api_auth.id), error=str(exc))
                        await _send_ack(f"⚠️ 配置不合法：{exc}" if _out_lang() == "zh" else f"⚠️ invalid config: {exc}")
                    except Exception:
                        repo.mark_telegram_task_failed(int(t_api_auth.id), error="apply failed")
                        await _send_ack("⚠️ 写入 .env 失败" if _out_lang() == "zh" else "⚠️ failed to write .env")

                    text2, kb2 = _api_menu()
                    await _send_with_markup(text=text2, reply_markup=kb2)
                    continue

                t_cfgag = repo.get_telegram_task_by_prompt_message(
                    chat_id=existing_chat_id,
                    prompt_message_id=reply_mid,
                    kind="config_agent",
                )
                if t_cfgag:
                    if (t_cfgag.status or "").strip() != "awaiting":
                        continue
                    raw = (text or "").strip()
                    low = raw.lower().strip()
                    if raw in {"0"} or low in {"cancel", "取消"}:
                        repo.mark_telegram_task_canceled(int(t_cfgag.id), reason="user_canceled")
                        if _out_lang() == "zh":
                            await _send_ack("✅ 已取消本次智能配置")
                        else:
                            await _send_ack("✅ Config request canceled")
                        continue

                    merged_prompt = (t_cfgag.query or "").strip()
                    if merged_prompt:
                        merged_prompt = merged_prompt + "\n\n补充/修订：\n" + raw
                    else:
                        merged_prompt = raw
                    placeholder_mid = -int(dt.datetime.utcnow().timestamp() * 1_000_000)
                    repo.mark_telegram_task_canceled(int(t_cfgag.id), reason="superseded_by_reply")
                    repo.cancel_telegram_tasks(chat_id=existing_chat_id, kind="config_agent", status="pending", reason="superseded_by_reply")
                    repo.create_telegram_task(
                        chat_id=existing_chat_id,
                        user_id=uid,
                        kind="config_agent",
                        status="pending",
                        prompt_message_id=placeholder_mid,
                        request_message_id=(msg_id if msg_id > 0 else 0),
                        query=merged_prompt,
                    )
                    if _out_lang() == "zh":
                        await _send_ack("⏳ 已加入智能配置修订队列…")
                    else:
                        await _send_ack("⏳ Queued config refinement…")
                    continue

            target_item_id = 0
            if reply_mid > 0:
                tm = repo.get_telegram_message(chat_id=existing_chat_id, message_id=reply_mid)
                target_item_id = int(getattr(tm, "item_id", 0) or 0) if tm else 0

            # Parse explicit item id in command text.
            if target_item_id <= 0:
                m = _CMD_ITEM_ID_RE.search(s)
                if m:
                    try:
                        target_item_id = int(m.group(1))
                    except Exception:
                        target_item_id = 0

            # Parse simple "/like 123" style.
            tokens = s.split()
            if tokens and tokens[0].startswith("/"):
                cmd = tokens[0].lstrip("/").strip().lower()
                if "@" in cmd:
                    cmd = cmd.split("@", 1)[0].strip()
            else:
                cmd = ""

            async def _send_one_with_markup(*, text: str, reply_markup: dict | None) -> int:
                msg = (text or "").strip()
                if not msg:
                    return 0
                try:
                    from tracker.push.telegram import TelegramPusher

                    p = TelegramPusher(token, timeout_seconds=int(settings.http_timeout_seconds or 20))
                    return int(
                        await p.send_raw_text(
                            chat_id=existing_chat_id,
                            text=msg,
                            disable_preview=True,
                            reply_markup=reply_markup,
                        )
                        or 0
                    )
                except Exception:
                    return 0

            # Quick setup: show readiness and next steps (no website).
            if cmd in {"setup", "init"}:
                is_zh = _out_lang() == "zh"
                # Compute a minimal readiness snapshot so this is a true "bootstrap" UX.
                tz = (repo.get_app_config("cron_timezone") or getattr(settings, "cron_timezone", "") or "").strip() or "UTC"
                ol = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
                has_lang = bool(ol.strip())

                llm_base = (repo.get_app_config("llm_base_url") or getattr(settings, "llm_base_url", "") or "").strip()
                llm_model = (repo.get_app_config("llm_model") or getattr(settings, "llm_model", "") or "").strip()
                llm_key_ok = bool((getattr(settings, "llm_api_key", "") or "").strip())
                llm_ready = bool(llm_base and llm_model and llm_key_ok)

                mini_model = (repo.get_app_config("llm_model_mini") or getattr(settings, "llm_model_mini", "") or "").strip()
                mini_base = (repo.get_app_config("llm_mini_base_url") or getattr(settings, "llm_mini_base_url", "") or "").strip()
                mini_key_ok = bool((getattr(settings, "llm_mini_api_key", "") or "").strip())
                mini_ready = bool(mini_model and (mini_key_ok or not mini_base))

                def _as_bool2(raw: str, fallback: bool) -> bool:
                    low = (raw or "").strip().lower()
                    if low in {"true", "1", "yes", "y", "on"}:
                        return True
                    if low in {"false", "0", "no", "n", "off"}:
                        return False
                    return bool(fallback)

                # Scheduling surface: Curated Info (batch, de-dupe-only).
                curated_enabled = _as_bool2(
                    (repo.get_app_config("digest_scheduler_enabled") or "").strip(),
                    bool(getattr(settings, "digest_scheduler_enabled", False)),
                )
                curated_push = _as_bool2(
                    (repo.get_app_config("digest_push_enabled") or "").strip(),
                    bool(getattr(settings, "digest_push_enabled", False)),
                )
                try:
                    hours = int(repo.get_app_config("digest_hours") or getattr(settings, "digest_hours", 0) or 0)
                except Exception:
                    hours = int(getattr(settings, "digest_hours", 0) or 0)
                schedule_ready = bool(curated_enabled and hours > 0)

                # Extra push channels (optional): DingTalk / Email / Webhook.
                # Telegram is already connected if we're here.
                has_dingtalk = bool((getattr(settings, "dingtalk_webhook_url", "") or "").strip())
                has_email = bool(
                    (getattr(settings, "smtp_host", "") or "").strip()
                    and (getattr(settings, "email_from", "") or "").strip()
                    and (getattr(settings, "email_to", "") or "").strip()
                    and (getattr(settings, "smtp_password", "") or "").strip()
                )
                has_webhook = bool((getattr(settings, "webhook_url", "") or "").strip())
                push_extra_ready = bool(has_dingtalk or has_email or has_webhook)

                # Auth / cookie (optional): needed for login-required sources (e.g. Discourse private categories).
                env = _read_env_assignments()
                discourse_cookie_set = bool(
                    (env.get("TRACKER_DISCOURSE_COOKIE") or str(getattr(settings, "discourse_cookie", "") or "")).strip()
                )
                cookie_jar_set = bool(
                    (env.get("TRACKER_COOKIE_JAR_JSON") or str(getattr(settings, "cookie_jar_json", "") or "")).strip()
                )
                auth_ready = bool(discourse_cookie_set or cookie_jar_set)

                has_discourse_sources = False
                try:
                    has_discourse_sources = any(
                        ((getattr(s, "type", "") or "").strip() == "discourse") and bool(getattr(s, "enabled", True))
                        for s in repo.list_sources()
                    )
                except Exception:
                    has_discourse_sources = False

                profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
                profile_topic = repo.get_topic_by_name(profile_topic_name)
                profile_prompt_ok = False
                if profile_topic:
                    pol = repo.get_topic_policy(topic_id=int(profile_topic.id))
                    profile_prompt_ok = bool(pol and (pol.llm_curation_prompt or "").strip())
                profile_ready = bool(profile_topic and profile_prompt_ok)

                topics = repo.list_topics()
                topics_total = len(topics)
                topics_enabled = sum(1 for t in topics if bool(getattr(t, "enabled", False)))

                def _ok_mark(ok: bool) -> str:
                    if is_zh:
                        return "✅" if ok else "⚠️"
                    return "✅" if ok else "⚠️"

                missing_llm: list[str] = []
                if not llm_base:
                    missing_llm.append("base_url")
                if not llm_model:
                    missing_llm.append("model")
                if not llm_key_ok:
                    missing_llm.append("api_key")

                next_steps: list[str] = []
                if not has_lang:
                    next_steps.append(
                        "1) /config 设置语言/时区" if is_zh else "1) /config set language/timezone"
                    )
                if not llm_ready:
                    if is_zh:
                        tail = f"（缺: {', '.join(missing_llm)}）" if missing_llm else ""
                        next_steps.append(f"2) /llm 配置 LLM{tail}")
                    else:
                        tail = f" (missing: {', '.join(missing_llm)})" if missing_llm else ""
                        next_steps.append(f"2) /llm configure LLM{tail}")
                if not schedule_ready:
                    next_steps.append(
                        "（推荐）/config 开启参考消息调度（批次去重，减少打扰）"
                        if is_zh
                        else "(recommended) /config enable Curated Info scheduling (batch + de-dupe)"
                    )
                if not profile_ready:
                    next_steps.append(
                        "3) /profile → Start → Apply（生成并应用 Profile prompt + sources）"
                        if is_zh
                        else "3) /profile → Start → Apply (generate/apply Profile prompt + sources)"
                    )
                if topics_enabled <= 0:
                    next_steps.append(
                        "4) /t 添加至少 1 个 Topic（或先用 Profile 作为唯一 Topic）"
                        if is_zh
                        else "4) /t add at least 1 Topic (or use Profile as the only Topic first)"
                    )
                if not push_extra_ready:
                    next_steps.append(
                        "5) （可选）/push 配置钉钉/邮件/Webhook"
                        if is_zh
                        else "5) (optional) /push configure DingTalk/Email/Webhook"
                    )
                if has_discourse_sources and not auth_ready:
                    next_steps.append(
                        "6) （可选）/auth 配置登录/Cookie（用于需要登录的信息源）"
                        if is_zh
                        else "6) (optional) /auth configure login/cookies (for login-required sources)"
                    )

                if not next_steps:
                    next_steps.append(
                        "✅ 已完成基础 bootstrap：等待推送（快速消息/参考消息）。"
                        if is_zh
                        else "✅ Bootstrap complete: wait for pushes (Quick Messages / Curated Info)."
                    )
                curated_mode = f"every {hours}h" if hours > 0 else "-"
                curated_line = (
                    f"参考消息：{'ON' if curated_enabled else 'OFF'} / push={'ON' if curated_push else 'OFF'} / cadence={curated_mode}"
                    if is_zh
                    else f"Curated Info: {'ON' if curated_enabled else 'OFF'} / push={'ON' if curated_push else 'OFF'} / cadence={curated_mode}"
                )
                msg2 = (
                    "快速配置向导（无网页）\n\n"
                    f"- 基础：语言={ol or '-'} / 时区={tz}  {_ok_mark(has_lang)}\n"
                    f"- LLM(Reasoning)：base_url/model/api_key  {_ok_mark(llm_ready)}\n"
                    f"- LLM(Mini)：model(+可选单独provider)  {_ok_mark(mini_ready)}\n"
                    f"- {curated_line}  {_ok_mark(schedule_ready)}\n"
                    f"- Push（可选）：钉钉/邮件/Webhook  {_ok_mark(push_extra_ready)}\n"
                    f"- Auth（可选）：登录/Cookie  {_ok_mark(auth_ready) if has_discourse_sources else _ok_mark(True)}\n"
                    f"- Profile：topic+prompt  {_ok_mark(profile_ready)}\n"
                    f"- Topics：enabled {topics_enabled}/{topics_total}\n\n"
                    "下一步（按需做缺的）：\n"
                    + "\n".join(next_steps)
                    + "\n\n高级：/env 粘贴 env 配置块（不会回显密钥；可覆盖几乎所有 TRACKER_*）\n"
                    "检查：/status"
                    if is_zh
                    else (
                        "Quick setup (no website)\n\n"
                        f"- Basics: language={ol or '-'} / timezone={tz}  {_ok_mark(has_lang)}\n"
                        f"- LLM (Reasoning): base_url/model/api_key  {_ok_mark(llm_ready)}\n"
                        f"- LLM (Mini): model(+optional separate provider)  {_ok_mark(mini_ready)}\n"
                        f"- {curated_line}  {_ok_mark(schedule_ready)}\n"
                        f"- Push (optional): DingTalk/Email/Webhook  {_ok_mark(push_extra_ready)}\n"
                        f"- Auth (optional): login/cookies  {_ok_mark(auth_ready) if has_discourse_sources else _ok_mark(True)}\n"
                        f"- Profile: topic+prompt  {_ok_mark(profile_ready)}\n"
                        f"- Topics: enabled {topics_enabled}/{topics_total}\n\n"
                        "Next (do the missing ones):\n"
                        + "\n".join(next_steps)
                        + "\n\nAdvanced: /env paste an env block (secrets not echoed; can override most TRACKER_*).\n"
                        "Check: /status"
                    )
                )
                kb2 = {
                    "inline_keyboard": [
                        [
                            {"text": ("配置中心" if is_zh else "Config"), "callback_data": "cfgc:menu"},
                            {"text": ("LLM" if is_zh else "LLM"), "callback_data": "llm:menu"},
                            {"text": ("Topics" if is_zh else "Topics"), "callback_data": "t:page:0"},
                        ],
                        [
                            {"text": ("Sources" if is_zh else "Sources"), "callback_data": "s:page:0"},
                            {"text": ("Profile" if is_zh else "Profile"), "callback_data": "profile:menu"},
                            {"text": ("Research" if is_zh else "Research"), "callback_data": "research:menu"},
                        ],
                        [
                            {"text": ("Push" if is_zh else "Push"), "callback_data": "push:menu"},
                            {"text": ("Auth" if is_zh else "Auth"), "callback_data": "auth:menu"},
                            {"text": ("API/Admin" if is_zh else "API/Admin"), "callback_data": "api:menu"},
                        ],
                    ]
                }
                await _send_with_markup(text=msg2, reply_markup=kb2)
                continue

            if cmd in {"why"}:
                # Explain "why was this pushed?" for an item.
                #
                # Supported anchors:
                # - reply to an alert message (uses TelegramMessage.item_id)
                # - "/why #<item_id>" (explicit id)
                # - "/why <url>" (lookup by canonical_url)
                is_zh = _out_lang() == "zh"

                item_id2 = int(target_item_id or 0)
                url2 = _extract_first_url(s)

                # Fallback: if replying, try extracting a URL from the replied message text.
                if not (item_id2 > 0 or url2) and isinstance(reply, dict):
                    rt = reply.get("text")
                    if isinstance(rt, str):
                        url2 = _extract_first_url(rt)

                if item_id2 <= 0 and url2:
                    try:
                        from sqlalchemy import select

                        from tracker.models import Item
                        from tracker.normalize import canonicalize_url

                        cu = canonicalize_url(url2)
                        row = repo.session.scalar(select(Item).where(Item.canonical_url == cu))
                        if not row:
                            row = repo.session.scalar(select(Item).where(Item.url == url2))
                        if row:
                            item_id2 = int(row.id)
                    except Exception:
                        item_id2 = 0

                if item_id2 <= 0:
                    await _send_ack(
                        "⚠️ /why 需要一个 item（请 reply 到某条 Alert，或发送 `/why #<item_id>` / `/why <url>`）"
                        if is_zh
                        else "⚠️ /why needs an item. Reply to an Alert, or send `/why #<item_id>` / `/why <url>`."
                    )
                    continue

                try:
                    from sqlalchemy import select

                    from tracker.models import Item, ItemTopic, Source, Topic
                except Exception:
                    await _send_ack("⚠️ /why unavailable" if not is_zh else "⚠️ /why 暂不可用")
                    continue

                item2 = repo.session.get(Item, int(item_id2))
                if not item2:
                    await _send_ack(
                        f"⚠️ 未找到 item_id={item_id2}" if is_zh else f"⚠️ item not found: {item_id2}"
                    )
                    continue

                url_final = (getattr(item2, "canonical_url", "") or "").strip() or (getattr(item2, "url", "") or "").strip()
                domain2 = _domain_from_url(url_final)
                src2 = repo.session.get(Source, int(getattr(item2, "source_id", 0) or 0)) if getattr(item2, "source_id", 0) else None

                rows = list(
                    repo.session.execute(
                        select(ItemTopic, Topic)
                        .join(Topic, Topic.id == ItemTopic.topic_id)
                        .where(ItemTopic.item_id == int(item2.id))
                        .order_by(Topic.id.asc())
                    ).all()
                )

                try:
                    muted2 = repo.is_muted(scope="domain", key=domain2, when=dt.datetime.utcnow()) if domain2 else False
                except Exception:
                    muted2 = False

                # Build a compact, audit-friendly explanation (facts + stored reasons).
                title2 = (getattr(item2, "title", "") or "").strip()
                header = (
                    f"为什么会推送（item_id={int(item2.id)}）"
                    if is_zh
                    else f"Why pushed (item_id={int(item2.id)})"
                )
                lines: list[str] = [header, ""]
                if title2:
                    lines.append(f"- title: {title2}")
                lines.append(f"- url: {url_final}")
                if domain2:
                    lines.append(f"- domain: {domain2} ({'muted' if muted2 else 'active'})")
                if src2 is not None:
                    sid2 = int(getattr(src2, "id", 0) or 0)
                    st = (getattr(src2, "type", "") or "").strip()
                    su = (getattr(src2, "url", "") or "").strip()
                    if st or su:
                        prefix = f"#{sid2} " if sid2 > 0 else ""
                        lines.append(f"- source: {prefix}{st} {su}".strip())

                if not rows:
                    lines.append("")
                    lines.append(
                        "（尚无 item_topics 记录：可能还在处理/未命中任何 topic）"
                        if is_zh
                        else "(No item_topics records yet: still processing, or did not match any topic.)"
                    )
                    await _send_ack("\n".join(lines).strip())
                    continue

                lines.append("")
                lines.append("topic decisions:" if not is_zh else "Topic 决策：")
                for it, tp in rows:
                    tname = str(getattr(tp, "name", "") or "").strip()
                    dec = str(getattr(it, "decision", "") or "").strip()
                    rs = int(getattr(it, "relevance_score", 0) or 0)
                    ns = int(getattr(it, "novelty_score", 0) or 0)
                    qs = int(getattr(it, "quality_score", 0) or 0)
                    reason = str(getattr(it, "reason", "") or "").strip()
                    reason_short = (reason[:260] + "…") if len(reason) > 260 else reason
                    score_tail = ""
                    if any(x > 0 for x in (rs, ns, qs)):
                        score_tail = f" (rel={rs}, nov={ns}, qual={qs})"
                    lines.append(f"- {tname}: {dec}{score_tail}")
                    if reason_short:
                        lines.append(f"  - reason: {reason_short}")

                await _send_ack("\n".join(lines).strip())
                continue

            if cmd in {"t", "topic", "topics"}:
                sub = tokens[1].strip().lower() if len(tokens) >= 2 else ""
                if sub in {"add", "new", "create"}:
                    try:
                        repo.cancel_telegram_tasks(
                            chat_id=existing_chat_id,
                            kind="topic_add",
                            status="awaiting",
                            reason="superseded",
                        )
                    except Exception:
                        pass
                    is_zh = _out_lang() == "zh"
                    prompt = (
                        "添加 Topic：请直接回复这条消息（建议一行一个），格式：\n"
                        "`name | query | digest_cron(可选)`\n\n"
                        "示例：\n"
                        "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                        "取消：回复 0 或 cancel"
                        if is_zh
                        else (
                            "Add Topic: reply to this message with:\n"
                            "`name | query | digest_cron(optional)`\n\n"
                            "Example:\n"
                            "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                            "Cancel: reply 0 or cancel"
                        )
                    )
                    prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                    if prompt_mid > 0:
                        try:
                            repo.create_telegram_task(
                                chat_id=existing_chat_id,
                                user_id=uid,
                                kind="topic_add",
                                status="awaiting",
                                prompt_message_id=prompt_mid,
                                request_message_id=msg_id,
                                query="topic_add",
                            )
                        except Exception:
                            pass
                    continue

                if sub in {"edit", "update", "set"}:
                    try:
                        repo.cancel_telegram_tasks(
                            chat_id=existing_chat_id,
                            kind="topic_edit",
                            status="awaiting",
                            reason="superseded",
                        )
                    except Exception:
                        pass
                    is_zh = _out_lang() == "zh"
                    prompt = (
                        "编辑 Topic：请直接回复这条消息（建议一行一个），格式：\n"
                        "`name | query | digest_cron(可选)`\n\n"
                        "示例：\n"
                        "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                        "取消：回复 0 或 cancel"
                        if is_zh
                        else (
                            "Edit Topic: reply to this message with:\n"
                            "`name | query | digest_cron(optional)`\n\n"
                            "Example:\n"
                            "`AI Tools | agent,workflow,tooling | 0 9 * * *`\n\n"
                            "Cancel: reply 0 or cancel"
                        )
                    )
                    prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                    if prompt_mid > 0:
                        try:
                            repo.create_telegram_task(
                                chat_id=existing_chat_id,
                                user_id=uid,
                                kind="topic_edit",
                                status="awaiting",
                                prompt_message_id=prompt_mid,
                                request_message_id=msg_id,
                                query="topic_edit",
                            )
                        except Exception:
                            pass
                    continue

                page_i = 0
                if sub.isdigit():
                    try:
                        page_i = max(0, int(sub))
                    except Exception:
                        page_i = 0
                text2, kb2 = _topic_menu(page=page_i)
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"s", "src", "source", "sources"}:
                sub = tokens[1].strip().lower() if len(tokens) >= 2 else ""
                if sub in {"add", "new", "import", "create"}:
                    try:
                        repo.cancel_telegram_tasks(
                            chat_id=existing_chat_id,
                            kind="source_import",
                            status="awaiting",
                            reason="superseded",
                        )
                    except Exception:
                        pass
                    is_zh = _out_lang() == "zh"
                    prompt = (
                        "添加 Sources（RSS/Atom）：请直接回复这条消息粘贴 URL（可多行）。\n"
                        "我会先给出导入预览，再让你确认。\n\n"
                        "取消：回复 0 或 cancel"
                        if is_zh
                        else "Add sources (RSS/Atom): reply with URLs (multi-line ok). You'll get a preview before applying. Cancel: reply 0 or cancel"
                    )
                    prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                    if prompt_mid > 0:
                        try:
                            repo.create_telegram_task(
                                chat_id=existing_chat_id,
                                user_id=uid,
                                kind="source_import",
                                status="awaiting",
                                prompt_message_id=prompt_mid,
                                request_message_id=msg_id,
                                query="source_import",
                            )
                        except Exception:
                            pass
                    continue

                page_i = 0
                if sub.isdigit():
                    try:
                        page_i = max(0, int(sub))
                    except Exception:
                        page_i = 0
                text2, kb2 = _sources_menu(page=page_i)
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"b", "bind", "binding", "bindings"}:
                sub = tokens[1].strip().lower() if len(tokens) >= 2 else ""
                page_i = 0
                if sub.isdigit():
                    try:
                        page_i = max(0, int(sub))
                    except Exception:
                        page_i = 0
                text2, kb2 = _bindings_topic_menu(page=page_i)
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"config", "cfg"}:
                text2, kb2 = _cfgc_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"llm"}:
                text2, kb2 = _llm_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"prompts", "prompt"}:
                text2, kb2 = _prompts_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"push"}:
                text2, kb2 = _push_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"auth"}:
                text2, kb2 = _auth_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"api", "net"}:
                text2, kb2 = _api_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"profile", "p"}:
                text2, kb2 = _profile_menu()
                await _send_with_markup(text=text2, reply_markup=kb2)
                continue

            if cmd in {"env"}:
                # Allow: "/env\\nKEY=VALUE..." in the same message, or prompt for a reply.
                rest = ""
                if tokens:
                    prefix = tokens[0]
                    try:
                        rest = (text[len(prefix) :] if len(text) >= len(prefix) else "").strip()
                    except Exception:
                        rest = ""
                if rest:
                    try:
                        from pathlib import Path

                        from tracker.dynamic_config import apply_env_block_updates, parse_settings_env_block

                        updates = parse_settings_env_block(
                            rest,
                            allow_remote_updates=True,
                            blank_values_mean_no_change=True,
                        )
                        if not updates:
                            if _out_lang() == "zh":
                                await _send_ack("⚠️ env 导入：没有变化")
                            else:
                                await _send_ack("⚠️ env import: no changes")
                            continue
                        res = apply_env_block_updates(
                            repo=repo,
                            settings=settings,
                            env_path=Path(_env_path()),
                            env_updates=updates,
                        )
                        keys = ", ".join(sorted(res.updated_env_keys))
                        if _out_lang() == "zh":
                            tail = (
                                "\n重启服务后生效：/restart（或 systemctl --user restart tracker tracker-api）"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ 已更新：{keys}{tail}")
                        else:
                            tail = (
                                "\nRestart to apply: /restart (or systemctl --user restart tracker tracker-api)"
                                if res.restart_required
                                else ""
                            )
                            await _send_ack(f"✅ updated: {keys}{tail}")
                    except ValueError as exc:
                        if _out_lang() == "zh":
                            await _send_ack(f"⚠️ env block 不合法：{exc}")
                        else:
                            await _send_ack(f"⚠️ invalid env block: {exc}")
                    except Exception:
                        if _out_lang() == "zh":
                            await _send_ack("⚠️ env 导入失败（请重试或用 Web Admin）")
                        else:
                            await _send_ack("⚠️ env import failed (retry or use Web Admin)")
                    continue

                try:
                    repo.cancel_telegram_tasks(
                        chat_id=existing_chat_id,
                        kind="env_import",
                        status="awaiting",
                        reason="superseded",
                    )
                except Exception:
                    pass
                prompt = (
                    "env 导入：请直接回复这条消息粘贴一段 `.env` 配置块（支持绝大多数 TRACKER_*；少数关键项拒绝；密钥不会回显）。\n"
                    "取消：回复 0 或 cancel"
                    if _out_lang() == "zh"
                    else "Env import: reply with a dotenv block (supports most TRACKER_*; some dangerous keys are forbidden; secrets are not echoed). Cancel: reply 0 or cancel"
                )
                prompt_mid = await _send_with_markup(text=prompt, reply_markup=None)
                if prompt_mid > 0:
                    try:
                        repo.create_telegram_task(
                            chat_id=existing_chat_id,
                            user_id=uid,
                            kind="env_import",
                            status="awaiting",
                            prompt_message_id=prompt_mid,
                            request_message_id=msg_id,
                            query="env_import",
                        )
                    except Exception:
                        pass
                continue

            if cmd in {"restart", "reboot"}:
                # Best-effort: when running under systemd user services, restart Tracker + API.
                from tracker.service_control import queue_restart_systemd_user, restart_hint_text

                res = queue_restart_systemd_user(units=["tracker", "tracker-api"], delay_seconds=0.8)
                if res.ok:
                    if _out_lang() == "zh":
                        await _send_ack(f"♻️ 已排队重启：{', '.join(res.units)}")
                    else:
                        await _send_ack(f"♻️ Restart queued: {', '.join(res.units)}")
                else:
                    # If restart can't be triggered from inside the service, at least show the command.
                    if _out_lang() == "zh":
                        await _send_ack(f"⚠️ 自动重启失败：{res.message}\n{restart_hint_text(lang='zh', units=res.units)}")
                    else:
                        await _send_ack(f"⚠️ Auto restart failed: {res.message}\n{restart_hint_text(lang='en', units=res.units)}")
                continue

            domain_hint = ""
            rating_hint = 0
            days_hint = 0

            # Feedback commands and free-form replies can be disabled independently
            # from other bot commands (config/setup).
            if cmd in {"like", "dislike", "rate", "mute", "unmute"} and not replies_enabled:
                if _out_lang() == "zh":
                    await _send_ack("⚠️ 已关闭 Telegram 回复反馈（可在配置中心 → Push → Advanced 开启）")
                else:
                    await _send_ack("⚠️ Telegram reply feedback is disabled (enable it in Config Center → Push → Advanced).")
                continue

            # Command args (when present) have higher precision than emoji heuristics.
            if cmd in {"like", "dislike"}:
                if len(tokens) >= 2 and tokens[1].strip().isdigit() and target_item_id <= 0:
                    try:
                        target_item_id = int(tokens[1].strip())
                    except Exception:
                        target_item_id = 0
            elif cmd == "rate":
                if len(tokens) >= 2 and tokens[1].strip().isdigit() and target_item_id <= 0:
                    try:
                        target_item_id = int(tokens[1].strip())
                    except Exception:
                        target_item_id = 0
                if len(tokens) >= 3 and tokens[2].strip().isdigit():
                    try:
                        rating_hint = int(tokens[2].strip())
                    except Exception:
                        rating_hint = 0
            elif cmd == "mute":
                if len(tokens) >= 2:
                    arg1 = tokens[1].strip()
                    if arg1.isdigit() and target_item_id <= 0:
                        try:
                            target_item_id = int(arg1)
                        except Exception:
                            target_item_id = 0
                    else:
                        domain_hint = arg1
                if len(tokens) >= 3 and tokens[2].strip().isdigit():
                    try:
                        days_hint = int(tokens[2].strip())
                    except Exception:
                        days_hint = 0
            elif cmd == "unmute":
                if len(tokens) >= 2:
                    arg1 = tokens[1].strip()
                    if arg1.isdigit() and target_item_id <= 0:
                        try:
                            target_item_id = int(arg1)
                        except Exception:
                            target_item_id = 0
                    else:
                        domain_hint = arg1

            kind = ""
            value_int = 0
            days = 0
            if replies_enabled and (s in like_emojis or any(ch in s for ch in like_emojis)):
                kind = "like"
            elif replies_enabled and (s in dislike_emojis or any(ch in s for ch in dislike_emojis)):
                kind = "dislike"
            elif replies_enabled and (s in mute_emojis or "mute" in s.lower() or "静音" in s):
                kind = "mute"
            elif cmd == "like":
                kind = "like"
            elif cmd == "dislike":
                kind = "dislike"
            elif cmd == "rate":
                kind = "rate"
            elif cmd == "mute":
                kind = "mute"
            elif cmd == "unmute":
                kind = "unmute"
            elif reply_mid > 0 and s.isdigit():
                # Bare numeric rating is only meaningful when replying to a *single-item* push (alert).
                try:
                    tm_rating = repo.get_telegram_message(chat_id=existing_chat_id, message_id=reply_mid)
                except Exception:
                    tm_rating = None
                iid = int(getattr(tm_rating, "item_id", 0) or 0) if tm_rating else 0
                if iid > 0:
                    try:
                        v = int(s)
                    except Exception:
                        v = 0
                    if 1 <= v <= 5:
                        kind = "rate"
                        rating_hint = v

            # Optional numbers: rating or mute days.
            if kind == "rate":
                value_int = max(1, min(5, int(rating_hint or 0))) if rating_hint else 0
            if kind == "mute":
                days = int(days_hint or 0)
                if days <= 0:
                    days = _default_mute_days()
                value_int = max(1, min(365, days))

            if kind not in {"like", "dislike", "rate", "mute", "unmute"}:
                if reply_mid <= 0 and not cmd and not s.startswith("/"):
                    placeholder_mid = -int(dt.datetime.utcnow().timestamp() * 1_000_000)
                    repo.cancel_telegram_tasks(chat_id=existing_chat_id, kind="config_agent", status="pending", reason="superseded")
                    repo.cancel_telegram_tasks(chat_id=existing_chat_id, kind="config_agent", status="awaiting", reason="superseded")
                    repo.create_telegram_task(
                        chat_id=existing_chat_id,
                        user_id=uid,
                        kind="config_agent",
                        status="pending",
                        prompt_message_id=placeholder_mid,
                        request_message_id=(msg_id if msg_id > 0 else 0),
                        query=s,
                    )
                    if _out_lang() == "zh":
                        await _send_ack("⏳ 已加入智能配置队列…")
                    else:
                        await _send_ack("⏳ Queued for config planning…")
                    continue

                # Free-form reply comment: when the operator replies to a pushed message with
                # natural language feedback (not a reaction/command), capture it as a "comment"
                # event and offer an interactive action menu. This keeps profile updates
                # confirmable (avoid drift) while still being fast.
                if not replies_enabled:
                    continue
                if reply_mid > 0 and not s.startswith("/"):
                    tm0 = repo.get_telegram_message(chat_id=existing_chat_id, message_id=reply_mid)
                    if tm0:
                        item_id_hint = int(getattr(tm0, "item_id", 0) or 0) if tm0 else 0
                        action_hint, ref_idx, remainder = _parse_reply_item_selector(s)
                        comment_text = remainder if (action_hint in {"like", "dislike"} and remainder) else (remainder if ref_idx else s)

                        url0 = ""
                        dom0 = ""
                        ref_title = ""
                        if item_id_hint > 0:
                            url0, dom0 = _item_from_id(item_id_hint)

                        # Report reader: allow "👎2"/"#2"/"第2条"/"2" to anchor to References[#2].
                        if (item_id_hint <= 0) and (not url0) and ref_idx and str(getattr(tm0, "idempotency_key", "") or "").strip():
                            report_kind = _report_kind_from_message_key_or_kind(
                                msg_kind=str(getattr(tm0, "kind", "") or ""),
                                idempotency_key=str(getattr(tm0, "idempotency_key", "") or ""),
                            )
                            if report_kind:
                                item2, url2, dom2, title2 = _resolve_reference_anchor(
                                    report_kind=report_kind,
                                    report_key=str(getattr(tm0, "idempotency_key", "") or "").strip(),
                                    ref_index=int(ref_idx),
                                    message_created_at=getattr(tm0, "created_at", None),
                                )
                                if url2:
                                    url0 = url2
                                    dom0 = dom2
                                    ref_title = title2
                                if item2 and item_id_hint <= 0:
                                    item_id_hint = int(item2)

                        # Fallback: allow comments to carry an explicit URL.
                        if not url0:
                            url0 = _extract_first_url(s)
                            dom0 = _domain_from_url(url0)

                        # Reply shortcut: `👍2 ...` / `👎2 ...` becomes a direct feedback event.
                        if action_hint in {"like", "dislike"} and replies_enabled and (url0 or item_id_hint > 0 or dom0):
                            raw2 = {
                                "text": comment_text[:2000] if comment_text else "",
                                "ref_index": int(ref_idx or 0) if ref_idx else 0,
                                "ref_title": ref_title[:200] if ref_title else "",
                                "reply_to_message_id": int(reply_mid or 0),
                                "reply_kind": str(getattr(tm0, "kind", "") or ""),
                                "reply_idempotency_key": str(getattr(tm0, "idempotency_key", "") or ""),
                            }
                            ev = repo.add_feedback_event(
                                channel="telegram",
                                user_id=uid,
                                chat_id=existing_chat_id,
                                message_id=(msg_id if msg_id > 0 else None),
                                kind=str(action_hint),
                                value_int=0,
                                item_id=(item_id_hint if item_id_hint > 0 else None),
                                url=url0,
                                domain=dom0,
                                note="reply_index",
                                raw=json.dumps(raw2, ensure_ascii=False),
                            )
                            try:
                                _apply_source_score_feedback(item_id=(item_id_hint if item_id_hint > 0 else None), feedback_event_id=int(ev.id), kind=str(action_hint))
                            except Exception:
                                pass
                            pending_feedback_for_profile.append(int(ev.id))

                            # Acknowledge quickly; for dislikes, offer a one-tap domain action menu.
                            if _out_lang() == "zh":
                                tag = "👍" if action_hint == "like" else "👎"
                                anchor = f"#{int(ref_idx)}" if ref_idx else ""
                                title3 = f"（{ref_title}）" if ref_title else ""
                                await _send_ack(f"✅ 已记录：{tag}{anchor}{title3}".strip())
                            else:
                                tag = "👍" if action_hint == "like" else "👎"
                                anchor = f"#{int(ref_idx)}" if ref_idx else ""
                                title3 = f" ({ref_title})" if ref_title else ""
                                await _send_ack(f"✅ recorded: {tag}{anchor}{title3}".strip())

                            if action_hint == "dislike" and dom0:
                                try:
                                    muted = repo.is_muted(scope="domain", key=dom0, when=dt.datetime.utcnow())
                                except Exception:
                                    muted = False
                                if not muted:
                                    days2 = _default_mute_days()
                                    is_zh = _out_lang() == "zh"
                                    text2 = (
                                        f"已记录：👎\n要静音域名 `{dom0}` {days2} 天吗？"
                                        if is_zh
                                        else f"Recorded: 👎\nMute `{dom0}` for {days2} days?"
                                    )
                                    kb2 = {
                                        "inline_keyboard": [
                                            [
                                                {"text": (f"🔕 静音 {days2} 天" if is_zh else f"🔕 Mute {days2}d"), "callback_data": f"fb:mute:{int(ev.id)}"},
                                                {"text": ("🚫 屏蔽域名" if is_zh else "🚫 Exclude domain"), "callback_data": f"fb:exclude_domain:{int(ev.id)}"},
                                            ],
                                            [
                                                {"text": ("忽略" if is_zh else "Ignore"), "callback_data": f"fb:ignore:{int(ev.id)}"},
                                            ],
                                        ]
                                    }
                                    await _send_one_with_markup(text=text2, reply_markup=kb2)
                            continue

                        ev0 = repo.add_feedback_event(
                            channel="telegram",
                            user_id=uid,
                            chat_id=existing_chat_id,
                            message_id=(msg_id if msg_id > 0 else None),
                            kind="comment",
                            value_int=0,
                            item_id=(item_id_hint if item_id_hint > 0 else None),
                            url=url0,
                            domain=dom0,
                            note="comment_reply",
                            raw=json.dumps(
                                {
                                    "text": comment_text[:2000],
                                    "ref_index": int(ref_idx or 0) if ref_idx else 0,
                                    "ref_title": ref_title[:200] if ref_title else "",
                                    "reply_to_message_id": int(reply_mid or 0),
                                    "reply_kind": str(getattr(tm0, "kind", "") or ""),
                                    "reply_idempotency_key": str(getattr(tm0, "idempotency_key", "") or ""),
                                },
                                ensure_ascii=False,
                            ),
                        )
                        # Auto-queue a profile_note from reply text so replies can improve future pushes
                        # without requiring an extra button click (still confirmable at Apply time).
                        try:
                            ev_note = repo.add_feedback_event(
                                channel="telegram",
                                user_id=uid,
                                chat_id=existing_chat_id,
                                message_id=(msg_id if msg_id > 0 else None),
                                kind="profile_note",
                                value_int=0,
                                item_id=(item_id_hint if item_id_hint > 0 else None),
                                url=url0,
                                domain=dom0,
                                note=f"auto_from_comment:{int(ev0.id)}",
                                raw=json.dumps(
                                    {
                                        "text": comment_text[:2000],
                                        "comment_id": int(ev0.id),
                                        "ref_index": int(ref_idx or 0) if ref_idx else 0,
                                        "ref_title": ref_title[:200] if ref_title else "",
                                        "reply_to_message_id": int(reply_mid or 0),
                                        "reply_kind": str(getattr(tm0, "kind", "") or ""),
                                        "reply_idempotency_key": str(getattr(tm0, "idempotency_key", "") or ""),
                                    },
                                    ensure_ascii=False,
                                ),
                            )
                            pending_feedback_for_profile.append(int(ev_note.id))
                        except Exception:
                            pass

                        is_zh = _out_lang() == "zh"
                        hint = "已锚定到该条推送" if tm0 else "未锚定到推送（仅作为通用反馈）"
                        if dom0:
                            hint += f"；domain={dom0}"
                        text2 = (
                            "收到你的回复反馈。请选择如何处理：\n"
                            "（不会自动改画像，必须你点按钮确认）\n\n"
                            f"反馈：{s[:120]}{'…' if len(s) > 120 else ''}\n"
                            f"{hint}"
                            if is_zh
                            else (
                                "Got your feedback. Pick what to do next:\n"
                                "(Profile won't change unless you confirm via buttons)\n\n"
                                f"Comment: {s[:120]}{'…' if len(s) > 120 else ''}\n"
                                f"{hint}"
                            )
                        )
                        prompt_delta_enabled = bool(getattr(settings, "telegram_prompt_delta_enabled", True))
                        kb2_rows: list[list[dict[str, str]]] = [
                            [
                                {"text": ("👎 低价值" if is_zh else "👎 Low value"), "callback_data": f"fb:dislike:{int(ev0.id)}"},
                                {"text": ("👍 有用" if is_zh else "👍 Useful"), "callback_data": f"fb:like:{int(ev0.id)}"},
                            ],
                            [
                                {"text": ("📝 更新画像" if is_zh else "📝 Update profile"), "callback_data": f"fb:note:{int(ev0.id)}"},
                                (
                                    {"text": ("🧩 修正提示词" if is_zh else "🧩 Fix prompt"), "callback_data": f"fb:prompt_note:{int(ev0.id)}"}
                                    if prompt_delta_enabled
                                    else {"text": ("🔕 静音域名" if is_zh else "🔕 Mute domain"), "callback_data": f"fb:mute:{int(ev0.id)}"}
                                ),
                            ],
                            [
                                {"text": ("🔕 静音域名" if is_zh else "🔕 Mute domain"), "callback_data": f"fb:mute:{int(ev0.id)}"},
                                {"text": ("⬇️ 降级域名" if is_zh else "⬇️ Downrank domain"), "callback_data": f"fb:downrank_domain:{int(ev0.id)}"},
                            ],
                            [
                                {"text": ("🚫 屏蔽域名" if is_zh else "🚫 Exclude domain"), "callback_data": f"fb:exclude_domain:{int(ev0.id)}"},
                            ],
                            [
                                {"text": ("忽略" if is_zh else "Ignore"), "callback_data": f"fb:ignore:{int(ev0.id)}"},
                            ],
                        ]
                        kb2 = {"inline_keyboard": kb2_rows}
                        await _send_one_with_markup(text=text2, reply_markup=kb2)
                        continue
                continue

            url = ""
            domain = ""
            if target_item_id > 0:
                url, domain = _item_from_id(target_item_id)
            if not domain and domain_hint:
                hint = domain_hint.strip()
                if hint.startswith("http://") or hint.startswith("https://"):
                    domain = _domain_from_url(hint)
                else:
                    domain = hint.lower()
            if not url:
                url = _extract_first_url(s)
                domain = _domain_from_url(url)

            ev = repo.add_feedback_event(
                channel="telegram",
                user_id=uid,
                chat_id=existing_chat_id,
                message_id=(msg_id if msg_id > 0 else None),
                kind=kind,
                value_int=value_int,
                item_id=(target_item_id if target_item_id > 0 else None),
                url=url,
                domain=domain,
                note=("reply" if reply_mid > 0 else "message"),
                raw=json.dumps({"text": s[:500]}),
            )

            if kind in {"like", "dislike", "rate"}:
                try:
                    _apply_source_score_feedback(item_id=(target_item_id if target_item_id > 0 else None), feedback_event_id=int(ev.id), kind=kind)
                except Exception:
                    pass
                pending_feedback_for_profile.append(int(ev.id))

            if kind == "mute":
                if domain:
                    until = dt.datetime.utcnow() + dt.timedelta(days=value_int)
                    repo.upsert_mute_rule(scope="domain", key=domain, muted_until=until, reason="telegram command")
                    if _out_lang() == "zh":
                        await _send_ack(f"🔕 已静音：{domain}（{value_int} 天）")
                    else:
                        await _send_ack(f"🔕 muted: {domain} ({value_int} days)")
                else:
                    if _out_lang() == "zh":
                        await _send_ack("⚠️ 未识别要静音的域名（请回复到某条 Alert 上，或发送 /mute <domain> <days?>）")
                    else:
                        await _send_ack("⚠️ Could not determine domain to mute. Reply to an Alert, or send /mute <domain> <days?>")
            elif kind == "unmute":
                if domain:
                    repo.delete_mute_rule(scope="domain", key=domain)
                    if _out_lang() == "zh":
                        await _send_ack(f"🔔 已取消静音：{domain}")
                    else:
                        await _send_ack(f"🔔 unmuted: {domain}")
                else:
                    if _out_lang() == "zh":
                        await _send_ack("⚠️ 未识别要取消静音的域名（发送 /unmute <domain>）")
                    else:
                        await _send_ack("⚠️ Could not determine domain to unmute. Send /unmute <domain>")
            else:
                # Light ack for scoring; keep it short.
                if _out_lang() == "zh":
                    if kind == "like":
                        await _send_ack("✅ 已记录：喜欢")
                    elif kind == "dislike":
                        await _send_ack("✅ 已记录：不喜欢")
                    elif kind == "rate":
                        await _send_ack(f"✅ 已记录：评分 {value_int}/5" if value_int else "✅ 已记录：评分")
                else:
                    if kind == "like":
                        await _send_ack("✅ recorded: like")
                    elif kind == "dislike":
                        await _send_ack("✅ recorded: dislike")
                    elif kind == "rate":
                        await _send_ack(f"✅ recorded: rating {value_int}/5" if value_int else "✅ recorded: rating")

                # If this was a dislike anchored on a pushed item (reply), offer a one-tap domain action menu.
                # This mirrors the reaction UX, and helps operators quickly mute/exclude low-signal domains.
                if kind == "dislike" and reply_mid > 0 and domain:
                    try:
                        muted = repo.is_muted(scope="domain", key=domain, when=dt.datetime.utcnow())
                    except Exception:
                        muted = False
                    if not muted:
                        days2 = _default_mute_days()
                        is_zh = _out_lang() == "zh"
                        text2 = (
                            f"已记录：👎\n要静音域名 `{domain}` {days2} 天吗？"
                            if is_zh
                            else f"Recorded: 👎\nMute `{domain}` for {days2} days?"
                        )
                        kb2 = {
                            "inline_keyboard": [
                                [
                                    {"text": (f"🔕 静音 {days2} 天" if is_zh else f"🔕 Mute {days2}d"), "callback_data": f"fb:mute:{int(ev.id)}"},
                                    {"text": ("🚫 屏蔽域名" if is_zh else "🚫 Exclude domain"), "callback_data": f"fb:exclude_domain:{int(ev.id)}"},
                                ],
                                [
                                    {"text": ("忽略" if is_zh else "Ignore"), "callback_data": f"fb:ignore:{int(ev.id)}"},
                                ],
                            ]
                        }
                        await _send_one_with_markup(text=text2, reply_markup=kb2)

        if max_update_id is not None:
            repo.set_app_config("telegram_update_offset", str(max_update_id + 1))

        # Optional: queue feedback-driven Profile delta updates for a background worker.
        #
        # Rationale:
        # - Telegram polling runs under the global `jobs` lock; it must stay fast.
        # - Profile delta updates use a reasoning model (slow). We enqueue and let a worker do it.
        # - Updates must be confirmable to avoid profile drift.
        # Record "last feedback event" time so we can debounce bursts (5s batching).
        if pending_feedback_for_profile:
            try:
                repo.set_app_config("profile_feedback_last_event_at_utc", dt.datetime.utcnow().isoformat() + "Z")
            except Exception:
                pass

        # Optional: queue feedback-driven Profile delta updates for a background worker.
        #
        # Debounce design (user requirement):
        # - Batch feedback within ~5 seconds into one proposal.
        # - Additional feedback beyond that window is queued (processed in later proposals).
        try:
            last_raw = (repo.get_app_config("profile_feedback_last_update_at_utc") or "").strip()
            last_dt: dt.datetime | None = None
            if last_raw:
                try:
                    last_dt = dt.datetime.fromisoformat(last_raw.replace("Z", "+00:00"))
                    if last_dt.tzinfo is not None:
                        last_dt = last_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)
                except Exception:
                    last_dt = None
            min_interval = 5
            if last_dt and (dt.datetime.utcnow() - last_dt).total_seconds() < float(min_interval):
                return {"status": "connected", "chat_id": existing_chat_id}

            # Avoid duplicate queued proposals.
            existing_tasks = repo.list_telegram_tasks(chat_id=existing_chat_id, kind="profile_delta", limit=5)
            if any(str(getattr(t, "status", "") or "") in {"awaiting", "pending", "running"} for t in existing_tasks):
                return {"status": "connected", "chat_id": existing_chat_id}

            pending = repo.list_pending_feedback_events(limit=50, kinds=["like", "dislike", "rate", "profile_note"])
            if not pending:
                return {"status": "connected", "chat_id": existing_chat_id}

            # Debounce: only enqueue after 5s of quiet since the most recent feedback event.
            debounce_seconds = 5
            cutoff_iso = ""
            try:
                raw_last_ev = (repo.get_app_config("profile_feedback_last_event_at_utc") or "").strip()
            except Exception:
                raw_last_ev = ""
            last_ev_dt: dt.datetime | None = None
            if raw_last_ev:
                try:
                    last_ev_dt = dt.datetime.fromisoformat(raw_last_ev.replace("Z", "+00:00"))
                    if last_ev_dt.tzinfo is not None:
                        last_ev_dt = last_ev_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)
                    cutoff_iso = raw_last_ev
                except Exception:
                    last_ev_dt = None
                    cutoff_iso = ""

            if last_ev_dt is not None and (dt.datetime.utcnow() - last_ev_dt).total_seconds() < float(debounce_seconds):
                return {"status": "connected", "chat_id": existing_chat_id}

            # Ensure profile exists & has an AI policy prompt (otherwise no-op).
            profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
            topic = repo.get_topic_by_name(profile_topic_name)
            pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
            if not (
                pol
                and (pol.llm_curation_prompt or "").strip()
                and settings.llm_base_url
                and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
            ):
                return {"status": "connected", "chat_id": existing_chat_id}

            if not cutoff_iso:
                cutoff_iso = dt.datetime.utcnow().isoformat() + "Z"

            # Use a negative placeholder as prompt_message_id (unique constraint), then the worker
            # will update it to the real Telegram message_id when sending the proposal.
            placeholder_mid = -int(dt.datetime.utcnow().timestamp() * 1000)
            repo.create_telegram_task(
                chat_id=existing_chat_id,
                user_id=(owner_user_id or ""),
                kind="profile_delta",
                status="pending",
                prompt_message_id=placeholder_mid,
                request_message_id=0,
                query=json.dumps({"cutoff_utc": cutoff_iso}, ensure_ascii=False),
            )
        except Exception as exc:
            logger.warning("telegram feedback enqueue skipped: %s", exc)
            pass

        return {"status": "connected", "chat_id": existing_chat_id}

    active_code = (code or repo.get_app_config("telegram_setup_code") or "").strip()
    if not active_code:
        return {"status": "no_code"}

    raw_off = (repo.get_app_config("telegram_update_offset") or "").strip()
    try:
        offset = int(raw_off) if raw_off else None
    except Exception:
        offset = None

    # Make sure polling works even if the bot was configured with a webhook elsewhere.
    try:
        await telegram_delete_webhook(bot_token=token, client_timeout_seconds=settings.http_timeout_seconds)
    except Exception:
        pass

    updates = await telegram_get_updates(
        bot_token=token,
        offset=offset,
        timeout_seconds=0,
        client_timeout_seconds=settings.http_timeout_seconds,
    )

    max_update_id: int | None = offset - 1 if offset is not None else None
    for upd in updates:
        if not isinstance(upd, dict):
            continue
        try:
            update_id = int(upd.get("update_id"))
        except Exception:
            update_id = None
        if update_id is not None:
            max_update_id = update_id if max_update_id is None else max(max_update_id, update_id)

        msg = upd.get("message")
        if not isinstance(msg, dict):
            continue
        from_obj = msg.get("from")
        text = msg.get("text")
        if not isinstance(text, str):
            continue
        start_payload = telegram_extract_start_payload(text)
        if start_payload is None:
            continue
        if start_payload.strip() != active_code:
            continue
        if owner_user_id and isinstance(from_obj, dict):
            uid = str(from_obj.get("id") or "").strip()
            if uid and uid != owner_user_id:
                continue
        chat = msg.get("chat")
        if not isinstance(chat, dict) or "id" not in chat:
            continue
        chat_id = str(chat.get("id")).strip()
        if not chat_id:
            continue
        # Record the first successful connector as the owner (private bot posture).
        if not owner_user_id and isinstance(from_obj, dict):
            uid = str(from_obj.get("id") or "").strip()
            if uid:
                repo.set_app_config("telegram_owner_user_id", uid)
        repo.set_app_config("telegram_chat_id", chat_id)
        repo.delete_app_config("telegram_setup_code")
        if max_update_id is not None:
            repo.set_app_config("telegram_update_offset", str(max_update_id + 1))
        try:
            await _telegram_send_welcome(settings=settings, chat_id=chat_id)
            repo.set_app_config("telegram_connected_notified", "1")
        except Exception:
            pass
        return {"status": "connected", "chat_id": chat_id}

    if max_update_id is not None:
        repo.set_app_config("telegram_update_offset", str(max_update_id + 1))
    return {"status": "pending"}


def telegram_disconnect(*, repo: Repo, settings: Settings) -> dict[str, Any]:
    # Apply runtime-effective settings so `.env` edits take effect without restart.
    try:
        from tracker.dynamic_config import effective_settings

        settings = effective_settings(repo=repo, settings=settings)
    except Exception:
        pass

    repo.delete_app_config("telegram_chat_id")
    repo.delete_app_config("telegram_connected_notified")
    repo.delete_app_config("telegram_owner_user_id")
    repo.delete_app_config("telegram_setup_code")
    repo.delete_app_config("telegram_update_offset")

    # Also clear env fallbacks, otherwise the connect flow can still see a "connected" chat_id.
    try:
        from pathlib import Path

        from tracker.dynamic_config import apply_env_block_updates

        apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=Path(str(getattr(settings, "env_path", "") or ".env")),
            env_updates={
                "TRACKER_TELEGRAM_CHAT_ID": "",
                "TRACKER_TELEGRAM_OWNER_USER_ID": "",
            },
        )
    except Exception:
        pass
    return {"ok": True}
