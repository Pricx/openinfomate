from __future__ import annotations

import asyncio
import httpx


class TelegramPartialDeliveryError(RuntimeError):
    def __init__(self, message: str, *, message_ids: list[int] | None = None):
        super().__init__(message)
        self.message_ids = [int(m) for m in (message_ids or []) if int(m or 0) > 0]


def _telegram_api_url(*, bot_token: str, method: str) -> str:
    token = (bot_token or "").strip()
    if not token:
        raise ValueError("missing bot_token")
    m = (method or "").strip().lstrip("/")
    if not m:
        raise ValueError("missing method")
    return f"https://api.telegram.org/bot{token}/{m}"


_TG_PUSH_HTTP_CLIENT: httpx.AsyncClient | None = None
_TG_PUSH_HTTP_CLIENT_LOCK = asyncio.Lock()


async def _tg_push_http_client() -> httpx.AsyncClient:
    """
    Shared Telegram HTTP client (connection reuse).

    Telegram UI interactions (inline keyboards) are latency-sensitive, and creating a new
    client per request adds TLS/TCP setup overhead.
    """
    global _TG_PUSH_HTTP_CLIENT
    if _TG_PUSH_HTTP_CLIENT and not _TG_PUSH_HTTP_CLIENT.is_closed:
        return _TG_PUSH_HTTP_CLIENT
    async with _TG_PUSH_HTTP_CLIENT_LOCK:
        if _TG_PUSH_HTTP_CLIENT and not _TG_PUSH_HTTP_CLIENT.is_closed:
            return _TG_PUSH_HTTP_CLIENT
        limits = httpx.Limits(max_connections=40, max_keepalive_connections=20)
        _TG_PUSH_HTTP_CLIENT = httpx.AsyncClient(follow_redirects=True, limits=limits)
        return _TG_PUSH_HTTP_CLIENT


def is_stale_telegram_edit_error(error: object) -> bool:
    msg = (str(error or "") or "").strip().lower()
    return ("message to edit not found" in msg) or ("message can't be edited" in msg)


def split_telegram_message(text: str, *, limit: int = 3800) -> list[str]:
    """
    Telegram's `sendMessage` hard limit is 4096 UTF-8 chars.

    We keep a small safety margin and split on line boundaries where possible.
    """
    s = (text or "").strip()
    if not s:
        return []
    limit = max(1, min(4096, int(limit)))
    if len(s) <= limit:
        return [s]

    out: list[str] = []
    cur = ""
    for line in s.splitlines():
        chunk = (line if not cur else cur + "\n" + line).strip()
        if len(chunk) <= limit:
            cur = chunk
            continue
        if cur:
            out.append(cur)
            cur = ""
        if len(line) <= limit:
            cur = line.strip()
            continue
        # Hard split long lines.
        raw = line
        while raw:
            part = raw[:limit]
            out.append(part)
            raw = raw[limit:]
    if cur:
        out.append(cur)
    return [c for c in out if c.strip()]


def _extract_required_message_id(payload: object, *, context: str) -> int:
    if isinstance(payload, dict):
        result = payload.get("result")
        if isinstance(result, dict):
            try:
                mid = int(result.get("message_id") or 0)
            except Exception:
                mid = 0
            if mid > 0:
                return mid
    raise RuntimeError(f"{context}: missing message_id")


class TelegramPusher:
    def __init__(self, bot_token: str, *, timeout_seconds: int = 20):
        self.bot_token = (bot_token or "").strip()
        self.timeout_seconds = int(timeout_seconds)

    async def edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> bool:
        """
        Best-effort edit. Raises on Telegram API errors.
        """
        url = _telegram_api_url(bot_token=self.bot_token, method="editMessageText")
        cid = (chat_id or "").strip()
        mid = int(message_id or 0)
        payload_text = (text or "").strip()
        if not (cid and mid > 0 and payload_text):
            raise ValueError("missing chat_id/message_id/text")
        payload = {
            "chat_id": cid,
            "message_id": mid,
            "text": payload_text,
            "disable_web_page_preview": bool(disable_preview),
        }
        if (parse_mode or "").strip():
            payload["parse_mode"] = str(parse_mode or "").strip()
        if isinstance(reply_markup, dict) and reply_markup:
            payload["reply_markup"] = reply_markup
        client = await _tg_push_http_client()
        resp = await client.post(url, json=payload, timeout=self.timeout_seconds)
        data: object | None = None
        try:
            data = resp.json()
        except Exception:
            data = None

        # Telegram commonly returns HTTP 400 with JSON:
        # {"ok": false, "error_code": 400, "description": "Bad Request: message is not modified"}
        if resp.status_code >= 400:
            if isinstance(data, dict):
                desc = (data.get("description") or "").strip()
                if resp.status_code == 400 and "message is not modified" in desc.lower():
                    return True
                raise RuntimeError(desc or f"telegram api error (status={resp.status_code})")
            resp.raise_for_status()
            raise RuntimeError(f"telegram api error (status={resp.status_code})")

        if data is None:
            data = resp.json()
        ok = bool(data.get("ok")) if isinstance(data, dict) else False
        if not ok:
            desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
            raise RuntimeError(str(desc))
        return True

    async def delete_message(self, *, chat_id: str, message_id: int) -> bool:
        """
        Best-effort delete. Telegram returns ok=false for messages that can't be deleted
        (e.g. already deleted, insufficient rights).
        """
        url = _telegram_api_url(bot_token=self.bot_token, method="deleteMessage")
        cid = (chat_id or "").strip()
        mid = int(message_id or 0)
        if not (cid and mid > 0):
            raise ValueError("missing chat_id or message_id")
        client = await _tg_push_http_client()
        resp = await client.post(url, json={"chat_id": cid, "message_id": mid}, timeout=self.timeout_seconds)
        resp.raise_for_status()
        data = resp.json()
        return bool(data.get("ok")) if isinstance(data, dict) else False

    async def edit_raw_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> bool:
        url = _telegram_api_url(bot_token=self.bot_token, method="editMessageText")
        cid = (chat_id or "").strip()
        mid = int(message_id or 0)
        payload_text = (text or "").strip()
        if not (cid and mid > 0 and payload_text):
            raise ValueError("missing chat_id/message_id or empty text")
        payload = {
            "chat_id": cid,
            "message_id": mid,
            "text": payload_text,
            "disable_web_page_preview": bool(disable_preview),
        }
        if (parse_mode or "").strip():
            payload["parse_mode"] = str(parse_mode or "").strip()
        if isinstance(reply_markup, dict) and reply_markup:
            payload["reply_markup"] = reply_markup
        client = await _tg_push_http_client()
        resp = await client.post(url, json=payload, timeout=self.timeout_seconds)
        data: object | None = None
        try:
            data = resp.json()
        except Exception:
            data = None
        if resp.status_code >= 400:
            if isinstance(data, dict):
                desc = (data.get("description") or "").strip()
                raise RuntimeError(desc or f"telegram api error (status={resp.status_code})")
            resp.raise_for_status()
            raise RuntimeError(f"telegram api error (status={resp.status_code})")
        if data is None:
            data = resp.json()
        ok = bool(data.get("ok")) if isinstance(data, dict) else False
        if not ok:
            desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
            raise RuntimeError(str(desc))
        return True

    async def send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:
        """
        Send one message as-is (no splitting, no auto-prefix).
        """
        url = _telegram_api_url(bot_token=self.bot_token, method="sendMessage")
        cid = (chat_id or "").strip()
        payload_text = (text or "").strip()
        if not (cid and payload_text):
            raise ValueError("missing chat_id or empty text")
        payload = {
            "chat_id": cid,
            "text": payload_text,
            "disable_web_page_preview": bool(disable_preview),
        }
        if (parse_mode or "").strip():
            payload["parse_mode"] = str(parse_mode or "").strip()
        if isinstance(reply_markup, dict) and reply_markup:
            payload["reply_markup"] = reply_markup
        client = await _tg_push_http_client()
        resp = await client.post(url, json=payload, timeout=self.timeout_seconds)
        data: object | None = None
        try:
            data = resp.json()
        except Exception:
            data = None
        if resp.status_code >= 400:
            if isinstance(data, dict):
                desc = (data.get("description") or "").strip()
                raise RuntimeError(desc or f"telegram api error (status={resp.status_code})")
            resp.raise_for_status()
            raise RuntimeError(f"telegram api error (status={resp.status_code})")
        if data is None:
            data = resp.json()
        ok = bool(data.get("ok")) if isinstance(data, dict) else False
        if not ok:
            desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
            raise RuntimeError(str(desc))
        return _extract_required_message_id(data, context="telegram sendMessage ok response")

    async def send_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
    ) -> list[int]:
        url = _telegram_api_url(bot_token=self.bot_token, method="sendMessage")
        cid = (chat_id or "").strip()
        if not cid:
            raise ValueError("missing chat_id")

        parts = split_telegram_message(text)
        if not parts:
            raise ValueError("empty text")

        client = await _tg_push_http_client()
        if len(parts) == 1:
            mid = await self._send_part(client, url=url, chat_id=cid, text=parts[0], disable_preview=disable_preview)
            return [mid] if mid > 0 else []

        total = len(parts)
        message_ids: list[int] = []
        for idx, part in enumerate(parts, start=1):
            prefix = f"[{idx}/{total}]\n"
            payload_text = prefix + part
            try:
                mid = await self._send_part(
                    client, url=url, chat_id=cid, text=payload_text, disable_preview=disable_preview
                )
            except Exception as exc:
                # Best-effort rollback: avoid leaving partial multi-part messages in chat.
                undeleted_ids = await self._rollback_sent_parts(chat_id=cid, message_ids=message_ids)
                if undeleted_ids:
                    raise TelegramPartialDeliveryError(
                        f"telegram multipart send failed; partial messages remain: {undeleted_ids}",
                        message_ids=undeleted_ids,
                    ) from exc
                raise
            if mid > 0:
                message_ids.append(mid)
        return message_ids

    async def _rollback_sent_parts(self, *, chat_id: str, message_ids: list[int]) -> list[int]:
        undeleted_ids: list[int] = []
        for sent_mid in reversed([int(m) for m in (message_ids or []) if int(m or 0) > 0]):
            try:
                deleted = await self.delete_message(chat_id=chat_id, message_id=sent_mid)
            except Exception:
                deleted = False
            if not deleted:
                undeleted_ids.append(int(sent_mid))
        undeleted_ids.reverse()
        return undeleted_ids

    async def _send_part(
        self,
        client: httpx.AsyncClient,
        *,
        url: str,
        chat_id: str,
        text: str,
        disable_preview: bool,
    ) -> int:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": bool(disable_preview),
        }
        resp = await client.post(url, json=payload, timeout=self.timeout_seconds)
        data: object | None = None
        try:
            data = resp.json()
        except Exception:
            data = None
        if resp.status_code >= 400:
            if isinstance(data, dict):
                desc = (data.get("description") or "").strip()
                raise RuntimeError(desc or f"telegram api error (status={resp.status_code})")
            resp.raise_for_status()
            raise RuntimeError(f"telegram api error (status={resp.status_code})")
        if data is None:
            data = resp.json()
        ok = bool(data.get("ok")) if isinstance(data, dict) else False
        if not ok:
            desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
            raise RuntimeError(str(desc))
        return _extract_required_message_id(data, context="telegram sendMessage ok response")

    async def send_document(
        self,
        *,
        chat_id: str,
        filename: str,
        content: bytes,
        caption: str = "",
        disable_notification: bool = False,
    ) -> int:
        """
        Send a small file (e.g., a full report) as a Telegram document.
        """
        url = _telegram_api_url(bot_token=self.bot_token, method="sendDocument")
        cid = (chat_id or "").strip()
        name = (filename or "").strip() or "report.txt"
        if not cid:
            raise ValueError("missing chat_id")
        if not isinstance(content, (bytes, bytearray)) or not content:
            raise ValueError("empty content")
        cap = (caption or "").strip()

        data: dict[str, object] = {
            "chat_id": cid,
            "disable_notification": bool(disable_notification),
        }
        if cap:
            data["caption"] = cap[:1024]

        files = {"document": (name, bytes(content))}
        client = await _tg_push_http_client()
        resp = await client.post(url, data=data, files=files, timeout=self.timeout_seconds)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, dict) or not payload.get("ok"):
            desc = (payload.get("description") if isinstance(payload, dict) else None) or "telegram api error"
            raise RuntimeError(str(desc))
        return _extract_required_message_id(payload, context="telegram sendDocument ok response")
