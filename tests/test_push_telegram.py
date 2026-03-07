from __future__ import annotations

import asyncio

import httpx
import pytest

from tracker.push.telegram import TelegramPusher, split_telegram_message


def test_split_telegram_message_preserves_lines():
    text = "a\nb\nc"
    assert split_telegram_message(text, limit=2) == ["a", "b", "c"]


def test_split_telegram_message_hard_splits_long_line():
    text = "x" * 25
    parts = split_telegram_message(text, limit=10)
    assert parts == ["x" * 10, "x" * 10, "x" * 5]


def test_telegram_pusher_send_text(monkeypatch):
    sent: list[dict] = []

    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        assert url.startswith("https://api.telegram.org/botTEST/sendMessage")
        assert json["chat_id"] == "123"
        assert "text" in json
        sent.append(json)
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    p = TelegramPusher("TEST", timeout_seconds=5)
    asyncio.run(p.send_text(chat_id="123", text="hello", disable_preview=True))
    assert len(sent) == 1
    assert sent[0]["disable_web_page_preview"] is True


def test_telegram_pusher_splits_long_messages(monkeypatch):
    sent_texts: list[str] = []

    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        sent_texts.append(str(json.get("text") or ""))
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": len(sent_texts)}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    text = ("line\n" * 2000).strip()
    p = TelegramPusher("TEST")
    asyncio.run(p.send_text(chat_id="123", text=text))

    assert len(sent_texts) >= 2
    assert sent_texts[0].startswith("[1/")


def test_telegram_pusher_reports_partial_delivery_when_rollback_cannot_delete(monkeypatch):
    sent_payloads: list[dict] = []

    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        if url.endswith("/sendMessage"):
            sent_payloads.append(json)
            if len(sent_payloads) == 1:
                return httpx.Response(200, json={"ok": True, "result": {"message_id": 101}}, request=req)
            return httpx.Response(500, json={"ok": False, "description": "boom"}, request=req)
        if url.endswith("/deleteMessage"):
            return httpx.Response(200, json={"ok": False}, request=req)
        raise AssertionError(url)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    from tracker.push.telegram import TelegramPartialDeliveryError

    text = ("line\n" * 2000).strip()
    p = TelegramPusher("TEST")
    with pytest.raises(TelegramPartialDeliveryError) as exc:
        asyncio.run(p.send_text(chat_id="123", text=text))

    assert exc.value.message_ids == [101]



def test_telegram_pusher_send_raw_text_requires_message_id(monkeypatch):
    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"ok": True, "result": {}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    p = TelegramPusher("TEST", timeout_seconds=5)
    with pytest.raises(RuntimeError, match="missing message_id"):
        asyncio.run(p.send_raw_text(chat_id="123", text="hello", disable_preview=True))



def test_telegram_pusher_send_text_requires_message_id(monkeypatch):
    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"ok": True, "result": {}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    p = TelegramPusher("TEST", timeout_seconds=5)
    with pytest.raises(RuntimeError, match="missing message_id"):
        asyncio.run(p.send_text(chat_id="123", text="hello", disable_preview=True))
