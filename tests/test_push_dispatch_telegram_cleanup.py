from __future__ import annotations

import pytest

from tracker.push_dispatch import push_telegram_text, push_telegram_text_card
from tracker.repo import Repo
from tracker.settings import Settings


@pytest.mark.asyncio
async def test_push_telegram_text_card_keeps_mapping_when_delete_returns_false(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")

    key = "digest:1:2026-03-07"
    push = repo.reserve_push_attempt(channel="telegram", idempotency_key=key, max_attempts=3)
    assert push is not None
    repo.mark_push_sent(push)
    repo.record_telegram_messages(chat_id="123", idempotency_key=key, message_ids=[200, 201], kind="digest")

    edited: list[int] = []
    deleted: list[int] = []

    async def fake_edit_text(self, *, chat_id: str, message_id: int, text: str, disable_preview: bool = True, reply_markup=None, parse_mode=None):  # noqa: ANN001, ARG001
        edited.append(message_id)
        return True

    async def fake_delete_message(self, *, chat_id: str, message_id: int):  # noqa: ANN001, ARG001
        deleted.append(message_id)
        return False

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup=None, parse_mode=None):  # noqa: ANN001, ARG001
        raise AssertionError("send_raw_text should not be used when edit succeeds")

    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.delete_message", fake_delete_message)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    ok = await push_telegram_text_card(
        repo=repo,
        settings=Settings(telegram_bot_token="TEST"),
        idempotency_key=key,
        text="Updated card",
        reply_markup={"inline_keyboard": [[{"text": "OK", "callback_data": "ok"}]]},
        replace_sent=True,
    )

    assert ok is True
    assert edited == [200]
    assert deleted == [201]
    assert repo.get_telegram_message(chat_id="123", message_id=201) is not None


@pytest.mark.asyncio
async def test_push_telegram_text_partial_delivery_persists_surviving_mapping_and_marks_failed(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        from tracker.push.telegram import TelegramPartialDeliveryError

        raise TelegramPartialDeliveryError("partial", message_ids=[301])

    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    with pytest.raises(Exception, match="partial"):
        await push_telegram_text(
            repo=repo,
            settings=Settings(telegram_bot_token="TEST"),
            idempotency_key="alert:2:7",
            text="long alert",
        )

    msg = repo.get_telegram_message(chat_id="123", message_id=301)
    assert msg is not None
    pushes = repo.list_pushes(channel="telegram", idempotency_key="alert:2:7", limit=5)
    assert pushes[0].status == "failed"


@pytest.mark.asyncio
async def test_push_telegram_text_reuses_existing_mapping_on_retry_even_without_replace_sent(db_session, monkeypatch):

    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")

    key = "alert:9:3"
    push = repo.reserve_push_attempt(channel="telegram", idempotency_key=key, max_attempts=3)
    assert push is not None
    repo.mark_push_failed(push, error="partial")
    repo.record_telegram_messages(chat_id="123", idempotency_key=key, message_ids=[401], kind="alert", item_id=9)

    edited: list[int] = []

    async def fake_edit_text(self, *, chat_id: str, message_id: int, text: str, disable_preview: bool = True, parse_mode=None, reply_markup=None):  # noqa: ANN001, ARG001
        edited.append(message_id)
        return True

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        raise AssertionError("retry should reuse existing mapping instead of sending a duplicate")

    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    ok = await push_telegram_text(
        repo=repo,
        settings=Settings(telegram_bot_token="TEST"),
        idempotency_key=key,
        text="Updated alert text",
        replace_sent=False,
    )

    assert ok is True
    assert edited == [401]


@pytest.mark.asyncio
async def test_push_telegram_text_replace_path_partial_new_messages_are_recorded(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")

    key = "alert:42:9"
    push = repo.reserve_push_attempt(channel="telegram", idempotency_key=key, max_attempts=3)
    assert push is not None
    repo.mark_push_failed(push, error="previous partial")
    repo.record_telegram_messages(chat_id="123", idempotency_key=key, message_ids=[200], kind="alert", item_id=42)

    async def fake_edit_text(self, *, chat_id: str, message_id: int, text: str, disable_preview: bool = True, parse_mode=None, reply_markup=None):  # noqa: ANN001, ARG001
        return True

    sent: list[str] = []

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, parse_mode=None, reply_markup=None):  # noqa: ANN001, ARG001
        sent.append(text)
        if len(sent) == 1:
            return 501
        raise RuntimeError("boom")

    monkeypatch.setattr("tracker.push_dispatch.split_telegram_message", lambda text: ["part1", "part2", "part3"])
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    with pytest.raises(Exception, match="partial messages remain"):
        await push_telegram_text(
            repo=repo,
            settings=Settings(telegram_bot_token="TEST"),
            idempotency_key=key,
            text="Updated alert text",
            replace_sent=False,
        )

    assert repo.get_telegram_message(chat_id="123", message_id=501) is not None
    pushes = repo.list_pushes(channel="telegram", idempotency_key=key, limit=5)
    assert pushes[0].status == "failed"
