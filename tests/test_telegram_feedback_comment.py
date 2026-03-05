from __future__ import annotations

import json

import pytest
from sqlalchemy import func, select

from tracker.models import FeedbackEvent, Item, Source
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_free_form_reply_creates_comment_event_and_action_menu(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    # Seed an item + a Telegram pushed message mapping so reply_mid can be resolved.
    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/post",
        canonical_url="https://example.com/post",
        title="Test",
    )
    db_session.add(item)
    db_session.commit()

    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Topic",
        message_ids=[42],
        kind="alert",
        item_id=int(item.id),
    )

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "message": {
                    "message_id": 100,
                    "text": "1 首先我不知道 Lumina 是什么；2 我看了简介也不知道我拿它有什么用",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 42},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    sent_raw: list[tuple[str, dict | None]] = []

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        sent_raw.append((text, reply_markup))
        return 200

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await telegram_poll(repo=repo, settings=settings)

    # Comment feedback event should be created and left pending.
    evs = list(db_session.scalars(select(FeedbackEvent).order_by(FeedbackEvent.id.asc())))
    # We also auto-queue a `profile_note` so replies can improve the profile (still confirmable later).
    assert len(evs) == 2
    ev0 = evs[0]
    assert ev0.kind == "comment"
    assert int(ev0.item_id or 0) == int(item.id)
    assert ev0.domain == "example.com"
    assert ev0.applied_at is None

    raw0 = json.loads(ev0.raw or "{}")
    assert "Lumina" in (raw0.get("text") or "")
    assert int(raw0.get("reply_to_message_id") or 0) == 42
    assert evs[1].kind == "profile_note"
    assert evs[1].applied_at is None

    # Bot should send an inline action menu with fb:* callbacks.
    assert sent_raw
    _menu_text, markup = sent_raw[-1]
    assert isinstance(markup, dict)
    kb = markup.get("inline_keyboard")
    assert isinstance(kb, list) and kb
    flat = [btn.get("callback_data") for row in kb for btn in (row or []) if isinstance(btn, dict)]
    assert f"fb:dislike:{int(ev0.id)}" in flat
    assert f"fb:note:{int(ev0.id)}" in flat
    assert f"fb:prompt_note:{int(ev0.id)}" in flat


@pytest.mark.asyncio
async def test_telegram_comment_menu_button_creates_dislike_and_marks_comment_applied(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    # Seed item + mapping.
    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/post",
        canonical_url="https://example.com/post",
        title="Test",
    )
    db_session.add(item)
    db_session.commit()
    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Topic",
        message_ids=[42],
        kind="alert",
        item_id=int(item.id),
    )

    settings = Settings(telegram_bot_token="TEST")

    comment_holder: dict[str, int] = {"id": 0}

    batches = [
        [
            {
                "update_id": 1,
                "message": {
                    "message_id": 100,
                    "text": "我看不懂这条推送有什么用",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 42},
                },
            }
        ],
        [
            {
                "update_id": 2,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "",  # filled after first poll
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ],
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        # Patch callback payload with the actual comment id once it exists.
        if batches:
            nxt0 = batches[0][0] if isinstance(batches[0], list) and batches[0] else None
            cq0 = nxt0.get("callback_query") if isinstance(nxt0, dict) else None
            if isinstance(cq0, dict) and not str(cq0.get("data") or "").strip():
                row = db_session.scalar(
                    select(FeedbackEvent).where(FeedbackEvent.kind == "comment").order_by(FeedbackEvent.id.desc())
                )
                if row:
                    comment_holder["id"] = int(row.id)
                    cq0["data"] = f"fb:dislike:{int(row.id)}"
        return batches.pop(0) if batches else []

    answered: list[str] = []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        answered.append(callback_query_id)

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        return 200

    sent_acks: list[str] = []

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    # 1) Receive the free-form comment -> creates comment event.
    await telegram_poll(repo=repo, settings=settings)
    cnt1 = int(db_session.scalar(select(func.count()).select_from(FeedbackEvent)) or 0)
    assert cnt1 == 2

    # 2) Click the inline "dislike" button -> creates dislike + marks comment applied.
    await telegram_poll(repo=repo, settings=settings)
    evs = list(db_session.scalars(select(FeedbackEvent).order_by(FeedbackEvent.id.asc())))
    assert len(evs) == 3
    assert evs[0].kind == "comment"
    assert evs[0].applied_at is not None
    assert evs[1].kind == "profile_note"
    assert evs[1].applied_at is None
    assert evs[2].kind == "dislike"
    assert evs[2].applied_at is None
    assert answered == ["cq1"]
    assert sent_acks  # we ack the action


@pytest.mark.asyncio
async def test_telegram_comment_menu_button_mute_creates_rule_and_marks_comment_applied(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    # Seed item + mapping.
    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/post",
        canonical_url="https://example.com/post",
        title="Test",
    )
    db_session.add(item)
    db_session.commit()
    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Topic",
        message_ids=[42],
        kind="alert",
        item_id=int(item.id),
    )

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "message": {
                    "message_id": 100,
                    "text": "这条对我没用",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 42},
                },
            }
        ],
        [
            {
                "update_id": 2,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "",  # filled after first poll
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ],
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        if batches:
            nxt0 = batches[0][0] if isinstance(batches[0], list) and batches[0] else None
            cq0 = nxt0.get("callback_query") if isinstance(nxt0, dict) else None
            if isinstance(cq0, dict) and not str(cq0.get("data") or "").strip():
                row = db_session.scalar(
                    select(FeedbackEvent).where(FeedbackEvent.kind == "comment").order_by(FeedbackEvent.id.desc())
                )
                if row:
                    cq0["data"] = f"fb:mute:{int(row.id)}"
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 20):  # noqa: ANN001, ARG001
        return

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        return 200

    sent_acks: list[str] = []

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    # 1) Create comment event.
    await telegram_poll(repo=repo, settings=settings)
    # 2) Click mute.
    await telegram_poll(repo=repo, settings=settings)

    row2 = db_session.scalar(select(FeedbackEvent).where(FeedbackEvent.kind == "comment").order_by(FeedbackEvent.id.desc()))
    assert row2 is not None
    assert row2.applied_at is not None
    assert repo.is_muted(scope="domain", key="example.com")
    assert sent_acks
