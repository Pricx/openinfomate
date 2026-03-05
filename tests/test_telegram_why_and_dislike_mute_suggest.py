from __future__ import annotations

import pytest
from sqlalchemy import func, select

from tracker.models import FeedbackEvent, Item, ItemTopic, Source, Topic
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_why_reply_to_alert_shows_item_topic_reason(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/p",
        canonical_url="https://example.com/p",
        title="Hello",
    )
    db_session.add(item)
    db_session.flush()
    t = Topic(name="Profile", query="", enabled=True)
    db_session.add(t)
    db_session.flush()
    it = ItemTopic(
        item_id=int(item.id),
        topic_id=int(t.id),
        decision="alert",
        relevance_score=10,
        novelty_score=9,
        quality_score=8,
        reason="来自一手来源，且与你的画像高度相关。",
    )
    db_session.add(it)
    db_session.commit()

    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Profile",
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
                    "text": "/why",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 42, "text": "dummy"},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    sent: list[str] = []

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent.append(text)
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    await telegram_poll(repo=repo, settings=settings)
    assert sent
    out = "\n".join(sent)
    assert "为什么会推送" in out
    assert "item_id=" in out
    assert "example.com" in out
    assert "Topic 决策" in out
    assert "Profile" in out
    assert "reason:" in out


@pytest.mark.asyncio
async def test_telegram_dislike_reaction_offers_one_tap_mute_suggestion(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/p",
        canonical_url="https://example.com/p",
        title="Hello",
    )
    db_session.add(item)
    db_session.commit()

    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Profile",
        message_ids=[42],
        kind="alert",
        item_id=int(item.id),
    )

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "message_reaction": {
                    "chat": {"id": 123},
                    "message_id": 42,
                    "user": {"id": 123},
                    "new_reaction": [{"emoji": "👎"}],
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    sent_raw: list[dict] = []

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        sent_raw.append({"text": text, "reply_markup": reply_markup})
        return 200

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await telegram_poll(repo=repo, settings=settings)

    assert sent_raw
    markup = sent_raw[-1]["reply_markup"]
    assert isinstance(markup, dict)
    kb = markup.get("inline_keyboard")
    assert isinstance(kb, list) and kb

    # Verify callback points at fb:mute:<event_id>.
    flat = [btn.get("callback_data") for row in kb for btn in (row or []) if isinstance(btn, dict)]
    assert any(str(x or "").startswith("fb:mute:") for x in flat)

    # The dislike feedback event should remain pending (so profile updates can consume it).
    cnt = db_session.scalar(select(func.count()).select_from(FeedbackEvent))
    assert int(cnt or 0) == 1


@pytest.mark.asyncio
async def test_telegram_dislike_reply_message_offers_one_tap_domain_actions(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/p",
        canonical_url="https://example.com/p",
        title="Hello",
    )
    db_session.add(item)
    db_session.commit()

    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Profile",
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
                    "text": "👎",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 42, "text": "dummy"},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    sent_raw: list[dict] = []

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        sent_raw.append({"text": text, "reply_markup": reply_markup})
        return 200

    # Ack messages are `send_text`; ignore.
    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    await telegram_poll(repo=repo, settings=settings)

    assert sent_raw
    markup = sent_raw[-1]["reply_markup"]
    assert isinstance(markup, dict)
    kb = markup.get("inline_keyboard")
    assert isinstance(kb, list) and kb

    flat = [btn.get("callback_data") for row in kb for btn in (row or []) if isinstance(btn, dict)]
    assert any(str(x or "").startswith("fb:mute:") for x in flat)
    assert any(str(x or "").startswith("fb:exclude_domain:") for x in flat)

    cnt = db_session.scalar(select(func.count()).select_from(FeedbackEvent))
    assert int(cnt or 0) == 1


@pytest.mark.asyncio
async def test_telegram_fb_mute_callback_creates_mute_rule_for_dislike_event(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/p",
        canonical_url="https://example.com/p",
        title="Hello",
    )
    db_session.add(item)
    db_session.commit()

    repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:Profile",
        message_ids=[42],
        kind="alert",
        item_id=int(item.id),
    )

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "message_reaction": {
                    "chat": {"id": 123},
                    "message_id": 42,
                    "user": {"id": 123},
                    "new_reaction": [{"emoji": "👎"}],
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
                    select(FeedbackEvent).where(FeedbackEvent.kind == "dislike").order_by(FeedbackEvent.id.desc())
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

    # 1) Reaction -> creates dislike event + suggests mute.
    await telegram_poll(repo=repo, settings=settings)
    # 2) Click mute button.
    await telegram_poll(repo=repo, settings=settings)

    assert repo.is_muted(scope="domain", key="example.com")
    ev = db_session.scalar(select(FeedbackEvent).where(FeedbackEvent.kind == "dislike").order_by(FeedbackEvent.id.desc()))
    assert ev is not None
    assert ev.applied_at is None  # do not swallow dislike; profile updater will consume it later.
    assert any("静音" in m or "muted" in m.lower() for m in sent_acks)


@pytest.mark.asyncio
async def test_telegram_fb_exclude_domain_callback_updates_exclude_domains(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    src = Source(type="rss", url="https://example.com/feed.xml")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com/p",
        canonical_url="https://example.com/p",
        title="Hello",
    )
    db_session.add(item)
    db_session.commit()

    # Simulate a dislike feedback event (e.g., created by a reaction).
    ev = repo.add_feedback_event(
        channel="telegram",
        user_id="123",
        chat_id="123",
        message_id=42,
        kind="dislike",
        value_int=0,
        item_id=int(item.id),
        url=item.canonical_url,
        domain="example.com",
        note="reaction:👎",
        raw="{}",
    )

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": f"fb:exclude_domain:{int(ev.id)}",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 20):  # noqa: ANN001, ARG001
        return

    sent_acks: list[str] = []

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    await telegram_poll(repo=repo, settings=settings)

    assert "example.com" in (repo.get_app_config("exclude_domains") or "")
    ev2 = db_session.scalar(select(FeedbackEvent).where(FeedbackEvent.id == int(ev.id)))
    assert ev2 is not None
    assert ev2.applied_at is None
    assert any("屏蔽域名" in m or "excluded domain" in m.lower() for m in sent_acks)
