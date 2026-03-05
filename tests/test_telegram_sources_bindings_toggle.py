from __future__ import annotations

import pytest
from sqlalchemy import func, select

from tracker.models import TopicSource
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_source_bind_toggle_creates_binding(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    topic = repo.add_topic(name="T", query="ai")
    src = repo.add_source(type="rss", url="https://example.com/feed.xml")

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": f"s:bind:toggle:{int(src.id)}:{int(topic.id)}:0:0",
                    "message": {"message_id": 999, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return

    sent_raw: list[str] = []
    sent_acks: list[str] = []

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        sent_raw.append(text)
        return 200

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [201]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    await telegram_poll(repo=repo, settings=settings)

    cnt = db_session.scalar(select(func.count()).select_from(TopicSource))
    assert int(cnt or 0) == 1
    assert sent_raw  # bind menu re-render
    assert sent_acks  # ack message

