from __future__ import annotations

import json

import pytest
from sqlalchemy import func, select

from tracker.models import Source, TopicSource
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_sources_import_preview_and_apply_no_bind(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    settings = Settings(telegram_bot_token="TEST")

    batches = [
        [
            {
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "text": "/src add",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                },
            }
        ],
        [
            {
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "text": "https://example.com/feed.xml\nhttps://another.example.com/rss)\n",
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 200},
                },
            }
        ],
        [
            {
                "update_id": 3,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "s:imp:apply:none",
                    "message": {"message_id": 201, "chat": {"id": 123}},
                },
            }
        ],
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return

    sent_raw: list[str] = []
    sent_acks: list[str] = []
    next_mid = {"n": 200}

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        sent_raw.append(text)
        mid = int(next_mid["n"])
        next_mid["n"] += 1
        return mid

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [999]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    # 1) Prompt for URLs.
    await telegram_poll(repo=repo, settings=settings)
    tasks0 = repo.list_telegram_tasks(chat_id="123", kind="source_import", limit=10)
    assert len(tasks0) == 1
    assert tasks0[0].status == "awaiting"
    assert tasks0[0].prompt_message_id == 200

    # 2) Reply with URLs -> preview prompt with confirm task.
    await telegram_poll(repo=repo, settings=settings)
    tasks1 = repo.list_telegram_tasks(chat_id="123", kind="source_import_confirm", limit=10)
    assert len(tasks1) == 1
    assert tasks1[0].status == "awaiting"
    assert tasks1[0].prompt_message_id == 201
    draft = json.loads(tasks1[0].query or "{}")
    assert set(draft.get("urls") or []) == {"https://example.com/feed.xml", "https://another.example.com/rss"}
    assert sent_raw and any("预览" in t for t in sent_raw)

    # 3) Click "apply (no bind)" -> should create sources.
    await telegram_poll(repo=repo, settings=settings)
    cnt = db_session.scalar(select(func.count()).select_from(Source).where(Source.type == "rss"))
    assert int(cnt or 0) == 2
    cnt_bind = db_session.scalar(select(func.count()).select_from(TopicSource))
    assert int(cnt_bind or 0) == 0
    assert sent_acks
