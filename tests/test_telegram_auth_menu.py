from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import func, select

from tracker.envfile import parse_env_assignments
from tracker.models import TelegramTask
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_auth_menu_renders(db_session, monkeypatch, tmp_path):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    env_path = Path(tmp_path) / ".env"
    settings = Settings(telegram_bot_token="TEST", env_path=str(env_path))

    batches = [
        [
            {
                "update_id": 1,
                "message": {
                    "message_id": 10,
                    "text": "/auth",
                    "chat": {"id": 123},
                    "from": {"id": 123},
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

    assert sent_raw
    text0, kb0 = sent_raw[-1]
    assert "Auth" in text0
    assert isinstance(kb0, dict)
    assert any(
        any(btn.get("callback_data") == "auth:menu" for btn in row) for row in (kb0.get("inline_keyboard") or [])
    )


@pytest.mark.asyncio
async def test_telegram_auth_set_secret_does_not_echo_value(db_session, monkeypatch, tmp_path):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    env_path = Path(tmp_path) / ".env"
    settings = Settings(telegram_bot_token="TEST", env_path=str(env_path))

    secret_cookie = "cf_clearance=SECRET; session=SECRET2"

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "auth:set:TRACKER_DISCOURSE_COOKIE",
                    "message": {"message_id": 999, "chat": {"id": 123}},
                },
            }
        ],
        [
            {
                "update_id": 2,
                "message": {
                    "message_id": 11,
                    "text": secret_cookie,
                    "chat": {"id": 123},
                    "from": {"id": 123},
                    "reply_to_message": {"message_id": 200},
                },
            }
        ],
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(
        *,
        bot_token: str,
        callback_query_id: str,
        text: str = "",
        show_alert: bool = False,
        client_timeout_seconds: int,
    ) -> None:  # noqa: ARG001
        return

    sent_acks: list[str] = []

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        sent_acks.append(text)
        return [201]

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        return 200

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await telegram_poll(repo=repo, settings=settings)
    await telegram_poll(repo=repo, settings=settings)

    env = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    assert env.get("TRACKER_DISCOURSE_COOKIE") == secret_cookie
    assert sent_acks
    assert secret_cookie not in sent_acks[-1]


@pytest.mark.asyncio
async def test_telegram_auth_set_forbidden_key_is_ignored(db_session, monkeypatch, tmp_path):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    env_path = Path(tmp_path) / ".env"
    settings = Settings(telegram_bot_token="TEST", env_path=str(env_path))

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "auth:set:TRACKER_DB_URL",
                    "message": {"message_id": 999, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(
        *,
        bot_token: str,
        callback_query_id: str,
        text: str = "",
        show_alert: bool = False,
        client_timeout_seconds: int,
    ) -> None:  # noqa: ARG001
        return

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
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await telegram_poll(repo=repo, settings=settings)

    assert sent_raw
    # Must not create a prompt task (no dangerous key set flow).
    cnt = db_session.scalar(select(func.count()).select_from(TelegramTask))
    assert int(cnt or 0) == 0
    assert not env_path.exists()
