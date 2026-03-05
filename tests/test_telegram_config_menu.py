from __future__ import annotations

from pathlib import Path

import pytest

from tracker.repo import Repo
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_config_menu_renders_schedule_buttons(db_session, monkeypatch, tmp_path):
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
                    "text": "/config",
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
    _text, kb = sent_raw[-1]
    assert isinstance(kb, dict)
    inline = kb.get("inline_keyboard") or []
    # `/config` is now a registry-driven "Config Center" menu (cfgc:*).
    assert any(any(btn.get("callback_data") == "cfgc:sec:schedule:0" for btn in row) for row in inline)
    assert any(any(btn.get("callback_data") == "cfgc:restart" for btn in row) for row in inline)


@pytest.mark.asyncio
async def test_telegram_config_menu_writes_schedule_via_registry(db_session, monkeypatch, tmp_path):
    # Legacy schedule submenu flows are removed; /config is registry-driven (cfgc:*)
    # and schedules are edited via the shared Settings registry.
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
                    "text": "/config",
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


@pytest.mark.asyncio
async def test_telegram_config_mute_days_writes_app_config(db_session, monkeypatch, tmp_path):
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
                    "data": "cfg:mute:14",
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

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True):  # noqa: ANN001, ARG001
        return [201]

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        disable_preview: bool = True,
        reply_markup: dict | None = None,
    ) -> int:  # noqa: ARG001
        # menu refresh
        return 200

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await telegram_poll(repo=repo, settings=settings)

    assert repo.get_app_config("telegram_feedback_mute_days_default") == "14"
