from __future__ import annotations

import json
from contextlib import asynccontextmanager
from pathlib import Path

import pytest

from tracker.config_agent_core import ConfigAgentPlanResult
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.service import _run_telegram_config_agent_worker_job, _run_telegram_connect_poll_job
from tracker.settings import Settings


@asynccontextmanager
async def _no_job_lock(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
    yield


@pytest.mark.asyncio
async def test_telegram_connect_poll_job_uses_runtime_env_token(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-poll-runtime.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text(
        'TRACKER_TELEGRAM_BOT_TOKEN="ENVTEST"\nTRACKER_TELEGRAM_CONNECT_POLL_SECONDS="3"\n',
        encoding="utf-8",
    )
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_setup_code", "setup-code")

    seen: dict[str, str] = {}

    async def fake_telegram_poll(*, repo, settings):  # noqa: ANN001
        seen["token"] = str(settings.telegram_bot_token or "")
        seen["chat_id"] = str(settings.telegram_chat_id or "")
        return {"ok": True}

    monkeypatch.setattr("tracker.service.job_lock_async", _no_job_lock)
    monkeypatch.setattr("tracker.telegram_connect.telegram_poll", fake_telegram_poll)

    await _run_telegram_connect_poll_job(make_session, settings)

    assert seen["token"] == "ENVTEST"


@pytest.mark.asyncio
async def test_telegram_config_agent_worker_uses_runtime_env_token(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-runtime.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_TELEGRAM_BOT_TOKEN="ENVTEST"\n', encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("output_language", "zh")
        repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=10,
            query="把 digest hours 改成 6",
        )

    async def fake_plan(**_kwargs):
        return ConfigAgentPlanResult(
            run_id=31,
            plan={"actions": [{"op": "mcp.setting.set", "field": "digest_hours", "value": "6"}]},
            warnings=[],
            preview_markdown="# Preview\n\n- digest_hours -> 6",
        )

    seen: dict[str, str] = {}

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        seen["token"] = str(self.bot_token or "")
        seen["chat_id"] = chat_id
        seen["text"] = text
        return 777

    monkeypatch.setattr("tracker.service.job_lock_async", _no_job_lock)
    monkeypatch.setattr("tracker.service.plan_config_agent_request", fake_plan)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await _run_telegram_config_agent_worker_job(make_session, settings)

    with make_session() as session:
        repo = Repo(session)
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        payload = json.loads(task.intent or "{}")
        assert task.status == "awaiting"
        assert task.prompt_message_id == 777
        assert payload["run_id"] == 31

    assert seen["token"] == "ENVTEST"
    assert seen["chat_id"] == "123"
    assert "智能配置计划已生成" in seen["text"]
