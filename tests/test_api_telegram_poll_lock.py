from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


@asynccontextmanager
async def _busy_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
    raise TimeoutError("busy")
    yield


def test_api_telegram_poll_returns_connected_when_poll_lock_is_busy(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-poll-lock.db",
        api_token="secret",
        telegram_bot_token="TEST",
    )
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)
    with make_session() as session:
        Repo(session).set_app_config("telegram_chat_id", "123")

    called = {"poll": False}

    async def fake_telegram_poll(*, repo, settings, code=None, make_session=None):  # noqa: ANN001, ARG001
        called["poll"] = True
        return {"status": "connected", "chat_id": "123"}

    monkeypatch.setattr("tracker.api.job_lock_async", _busy_job_lock_async)
    monkeypatch.setattr("tracker.telegram_connect.telegram_poll", fake_telegram_poll)

    client = TestClient(create_app(settings))
    resp = client.post("/telegram/poll", headers={"x-tracker-token": "secret"}, json={})

    assert resp.status_code == 200
    assert resp.json() == {"status": "connected", "chat_id": "123"}
    assert called["poll"] is False


def test_api_telegram_poll_returns_pending_when_poll_lock_is_busy_and_setup_code_exists(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-poll-lock-pending.db",
        api_token="secret",
        telegram_bot_token="TEST",
    )
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)
    with make_session() as session:
        Repo(session).set_app_config("telegram_setup_code", "setup-code")

    called = {"poll": False}

    async def fake_telegram_poll(*, repo, settings, code=None, make_session=None):  # noqa: ANN001, ARG001
        called["poll"] = True
        return {"status": "pending"}

    monkeypatch.setattr("tracker.api.job_lock_async", _busy_job_lock_async)
    monkeypatch.setattr("tracker.telegram_connect.telegram_poll", fake_telegram_poll)

    client = TestClient(create_app(settings))
    resp = client.post("/telegram/poll", headers={"x-tracker-token": "secret"}, json={})

    assert resp.status_code == 200
    assert resp.json() == {"status": "pending"}
    assert called["poll"] is False
