from __future__ import annotations

import httpx
from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


def test_api_telegram_link_and_poll_connects(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
        env_path=str(env_path),
        telegram_bot_token="TEST",
        telegram_bot_username="TrackerHotBot",
    )

    code_holder: dict[str, str] = {}

    async def fake_get(self: httpx.AsyncClient, url: str, params=None, **_kwargs):  # noqa: ANN001
        assert url.startswith("https://api.telegram.org/botTEST/getUpdates")
        code = code_holder.get("code") or ""
        payload = {
            "ok": True,
            "result": [
                {
                    "update_id": 1,
                    "message": {"text": f"/start {code}", "chat": {"id": 123}},
                }
            ],
        }
        req = httpx.Request("GET", url)
        return httpx.Response(200, json=payload, request=req)

    async def fake_post(self: httpx.AsyncClient, url: str, json=None, **_kwargs):  # noqa: ANN001
        if url.startswith("https://api.telegram.org/botTEST/deleteWebhook"):
            req = httpx.Request("POST", url)
            return httpx.Response(200, json={"ok": True, "result": True}, request=req)

        assert url.startswith("https://api.telegram.org/botTEST/sendMessage")
        assert json["chat_id"] == "123"
        assert "Tracker" in str(json.get("text") or "")
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/telegram/link", headers=headers, json={})
    assert resp.status_code == 200
    data = resp.json()
    assert "link" in data and data["link"].startswith("https://t.me/")
    code_holder["code"] = data["code"]

    resp2 = client.post("/telegram/poll", headers=headers, json={})
    assert resp2.status_code == 200
    assert resp2.json()["status"] == "connected"

    st = client.get("/telegram/status", headers=headers).json()
    assert st["connected"] is True
    assert st["chat_id"] == "123"

    # Privacy: once connected, do not allow generating a new connect link without disconnecting.
    resp3 = client.post("/telegram/link", headers=headers, json={})
    assert resp3.status_code == 409



def test_api_telegram_link_force_rebind_replaces_existing_connection(tmp_path):
    env_path = tmp_path / ".env"
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-rebind.db",
        api_token="secret",
        env_path=str(env_path),
        telegram_bot_token="TEST",
        telegram_bot_username="TrackerHotBot",
    )
    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")

    resp = client.post("/telegram/link", headers=headers, json={"force_rebind": True})
    assert resp.status_code == 200
    data = resp.json()
    assert data["link"].startswith("https://t.me/")

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        assert (repo.get_app_config("telegram_chat_id") or "") == ""
        assert (repo.get_app_config("telegram_setup_code") or "") == data["code"]


def test_api_telegram_poll_forwards_external_pro_bind_code_without_claiming_local_chat(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-forward.db",
        api_token="secret",
        env_path=str(env_path),
        telegram_bot_token="TEST",
        telegram_bot_username="TrackerHotBot",
        telegram_external_bind_base_url="http://127.0.0.1:9988",
        telegram_external_bind_code_prefix="oim_",
    )

    async def fake_get(self: httpx.AsyncClient, url: str, params=None, **_kwargs):  # noqa: ANN001
        assert url.startswith("https://api.telegram.org/botTEST/getUpdates")
        payload = {
            "ok": True,
            "result": [
                {
                    "update_id": 1,
                    "message": {
                        "text": "/start oim_bind_123",
                        "chat": {"id": 123},
                        "from": {"id": 456, "username": "alice"},
                    },
                }
            ],
        }
        req = httpx.Request("GET", url)
        return httpx.Response(200, json=payload, request=req)

    async def fake_post(self: httpx.AsyncClient, url: str, json=None, headers=None, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        if url.startswith("https://api.telegram.org/botTEST/deleteWebhook"):
            return httpx.Response(200, json={"ok": True, "result": True}, request=req)
        if url == "http://127.0.0.1:9988/api/internal/telegram/upstream-bind/consume":
            assert json["code"] == "oim_bind_123"
            assert json["telegramChatId"] == "123"
            return httpx.Response(
                200,
                json={"ok": True, "workspaceId": "ws_1", "replyText": "✅ 已完成绑定：Deep Research Ops"},
                request=req,
            )
        assert url.startswith("https://api.telegram.org/botTEST/sendMessage")
        assert json["chat_id"] == "123"
        assert "已完成绑定" in str(json.get("text") or "")
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 1}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/telegram/poll", headers=headers, json={})
    assert resp.status_code == 200
    assert resp.json()["status"] == "pending"

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        assert (repo.get_app_config("telegram_chat_id") or "") == ""


def test_api_telegram_poll_forwards_external_pro_bind_code_from_non_owner_chat_when_local_chat_is_bound(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-forward-bound.db",
        api_token="secret",
        env_path=str(env_path),
        telegram_bot_token="TEST",
        telegram_bot_username="TrackerHotBot",
        telegram_external_bind_base_url="http://127.0.0.1:9988",
        telegram_external_bind_code_prefix="oim_",
    )

    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "999")
        repo.set_app_config("telegram_connected_notified", "1")

    async def fake_get(self: httpx.AsyncClient, url: str, params=None, **_kwargs):  # noqa: ANN001
        assert url.startswith("https://api.telegram.org/botTEST/getUpdates")
        payload = {
            "ok": True,
            "result": [
                {
                    "update_id": 2,
                    "message": {
                        "text": "/start oim_bind_456",
                        "chat": {"id": 123},
                        "from": {"id": 456, "username": "alice"},
                    },
                }
            ],
        }
        req = httpx.Request("GET", url)
        return httpx.Response(200, json=payload, request=req)

    async def fake_post(self: httpx.AsyncClient, url: str, json=None, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        if url.startswith("https://api.telegram.org/botTEST/deleteWebhook"):
            return httpx.Response(200, json={"ok": True, "result": True}, request=req)
        if url == "http://127.0.0.1:9988/api/internal/telegram/upstream-bind/consume":
            assert json["code"] == "oim_bind_456"
            assert json["telegramChatId"] == "123"
            return httpx.Response(200, json={"ok": True, "replyText": "✅ 已完成绑定：Workspace"}, request=req)
        assert url.startswith("https://api.telegram.org/botTEST/sendMessage")
        assert json["chat_id"] == "123"
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 2}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/telegram/poll", headers=headers, json={})
    assert resp.status_code == 200
    assert resp.json()["status"] == "connected"

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        assert (repo.get_app_config("telegram_chat_id") or "") == "999"


def test_api_telegram_poll_forwards_external_pro_bind_code_from_existing_local_chat(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api-forward-existing-chat.db",
        api_token="secret",
        env_path=str(env_path),
        telegram_bot_token="TEST",
        telegram_bot_username="TrackerHotBot",
        telegram_external_bind_base_url="http://127.0.0.1:9988",
        telegram_external_bind_code_prefix="oim_",
    )

    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")

    async def fake_get(self: httpx.AsyncClient, url: str, params=None, **_kwargs):  # noqa: ANN001
        assert url.startswith("https://api.telegram.org/botTEST/getUpdates")
        payload = {
            "ok": True,
            "result": [
                {
                    "update_id": 3,
                    "message": {
                        "text": "/start oim_bind_existing",
                        "chat": {"id": 123},
                        "from": {"id": 123, "username": "alice"},
                    },
                }
            ],
        }
        req = httpx.Request("GET", url)
        return httpx.Response(200, json=payload, request=req)

    async def fake_post(self: httpx.AsyncClient, url: str, json=None, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        if url.startswith("https://api.telegram.org/botTEST/deleteWebhook"):
            return httpx.Response(200, json={"ok": True, "result": True}, request=req)
        if url == "http://127.0.0.1:9988/api/internal/telegram/upstream-bind/consume":
            assert json["code"] == "oim_bind_existing"
            assert json["telegramChatId"] == "123"
            return httpx.Response(200, json={"ok": True, "replyText": "✅ 已完成绑定：Workspace"}, request=req)
        assert url.startswith("https://api.telegram.org/botTEST/sendMessage")
        assert json["chat_id"] == "123"
        assert "已完成绑定" in str(json.get("text") or "")
        return httpx.Response(200, json={"ok": True, "result": {"message_id": 3}}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/telegram/poll", headers=headers, json={})
    assert resp.status_code == 200
    assert resp.json()["status"] == "connected"

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        assert (repo.get_app_config("telegram_chat_id") or "") == "123"
