from __future__ import annotations

import httpx
from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_api_telegram_link_and_poll_connects(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
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
