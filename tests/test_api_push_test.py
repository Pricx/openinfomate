from __future__ import annotations

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_api_push_test_all_channels(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
        dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example",
        webhook_url="https://example.invalid/webhook",
        smtp_host="smtp.example.com",
        smtp_port=587,
        email_from="from@example.com",
        email_to="to@example.com",
    )

    # Avoid real network.
    async def noop_send_markdown(self, *, title: str, markdown: str) -> None:  # type: ignore[no-untyped-def]
        return None

    async def noop_send_json(self, payload: dict) -> None:  # type: ignore[no-untyped-def]
        return None

    def noop_email_send(self, *, subject: str, text: str) -> None:  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.push.dingtalk.DingTalkPusher.send_markdown", noop_send_markdown)
    monkeypatch.setattr("tracker.push.webhook.WebhookPusher.send_json", noop_send_json)
    monkeypatch.setattr("tracker.push.email.EmailPusher.send", noop_email_send)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/pushes/test", headers=headers, json={})
    assert resp.status_code == 200
    data = resp.json()
    statuses = {r["channel"]: r["status"] for r in data["results"]}
    assert statuses["dingtalk"] == "sent"
    assert statuses["email"] == "sent"
    assert statuses["webhook"] == "sent"


def test_api_push_test_only_one_channel(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
        dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example",
        webhook_url="https://example.invalid/webhook",
    )

    async def noop_send_markdown(self, *, title: str, markdown: str) -> None:  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.push.dingtalk.DingTalkPusher.send_markdown", noop_send_markdown)

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.post("/pushes/test", headers=headers, json={"only": "dingtalk"})
    assert resp.status_code == 200
    data = resp.json()
    assert [r["channel"] for r in data["results"]] == ["dingtalk"]
    assert data["results"][0]["status"] == "sent"


def test_api_push_test_invalid_only(tmp_path):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
    )

    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}
    resp = client.post("/pushes/test", headers=headers, json={"only": "nope"})
    assert resp.status_code == 400

