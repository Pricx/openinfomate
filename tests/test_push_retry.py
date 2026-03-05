from __future__ import annotations
import json
from pathlib import Path

from fastapi.testclient import TestClient
from typer.testing import CliRunner

from tracker.api import create_app
from tracker.cli import app as cli_app
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_cli_push_list_and_retry_digest(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    # Configure all channels.
    monkeypatch.setenv(
        "TRACKER_DINGTALK_WEBHOOK_URL",
        "https://oapi.dingtalk.com/robot/send?access_token=example",
    )
    monkeypatch.setenv("TRACKER_WEBHOOK_URL", "https://example.invalid/webhook")
    monkeypatch.setenv("TRACKER_SMTP_HOST", "smtp.example.com")
    monkeypatch.setenv("TRACKER_SMTP_PORT", "587")
    monkeypatch.setenv("TRACKER_EMAIL_FROM", "from@example.com")
    monkeypatch.setenv("TRACKER_EMAIL_TO", "to@example.com")

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

    init = runner.invoke(cli_app, ["db", "init"])
    assert init.exit_code == 0

    # Seed a digest report to retry.
    settings = Settings(db_url="sqlite:///./test.db")
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai", digest_cron="0 9 * * *")
        key = f"digest:{topic.id}:2020-01-01"
        repo.upsert_report(kind="digest", idempotency_key=key, topic_id=topic.id, title="Digest: T", markdown="# T\n")

    r = runner.invoke(cli_app, ["push", "retry", "--key", key])
    assert r.exit_code == 0
    assert "dingtalk: sent" in r.stdout
    assert "email: sent" in r.stdout
    assert "webhook: sent" in r.stdout

    r = runner.invoke(cli_app, ["push", "list", "--key", key, "--json"])
    assert r.exit_code == 0
    pushes = json.loads(r.stdout)
    assert {p["channel"] for p in pushes} == {"dingtalk", "email", "webhook"}


def test_api_pushes_list_and_retry(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example",
        webhook_url="https://example.invalid/webhook",
        smtp_host="smtp.example.com",
        smtp_port=587,
        email_from="from@example.com",
        email_to="to@example.com",
    )
    app = create_app(settings)
    client = TestClient(app)
    headers = {"x-tracker-token": "secret"}

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

    # Seed digest report.
    engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai", digest_cron="0 9 * * *")
        key = f"digest:{topic.id}:2020-01-01"
        repo.upsert_report(kind="digest", idempotency_key=key, topic_id=topic.id, title="Digest: T", markdown="# T\n")

    resp = client.post("/pushes/retry", headers=headers, json={"idempotency_key": key})
    assert resp.status_code == 200
    data = resp.json()
    assert data["idempotency_key"] == key
    statuses = {r["channel"]: r["status"] for r in data["results"]}
    assert statuses["dingtalk"] == "sent"
    assert statuses["email"] == "sent"
    assert statuses["webhook"] == "sent"

    resp = client.get("/pushes", headers=headers, params={"key": key, "limit": "10"})
    assert resp.status_code == 200
    rows = resp.json()
    assert {r["channel"] for r in rows} == {"dingtalk", "email", "webhook"}
