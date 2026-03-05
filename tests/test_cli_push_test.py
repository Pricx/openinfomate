from __future__ import annotations

from typer.testing import CliRunner

from tracker.cli import app


def test_push_test_uninitialized_db_shows_hint(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")
    monkeypatch.setenv("TRACKER_WEBHOOK_URL", "https://example.invalid/webhook")

    result = runner.invoke(app, ["push", "test"])
    assert result.exit_code != 0
    assert "DB is not initialized" in result.stdout
    assert "tracker db init" in result.stdout


def test_push_test_sends_configured_channels(tmp_path, monkeypatch):
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

    # Init DB.
    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0

    result = runner.invoke(app, ["push", "test"])
    assert result.exit_code == 0
    assert "dingtalk: sent" in result.stdout
    assert "email: sent" in result.stdout
    assert "webhook: sent" in result.stdout
