from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_api_admin_env_update_writes_env_file(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    r = client.post(
        "/admin/env/update?token=secret",
        data={
            "cron_timezone": "Asia/Shanghai",
            "dingtalk_webhook_url": "https://oapi.dingtalk.com/robot/send?access_token=abc",
            "smtp_host": "smtp.example.com",
            "smtp_port": "587",
            "email_from": "me@example.com",
            "email_to": "me@example.com",
            "smtp_starttls": "true",
            "smtp_use_ssl": "false",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    text = env_path.read_text(encoding="utf-8")
    assert "TRACKER_CRON_TIMEZONE=\"Asia/Shanghai\"" in text
    assert "TRACKER_DINGTALK_WEBHOOK_URL=\"https://oapi.dingtalk.com/robot/send?access_token=abc\"" in text
    assert "TRACKER_SMTP_HOST=\"smtp.example.com\"" in text
    assert "TRACKER_SMTP_PORT=\"587\"" in text
    assert "TRACKER_EMAIL_FROM=\"me@example.com\"" in text
    assert "TRACKER_EMAIL_TO=\"me@example.com\"" in text
    assert "TRACKER_SMTP_STARTTLS=\"true\"" in text
    assert "TRACKER_SMTP_USE_SSL=\"false\"" in text
