from __future__ import annotations

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_api_doctor_requires_token_when_configured(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_token="secret")
    client = TestClient(create_app(settings))

    resp = client.get("/doctor")
    assert resp.status_code == 401


def test_api_doctor_returns_missing_env_keys(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_token="secret")
    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    resp = client.get("/doctor", headers=headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["db_ok"] is True
    assert data["push"]["dingtalk_configured"] is False
    assert "dingtalk" in data["push"]["missing_env"]
    assert "TRACKER_DINGTALK_WEBHOOK_URL" in data["push"]["missing_env"]["dingtalk"]
