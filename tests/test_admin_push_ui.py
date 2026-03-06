from __future__ import annotations

import re

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def _client(tmp_path) -> TestClient:
    db_path = Path(tmp_path) / "admin-push-ui.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    return TestClient(create_app(settings))


def test_admin_overview_keeps_doctor_card(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=overview")

    assert resp.status_code == 200
    assert "Doctor" in resp.text
    assert "Setup Wizard" in resp.text


def test_admin_push_hides_redundant_doctor_health_actions(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=push")

    assert resp.status_code == 200
    assert "Push Hub" in resp.text
    assert "Health (no push)" not in resp.text
    assert "Health (push)" not in resp.text


def test_admin_config_hides_telegram_bot_token_field(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=config")

    assert resp.status_code == 200
    assert 'name="telegram_bot_token"' not in resp.text
    assert 'data-cfg-input="telegram_bot_token"' not in resp.text


def test_admin_config_llm_keys_autosave_and_base_url_help(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=config")

    assert resp.status_code == 200
    assert "`/v1` is optional" in resp.text
    assert "OpenInfoMate will add it automatically" in resp.text
    assert "LLM API keys autosave and immediately retrigger connectivity tests" in resp.text
    assert re.search(r'data-cfg-field="llm_api_key".*?data-cfg-autosave-secret="1"', resp.text, re.S)
    assert re.search(r'data-cfg-field="llm_mini_api_key".*?data-cfg-autosave-secret="1"', resp.text, re.S)
    assert "'llm_api_key'" in resp.text
    assert "'llm_mini_api_key'" in resp.text


def test_base_theme_follows_system_and_can_be_disabled_in_config(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=config")

    assert resp.status_code == 200
    assert 'data-theme-follow-system="1"' in resp.text
    assert "prefers-color-scheme: dark" in resp.text
    assert "trackerSetThemeFollowSystem(false)" in resp.text
    assert "ui_theme_follow_system" in resp.text
    assert "_trackerMaybeApplyThemeSettingAfterSave(field, row)" in resp.text


def test_admin_push_includes_telegram_connect_dialog_and_gate(tmp_path):
    client = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=push")

    assert resp.status_code == 200
    assert 'id="dlgTelegramConnectReady"' in resp.text
    assert 'id="telegramConnectLinkAnchor"' in resp.text
    assert "Finish Telegram binding" in resp.text
    assert "Open Telegram link" in resp.text
    assert "const tested = await trackerTelegramTestBotToken();" in resp.text
    assert "if (!tested) return;" in resp.text
    assert "trackerOpenDialog('dlgTelegramConnectReady')" in resp.text
