from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings
from tracker.web.onboarding import build_onboarding_state


def _client(tmp_path, **kwargs) -> tuple[TestClient, Settings]:
    db_path = Path(tmp_path) / "onboarding-ui.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", **kwargs)
    return TestClient(create_app(settings)), settings


def _seed_complete_state(settings: Settings) -> None:
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("llm_test_reasoning_last_ok", "true")
        repo.set_app_config(
            "llm_test_reasoning_last_fingerprint",
            f"{settings.llm_base_url}|{settings.llm_model_reasoning}",
        )
        repo.set_app_config("profile_text", "user profile")
        repo.add_config_agent_run(kind="tracking_ai_setup", status="applied")
        repo.set_app_config("telegram_chat_id", "123")


def test_admin_shows_install_guide_banner_when_setup_incomplete(tmp_path):
    client, _settings = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=config")

    assert resp.status_code == 200
    assert "Install Guide" in resp.text
    assert "Finish the required setup in order" in resp.text
    assert "Continue current step" in resp.text
    assert "Access Control" in resp.text


def test_setup_wizard_includes_detailed_playbook_and_bookmark_export_help(tmp_path):
    client, _settings = _client(tmp_path)

    resp = client.get("/setup/wizard?token=secret")

    assert resp.status_code == 200
    assert "Required Setup Checklist" in resp.text
    assert "Step-by-step Playbook" in resp.text
    assert "How to export browser bookmarks" in resp.text
    assert "Chrome: Bookmarks Manager" in resp.text
    assert "BotFather" in resp.text
    assert resp.text.index("Required Setup Checklist") < resp.text.index("Step-by-step Playbook") < resp.text.index("Doctor")


def test_push_center_includes_rebind_flow_and_bot_tutorial(tmp_path):
    client, _settings = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=push")

    assert resp.status_code == 200
    assert "Telegram Bot quick tutorial" in resp.text
    assert "Save TRACKER_TELEGRAM_BOT_TOKEN?" not in resp.text
    assert "force_rebind: !!forceRebind" in resp.text


def test_ai_setup_leave_guard_tracks_local_unapplied_marker(tmp_path):
    client, _settings = _client(tmp_path)

    resp = client.get("/admin?token=secret&section=ai_setup")

    assert resp.status_code == 200
    assert "localStorage.getItem('tracker_ai_setup_unapplied_run_id')" in resp.text
    assert "pending > 0 && pending === rid" in resp.text


def test_onboarding_push_requires_telegram_token_and_chat_binding(tmp_path):
    client, settings = _client(tmp_path)
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        state = build_onboarding_state(repo=repo, settings=settings, token="secret", page_id="wizard")

    assert state["telegram_connected"] is True
    assert state["telegram_ok"] is False
    assert state["push_ok"] is False

    resp = client.get("/setup/wizard?token=secret")
    assert resp.status_code == 200
    assert "status=missing" in resp.text


def test_advanced_tour_can_be_dismissed_permanently(tmp_path):
    client, settings = _client(
        tmp_path,
        admin_password="pw",
        llm_base_url="https://example.com/v1",
        llm_model_reasoning="gpt-4.1",
        llm_api_key="secret",
        telegram_bot_token="TEST",
    )
    _seed_complete_state(settings)

    resp = client.get("/admin?token=secret&section=overview")
    assert resp.status_code == 200
    assert "Advanced Setup Tour" in resp.text

    dismiss = client.post("/admin/onboarding/dismiss-advanced-tour?token=secret")
    assert dismiss.status_code == 200
    assert dismiss.json()["state"]["show_advanced_tour"] is False

    resp2 = client.get("/admin?token=secret&section=overview")
    assert resp2.status_code == 200
    assert "Advanced Setup Tour" not in resp2.text

def test_admin_renders_floating_config_chat_widget(tmp_path):
    client, settings = _client(
        tmp_path,
        admin_password="pw",
        llm_base_url="https://example.com/v1",
        llm_model_reasoning="gpt-4.1",
        llm_api_key="secret",
    )
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("llm_test_reasoning_last_ok", "true")
        repo.set_app_config("llm_test_reasoning_last_fingerprint", f"{settings.llm_base_url}|{settings.llm_model_reasoning}")

    resp = client.get("/admin?token=secret&section=overview")

    assert resp.status_code == 200
    assert 'trackerConfigChatLauncher' in resp.text
    assert 'trackerConfigChatInput' in resp.text
    assert 'trackerConfigChatPanel' in resp.text
    assert 'aria-hidden="true"' in resp.text
    assert '.cfg-chat-panel[hidden] { display: none !important; }' in resp.text
    assert 'hasStoredOpen: false' in resp.text
    assert "Object.prototype.hasOwnProperty.call(obj, 'open')" in resp.text
    assert 'open: !!_trackerConfigChat.open' in resp.text
    assert 'if (_trackerConfigChat.hasStoredOpen) {' in resp.text
    assert 'trackerConfigChatScrollToBottom()' in resp.text
    assert "window.requestAnimationFrame(() => window.requestAnimationFrame(apply));" in resp.text
    assert "\\nElapsed: " in resp.text
    assert "/admin/config-agent/recent" in resp.text
    assert "trackerConfigChatInsertPendingMessage(want)" in resp.text
    assert "trackerConfigChatEnsurePendingWatch()" in resp.text
    assert "data-cfg-chat-pending-id" in resp.text
    assert 'conversation_json' in resp.text
    assert 'Smart Config' in resp.text or '智能配置' in resp.text

