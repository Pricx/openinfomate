from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.repo import Repo
from tracker.settings import Settings


def test_admin_settings_patch_updates_env_and_db(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    r = client.post(
        "/admin/settings/patch?token=secret&section=config",
        data={
            "output_language": "en",
            "cron_timezone": "Asia/Shanghai",
            "ui_theme_follow_system": "false",
            # Default is True; set False to validate env+DB persistence.
            "llm_curation_enabled": "false",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    text = env_path.read_text(encoding="utf-8")
    assert 'TRACKER_OUTPUT_LANGUAGE="en"' in text
    assert 'TRACKER_CRON_TIMEZONE="Asia/Shanghai"' in text
    assert 'TRACKER_UI_THEME_FOLLOW_SYSTEM="false"' in text
    assert 'TRACKER_LLM_CURATION_ENABLED="false"' in text

    # Non-secret fields are also stored in DB app_config for dynamic overrides.
    from tracker.db import session_factory

    engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        assert repo.get_app_config("output_language") == "en"
        assert repo.get_app_config("cron_timezone") == "Asia/Shanghai"
        assert repo.get_app_config("ui_theme_follow_system") == "false"
        assert repo.get_app_config("llm_curation_enabled") == "false"


def test_admin_settings_patch_secret_blank_means_keep_and_clear_secret(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    # Set a secret via patch.
    r = client.post(
        "/admin/settings/patch?token=secret&section=config",
        data={"llm_api_key": "k"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert 'TRACKER_LLM_API_KEY="k"' in env_path.read_text(encoding="utf-8")

    # Blank should NOT clear secrets.
    r = client.post(
        "/admin/settings/patch?token=secret&section=config",
        data={"llm_api_key": ""},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert 'TRACKER_LLM_API_KEY="k"' in env_path.read_text(encoding="utf-8")

    # Explicit clear endpoint clears it.
    r = client.post(
        "/admin/settings/clear-secret?token=secret&section=config",
        data={"field": "llm_api_key"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert 'TRACKER_LLM_API_KEY=""' in env_path.read_text(encoding="utf-8")


def test_admin_settings_patch_optional_blank_does_not_write(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    r = client.post(
        "/admin/settings/patch?token=secret&section=config",
        data={
            "cron_timezone": "Asia/Shanghai",
            # Optional field left blank should mean "no change" (not "write empty string").
            "smtp_host": "",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    text = env_path.read_text(encoding="utf-8")
    assert 'TRACKER_CRON_TIMEZONE="Asia/Shanghai"' in text
    assert "TRACKER_SMTP_HOST" not in text
