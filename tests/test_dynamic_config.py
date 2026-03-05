from __future__ import annotations

import datetime as dt
import os
from pathlib import Path

import pytest

from tracker.dynamic_config import (
    apply_env_block_updates,
    effective_settings,
    export_settings_env_block,
    parse_settings_env_block,
    sync_env_and_db,
)
from tracker.envfile import parse_env_assignments
from tracker.repo import Repo


def test_parse_settings_env_block_accepts_known_keys_and_normalizes():
    text = """
    TRACKER_OUTPUT_LANGUAGE=中文
    TRACKER_DIGEST_HOURS=6
    TRACKER_DIGEST_PUSH_ENABLED=false
    TRACKER_MAX_CONCURRENT_FETCHES=12
    """
    updates = parse_settings_env_block(text)
    assert updates["TRACKER_OUTPUT_LANGUAGE"] == "zh"
    assert updates["TRACKER_DIGEST_HOURS"] == "6"
    assert updates["TRACKER_DIGEST_PUSH_ENABLED"] == "false"
    assert updates["TRACKER_MAX_CONCURRENT_FETCHES"] == "12"


def test_parse_settings_env_block_rejects_unknown_keys():
    with pytest.raises(ValueError):
        parse_settings_env_block("TRACKER_NOT_A_REAL_KEY=1\n")


def test_parse_settings_env_block_rejects_remote_dangerous_keys():
    with pytest.raises(ValueError):
        parse_settings_env_block("TRACKER_ENV_PATH=/tmp/x\n")


def test_apply_env_block_updates_updates_env_and_db(db_session, tmp_path: Path):
    repo = Repo(db_session)
    from tracker.settings import get_settings

    settings = get_settings()
    env_path = tmp_path / ".env"

    updates = {
        "TRACKER_OUTPUT_LANGUAGE": "zh",
        # env-only (secret-ish) should not be written into DB.
        "TRACKER_TELEGRAM_BOT_TOKEN": "secret",
    }
    res = apply_env_block_updates(repo=repo, settings=settings, env_path=env_path, env_updates=updates)
    assert "TRACKER_OUTPUT_LANGUAGE" in res.updated_env_keys
    assert "TRACKER_TELEGRAM_BOT_TOKEN" in res.updated_env_keys
    assert "output_language" in res.updated_db_keys
    assert repo.get_app_config("output_language") == "zh"
    assert repo.get_app_config("telegram_bot_token") is None

    env_text = env_path.read_text(encoding="utf-8")
    env = parse_env_assignments(env_text)
    assert env.get("TRACKER_OUTPUT_LANGUAGE") == "zh"
    assert env.get("TRACKER_TELEGRAM_BOT_TOKEN") == "secret"


def test_sync_env_and_db_prefers_newer_env_file(db_session, tmp_path: Path):
    repo = Repo(db_session)
    from tracker.settings import get_settings

    settings = get_settings()
    env_path = tmp_path / ".env"
    env_path.write_text('TRACKER_OUTPUT_LANGUAGE="en"\n', encoding="utf-8")

    # DB has a different value but is older -> env wins.
    repo.set_app_config("output_language", "zh")
    entry = repo.get_app_config_entry("output_language")
    assert entry is not None
    entry.updated_at = dt.datetime.utcnow() - dt.timedelta(hours=2)  # force older than env mtime
    db_session.commit()

    # Ensure env mtime is "now".
    os.utime(env_path, None)

    res = sync_env_and_db(repo=repo, settings=settings, env_path=env_path)
    assert "output_language" in res.updated_db_keys
    assert repo.get_app_config("output_language") == "en"


def test_sync_env_and_db_prefers_newer_db(db_session, tmp_path: Path):
    repo = Repo(db_session)
    from tracker.settings import get_settings

    settings = get_settings()
    env_path = tmp_path / ".env"
    env_path.write_text('TRACKER_OUTPUT_LANGUAGE="en"\n', encoding="utf-8")

    # Make env old.
    old = dt.datetime.utcnow() - dt.timedelta(hours=3)
    os.utime(env_path, (old.timestamp(), old.timestamp()))

    # DB is newer -> DB wins and overwrites env.
    repo.set_app_config("output_language", "zh")
    res = sync_env_and_db(repo=repo, settings=settings, env_path=env_path)
    assert "TRACKER_OUTPUT_LANGUAGE" in res.updated_env_keys
    env = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    assert env.get("TRACKER_OUTPUT_LANGUAGE") == "zh"


def test_effective_settings_applies_db_overrides(db_session):
    repo = Repo(db_session)
    from tracker.settings import get_settings

    settings = get_settings()
    assert settings.digest_hours != 48

    repo.set_app_config("digest_hours", "48")
    eff = effective_settings(repo=repo, settings=settings)
    assert eff.digest_hours == 48


def test_export_settings_env_block_excludes_secrets_by_default(db_session, tmp_path: Path):
    repo = Repo(db_session)
    from tracker.settings import get_settings

    settings = get_settings()
    env_path = tmp_path / ".env"
    env_path.write_text('TRACKER_TELEGRAM_BOT_TOKEN="secret"\nTRACKER_OUTPUT_LANGUAGE="zh"\n', encoding="utf-8")
    repo.set_app_config("output_language", "zh")

    block = export_settings_env_block(repo=repo, settings=settings, env_path=env_path)
    assert "TRACKER_OUTPUT_LANGUAGE" in block
    assert "TRACKER_TELEGRAM_BOT_TOKEN" not in block
