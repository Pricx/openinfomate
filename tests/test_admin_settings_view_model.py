from __future__ import annotations

from pathlib import Path

from tracker.admin_settings import build_settings_view
from tracker.api import create_app
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_build_settings_view_source_and_secret_set(tmp_path):
    db_path = Path(tmp_path) / "vm.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text(
        "\n".join(
            [
                'TRACKER_LLM_API_KEY="k"',
                'TRACKER_CRON_TIMEZONE="Asia/Shanghai"',
                "",
            ]
        ),
        encoding="utf-8",
    )

    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path))
    # Ensure tables exist (Repo uses app_config).
    create_app(settings)
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        # DB override should win over env when both exist.
        repo.set_app_config("cron_timezone", "UTC")

        ui = build_settings_view(repo=repo, settings=settings, env_path=env_path)
        views = ui["views"]

        assert views["cron_timezone"]["source"] == "db"
        assert views["cron_timezone"]["current_value_str"] == "UTC"

        assert views["llm_api_key"]["secret"] is True
        assert views["llm_api_key"]["source"] == "env"
        assert views["llm_api_key"]["secret_is_set"] is True

        assert views["ui_theme_follow_system"]["current_value"] is True
        assert views["ui_theme_follow_system"]["current_value_str"] == "True"
