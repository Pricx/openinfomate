from __future__ import annotations

from typer.testing import CliRunner

from tracker.cli import app


def test_stats_uninitialized_db_shows_hint(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    result = runner.invoke(app, ["stats"])
    assert result.exit_code != 0
    assert "DB is not initialized" in result.stdout
    assert "tracker db init" in result.stdout

