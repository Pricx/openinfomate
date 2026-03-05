from __future__ import annotations

from typer.testing import CliRunner

from tracker.cli import app


def test_doctor_uninitialized_db_shows_hint(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    result = runner.invoke(app, ["doctor"])
    assert result.exit_code != 0
    assert "DB is not initialized" in result.stdout
    assert "tracker db init" in result.stdout


def test_doctor_initialized_db_prints_recommendations(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0

    result = runner.invoke(app, ["doctor"])
    assert result.exit_code == 0
    assert "OpenInfoMate Doctor" in result.stdout
    assert "topics_total: 0" in result.stdout
    assert "recommendations" in result.stdout
