from __future__ import annotations

from typer.testing import CliRunner

from tracker.cli import app


def test_topic_bootstrap_seeds_sources_and_is_idempotent(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    r = runner.invoke(
        app,
        [
            "topic",
            "bootstrap",
            "--name",
            "T",
            "--query",
            "gpu,asic",
            "--searxng-base-url",
            "http://127.0.0.1:8888",
            "--discourse-base-url",
            "https://forum.example.com",
        ],
    )
    assert r.exit_code == 0

    s = runner.invoke(app, ["stats"])
    assert s.exit_code == 0
    assert "- topics_total: 1" in s.stdout
    assert "- sources_total: 3" in s.stdout
    assert "- bindings_total: 3" in s.stdout

    # Idempotent second run.
    r2 = runner.invoke(
        app,
        [
            "topic",
            "bootstrap",
            "--name",
            "T",
            "--query",
            "gpu,asic",
            "--searxng-base-url",
            "http://127.0.0.1:8888",
            "--discourse-base-url",
            "https://forum.example.com",
        ],
    )
    assert r2.exit_code == 0

    s2 = runner.invoke(app, ["stats"])
    assert s2.exit_code == 0
    assert "- topics_total: 1" in s2.stdout
    assert "- sources_total: 3" in s2.stdout
    assert "- bindings_total: 3" in s2.stdout


def test_topic_bootstrap_adds_nodeseek_when_flag_enabled(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    r = runner.invoke(
        app,
        [
            "topic",
            "bootstrap",
            "--name",
            "China Dev社区",
            "--query",
            "AI,LLM,开源,安全,系统,infra,工程化",
            "--searxng-base-url",
            "http://127.0.0.1:8888",
            "--discourse-base-url",
            "https://forum.example.com",
            "--add-nodeseek",
        ],
    )
    assert r.exit_code == 0

    s = runner.invoke(app, ["source", "list"])
    assert s.exit_code == 0
    assert "https://rss.nodeseek.com/" in s.stdout
