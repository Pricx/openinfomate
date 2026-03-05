from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from tracker.cli import app


def test_topic_bootstrap_file_seeds_multiple_and_is_idempotent(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    topics_file = Path(tmp_path).joinpath("topics.txt")
    topics_file.write_text(
        "# name|query\n"
        "AI Chips|ai chips,gpu,asic\n"
        "Rust|rust,cargo,compiler\n",
        encoding="utf-8",
    )

    args = [
        "topic",
        "bootstrap-file",
        "--in",
        str(topics_file),
        "--searxng-base-url",
        "http://127.0.0.1:8888",
        "--discourse-base-url",
        "https://forum.example.com",
    ]
    r1 = runner.invoke(app, args)
    assert r1.exit_code == 0

    s1 = runner.invoke(app, ["stats"])
    assert s1.exit_code == 0
    assert "- topics_total: 2" in s1.stdout
    # hn_search(2) + searxng_search(2) + discourse(1)
    assert "- sources_total: 5" in s1.stdout
    assert "- bindings_total: 6" in s1.stdout

    # Idempotent second run.
    r2 = runner.invoke(app, args)
    assert r2.exit_code == 0

    s2 = runner.invoke(app, ["stats"])
    assert s2.exit_code == 0
    assert "- topics_total: 2" in s2.stdout
    assert "- sources_total: 5" in s2.stdout
    assert "- bindings_total: 6" in s2.stdout


def test_topic_bootstrap_file_adds_nodeseek_when_flag_enabled(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    r = runner.invoke(app, ["db", "init"])
    assert r.exit_code == 0

    topics_file = Path(tmp_path).joinpath("topics.txt")
    topics_file.write_text(
        "China Dev论坛|AI,LLM,开源,安全,系统,infra,工程化\n",
        encoding="utf-8",
    )

    args = [
        "topic",
        "bootstrap-file",
        "--in",
        str(topics_file),
        "--searxng-base-url",
        "http://127.0.0.1:8888",
        "--discourse-base-url",
        "https://forum.example.com",
        "--add-nodeseek",
    ]
    r1 = runner.invoke(app, args)
    assert r1.exit_code == 0

    s = runner.invoke(app, ["source", "list"])
    assert s.exit_code == 0
    assert "https://rss.nodeseek.com/" in s.stdout
