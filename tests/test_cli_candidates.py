from __future__ import annotations

from typer.testing import CliRunner

from tracker.cli import app
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_cli_candidate_list_accept_ignore(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0

    # Create a topic via CLI.
    t = runner.invoke(app, ["topic", "add", "--name", "T", "--query", "x"])
    assert t.exit_code == 0

    # Insert a candidate via Repo.
    settings = Settings(db_url="sqlite:///./test.db")
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        topic = repo.get_topic_by_name("T")
        assert topic is not None
        cand, created = repo.add_source_candidate(
            topic_id=topic.id,
            source_type="rss",
            url="https://example.com/feed",
            discovered_from_url="https://example.com/blog",
        )
        assert created is True
        cand_id = cand.id

    listed = runner.invoke(app, ["candidate", "list", "--status", "new"])
    assert listed.exit_code == 0
    assert f"#{cand_id}" in listed.stdout

    ok = runner.invoke(app, ["candidate", "accept", str(cand_id)])
    assert ok.exit_code == 0

    accepted = runner.invoke(app, ["candidate", "list", "--status", "accepted"])
    assert accepted.exit_code == 0
    assert f"#{cand_id}" in accepted.stdout

    ignore = runner.invoke(app, ["candidate", "ignore", str(cand_id)])
    assert ignore.exit_code == 0

    ignored = runner.invoke(app, ["candidate", "list", "--status", "ignored"])
    assert ignored.exit_code == 0
    assert f"#{cand_id}" in ignored.stdout


def test_cli_candidate_cleanup_comment_feeds(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0
    t = runner.invoke(app, ["topic", "add", "--name", "T", "--query", "x"])
    assert t.exit_code == 0

    settings = Settings(db_url="sqlite:///./test.db")
    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        topic = repo.get_topic_by_name("T")
        assert topic is not None
        comment_cand, created = repo.add_source_candidate(
            topic_id=topic.id,
            source_type="rss",
            url="https://example.com/comments/feed/",
            discovered_from_url="https://example.com/blog",
        )
        assert created is True
        normal_cand, created = repo.add_source_candidate(
            topic_id=topic.id,
            source_type="rss",
            url="https://example.com/feed.xml",
            discovered_from_url="https://example.com/blog",
        )
        assert created is True

        comment_id = comment_cand.id
        normal_id = normal_cand.id

    dry = runner.invoke(app, ["candidate", "cleanup"])
    assert dry.exit_code == 0
    assert f"#{comment_id}" in dry.stdout
    assert f"#{normal_id}" not in dry.stdout

    ok = runner.invoke(app, ["candidate", "cleanup", "--apply"])
    assert ok.exit_code == 0

    new_list = runner.invoke(app, ["candidate", "list", "--status", "new"])
    assert new_list.exit_code == 0
    assert f"#{normal_id}" in new_list.stdout
    assert f"#{comment_id}" not in new_list.stdout
