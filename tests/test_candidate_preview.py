from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from tracker.api import create_app
from tracker.cli import app as cli_app
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


def _write_rss(path: Path) -> str:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Entry One</title>
      <link>https://example.com/one</link>
      <pubDate>Wed, 11 Feb 2026 00:00:00 GMT</pubDate>
      <description>One</description>
    </item>
    <item>
      <title>Entry Two</title>
      <link>https://example.com/two</link>
      <pubDate>Wed, 11 Feb 2026 01:00:00 GMT</pubDate>
      <description>Two</description>
    </item>
  </channel>
</rss>
"""
    path.write_text(xml, encoding="utf-8")
    return path.resolve().as_uri()


def test_cli_candidate_preview_prints_entries(tmp_path, monkeypatch):
    runner = CliRunner()
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(cli_app, ["db", "init"])
    assert init.exit_code == 0

    add_topic = runner.invoke(cli_app, ["topic", "add", "--name", "T", "--query", "ai", "--alert-keywords", "breaking"])
    assert add_topic.exit_code == 0

    feed_uri = _write_rss(tmp_path / "feed.xml")

    engine = create_engine("sqlite:///./test.db", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        repo = Repo(session)
        topic = repo.get_topic_by_name("T")
        assert topic is not None
        cand, created = repo.add_source_candidate(topic_id=topic.id, source_type="rss", url=feed_uri, title="feed")
        assert created is True
        cand_id = cand.id

    out = runner.invoke(cli_app, ["candidate", "preview", str(cand_id), "--limit", "10"])
    assert out.exit_code == 0
    assert "Entry One" in out.stdout
    assert "Entry Two" in out.stdout


def test_api_candidate_preview_returns_entries(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    app = create_app(settings)
    client = TestClient(app)

    engine = create_engine(settings.db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai")
        feed_uri = _write_rss(Path(tmp_path) / "feed.xml")
        cand, _created = repo.add_source_candidate(topic_id=topic.id, source_type="rss", url=feed_uri, title="feed")
        cand_id = cand.id

    headers = {"x-tracker-token": "secret"}
    r = client.get(f"/candidates/{cand_id}/preview", headers=headers)
    assert r.status_code == 200
    data = r.json()
    assert data["ok"] is True
    titles = [e["title"] for e in data["entries"]]
    assert titles[:2] == ["Entry One", "Entry Two"]

