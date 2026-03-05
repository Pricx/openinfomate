from __future__ import annotations

import datetime as dt
import json

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from typer.testing import CliRunner

from tracker.cli import app
from tracker.models import Item, ItemTopic
from tracker.repo import Repo


def test_cli_event_list_json(tmp_path, monkeypatch):
    runner = CliRunner()

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TRACKER_DB_URL", "sqlite:///./test.db")

    init = runner.invoke(app, ["db", "init"])
    assert init.exit_code == 0

    engine = create_engine("sqlite:///./test.db", connect_args={"check_same_thread": False})
    with Session(engine) as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai", digest_cron="0 9 * * *")
        source = repo.add_source(type="rss", url="file:///tmp/test.xml")

        now = dt.datetime.utcnow()
        item = Item(
            source_id=source.id,
            url="https://example.com/x",
            canonical_url="https://example.com/x",
            title="Hello",
            published_at=now,
            content_text="x",
            content_hash="0" * 64,
            simhash64=0,
        )
        session.add(item)
        session.commit()

        it = ItemTopic(
            item_id=item.id,
            topic_id=topic.id,
            decision="digest",
            relevance_score=1,
            novelty_score=2,
            quality_score=3,
            reason="r",
        )
        session.add(it)
        session.commit()

    result = runner.invoke(
        app,
        ["event", "list", "--topic", "T", "--decision", "digest", "--hours", "1", "--limit", "10", "--json"],
    )
    assert result.exit_code == 0
    data = json.loads(result.stdout)
    assert len(data) == 1
    assert data[0]["decision"] == "digest"
    assert data[0]["topic"] == "T"

