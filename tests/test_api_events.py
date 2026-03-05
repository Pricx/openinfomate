from __future__ import annotations

import datetime as dt
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tracker.api import create_app
from tracker.models import Base, Item, ItemTopic
from tracker.repo import Repo
from tracker.settings import Settings


def test_api_events_requires_token_and_filters(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    app = create_app(settings)
    client = TestClient(app)

    # Token required for management endpoints.
    assert client.get("/events").status_code == 401

    # Seed DB with one event.
    engine = create_engine(settings.db_url, connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
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
            decision="alert",
            relevance_score=1,
            novelty_score=2,
            quality_score=3,
            reason="r",
        )
        session.add(it)
        session.commit()

    headers = {"x-tracker-token": "secret"}

    r = client.get(
        "/events",
        headers=headers,
        params={"topic": "T", "decision": "alert", "hours": "1", "limit": "10"},
    )
    assert r.status_code == 200
    data = r.json()
    assert len(data) == 1
    assert data[0]["decision"] == "alert"
    assert data[0]["topic"] == "T"
    assert data[0]["item_url"] == "https://example.com/x"

    r = client.get("/events", headers=headers, params={"decision": "bad"})
    assert r.status_code == 400

    r = client.get("/events", headers=headers, params={"topic": "Nope"})
    assert r.status_code == 404

