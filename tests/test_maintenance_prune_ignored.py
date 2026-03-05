from __future__ import annotations

import datetime as dt
from pathlib import Path

from sqlalchemy.orm import Session

from tracker.db import create_engine_from_settings
from tracker.maintenance import run_prune_ignored
from tracker.models import Base, Item, ItemTopic
from tracker.repo import Repo
from tracker.settings import Settings


def test_run_prune_ignored_prunes_old_ignored(tmp_path: Path):
    db_path = tmp_path / "tracker.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", prune_ignored_days=10)

    engine = create_engine_from_settings(settings)
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="x")
        source = repo.add_source(type="rss", url="https://example.com/feed")

        old = dt.datetime(2026, 1, 1, 0, 0, 0)
        item = Item(
            source_id=source.id,
            url="https://a",
            canonical_url="https://a",
            title="t",
            published_at=None,
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=old,
        )
        session.add(item)
        session.flush()
        session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="ignore", reason="", created_at=old))
        session.commit()

    now = dt.datetime(2026, 2, 1, 0, 0, 0)
    result = run_prune_ignored(settings=settings, now=now)
    assert result["item_topics_deleted"] == 1
    assert result["items_deleted"] == 1

    # Verify it actually deleted.
    with Session(engine) as session:
        assert Repo(session).get_item_by_canonical_url("https://a") is None

