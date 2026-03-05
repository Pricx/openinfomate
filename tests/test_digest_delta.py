from __future__ import annotations

import asyncio
import datetime as dt

from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest
from tracker.settings import Settings


def test_digest_includes_item_counts(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai")
    source = repo.add_source(type="rss", url="file://fixture")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    now = dt.datetime.utcnow()

    prev_item = Item(
        source_id=source.id,
        url="https://example.com/prev",
        canonical_url="https://example.com/prev",
        title="Prev",
        published_at=None,
        content_text="prev",
        content_hash="prev",
        simhash64=0,
        created_at=now - dt.timedelta(hours=30),
    )
    db_session.add(prev_item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=prev_item.id, topic_id=topic.id, decision="digest", reason="prev"))

    cur_item_1 = Item(
        source_id=source.id,
        url="https://example.com/cur1",
        canonical_url="https://example.com/cur1",
        title="Cur1",
        published_at=None,
        content_text="cur1",
        content_hash="cur1",
        simhash64=0,
        created_at=now - dt.timedelta(hours=2),
    )
    db_session.add(cur_item_1)
    db_session.flush()
    db_session.add(ItemTopic(item_id=cur_item_1.id, topic_id=topic.id, decision="digest", reason="cur1"))

    cur_item_2 = Item(
        source_id=source.id,
        url="https://example.com/cur2",
        canonical_url="https://example.com/cur2",
        title="Cur2",
        published_at=None,
        content_text="cur2",
        content_hash="cur2",
        simhash64=0,
        created_at=now - dt.timedelta(hours=1),
    )
    db_session.add(cur_item_2)
    db_session.flush()
    db_session.add(ItemTopic(item_id=cur_item_2.id, topic_id=topic.id, decision="alert", reason="cur2"))

    db_session.commit()

    result = asyncio.run(run_digest(session=db_session, settings=Settings(), hours=24, push=False))
    assert len(result.per_topic) == 1
    markdown = result.per_topic[0].markdown
    assert "条目: 2 (1 告警, 1 摘要)" in markdown
    assert "Previous:" not in markdown
    assert "Delta:" not in markdown


def test_digest_filters_by_published_at_when_present(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai")
    source = repo.add_source(type="rss", url="file://fixture")
    repo.bind_topic_source(topic=topic, source=source)

    now = dt.datetime.utcnow()

    # Old published_at but recently ingested (created_at). Should NOT appear in a "last 24h" digest.
    old_item = Item(
        source_id=source.id,
        url="https://example.com/old",
        canonical_url="https://example.com/old",
        title="Old",
        published_at=now - dt.timedelta(days=7),
        content_text="old",
        content_hash="old",
        simhash64=0,
        created_at=now - dt.timedelta(hours=1),
    )
    db_session.add(old_item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=old_item.id, topic_id=topic.id, decision="digest", reason="old"))
    db_session.commit()

    result = asyncio.run(run_digest(session=db_session, settings=Settings(), hours=24, push=False))
    assert len(result.per_topic) == 1
    markdown = result.per_topic[0].markdown
    assert "https://example.com/old" not in markdown
    assert "_暂无新条目。_" in markdown
