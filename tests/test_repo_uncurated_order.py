from __future__ import annotations

import datetime as dt

from tracker.models import Item, ItemTopic, Source, Topic
from tracker.repo import Repo


def test_list_uncurated_item_topics_ordering(db_session):
    repo = Repo(db_session)
    topic = Topic(name="T", query="", enabled=True, digest_cron="0 9 * * *")
    db_session.add(topic)
    source = Source(type="rss", url="https://example.com/feed")
    db_session.add(source)
    db_session.commit()

    base = dt.datetime(2026, 2, 1, 0, 0, 0)
    items = []
    for i in range(3):
        it = Item(
            source_id=int(source.id),
            url=f"https://example.com/{i}",
            title=f"item-{i}",
            canonical_url=f"https://example.com/{i}",
            created_at=base + dt.timedelta(days=i),
        )
        db_session.add(it)
        db_session.flush()
        db_session.add(
            ItemTopic(
                item_id=int(it.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
            )
        )
        items.append(it)
    db_session.commit()

    since = base - dt.timedelta(days=1)

    newest = repo.list_uncurated_item_topics_for_topic(topic=topic, since=since, limit=3, order="desc")
    assert [row[1].title for row in newest] == ["item-2", "item-1", "item-0"]

    oldest = repo.list_uncurated_item_topics_for_topic(topic=topic, since=since, limit=3, order="asc")
    assert [row[1].title for row in oldest] == ["item-0", "item-1", "item-2"]
