from __future__ import annotations

import datetime as dt

from tracker.models import Item, ItemTopic
from tracker.repo import Repo


def _mk_item(*, source_id: int, url: str, created_at: dt.datetime) -> Item:
    return Item(
        source_id=source_id,
        url=url,
        canonical_url=url,
        title="t",
        published_at=None,
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=created_at,
    )


def test_prune_ignored_deletes_old_ignored_and_orphan_items(db_session):
    repo = Repo(db_session)
    topic1 = repo.add_topic(name="T1", query="x")
    topic2 = repo.add_topic(name="T2", query="y")
    source = repo.add_source(type="rss", url="https://example.com/feed")

    old = dt.datetime(2026, 1, 1, 0, 0, 0)
    new = dt.datetime(2026, 2, 9, 0, 0, 0)

    # Item A: only old ignore -> should be removed (orphan)
    item_a = repo.add_item(_mk_item(source_id=source.id, url="https://a", created_at=old))
    repo.add_item_topic(
        ItemTopic(item_id=item_a.id, topic_id=topic1.id, decision="ignore", reason="", created_at=old)
    )

    # Item B: old ignore (topic1) + old digest (topic2) -> keep item and digest, remove ignore
    item_b = repo.add_item(_mk_item(source_id=source.id, url="https://b", created_at=old))
    repo.add_item_topic(
        ItemTopic(item_id=item_b.id, topic_id=topic1.id, decision="ignore", reason="", created_at=old)
    )
    repo.add_item_topic(
        ItemTopic(item_id=item_b.id, topic_id=topic2.id, decision="digest", reason="", created_at=old)
    )

    # Item C: new ignore -> keep
    item_c = repo.add_item(_mk_item(source_id=source.id, url="https://c", created_at=new))
    repo.add_item_topic(
        ItemTopic(item_id=item_c.id, topic_id=topic1.id, decision="ignore", reason="", created_at=new)
    )

    cutoff = dt.datetime(2026, 2, 1, 0, 0, 0)
    result = repo.prune_ignored(older_than=cutoff, delete_orphan_items=True, dry_run=False)
    assert result["item_topics_deleted"] == 2
    assert result["items_deleted"] == 1

    assert repo.get_item_by_canonical_url("https://a") is None
    assert repo.get_item_by_canonical_url("https://b") is not None
    assert repo.get_item_by_canonical_url("https://c") is not None


def test_prune_ignored_dry_run_does_not_delete(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="https://example.com/feed")

    old = dt.datetime(2026, 1, 1, 0, 0, 0)
    item = repo.add_item(_mk_item(source_id=source.id, url="https://a", created_at=old))
    repo.add_item_topic(ItemTopic(item_id=item.id, topic_id=topic.id, decision="ignore", reason="", created_at=old))

    cutoff = dt.datetime(2026, 2, 1, 0, 0, 0)
    result = repo.prune_ignored(older_than=cutoff, delete_orphan_items=True, dry_run=True)
    assert result["item_topics_deleted"] == 1
    assert result["items_deleted"] == 1

    assert repo.get_item_by_canonical_url("https://a") is not None
