from __future__ import annotations

from tracker.connectors.base import FetchedEntry
from tracker.pipeline import ingest_entries_for_topic_source
from tracker.repo import Repo


def test_ingest_uses_title_when_summary_missing_and_dedupes(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    entries = [
        FetchedEntry(url="https://example.com/a", title="Same Title", summary=""),
        FetchedEntry(url="https://example.com/b", title="Same Title", summary=""),
    ]

    created = ingest_entries_for_topic_source(
        session=db_session,
        topic=topic,
        source=source,
        entries=entries,
        match_mode="llm",
    )
    assert len(created) == 1

    item = repo.get_item_by_canonical_url("https://example.com/a")
    assert item is not None
    assert item.content_text == "Same Title"

