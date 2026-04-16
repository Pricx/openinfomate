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


def test_ingest_refreshes_existing_item_when_refetch_has_richer_summary(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    ingest_entries_for_topic_source(
        session=db_session,
        topic=topic,
        source=source,
        entries=[FetchedEntry(url="https://example.com/a", title="Launch", summary="")],
        match_mode="llm",
    )

    item = repo.get_item_by_canonical_url("https://example.com/a")
    assert item is not None
    assert item.content_text == "Launch"

    created = ingest_entries_for_topic_source(
        session=db_session,
        topic=topic,
        source=source,
        entries=[
            FetchedEntry(
                url="https://example.com/a",
                title="Launch",
                summary="论坛一线反馈：新版本加入 accelerator rollback 开关，并给出 CLI 示例。",
            )
        ],
        match_mode="llm",
    )

    assert created == []
    refreshed = repo.get_item_by_canonical_url("https://example.com/a")
    assert refreshed is not None
    assert "accelerator rollback" in refreshed.content_text
    assert refreshed.content_text != "Launch"


def test_ingest_preserves_existing_richer_summary_when_refetch_is_weaker(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    ingest_entries_for_topic_source(
        session=db_session,
        topic=topic,
        source=source,
        entries=[
            FetchedEntry(
                url="https://example.com/a",
                title="Launch",
                summary="论坛一线反馈：新版本加入 accelerator rollback 开关，并给出 CLI 示例。",
            )
        ],
        match_mode="llm",
    )

    created = ingest_entries_for_topic_source(
        session=db_session,
        topic=topic,
        source=source,
        entries=[FetchedEntry(url="https://example.com/a", title="Launch", summary="")],
        match_mode="llm",
    )

    assert created == []
    item = repo.get_item_by_canonical_url("https://example.com/a")
    assert item is not None
    assert "accelerator rollback" in item.content_text
