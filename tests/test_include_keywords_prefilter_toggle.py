from __future__ import annotations

import asyncio

from tracker.connectors.base import FetchedEntry
from tracker.connectors.rss import RssConnector
from tracker.models import ItemTopic
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_include_keywords_prefilter_disabled_allows_keywords_mode_digest(db_session, monkeypatch):
    async def fake_fetch_with_state(  # noqa: ANN001
        self, *, url: str, etag: str | None, last_modified: str | None, cookie_header: str | None = None
    ):
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="contains QUERY",
                summary="",
            )
        ], None

    monkeypatch.setattr(RssConnector, "fetch_with_state", fake_fetch_with_state)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="query")
    source = repo.add_source(type="rss", url="http://example.com/feed")
    ts = repo.bind_topic_source(topic=topic, source=source)
    ts.include_keywords = "mustmatch"  # does NOT appear in title/summary
    db_session.commit()

    settings = Settings(include_keywords_prefilter_enabled=False)
    asyncio.run(run_tick(session=db_session, settings=settings, push=False))

    it = db_session.query(ItemTopic).one()
    assert it.decision == "digest"
    assert "include_keywords" not in (it.reason or "")


def test_include_keywords_prefilter_enabled_keeps_legacy_hard_filter(db_session, monkeypatch):
    async def fake_fetch_with_state(  # noqa: ANN001
        self, *, url: str, etag: str | None, last_modified: str | None, cookie_header: str | None = None
    ):
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="contains QUERY",
                summary="",
            )
        ], None

    monkeypatch.setattr(RssConnector, "fetch_with_state", fake_fetch_with_state)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="query")
    source = repo.add_source(type="rss", url="http://example.com/feed")
    ts = repo.bind_topic_source(topic=topic, source=source)
    ts.include_keywords = "mustmatch"  # does NOT appear in title/summary
    db_session.commit()

    settings = Settings(include_keywords_prefilter_enabled=True)
    asyncio.run(run_tick(session=db_session, settings=settings, push=False))

    it = db_session.query(ItemTopic).one()
    assert it.decision == "ignore"
    assert "include_keywords" in (it.reason or "")
