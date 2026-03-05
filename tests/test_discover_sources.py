from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from tracker.connectors.base import FetchedEntry
from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_discover_sources
from tracker.settings import Settings


def test_run_discover_sources_creates_candidates(db_session, monkeypatch):
    html = Path(__file__).with_name("fixtures").joinpath("feed_discovery_sample.html").read_text(encoding="utf-8")

    async def fake_fetch_entries_for_source(*, source, timeout_seconds=20):  # type: ignore[no-untyped-def]
        assert source.type == "searxng_search"
        return [FetchedEntry(url="https://example.com/blog/", title="Blog")]

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            assert url == "https://example.com/blog/"
            return FakeResp(html)

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=x&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(discover_sources_max_results_per_topic=5)
    result1 = asyncio.run(run_discover_sources(session=db_session, settings=settings))
    assert result1.per_topic and result1.per_topic[0].candidates_created == 2

    stats1 = repo.get_stats()
    assert stats1["source_candidates_total"] == 2
    assert stats1["source_candidates_new"] == 2

    # Idempotent second run (no new rows, but seen_count increments).
    result2 = asyncio.run(run_discover_sources(session=db_session, settings=settings))
    assert result2.per_topic and result2.per_topic[0].candidates_created == 0

    stats2 = repo.get_stats()
    assert stats2["source_candidates_total"] == 2

    rows = repo.list_source_candidates(limit=10)
    assert {c.url for c, _t in rows} == {"https://example.com/atom.xml", "https://example.com/feed.xml"}
    assert all(c.seen_count == 2 for c, _t in rows)


def test_run_discover_sources_can_seed_from_recent_items(db_session, monkeypatch):
    html = Path(__file__).with_name("fixtures").joinpath("feed_discovery_sample.html").read_text(encoding="utf-8")

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            assert url == "https://example.com/blog/"
            return FakeResp(html)

    monkeypatch.setattr("tracker.runner.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/blog/",
        canonical_url="https://example.com/blog/",
        title="Seed",
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    settings = Settings(discover_sources_max_results_per_topic=5)
    result1 = asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))
    assert result1.per_topic and result1.per_topic[0].candidates_created == 2

    result2 = asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))
    assert result2.per_topic and result2.per_topic[0].candidates_created == 0

    rows = repo.list_source_candidates(topic=topic, limit=10)
    assert {c.url for c, _t in rows} == {"https://example.com/atom.xml", "https://example.com/feed.xml"}
    assert all(c.seen_count == 2 for c, _t in rows)


def test_run_discover_sources_uses_llm_fallback(db_session, monkeypatch):
    html = "<html><head><title>No feeds</title></head><body>hello</body></html>"

    async def fake_fetch_entries_for_source(*, source, timeout_seconds=20):  # type: ignore[no-untyped-def]
        assert source.type == "searxng_search"
        return [FetchedEntry(url="https://example.com/blog/", title="Blog")]

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            assert url == "https://example.com/blog/"
            return FakeResp(html)

    async def fake_llm_guess_feed_urls(  # type: ignore[no-untyped-def]
        *, settings, page_url: str, html_snippet: str, usage_cb=None
    ):
        assert page_url == "https://example.com/blog/"
        assert "No feeds" in html_snippet
        return ["https://example.com/feed.xml", "https://example.com/atom.xml"]

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("tracker.runner.llm_guess_feed_urls", fake_llm_guess_feed_urls)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=x&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(
        discover_sources_max_results_per_topic=5,
        discover_sources_ai_enabled=True,
        discover_sources_ai_max_pages_per_topic=1,
        llm_base_url="http://llm",
        llm_model="mirothinker",
    )
    result = asyncio.run(run_discover_sources(session=db_session, settings=settings))
    assert result.per_topic and result.per_topic[0].candidates_created == 2

    rows = repo.list_source_candidates(limit=10)
    assert {c.url for c, _t in rows} == {"https://example.com/atom.xml", "https://example.com/feed.xml"}


def test_run_discover_sources_can_derive_searx_base_from_bound_source(db_session, monkeypatch):
    """
    When `TRACKER_SEARXNG_BASE_URL` is not configured, discover-sources should still be
    able to use SearxNG by deriving the base URL from any bound searxng_search source.
    """

    async def fake_fetch_entries_for_source(*, source, timeout_seconds=20):  # type: ignore[no-untyped-def]
        # Force the "direct web search" fallback path (no seed pages from bound sources).
        return []

    class FakeResp:
        def __init__(self, *, text: str = "", headers: dict | None = None, json_data: dict | None = None):
            self.text = text
            self.headers = headers or {}
            self._json_data = json_data or {}

        def raise_for_status(self):
            return None

        def json(self):
            return self._json_data

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            if url.startswith("http://127.0.0.1:8888/search?"):
                return FakeResp(
                    json_data={
                        "results": [
                            {"url": "https://example.com/feed.xml", "title": "Feed", "content": "RSS feed"},
                        ]
                    }
                )
            assert url == "https://example.com/feed.xml"
            return FakeResp(
                text="<?xml version='1.0'?><rss><channel><title>x</title></channel></rss>",
                headers={"content-type": "application/xml"},
            )

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="agent memory systems")
    # Intentionally include a repeated `/search/search` in the stored URL (common operator/LLM mistake).
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search/search?q=x&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(discover_sources_max_results_per_topic=5, searxng_base_url="")
    result = asyncio.run(run_discover_sources(session=db_session, settings=settings, topic_ids=[topic.id]))
    assert result.per_topic and result.per_topic[0].candidates_created == 1

    rows = repo.list_source_candidates(topic=topic, limit=10)
    assert {c.url for c, _t in rows} == {"https://example.com/feed.xml"}
