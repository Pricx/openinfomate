from __future__ import annotations

import asyncio

from tracker.connectors.base import FetchedEntry
from tracker.repo import Repo
from tracker.runner import run_discover_sources
from tracker.settings import Settings


def test_run_discover_sources_derives_github_releases_feed(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*, source, timeout_seconds=20):  # type: ignore[no-untyped-def]
        assert source.type == "searxng_search"
        return [FetchedEntry(url="https://github.com/acme/widgets", title="Repo")]

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
            assert url == "https://github.com/acme/widgets"
            # Simulate a page with no explicit feed links.
            return FakeResp("<html><head><title>Repo</title></head><body>ok</body></html>")

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=x&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(discover_sources_max_results_per_topic=5)
    result1 = asyncio.run(run_discover_sources(session=db_session, settings=settings))
    assert result1.per_topic and result1.per_topic[0].candidates_created == 1

    rows = repo.list_source_candidates(limit=10)
    assert {c.url for c, _t in rows} == {"https://github.com/acme/widgets/releases.atom"}

    # Idempotent second run (no new rows, but seen_count increments).
    result2 = asyncio.run(run_discover_sources(session=db_session, settings=settings))
    assert result2.per_topic and result2.per_topic[0].candidates_created == 0
    rows2 = repo.list_source_candidates(limit=10)
    assert all(c.seen_count == 2 for c, _t in rows2)

