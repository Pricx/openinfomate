from __future__ import annotations

import asyncio

from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_captures_source_errors(db_session, monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("fetch failed")

    # runner imports `fetch_entries_for_source` into its module scope; patch that symbol.
    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", boom)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="searxng_search", url="http://localhost:8888/search?q=x&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings()
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 0
    assert result.per_source[0].error
