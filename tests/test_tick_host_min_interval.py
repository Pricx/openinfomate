from __future__ import annotations

import asyncio

from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_enforces_host_min_interval(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int):
        return []

    sleeps: list[float] = []

    async def fake_sleep(seconds: float):
        sleeps.append(float(seconds))
        return None

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.time.monotonic", lambda: 0.0)
    monkeypatch.setattr("tracker.runner.asyncio.sleep", fake_sleep)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    s1 = repo.add_source(type="rss", url="http://example.com/feed1")
    s2 = repo.add_source(type="rss", url="http://example.com/feed2")
    repo.bind_topic_source(topic=topic, source=s1)
    repo.bind_topic_source(topic=topic, source=s2)

    settings = Settings(host_min_interval_seconds=10)
    asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert sleeps == [10.0]

