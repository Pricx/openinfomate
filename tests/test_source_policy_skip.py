from __future__ import annotations

import asyncio
import datetime as dt

from tracker.models import SourceHealth
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_skips_by_min_interval(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="file:///tmp/does-not-matter.xml")
    source.last_checked_at = dt.datetime.utcnow()
    db_session.commit()
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(rss_min_interval_seconds=10_000)
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 0
    assert "min_interval" in (result.per_source[0].error or "")


def test_run_tick_skips_by_backoff(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="file:///tmp/does-not-matter.xml")
    repo.bind_topic_source(topic=topic, source=source)

    health = SourceHealth(source_id=source.id, next_fetch_at=dt.datetime.utcnow() + dt.timedelta(hours=1))
    db_session.add(health)
    db_session.commit()

    settings = Settings(rss_min_interval_seconds=0)
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 0
    assert "backoff" in (result.per_source[0].error or "")

