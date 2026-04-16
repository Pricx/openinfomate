from __future__ import annotations

import asyncio
import datetime as dt

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from tracker.models import Base, Topic
from tracker.service import _sync_collect_jobs, _sync_digest_jobs
from tracker.settings import Settings


def test_sync_digest_jobs_uses_utc_timezone(monkeypatch):
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(Topic(name="T", query="x", digest_cron="0 9 * * *", enabled=True))
        session.commit()

    def make_session():
        return Session(engine)

    tz_seen: list[dt.tzinfo | None] = []
    orig = CronTrigger.from_crontab

    def wrapped(expr: str, timezone=None):  # type: ignore[no-untyped-def]
        tz_seen.append(timezone)
        return orig(expr, timezone=timezone)

    monkeypatch.setattr("tracker.service.CronTrigger.from_crontab", wrapped)

    scheduler = AsyncIOScheduler(timezone=dt.timezone.utc)
    settings = Settings()
    digest_cron_map: dict[str, str] = {}
    digest_sem = asyncio.Semaphore(1)

    asyncio.run(_sync_digest_jobs(scheduler, make_session, settings, digest_cron_map, digest_sem))
    assert tz_seen and tz_seen[0] == dt.timezone(dt.timedelta(hours=8))


def test_sync_digest_jobs_uses_configured_timezone(monkeypatch):
    from zoneinfo import ZoneInfo

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        session.add(Topic(name="T", query="x", digest_cron="0 9 * * *", enabled=True))
        session.commit()

    def make_session():
        return Session(engine)

    tz_seen: list[dt.tzinfo | None] = []
    orig = CronTrigger.from_crontab

    def wrapped(expr: str, timezone=None):  # type: ignore[no-untyped-def]
        tz_seen.append(timezone)
        return orig(expr, timezone=timezone)

    monkeypatch.setattr("tracker.service.CronTrigger.from_crontab", wrapped)

    scheduler = AsyncIOScheduler(timezone=dt.timezone.utc)
    settings = Settings(cron_timezone="Asia/Shanghai")
    digest_cron_map: dict[str, str] = {}
    digest_sem = asyncio.Semaphore(1)

    asyncio.run(_sync_digest_jobs(scheduler, make_session, settings, digest_cron_map, digest_sem))
    assert tz_seen and isinstance(tz_seen[0], ZoneInfo)
    assert getattr(tz_seen[0], "key", None) == "Asia/Shanghai"


def test_sync_collect_jobs_merges_same_cron_rules():
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)

    def make_session():
        return Session(engine)

    scheduler = AsyncIOScheduler(timezone=dt.timezone.utc)
    settings = Settings(
        collect_message_rules_json=(
            '[{"id":"arxiv","name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[123]},'
            '{"id":"papers","name":"Papers","cron":"0 19 * * *","lookback_hours":24,"source_ids":[124]}]'
        )
    )
    collect_cron_map: dict[str, str] = {}
    digest_sem = asyncio.Semaphore(1)

    asyncio.run(_sync_collect_jobs(scheduler, make_session, settings, collect_cron_map, digest_sem))

    collect_jobs = [job for job in scheduler.get_jobs() if str(getattr(job, "id", "")).startswith("collect:")]
    assert len(collect_jobs) == 1
    assert str(collect_jobs[0].id).startswith("collect:batch-")
