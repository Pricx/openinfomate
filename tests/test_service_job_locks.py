from __future__ import annotations

import asyncio
import datetime as dt
from contextlib import asynccontextmanager

import tracker.runner as runner_mod
from tracker.service import (
    _run_curated_job,
    _run_curated_recovery_queue_job,
    _run_curated_startup_catchup_job,
    _run_digest_job,
    _run_discover_sources_job,
    _run_tick_job,
)
from tracker.settings import Settings


class DummySession:
    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
        return False


def make_session():
    return DummySession()


def test_run_tick_job_uses_tick_lock(monkeypatch):
    seen: list[str] = []

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        seen.append(name)
        yield

    async def fake_run_tick(*, session, settings, push: bool):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr("tracker.service.run_tick", fake_run_tick)

    asyncio.run(_run_tick_job(make_session, Settings()))
    assert seen == ["svc.pipeline", "svc.tick"]



def test_run_digest_job_uses_topic_scoped_lock(monkeypatch):
    seen: list[str] = []

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        seen.append(name)
        yield

    async def fake_run_digest(*, session, settings, hours: int, push: bool, topic_ids):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr("tracker.service.run_digest", fake_run_digest)

    asyncio.run(_run_digest_job(make_session, Settings(), topic_id=7))
    assert seen == ["svc.digest.7"]



def test_run_curated_job_uses_curated_lock(monkeypatch):
    seen: list[str] = []
    calls: list[str] = []

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        seen.append(name)
        yield

    async def fake_run_tick(*, session, settings, push: bool):  # type: ignore[no-untyped-def]
        calls.append(f"tick:{push}")

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, now=None):  # type: ignore[no-untyped-def]  # noqa: ANN001, ARG001
        calls.append(f"curated:{hours}:{push}")
        return None

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr("tracker.service.run_tick", fake_run_tick)
    monkeypatch.setattr(runner_mod, "run_curated_info", fake_run_curated_info, raising=True)

    asyncio.run(_run_curated_job(make_session, Settings(), asyncio.Semaphore(1)))
    assert seen == ["svc.pipeline", "svc.curated"]
    assert calls == ["tick:True", "curated:2:True"]



def test_run_discover_sources_job_uses_discovery_lock(monkeypatch):
    seen: list[str] = []

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        seen.append(name)
        yield

    async def fake_run_discover_sources(*, session, settings, topic_ids=None):  # type: ignore[no-untyped-def]
        return None

    async def fake_notify(*args, **kwargs):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr("tracker.service.run_discover_sources", fake_run_discover_sources)
    monkeypatch.setattr("tracker.service._maybe_notify_source_candidates_batch", fake_notify)

    asyncio.run(_run_discover_sources_job(make_session, Settings()))
    assert seen == ["svc.discover_sources"]


def test_run_curated_recovery_queue_job_uses_curated_lock(monkeypatch):
    seen: list[str] = []

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        seen.append(name)
        yield

    class _RepoWithQueue:
        def __init__(self):
            self._status = ""

        def get_app_config(self, key: str) -> str:
            if key == "curated_recovery_queue_json":
                return '{"version":1,"queue":[{"window_end_utc":"2026-03-21T08:00:00Z","hours":2,"push":true,"pending_topic_ids":[1],"created_at":"2026-03-21T08:01:00Z","attempts":0,"last_error":"","last_attempt_at":""}]}'
            return self._status

        def set_app_config(self, key: str, value: str) -> None:
            self._status = value

        def delete_app_config(self, key: str) -> bool:  # noqa: ARG002
            return True

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, key_suffix: str | None = None, now=None, allow_recovery_enqueue: bool = True):  # noqa: ANN001, ARG001
        return runner_mod.CuratedInfoResult(since=now, pushed=0, markdown="# ok")

    repo_holder = _RepoWithQueue()

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr(runner_mod, "run_curated_info", fake_run_curated_info, raising=True)
    monkeypatch.setattr("tracker.service.Repo", lambda session: repo_holder)  # type: ignore[arg-type]

    asyncio.run(_run_curated_recovery_queue_job(make_session, Settings(curated_recovery_queue_enabled=True)))
    assert seen == ["svc.curated"]


def test_run_curated_startup_catchup_runs_missing_slot(monkeypatch):
    fire_time = dt.datetime(2026, 4, 9, 14, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    called: list[dt.datetime] = []
    seen_key: dict[str, str] = {}

    class _RepoNoPush:
        def any_push_sent(self, *, idempotency_key: str) -> bool:
            seen_key["value"] = idempotency_key
            return False

        def get_report_by_key(self, *, kind: str, idempotency_key: str):  # noqa: ANN001
            return None

    async def fake_run_curated_job_once(make_session, settings, digest_sem, *, now=None):  # noqa: ANN001, ARG001
        called.append(now)

    monkeypatch.setattr("tracker.service._last_fire_time_within", lambda **kwargs: fire_time)
    monkeypatch.setattr("tracker.service._run_curated_job_once", fake_run_curated_job_once)
    monkeypatch.setattr("tracker.service.Repo", lambda session: _RepoNoPush())  # type: ignore[arg-type]

    ran = asyncio.run(
        _run_curated_startup_catchup_job(
            make_session,
            Settings(digest_hours=2, digest_push_enabled=True, cron_timezone="Asia/Shanghai", cron_misfire_grace_seconds=21600),
            asyncio.Semaphore(1),
            now=fire_time + dt.timedelta(minutes=1),
        )
    )

    assert ran is True
    assert seen_key["value"] == "digest:0:2026-04-09:1400"
    assert called == [fire_time]


def test_run_curated_startup_catchup_skips_when_slot_already_sent(monkeypatch):
    fire_time = dt.datetime(2026, 4, 9, 14, 0, 0, tzinfo=dt.timezone(dt.timedelta(hours=8)))
    called: list[dt.datetime] = []

    class _RepoSent:
        def any_push_sent(self, *, idempotency_key: str) -> bool:
            return True

        def get_report_by_key(self, *, kind: str, idempotency_key: str):  # noqa: ANN001
            return object()

    async def fake_run_curated_job_once(make_session, settings, digest_sem, *, now=None):  # noqa: ANN001, ARG001
        called.append(now)

    monkeypatch.setattr("tracker.service._last_fire_time_within", lambda **kwargs: fire_time)
    monkeypatch.setattr("tracker.service._run_curated_job_once", fake_run_curated_job_once)
    monkeypatch.setattr("tracker.service.Repo", lambda session: _RepoSent())  # type: ignore[arg-type]

    ran = asyncio.run(
        _run_curated_startup_catchup_job(
            make_session,
            Settings(digest_hours=2, digest_push_enabled=True, cron_timezone="Asia/Shanghai", cron_misfire_grace_seconds=21600),
            asyncio.Semaphore(1),
            now=fire_time + dt.timedelta(minutes=1),
        )
    )

    assert ran is False
    assert called == []
