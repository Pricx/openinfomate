from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path

import tracker.runner as runner_mod
from tracker.curated_recovery_queue import (
    complete_curated_recovery_job,
    enqueue_curated_recovery_job,
    peek_curated_recovery_job,
    record_curated_recovery_attempt,
)
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import CuratedInfoResult
from tracker.service import _run_curated_recovery_queue_job
from tracker.settings import Settings


def test_curated_recovery_queue_roundtrip(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        assert enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-21T08:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[1, 2],
            last_error="timeout",
        )
        assert enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-21T08:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[2, 3],
            last_error="still timeout",
        )
        job = peek_curated_recovery_job(repo=repo)
        assert job is not None
        assert job.pending_topic_ids == [1, 2, 3]
        assert job.last_error == "still timeout"

        job2 = record_curated_recovery_attempt(repo=repo, job=job, error="retry failed", pending_topic_ids=[1, 3])
        assert job2.attempts == 1
        assert job2.pending_topic_ids == [1, 3]
        assert job2.last_error == "retry failed"

        assert complete_curated_recovery_job(repo=repo, job=job2) is True
        assert peek_curated_recovery_job(repo=repo) is None


def test_curated_recovery_worker_drains_queue_after_success(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-21T08:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[1, 2],
            last_error="timeout",
        )

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        yield

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, key_suffix: str | None = None, now=None, allow_recovery_enqueue: bool = True):  # noqa: ANN001, ARG001
        return CuratedInfoResult(since=now, pushed=1, markdown="# ok", idempotency_key="digest:0:test")

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr(runner_mod, "run_curated_info", fake_run_curated_info, raising=True)

    asyncio.run(_run_curated_recovery_queue_job(make_session, settings))

    with make_session() as session:
        repo = Repo(session)
        assert peek_curated_recovery_job(repo=repo) is None
        status = json.loads(repo.get_app_config("curated_recovery_last_json") or "{}")
    assert status["ok"] is True
    assert status["queued"] is False


def test_curated_recovery_worker_keeps_job_when_backlog_remains(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-21T08:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[1],
            last_error="timeout",
        )

    @asynccontextmanager
    async def fake_job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):  # noqa: ARG001
        yield

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, key_suffix: str | None = None, now=None, allow_recovery_enqueue: bool = True):  # noqa: ANN001, ARG001
        return CuratedInfoResult(
            since=now,
            pushed=0,
            markdown="# delayed",
            recovery_pending=True,
            pending_topic_ids=(1, 2),
        )

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr(runner_mod, "run_curated_info", fake_run_curated_info, raising=True)

    asyncio.run(_run_curated_recovery_queue_job(make_session, settings))

    with make_session() as session:
        repo = Repo(session)
        job = peek_curated_recovery_job(repo=repo)
        status = json.loads(repo.get_app_config("curated_recovery_last_json") or "{}")

    assert job is not None
    assert job.attempts == 1
    assert job.pending_topic_ids == [1, 2]
    assert status["ok"] is False
    assert status["queued"] is True
