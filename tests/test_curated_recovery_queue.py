from __future__ import annotations

import asyncio
import datetime as dt
import json
from contextlib import asynccontextmanager
from pathlib import Path

import tracker.runner as runner_mod
from tracker.curated_recovery_queue import (
    complete_curated_recovery_job,
    complete_curated_recovery_job_for_window,
    enqueue_curated_recovery_job,
    peek_curated_recovery_job,
    record_curated_recovery_attempt,
)
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import CuratedInfoResult, run_curated_info
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
        curated_recovery_queue_enabled=True,
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
        curated_recovery_queue_enabled=True,
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
        curated_recovery_queue_enabled=True,
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


def test_curated_recovery_worker_uses_bounded_fulltext_budget(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=6,
        fulltext_timeout_seconds=25,
        llm_curation_max_candidates=0,
        curated_recovery_queue_enabled=True,
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

    captured: dict[str, int] = {}

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, key_suffix: str | None = None, now=None, allow_recovery_enqueue: bool = True):  # noqa: ANN001, ARG001
        captured["fulltext_max_fetches_per_topic"] = int(settings.fulltext_max_fetches_per_topic)
        captured["fulltext_timeout_seconds"] = int(settings.fulltext_timeout_seconds)
        captured["llm_curation_max_candidates"] = int(settings.llm_curation_max_candidates)
        return CuratedInfoResult(since=now, pushed=1, markdown="# ok", idempotency_key="digest:0:test")

    monkeypatch.setattr("tracker.service.job_lock_async", fake_job_lock_async)
    monkeypatch.setattr(runner_mod, "run_curated_info", fake_run_curated_info, raising=True)

    asyncio.run(_run_curated_recovery_queue_job(make_session, settings))

    assert captured == {
        "fulltext_max_fetches_per_topic": 1,
        "fulltext_timeout_seconds": 5,
        "llm_curation_max_candidates": 20,
    }


def test_complete_curated_recovery_job_for_window_roundtrip(tmp_path):
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
        topic = repo.add_topic(name="T", query="ai")
        assert enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-22T06:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[int(topic.id)],
            last_error="timeout",
        )
        assert complete_curated_recovery_job_for_window(
            repo=repo,
            window_end_utc="2026-03-22T06:00:00Z",
            hours=2,
            push=True,
        )
        assert peek_curated_recovery_job(repo=repo) is None


def test_run_curated_info_clears_matching_recovery_job_after_success(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        digest_push_empty=False,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai")
        assert enqueue_curated_recovery_job(
            repo=repo,
            window_end_utc="2026-03-22T06:00:00Z",
            hours=2,
            push=True,
            pending_topic_ids=[int(topic.id)],
            last_error="pending backlog remained after auto-repair",
        )
        assert peek_curated_recovery_job(repo=repo) is not None

    async def _run() -> None:
        with make_session() as session:
            await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                now=dt.datetime.fromisoformat("2026-03-22T14:00:00+08:00"),
                allow_auto_repair=False,
                allow_recovery_enqueue=False,
            )

    asyncio.run(_run())

    with make_session() as session:
        repo = Repo(session)
        assert peek_curated_recovery_job(repo=repo) is None
