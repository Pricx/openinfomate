from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

import tracker.service as service_mod


class FakeScheduler:
    def __init__(self):
        self.jobs: dict[str, dict] = {}

    def add_job(self, func, trigger=None, args=None, id=None, **kwargs):
        self.jobs[str(id)] = {
            "func": func,
            "trigger": trigger,
            "args": list(args or []),
            **kwargs,
        }

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def remove_job(self, job_id):
        self.jobs.pop(job_id, None)


@pytest.mark.asyncio
async def test_install_digest_scheduler_jobs_registers_digest_sync_and_curated_sync(monkeypatch):
    calls: list[str] = []

    async def fake_sync_digest_jobs(scheduler, make_session, settings, digest_cron_map, digest_sem):
        calls.append("digest")
        digest_cron_map["digest:1"] = "0 9 * * *"

    async def fake_sync_curated_job_from_digest_hours(scheduler, make_session, settings, curated_cron_map, digest_sem):
        calls.append("curated")
        curated_cron_map["digest:curated"] = "0 */2 * * *"

    monkeypatch.setattr(service_mod, "_sync_digest_jobs", fake_sync_digest_jobs)
    monkeypatch.setattr(service_mod, "_sync_curated_job_from_digest_hours", fake_sync_curated_job_from_digest_hours)

    scheduler = FakeScheduler()
    settings = SimpleNamespace(digest_scheduler_enabled=True, max_concurrent_digests=3)

    digest_sem = await service_mod._install_digest_scheduler_jobs(
        scheduler,
        make_session=lambda: None,
        settings=settings,
        misfire=120,
    )

    assert isinstance(digest_sem, asyncio.Semaphore)
    assert calls == ["digest", "curated"]
    assert "digest:sync" in scheduler.jobs
    assert "curated:sync" in scheduler.jobs
    assert scheduler.jobs["digest:sync"]["args"][3] == {"digest:1": "0 9 * * *"}
    assert scheduler.jobs["curated:sync"]["args"][3] == {"digest:curated": "0 */2 * * *"}


@pytest.mark.asyncio
async def test_install_digest_scheduler_jobs_skips_when_disabled():
    scheduler = FakeScheduler()
    settings = SimpleNamespace(digest_scheduler_enabled=False, max_concurrent_digests=2)

    digest_sem = await service_mod._install_digest_scheduler_jobs(
        scheduler,
        make_session=lambda: None,
        settings=settings,
        misfire=120,
    )

    assert isinstance(digest_sem, asyncio.Semaphore)
    assert scheduler.jobs == {}



def test_digest_scheduler_invariant_issues_catch_missing_progression_jobs():
    scheduler = FakeScheduler()
    scheduler.add_job(lambda: None, id="digest:sync")
    scheduler.add_job(lambda: None, id="curated:sync")

    issues = service_mod._digest_scheduler_invariant_issues(
        scheduler,
        digest_scheduler_enabled=True,
        enabled_topic_count=2,
    )

    assert any("digest:curated" in issue for issue in issues)
    assert any("per-topic digest progression jobs" in issue for issue in issues)
