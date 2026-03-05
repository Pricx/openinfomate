from __future__ import annotations

import asyncio

from tracker.service import _run_digest_job
from tracker.settings import Settings


def test_run_digest_job_respects_digest_push_enabled(monkeypatch):
    calls: list[bool] = []

    async def fake_run_digest(*, session, settings, hours: int, push: bool, topic_ids):  # type: ignore[no-untyped-def]
        calls.append(bool(push))

    class DummySession:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, exc_type, exc, tb):  # type: ignore[no-untyped-def]
            return False

    def make_session():
        return DummySession()

    monkeypatch.setattr("tracker.service.run_digest", fake_run_digest)

    settings = Settings(digest_push_enabled=False)
    asyncio.run(_run_digest_job(make_session, settings, topic_id=1))
    assert calls == [False]

