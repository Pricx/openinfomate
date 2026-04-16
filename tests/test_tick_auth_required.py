from __future__ import annotations

import asyncio

from tracker.connectors.errors import TemporaryFetchBlockError
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_tick_auth_required_disables_source(db_session, monkeypatch):
    class FakeResp:
        def __init__(self):
            self.text = ""
            self.status_code = 401
            self.headers = {}

        def raise_for_status(self):
            raise RuntimeError("401 Unauthorized")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            _ = headers
            return FakeResp()

    monkeypatch.setattr("tracker.connectors.rss.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="https://private.example.com/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings()
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.per_source
    assert result.per_source[0].error == "auth_required"

    src = repo.get_source_by_id(source.id)
    assert src is not None and bool(src.enabled) is False

    health = repo.get_source_health(source_id=source.id)
    assert health is not None
    assert health.error_count == 0
    assert health.next_fetch_at is not None

    seen = (repo.get_app_config("auth_cookie_domains_seen") or "").strip()
    assert "private.example.com" in seen


def test_tick_temporary_block_keeps_source_enabled(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*_args, **_kwargs):
        raise TemporaryFetchBlockError(
            url="https://linux.do/latest.json",
            status_code=429,
            retry_after_seconds=1800,
            reason="rate_limited",
        )

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="discourse", url="https://linux.do/latest.json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(discourse_min_interval_seconds=0, source_backoff_max_seconds=7200)
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.per_source
    assert result.per_source[0].error == "temporary_block"

    src = repo.get_source_by_id(source.id)
    assert src is not None and bool(src.enabled) is True

    health = repo.get_source_health(source_id=source.id)
    assert health is not None
    assert health.next_fetch_at is not None
    assert int(health.error_count or 0) >= 1
