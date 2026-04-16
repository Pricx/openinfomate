from __future__ import annotations

import asyncio
import datetime as dt
from urllib.parse import parse_qs, urlsplit

from tracker.connectors.errors import TemporaryFetchBlockError
from tracker.repo import Repo
from tracker.runner import (
    _SEARXNG_BACKEND_BLOCK_REASON_KEY,
    _SEARXNG_BACKEND_BLOCK_UNTIL_KEY,
    run_tick,
)
from tracker.settings import Settings


def test_tick_opens_searxng_backend_circuit_on_temporary_block(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*_args, **_kwargs):
        raise TemporaryFetchBlockError(
            url="http://127.0.0.1:8888/search?q=linux&format=json",
            reason="searxng_upstream_unavailable: duckduckgo:CAPTCHA",
        )

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="linux")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=linux&format=json")
    repo.bind_topic_source(topic=topic, source=source)
    repo.set_app_config_many({"searxng_search_repair_last_at_utc": dt.datetime.utcnow().isoformat() + "Z"})

    result = asyncio.run(
        run_tick(
            session=db_session,
            settings=Settings(searxng_base_url="", searxng_min_interval_seconds=0),
            push=False,
        )
    )
    assert result.per_source and result.per_source[0].error == "temporary_block"

    block_until = repo.get_app_config(_SEARXNG_BACKEND_BLOCK_UNTIL_KEY)
    assert block_until
    assert "searxng_upstream_unavailable" in (repo.get_app_config(_SEARXNG_BACKEND_BLOCK_REASON_KEY) or "")

    health = repo.get_source_health(source_id=source.id)
    assert health is not None
    assert int(health.error_count or 0) == 0
    assert (health.last_error or "") == ""
    assert health.next_fetch_at is None


def test_tick_skips_searxng_sources_while_backend_circuit_active(db_session, monkeypatch):
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("searxng fetch should be skipped while backend circuit is active")

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fail_if_called)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="linux")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=linux&format=json")
    repo.bind_topic_source(topic=topic, source=source)
    repo.set_app_config_many(
        {
            _SEARXNG_BACKEND_BLOCK_UNTIL_KEY: (dt.datetime.utcnow() + dt.timedelta(minutes=15)).isoformat() + "Z",
            _SEARXNG_BACKEND_BLOCK_REASON_KEY: "probe degraded",
            "searxng_search_repair_last_at_utc": dt.datetime.utcnow().isoformat() + "Z",
        }
    )

    result = asyncio.run(
        run_tick(
            session=db_session,
            settings=Settings(searxng_base_url="http://127.0.0.1:8888", searxng_min_interval_seconds=0),
            push=False,
        )
    )
    assert result.per_source
    assert result.per_source[0].error.startswith("skipped: searxng backend degraded until ")


def test_tick_clears_searxng_tempblock_health_after_probe_recovery(db_session, monkeypatch):
    payload_by_query = {
        "python": {"results": [{"url": "https://example.com/python", "title": "Python"}]},
        "linux": {"results": [{"url": "https://example.com/linux", "title": "Linux"}]},
        "open source": {"results": [], "unresponsive_engines": [["duckduckgo", "timeout"]]},
        "artificial intelligence": {"results": [], "unresponsive_engines": [["duckduckgo", "timeout"]]},
    }

    class FakeResp:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload
            self.status_code = 200

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            _ = headers
            query = parse_qs(urlsplit(url).query).get("q", [""])[0].replace("+", " ")
            return FakeResp(payload_by_query.get(query, {"results": []}))

    async def fake_fetch_entries_for_source(*_args, **_kwargs):
        return []

    monkeypatch.setattr("tracker.connectors.searxng.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="linux")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=linux&format=json")
    repo.bind_topic_source(topic=topic, source=source)
    health = repo.get_or_create_source_health(source_id=source.id)
    health.error_count = 9
    health.last_error = "temporary_block"
    health.last_error_at = dt.datetime.utcnow()
    health.next_fetch_at = dt.datetime.utcnow() + dt.timedelta(minutes=30)
    db_session.commit()

    repo.set_app_config_many(
        {
            _SEARXNG_BACKEND_BLOCK_UNTIL_KEY: (dt.datetime.utcnow() - dt.timedelta(minutes=1)).isoformat() + "Z",
            _SEARXNG_BACKEND_BLOCK_REASON_KEY: "probe degraded",
            "searxng_search_repair_last_at_utc": "",
        }
    )

    asyncio.run(
        run_tick(
            session=db_session,
            settings=Settings(searxng_base_url="http://127.0.0.1:8888", searxng_min_interval_seconds=0),
            push=False,
        )
    )

    health = repo.get_source_health(source_id=source.id)
    assert health is not None
    assert int(health.error_count or 0) == 0
    assert (health.last_error or "") == ""
    assert health.last_error_at is None
    assert health.next_fetch_at is None
    assert (repo.get_app_config(_SEARXNG_BACKEND_BLOCK_UNTIL_KEY) or "").strip() == ""
    assert (repo.get_app_config(_SEARXNG_BACKEND_BLOCK_REASON_KEY) or "").strip() == ""


def test_tick_derives_searx_base_from_existing_source_when_setting_missing(db_session, monkeypatch):
    payload_by_query = {
        "python": {"results": [{"url": "https://example.com/python", "title": "Python"}]},
        "linux": {"results": [{"url": "https://example.com/linux", "title": "Linux"}]},
        "open source": {"results": [], "unresponsive_engines": [["duckduckgo", "timeout"]]},
        "artificial intelligence": {"results": [], "unresponsive_engines": [["duckduckgo", "timeout"]]},
    }

    class FakeResp:
        def __init__(self, payload: dict[str, object]):
            self._payload = payload
            self.status_code = 200

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            _ = headers
            query = parse_qs(urlsplit(url).query).get("q", [""])[0].replace("+", " ")
            return FakeResp(payload_by_query.get(query, {"results": []}))

    async def fake_fetch_entries_for_source(*_args, **_kwargs):
        return []

    monkeypatch.setattr("tracker.connectors.searxng.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="linux")
    source = repo.add_source(type="searxng_search", url="http://127.0.0.1:8888/search?q=linux&format=json")
    repo.bind_topic_source(topic=topic, source=source)
    repo.set_app_config_many(
        {
            _SEARXNG_BACKEND_BLOCK_UNTIL_KEY: (dt.datetime.utcnow() - dt.timedelta(minutes=1)).isoformat() + "Z",
            _SEARXNG_BACKEND_BLOCK_REASON_KEY: "probe degraded",
            "searxng_search_repair_last_at_utc": "",
        }
    )

    asyncio.run(run_tick(session=db_session, settings=Settings(searxng_base_url=""), push=False))
    assert (repo.get_app_config(_SEARXNG_BACKEND_BLOCK_UNTIL_KEY) or "").strip() == ""
