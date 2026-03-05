from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_ingests_searxng_search(db_session, monkeypatch):
    payload = Path(__file__).with_name("fixtures").joinpath("searxng_search.json").read_text(encoding="utf-8")

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            return FakeResp(payload)

    monkeypatch.setattr("tracker.connectors.searxng.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai chips")
    source = repo.add_source(type="searxng_search", url="http://localhost:8888/search?q=ai+chips&format=json")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings()
    result = asyncio.run(run_tick(session=db_session, settings=settings, push=False))
    assert result.total_created == 2

