from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.connectors.searxng import SearxngConnector


def test_searxng_fetch_parses_results(monkeypatch):
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

    connector = SearxngConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="http://localhost:8888/search?q=ai+chips&format=json"))
    assert len(entries) == 2
    assert entries[0].url == "https://example.com/accel"
    assert entries[1].url.startswith("https://example.com/roadmap")

