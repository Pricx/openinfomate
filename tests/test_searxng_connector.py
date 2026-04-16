from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from tracker.connectors.errors import TemporaryFetchBlockError
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


def test_searxng_fetch_raises_on_degraded_backend(monkeypatch):
    payload = json.dumps(
        {
            "results": [],
            "answers": [],
            "corrections": [],
            "infoboxes": [],
            "suggestions": [],
            "unresponsive_engines": [["duckduckgo", "timeout"], ["brave", "timeout"]],
        }
    )

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
    with pytest.raises(TemporaryFetchBlockError, match="searxng_upstream_unavailable"):
        asyncio.run(connector.fetch(url="http://localhost:8888/search?q=openai&format=json"))


def test_searxng_fetch_allows_legitimate_empty_results(monkeypatch):
    payload = json.dumps(
        {
            "results": [],
            "answers": [],
            "corrections": [],
            "infoboxes": [],
            "suggestions": [],
            "unresponsive_engines": [],
        }
    )

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
    entries = asyncio.run(connector.fetch(url="http://localhost:8888/search?q=quiet&format=json"))
    assert entries == []
