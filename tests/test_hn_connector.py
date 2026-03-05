from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.connectors.hn_algolia import HnAlgoliaConnector


def test_hn_algolia_fetch_parses_hits(monkeypatch):
    payload = Path(__file__).with_name("fixtures").joinpath("hn_search.json").read_text(encoding="utf-8")

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

    monkeypatch.setattr("tracker.connectors.hn_algolia.httpx.AsyncClient", FakeClient)

    connector = HnAlgoliaConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://hn.algolia.com/api/v1/search_by_date?query=gpu&tags=story"))

    assert len(entries) == 2
    assert entries[0].url == "https://example.com/gpu"
    assert entries[0].title == "Breaking: New GPU architecture"
    assert entries[0].published_at_iso.startswith("2026-02-10")

    # When `url` is null, use the HN item URL.
    assert entries[1].url == "https://news.ycombinator.com/item?id=789012"
    assert "Ask HN" in entries[1].title

