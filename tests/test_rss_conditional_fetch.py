from __future__ import annotations

import asyncio

import httpx

from tracker.connectors.rss import RssConnector
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_rss_fetch_with_state_uses_conditional_headers(monkeypatch):
    seen_headers: list[dict[str, str]] = []

    async def fake_get(self, url, headers=None):  # type: ignore[no-untyped-def]
        seen_headers.append(headers or {})
        return httpx.Response(304, headers={"ETag": "\"abc\""})

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    entries, update = asyncio.run(
        RssConnector(timeout_seconds=1).fetch_with_state(
            url="https://example.com/feed.xml",
            etag="\"abc\"",
            last_modified="Tue, 10 Feb 2026 00:00:00 GMT",
        )
    )
    assert entries == []
    assert update is None
    assert seen_headers
    assert seen_headers[0].get("If-None-Match") == "\"abc\""
    assert "If-Modified-Since" in seen_headers[0]


def test_run_tick_updates_rss_etag_last_modified(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_with_state(  # type: ignore[no-untyped-def]
        self, *, url: str, etag: str | None, last_modified: str | None, cookie_header: str | None = None
    ):
        return [], {"etag": "\"new\"", "last_modified": "Wed, 11 Feb 2026 00:00:00 GMT"}

    monkeypatch.setattr("tracker.connectors.rss.RssConnector.fetch_with_state", fake_fetch_with_state)

    settings = Settings(rss_min_interval_seconds=0)
    asyncio.run(run_tick(session=db_session, settings=settings, push=False))

    src2 = repo.get_source_by_id(source.id)
    assert src2
    assert src2.etag == "\"new\""
    assert src2.last_modified == "Wed, 11 Feb 2026 00:00:00 GMT"


def test_rss_fetch_with_state_falls_back_from_export_arxiv_to_arxiv(monkeypatch):
    seen_urls: list[str] = []
    xml = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>arXiv cs.AI</title>
    <item>
      <title>Test Paper</title>
      <link>https://arxiv.org/abs/2604.99999</link>
      <pubDate>Sun, 12 Apr 2026 16:00:00 GMT</pubDate>
      <description>paper summary</description>
    </item>
  </channel>
</rss>
"""

    async def fake_get(self, url, headers=None):  # type: ignore[no-untyped-def]
        seen_urls.append(url)
        if "export.arxiv.org" in url:
            raise httpx.ConnectError("dns failed", request=httpx.Request("GET", url))
        return httpx.Response(
            200,
            text=xml,
            headers={"ETag": "\"etag-new\"", "Last-Modified": "Sun, 12 Apr 2026 16:00:00 GMT"},
            request=httpx.Request("GET", url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    entries, update = asyncio.run(
        RssConnector(timeout_seconds=1).fetch_with_state(
            url="https://export.arxiv.org/rss/cs.AI",
            etag=None,
            last_modified=None,
        )
    )

    assert seen_urls == [
        "https://export.arxiv.org/rss/cs.AI",
        "https://arxiv.org/rss/cs.AI",
    ]
    assert len(entries) == 1
    assert entries[0].url == "https://arxiv.org/abs/2604.99999"
    assert update == {"etag": "\"etag-new\"", "last_modified": "Sun, 12 Apr 2026 16:00:00 GMT"}
