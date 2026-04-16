from __future__ import annotations

import asyncio
from pathlib import Path

import httpx

from tracker.connectors.discourse import DiscourseConnector
from tracker.connectors.errors import TemporaryFetchBlockError


def test_discourse_fetch_parses_topics(monkeypatch):
    # Ensure global Cloudflare challenge cache doesn't affect this test.
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )

    class FakeResp:
        def __init__(self, text: str):
            self.text = text
            self.status_code = 200
            self.headers = {}

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

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert len(entries) == 2
    assert entries[0].url == "https://forum.example.com/t/ai-chips-new-accelerator/111"
    assert entries[0].title.startswith("AI chips")
    assert "accelerator" in (entries[0].summary or "")


def test_discourse_fetch_merges_top_daily_when_requested(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    json_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )
    top_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_top.rss").read_text(encoding="utf-8")
    )

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            if "/top.rss" in url:
                return FakeResp(top_payload)
            return FakeResp(json_payload)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json", include_top_daily=True))
    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1615965" in urls


def test_discourse_fetch_merges_latest_rss_pages_when_stale_and_json_ok(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    json_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )

    page0 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""
    page1 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>B</title><link>https://forum.example.com/t/topic/1610998</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp(json_payload, status_code=200)
            if "/top.rss" in url:
                return FakeResp("<rss><channel></channel></rss>", status_code=200)
            if "page=1" in url:
                return FakeResp(page1, status_code=200)
            return FakeResp(page0, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1, rss_catchup_pages=2)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json", include_top_daily=True))

    assert any("/latest.rss" in u and "page=" not in u for u in seen_urls)
    assert any("/latest.rss" in u and "page=1" in u for u in seen_urls)

    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1610998" in urls


def test_discourse_fetch_merges_latest_rss_pages_when_configured_even_if_not_stale(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    json_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )

    page0 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""
    page1 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>B</title><link>https://forum.example.com/t/topic/1610998</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp(json_payload, status_code=200)
            if "page=1" in url:
                return FakeResp(page1, status_code=200)
            return FakeResp(page0, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1, rss_catchup_pages=2)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json", include_top_daily=False))

    assert any("/latest.rss" in u and "page=" not in u for u in seen_urls)
    assert any("/latest.rss" in u and "page=1" in u for u in seen_urls)
    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1610998" in urls


def test_discourse_fetch_falls_back_to_rss_on_cloudflare_challenge(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    latest_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.rss").read_text(encoding="utf-8")
    )
    new_payload = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>New</title>
    <item><title>Backstop</title><link>https://forum.example.com/t/topic/1615965</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            if url.endswith(".json"):
                return FakeResp(
                    "<html>challenge</html>",
                    status_code=403,
                    headers={"cf-mitigated": "challenge"},
                )
            if "/new.rss" in url:
                return FakeResp(new_payload, status_code=200)
            return FakeResp(latest_payload, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/test-topic/123" in urls
    assert "https://forum.example.com/t/topic/1615965" in urls
    first = next(e for e in entries if e.url == "https://forum.example.com/t/test-topic/123")
    assert (first.summary or "").strip() == "hello"


def test_discourse_fetch_falls_back_to_html_latest_when_json_blocked_and_rss_empty(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    monkeypatch.setattr("tracker.connectors.discourse._RSS_OPTIONAL_UNAVAILABLE_URLS", set())

    latest_html = """
<!DOCTYPE html>
<html>
  <body class="crawler">
    <div class="topic-list-container">
      <table class="topic-list">
        <tbody>
          <tr class="topic-list-item">
            <td class="main-link">
              <span class="link-top-line">
                <a class="title raw-link raw-topic-link" href="https://forum.example.com/t/topic/1911417">调整帖子最小长度</a>
              </span>
              <p class="excerpt">这是一个可抓取的 HTML fallback 摘要。</p>
            </td>
            <td>2026 年4 月 11 日</td>
          </tr>
        </tbody>
      </table>
    </div>
  </body>
</html>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int, headers: dict[str, str] | None = None, url: str | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}
            self.url = url or "https://forum.example.com/latest"

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp(
                    "<html>challenge</html>",
                    status_code=403,
                    headers={"cf-mitigated": "challenge"},
                    url=url,
                )
            if url.endswith("/latest.rss"):
                return FakeResp("", status_code=200, url=url)
            if url.endswith("/new.rss"):
                return FakeResp("<rss><channel></channel></rss>", status_code=200, url=url)
            if url.endswith("/latest"):
                return FakeResp(latest_html, status_code=200, url=url)
            return FakeResp("<rss><channel></channel></rss>", status_code=200, url=url)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))

    assert any(u.endswith("/latest") for u in seen_urls)
    assert any(u.endswith("/latest.rss") for u in seen_urls)
    assert entries
    assert entries[0].url == "https://forum.example.com/t/topic/1911417"
    assert entries[0].title == "调整帖子最小长度"
    assert "HTML fallback" in (entries[0].summary or "")


def test_discourse_fetch_uses_requests_fallback_when_httpx_connect_fails(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            raise httpx.ConnectError("boom")

    class FakeRequestsResponse:
        def __init__(self, text: str, *, status_code: int = 200, url: str):
            self.text = text
            self.status_code = status_code
            self.headers = {}
            self.url = url

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    def fake_requests_get(url: str, *, headers: dict, timeout: int, allow_redirects: bool):
        assert timeout == 1
        assert allow_redirects is True
        return FakeRequestsResponse(payload, status_code=200, url=url)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("tracker.connectors.discourse.requests.get", fake_requests_get)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))

    assert len(entries) == 2
    assert entries[0].url == "https://forum.example.com/t/ai-chips-new-accelerator/111"


def test_discourse_fetch_uses_requests_fallback_across_json_rss_and_html(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    monkeypatch.setattr("tracker.connectors.discourse._RSS_OPTIONAL_UNAVAILABLE_URLS", set())

    latest_html = """
<!DOCTYPE html>
<html>
  <body class="crawler">
    <table class="topic-list">
      <tbody>
        <tr class="topic-list-item">
          <td class="main-link">
            <a class="title raw-link raw-topic-link" href="https://forum.example.com/t/topic/1911417">HTML 通路恢复</a>
            <p class="excerpt">requests fallback 解析到这条记录。</p>
          </td>
        </tr>
      </tbody>
    </table>
  </body>
</html>
"""

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            raise httpx.ConnectError("boom")

    class FakeRequestsResponse:
        def __init__(self, text: str, *, status_code: int, url: str, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}
            self.url = url

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    def fake_requests_get(url: str, *, headers: dict, timeout: int, allow_redirects: bool):
        seen_urls.append(url)
        if url.endswith(".json"):
            return FakeRequestsResponse(
                "<html>challenge</html>",
                status_code=403,
                headers={"cf-mitigated": "challenge"},
                url=url,
            )
        if url.endswith("/latest.rss"):
            return FakeRequestsResponse("", status_code=200, url=url)
        if url.endswith("/new.rss"):
            return FakeRequestsResponse("<rss><channel></channel></rss>", status_code=200, url=url)
        if url.endswith("/latest"):
            return FakeRequestsResponse(latest_html, status_code=200, url=url)
        return FakeRequestsResponse("<rss><channel></channel></rss>", status_code=200, url=url)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)
    monkeypatch.setattr("tracker.connectors.discourse.requests.get", fake_requests_get)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))

    assert any(u.endswith("/latest.json") for u in seen_urls)
    assert any(u.endswith("/latest.rss") for u in seen_urls)
    assert any(u.endswith("/latest") for u in seen_urls)
    assert entries
    assert entries[0].title == "HTML 通路恢复"
    assert "requests fallback" in (entries[0].summary or "")


def test_discourse_skips_optional_rss_feed_after_404(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", {"forum.example.com"})
    monkeypatch.setattr("tracker.connectors.discourse._RSS_OPTIONAL_UNAVAILABLE_URLS", set())

    latest_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.rss").read_text(encoding="utf-8")
    )

    seen_urls: list[str] = []

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if "/new.rss" in url:
                return FakeResp("missing", status_code=404)
            return FakeResp(latest_payload, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries1 = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert entries1
    assert any("/new.rss" in u for u in seen_urls)

    seen_urls.clear()
    entries2 = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert entries2
    assert not any("/new.rss" in u for u in seen_urls)


def test_discourse_fetch_uses_cached_rss_after_challenge(monkeypatch):
    cache: set[str] = set()
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", cache)

    latest_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.rss").read_text(encoding="utf-8")
    )
    new_payload = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>New</title>
    <item><title>Backstop</title><link>https://forum.example.com/t/topic/1615965</link></item>
  </channel>
</rss>
"""

    seen_urls: list[str] = []

    class FakeResp:
        def __init__(self, text: str, *, status_code: int, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp(
                    "<html>challenge</html>",
                    status_code=403,
                    headers={"cf-mitigated": "challenge"},
                )
            if "/new.rss" in url:
                return FakeResp(new_payload, status_code=200)
            return FakeResp(latest_payload, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)

    # First call: hits JSON first, then RSS.
    entries1 = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert len(entries1) >= 1

    # Second call: should skip JSON due to cached netloc.
    seen_urls.clear()
    entries2 = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert len(entries2) >= 1
    assert any("/latest.rss" in u for u in seen_urls)
    assert any("/new.rss" in u for u in seen_urls)
    assert not any(u.endswith(".json") for u in seen_urls)


def test_discourse_fetch_rss_catchup_pages_when_stale(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())

    page0 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""
    page1 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>B</title><link>https://forum.example.com/t/topic/1610998</link></item>
  </channel>
</rss>
"""
    page2 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>C</title><link>https://forum.example.com/t/topic/1615965</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp("<html>challenge</html>", status_code=403, headers={"cf-mitigated": "challenge"})
            if "/top.rss" in url:
                return FakeResp("<rss><channel></channel></rss>", status_code=200)
            if "page=2" in url:
                return FakeResp(page2, status_code=200)
            if "page=1" in url:
                return FakeResp(page1, status_code=200)
            return FakeResp(page0, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1, rss_catchup_pages=3)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json", include_top_daily=True))

    assert any("/latest.rss" in u and "page=" not in u for u in seen_urls)
    assert any("/latest.rss" in u and "page=1" in u for u in seen_urls)
    assert any("/latest.rss" in u and "page=2" in u for u in seen_urls)

    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1610998" in urls


def test_discourse_rss_catchup_tolerates_transient_page_errors(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", {"forum.example.com"})

    page0 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""
    page2 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>Older</title><link>https://forum.example.com/t/topic/1717220</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if "page=1" in url:
                return FakeResp("unavailable", status_code=503)
            if "page=2" in url:
                return FakeResp(page2, status_code=200)
            return FakeResp(page0, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1, rss_catchup_pages=8)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))

    assert any("/latest.rss" in u and "page=2" in u for u in seen_urls)
    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1717220" in urls


def test_discourse_fetch_sends_cookie_header_when_configured(monkeypatch):
    cache: set[str] = {"forum.example.com"}
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", cache)

    latest_payload = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str):
            self.text = text
            self.status_code = 200
            self.headers = {}

        def raise_for_status(self):
            return None

    seen_cookies: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            if "Cookie" in headers:
                seen_cookies.append(str(headers.get("Cookie") or ""))
            return FakeResp(latest_payload)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1, cookie="a=b; cf_clearance=xyz")
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
    assert len(entries) == 1
    assert seen_cookies
    assert all(c == "a=b; cf_clearance=xyz" for c in seen_cookies)


def test_discourse_fetch_uses_multi_page_latest_and_new_rss_recall_by_default(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())
    json_payload = (
        Path(__file__).with_name("fixtures").joinpath("discourse_latest.json").read_text(encoding="utf-8")
    )

    page0 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>A</title><link>https://forum.example.com/t/test-topic/123</link></item>
  </channel>
</rss>
"""
    page1 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>B</title><link>https://forum.example.com/t/topic/1610998</link></item>
  </channel>
</rss>
"""
    page2 = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Latest</title>
    <item><title>C</title><link>https://forum.example.com/t/topic/1615965</link></item>
  </channel>
</rss>
"""
    new_payload = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>New</title>
    <item><title>D</title><link>https://forum.example.com/t/topic/1702035</link></item>
  </channel>
</rss>
"""

    class FakeResp:
        def __init__(self, text: str, *, status_code: int = 200, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    seen_urls: list[str] = []

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            seen_urls.append(url)
            if url.endswith(".json"):
                return FakeResp(json_payload, status_code=200)
            if "/new.rss" in url:
                return FakeResp(new_payload, status_code=200)
            if "page=2" in url:
                return FakeResp(page2, status_code=200)
            if "page=1" in url:
                return FakeResp(page1, status_code=200)
            return FakeResp(page0, status_code=200)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    entries = asyncio.run(connector.fetch(url="https://forum.example.com/latest.json", include_top_daily=False))

    assert any("/latest.rss" in u and "page=1" in u for u in seen_urls)
    assert any("/latest.rss" in u and "page=2" in u for u in seen_urls)
    assert any("/new.rss" in u for u in seen_urls)
    urls = {e.url for e in entries}
    assert "https://forum.example.com/t/topic/1610998" in urls
    assert "https://forum.example.com/t/topic/1615965" in urls
    assert "https://forum.example.com/t/topic/1702035" in urls


def test_discourse_fetch_raises_temporary_block_when_json_and_rss_are_both_blocked(monkeypatch):
    monkeypatch.setattr("tracker.connectors.discourse._CF_CHALLENGED_NETLOCS", set())

    class FakeResp:
        def __init__(self, text: str, *, status_code: int, headers: dict[str, str] | None = None):
            self.text = text
            self.status_code = status_code
            self.headers = headers or {}

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(f"HTTP {self.status_code}")
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            if url.endswith(".json"):
                return FakeResp("blocked", status_code=429, headers={"Retry-After": "120"})
            return FakeResp("blocked", status_code=429)

    monkeypatch.setattr("tracker.connectors.discourse.httpx.AsyncClient", FakeClient)

    connector = DiscourseConnector(timeout_seconds=1)
    try:
        asyncio.run(connector.fetch(url="https://forum.example.com/latest.json"))
        raise AssertionError("expected TemporaryFetchBlockError")
    except TemporaryFetchBlockError as exc:
        assert exc.status_code == 429
        assert exc.retry_after_seconds == 120
