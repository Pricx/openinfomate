from __future__ import annotations

import asyncio

from tracker.fulltext import fetch_fulltext_for_url


def test_fetch_fulltext_for_url_discourse_uses_topic_json(monkeypatch):
    calls: list[str] = []

    class FakeResp:
        status_code = 200
        headers = {"content-type": "application/json"}

        def __init__(self, url: str):
            self._url = url
            self.text = (
                '{"post_stream":{"posts":[{"cooked":"<p>Hello <b>World</b> '
                '<a href=\\\"https://github.com/foo/bar\\\">Repo</a></p>"}]}}'
            )

        def json(self):
            return {
                "post_stream": {
                    "posts": [{"cooked": "<p>Hello <b>World</b> <a href=\"https://github.com/foo/bar\">Repo</a></p>"}]
                }
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):  # type: ignore[no-untyped-def]
            calls.append(url)
            assert url == "https://forum.example.com/t/topic/1610459.json"
            return FakeResp(url)

    monkeypatch.setattr("tracker.fulltext.httpx.AsyncClient", FakeClient)

    out = asyncio.run(
        fetch_fulltext_for_url(
            url="https://forum.example.com/t/topic/1610459",
            timeout_seconds=5,
            max_chars=10_000,
        )
    )
    assert "Hello World" in out
    assert "https://github.com/foo/bar" in out
    assert calls == ["https://forum.example.com/t/topic/1610459.json"]


def test_fetch_fulltext_for_url_discourse_falls_back_to_rss_on_cf_challenge(monkeypatch):
    calls: list[str] = []

    class FakeResp:
        def __init__(self, *, status_code: int, headers: dict[str, str], text: str):
            self.status_code = status_code
            self.headers = headers
            self.text = text

        def json(self):
            raise ValueError("not json")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):  # type: ignore[no-untyped-def]
            calls.append(url)
            if url == "https://forum.example.com/t/topic/1610459.json":
                return FakeResp(
                    status_code=403,
                    headers={"cf-mitigated": "challenge", "content-type": "text/html; charset=UTF-8"},
                    text="<html>Just a moment...</html>",
                )
            if url == "https://forum.example.com/t/topic/1610459.rss":
                return FakeResp(
                    status_code=200,
                    headers={"content-type": "application/rss+xml; charset=utf-8"},
                    text=(
                        "<?xml version=\"1.0\"?><rss version=\"2.0\"><channel>"
                        "<description>Hello RSS fallback</description>"
                        "</channel></rss>"
                    ),
                )
            raise AssertionError(f"unexpected url: {url}")

    monkeypatch.setattr("tracker.fulltext.httpx.AsyncClient", FakeClient)

    out = asyncio.run(
        fetch_fulltext_for_url(
            url="https://forum.example.com/t/topic/1610459",
            timeout_seconds=5,
            max_chars=10_000,
        )
    )
    assert "Hello RSS fallback" in out
    assert calls == [
        "https://forum.example.com/t/topic/1610459.json",
        "https://forum.example.com/t/topic/1610459.rss",
    ]
