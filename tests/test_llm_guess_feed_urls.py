from __future__ import annotations

import asyncio

from tracker.llm import llm_guess_feed_urls
from tracker.llm import llm_guess_api_endpoints
from tracker.settings import Settings


def test_llm_guess_feed_urls_parses_and_filters(monkeypatch):
    class FakeResp:
        def __init__(self, payload: dict):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url: str, headers: dict, json: dict):
            assert url.endswith("/v1/chat/completions")
            assert json.get("model") == "mirothinker"
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"feed_urls":["/feed.xml","https://example.com/comments/feed.xml","https://example.com/atom.xml"]}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="mirothinker")
    urls = asyncio.run(
        llm_guess_feed_urls(
            settings=settings,
            page_url="https://example.com/blog/",
            html_snippet="<html>...</html>",
        )
    )

    assert urls == ["https://example.com/feed.xml", "https://example.com/atom.xml"]


def test_llm_guess_api_endpoints_parses(monkeypatch):
    class FakeResp:
        def __init__(self, payload: dict):
            self._payload = payload

        def raise_for_status(self):
            return None

        def json(self):
            return self._payload

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def post(self, url: str, headers: dict, json: dict):
            assert url.endswith("/v1/chat/completions")
            assert json.get("model") == "mirothinker"
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"api_endpoints":["/api/posts.json","https://example.com/graphql"]}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="mirothinker")
    urls = asyncio.run(
        llm_guess_api_endpoints(
            settings=settings,
            page_url="https://example.com/blog/",
            html_snippet="<html>...</html>",
        )
    )

    assert urls == ["https://example.com/api/posts.json", "https://example.com/graphql"]
