from __future__ import annotations

import asyncio

from tracker.llm import llm_summarize_digest
from tracker.models import Topic
from tracker.settings import Settings


def test_llm_summarize_digest_uses_mini_model(monkeypatch):
    captured = {}

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

        async def post(self, url: str, headers: dict, json: dict):  # type: ignore[no-untyped-def]
            assert url.endswith("/v1/chat/completions")
            captured["payload"] = json
            assert json.get("model") == "gpt-5.2-mini"
            assert json.get("reasoning", {}).get("effort") == "high"
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": (
                                    "{"
                                    '"summary":"S",'
                                    '"highlights":["h"],'
                                    '"risks":["r"],'
                                    '"next_actions":["a"]'
                                    "}"
                                )
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(
        llm_base_url="http://llm",
        llm_model="gpt-5.2",
        llm_model_mini="gpt-5.2-mini",
        llm_extra_body_json='{"reasoning":{"effort":"high"}}',
        llm_digest_max_items=5,
    )
    topic = Topic(name="T", query="q", digest_cron="0 9 * * *", enabled=True)
    out = asyncio.run(
        llm_summarize_digest(
            settings=settings,
            topic=topic,
            policy_prompt="",
            since="2026-02-12T00:00:00",
            items=[{"title": "A", "url": "https://a", "published_at": "2026-02-12T00:00:00", "decision": "digest"}],
            previous_items=[],
            metrics={},
        )
    )
    assert out is not None
    assert out.summary == "S"
