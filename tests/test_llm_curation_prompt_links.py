from __future__ import annotations

import asyncio

from tracker.llm import llm_curate_topic_items
from tracker.models import Topic
from tracker.settings import Settings


def test_llm_curation_includes_notable_links_even_if_snippet_truncated(monkeypatch):
    captured: dict[str, object] = {}

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
            captured["messages"] = json.get("messages") or []
            return FakeResp({"choices": [{"message": {"content": '{"decisions":[{"item_id":1,"decision":"ignore","why":"","summary":""}]}'}}]})

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="gpt-5.2")
    topic = Topic(name="T", query="ai", digest_cron="0 9 * * *")

    long = "x" * 1300 + " https://github.com/foo/bar " + "y" * 20
    asyncio.run(
        llm_curate_topic_items(
            settings=settings,
            topic=topic,
            policy_prompt="",
            candidates=[
                {"item_id": 1, "title": "A", "url": "https://forum.example.com/t/topic/1", "snippet": long},
            ],
            max_digest=1,
            max_alert=0,
        )
    )

    msgs = captured.get("messages") or []
    assert isinstance(msgs, list) and len(msgs) >= 2
    user = msgs[1].get("content") if isinstance(msgs[1], dict) else ""
    assert isinstance(user, str)
    assert "links=https://github.com/foo/bar" in user
