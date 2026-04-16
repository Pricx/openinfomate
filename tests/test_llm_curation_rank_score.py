from __future__ import annotations

import asyncio

from tracker.llm import llm_curate_topic_items
from tracker.models import Topic
from tracker.settings import Settings


def test_llm_curate_topic_items_parses_rank_score(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": (
                                '{"decisions":['
                                '{"item_id":1,"decision":"digest","rank_score":78,"why":"e1","summary":"s1"},'
                                '{"item_id":2,"decision":"alert","rank_score":96,"why":"e2","summary":"s2"}'
                                "]}"
                            )
                        }
                    }
                ]
            }

    class FakeClient:
        def __init__(self, *args, **kwargs):  # noqa: ANN002, ANN003
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        async def post(self, url, headers=None, json=None):  # noqa: ANN001
            return FakeResponse()

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="gpt-5.2")
    topic = Topic(name="T", query="ai", digest_cron="0 9 * * *")

    decisions = asyncio.run(
        llm_curate_topic_items(
            settings=settings,
            topic=topic,
            policy_prompt="",
            candidates=[
                {"item_id": 1, "title": "A", "url": "https://example.com/a", "snippet": "a"},
                {"item_id": 2, "title": "B", "url": "https://example.com/b", "snippet": "b"},
            ],
            max_digest=0,
            max_alert=0,
        )
    )

    assert decisions is not None
    assert [d.rank_score for d in decisions] == [78, 96]
