from __future__ import annotations

import asyncio

from tracker.llm import llm_propose_topic_setup
from tracker.settings import Settings


def test_llm_propose_topic_setup_parses_and_normalizes(monkeypatch):
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
            assert json.get("model") == "gpt-5.2"
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{\n'
                                '  "topic_name": "AI Chips",\n'
                                '  "query_keywords": "GPU，HBM；光互连\\nASIC",\n'
                                '  "alert_keywords": "zero-day;CVE",\n'
                                '  "ai_prompt": "pick only signals"\n'
                                "}"
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="gpt-5.2")
    out = asyncio.run(
        llm_propose_topic_setup(settings=settings, topic_name="AI Chips", brief="track cutting-edge chips")
    )
    assert out is not None
    assert out.topic_name == "AI Chips"
    assert out.query_keywords == "GPU,HBM,光互连,ASIC"
    assert out.alert_keywords == "zero-day,CVE"
    assert out.ai_prompt == "pick only signals"


def test_llm_propose_topic_setup_falls_back_to_default_prompt(monkeypatch):
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
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"topic_name":"T","query_keywords":"a,b","alert_keywords":""}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="gpt-5.2")
    out = asyncio.run(llm_propose_topic_setup(settings=settings, topic_name="T", brief="B"))
    assert out is not None
    assert out.query_keywords == "a,b"
    assert out.ai_prompt
    assert "主题：T" in out.ai_prompt
    assert "关注点：B" in out.ai_prompt


def test_llm_propose_topic_setup_hints_nodeseek_for_china_dev(monkeypatch):
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
            return FakeResp(
                {
                    "choices": [
                        {
                            "message": {
                                "content": '{"topic_name":"China Dev社区","query_keywords":"AI,LLM,开源","alert_keywords":"","ai_prompt":"p"}'
                            }
                        }
                    ]
                }
            )

    monkeypatch.setattr("tracker.llm.httpx.AsyncClient", FakeClient)

    settings = Settings(llm_base_url="http://llm", llm_model="gpt-5.2")
    out = asyncio.run(
        llm_propose_topic_setup(
            settings=settings,
            topic_name="China Dev社区",
            brief="追踪国内/中文开发者社区与论坛里的最 cutting-edge 的进展",
        )
    )
    assert out is not None
    assert out.source_hints is None
