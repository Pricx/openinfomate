from __future__ import annotations

import asyncio

import httpx
import pytest

import tracker.llm as llm_mod
from tracker.settings import Settings


def test_post_llm_json_retries_empty_text_response(monkeypatch):
    calls = {"count": 0}
    provider_results: list[dict] = []

    async def _fake_post_openai_compat_json(**_kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            return {"choices": [{"message": {"content": None}}]}, "chat_completions"
        return {"choices": [{"message": {"content": "pong"}}]}, "chat_completions"

    async def _fake_record_llm_provider_result(**kwargs):
        provider_results.append(kwargs)

    monkeypatch.setattr(llm_mod, "post_openai_compat_json", _fake_post_openai_compat_json, raising=True)
    monkeypatch.setattr(llm_mod, "_record_llm_provider_result", _fake_record_llm_provider_result, raising=True)

    settings = Settings(
        llm_retry_attempts=2,
        llm_retry_min_wait_seconds=0,
        llm_retry_max_wait_seconds=0,
    )

    async def _run():
        async with httpx.AsyncClient(timeout=1.0) as client:
            return await llm_mod._post_llm_json(
                repo=None,
                settings=settings,
                kind="reasoning",
                model="gpt-test",
                client=client,
                base_url="https://example.com/v1",
                headers={},
                payload_chat={"model": "gpt-test", "messages": [{"role": "user", "content": "ping"}]},
            )

    data, mode = asyncio.run(_run())
    assert mode == "chat_completions"
    assert data["choices"][0]["message"]["content"] == "pong"
    assert calls["count"] == 2
    assert len(provider_results) == 1
    assert provider_results[0]["ok"] is True


def test_post_llm_json_fails_after_semantic_retry_budget(monkeypatch):
    calls = {"count": 0}
    provider_results: list[dict] = []

    async def _fake_post_openai_compat_json(**_kwargs):
        calls["count"] += 1
        return {"choices": [{"message": {"content": None}}]}, "chat_completions"

    async def _fake_record_llm_provider_result(**kwargs):
        provider_results.append(kwargs)

    monkeypatch.setattr(llm_mod, "post_openai_compat_json", _fake_post_openai_compat_json, raising=True)
    monkeypatch.setattr(llm_mod, "_record_llm_provider_result", _fake_record_llm_provider_result, raising=True)

    settings = Settings(
        llm_retry_attempts=3,
        llm_retry_min_wait_seconds=0,
        llm_retry_max_wait_seconds=0,
    )

    async def _run():
        async with httpx.AsyncClient(timeout=1.0) as client:
            return await llm_mod._post_llm_json(
                repo=None,
                settings=settings,
                kind="reasoning",
                model="gpt-test",
                client=client,
                base_url="https://example.com/v1",
                headers={},
                payload_chat={"model": "gpt-test", "messages": [{"role": "user", "content": "ping"}]},
            )

    with pytest.raises(llm_mod.RetryableLlmResponseError, match="missing text"):
        asyncio.run(_run())
    assert calls["count"] == 3
    assert len(provider_results) == 1
    assert provider_results[0]["ok"] is False
    assert "missing text" in provider_results[0]["error_message"]
