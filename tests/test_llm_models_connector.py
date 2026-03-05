from __future__ import annotations

import asyncio

import httpx
import pytest

from tracker.connectors.llm_models import LlmModelsConnector


def test_llm_models_requires_api_key():
    with pytest.raises(RuntimeError):
        asyncio.run(LlmModelsConnector(timeout_seconds=1, api_key=None).fetch(url="http://127.0.0.1:8317"))


def test_llm_models_fetch_parses_ids_and_sends_auth(monkeypatch):
    seen: list[tuple[str, dict[str, str]]] = []

    async def fake_get(self, url, headers=None):  # type: ignore[no-untyped-def]
        seen.append((url, headers or {}))
        return httpx.Response(
            200,
            request=httpx.Request("GET", url),
            json={"data": [{"id": "gpt-5.3-codex-spark"}]},
        )

    monkeypatch.setattr(httpx.AsyncClient, "get", fake_get)

    entries = asyncio.run(LlmModelsConnector(timeout_seconds=1, api_key="k").fetch(url="http://127.0.0.1:8317/v1"))
    assert len(entries) == 1
    assert entries[0].url.endswith("/v1/models/gpt-5.3-codex-spark")
    assert entries[0].title
    assert seen
    assert seen[0][1].get("Authorization") == "Bearer k"
