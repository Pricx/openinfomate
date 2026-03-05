from __future__ import annotations

import datetime as dt
from urllib.parse import urlsplit

import httpx

from tracker.connectors.base import Connector, FetchedEntry


def _normalize_base_url(url: str) -> str:
    raw = (url or "").strip().rstrip("/")
    if not raw:
        return ""
    # Allow operators to pass either `http://host:port` or `http://host:port/v1`.
    if raw.endswith("/v1"):
        return raw[: -len("/v1")]
    return raw


class LlmModelsConnector(Connector):
    """
    Fetch the OpenAI-compatible `/v1/models` list from an LLM gateway.

    Use case: catch "new model available" events even when no external sources are tracked.
    """

    type = "llm_models"

    def __init__(self, *, timeout_seconds: int = 20, api_key: str | None = None):
        self.timeout_seconds = timeout_seconds
        self.api_key = (api_key or "").strip() or None

    async def fetch(self, *, url: str) -> list[FetchedEntry]:
        base = _normalize_base_url(url)
        if not base:
            return []
        if not self.api_key:
            raise RuntimeError("missing api key for llm_models (set TRACKER_LLM_API_KEY)")

        endpoint = base.rstrip("/") + "/v1/models"
        headers = {"Authorization": f"Bearer {self.api_key}", "User-Agent": "tracker/0.1"}

        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = await client.get(endpoint, headers=headers)
            resp.raise_for_status()
            data = resp.json()

        raw = data.get("data") if isinstance(data, dict) else None
        if not isinstance(raw, list):
            return []

        now = dt.datetime.utcnow().isoformat() + "Z"
        entries: list[FetchedEntry] = []
        for m in raw:
            if not isinstance(m, dict):
                continue
            mid = (m.get("id") or "").strip()
            if not mid:
                continue
            # Use a stable synthetic URL per model id (does not need to be reachable).
            model_url = base.rstrip("/") + "/v1/models/" + mid
            # Basic hygiene: ensure it's parseable by urlsplit.
            parts = urlsplit(model_url)
            if parts.scheme not in {"http", "https"}:
                continue
            title = f"LLM gateway model: {mid}"
            summary = f"model_id: {mid}"
            entries.append(FetchedEntry(url=model_url, title=title, published_at_iso=now, summary=summary))

        return entries

