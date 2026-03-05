from __future__ import annotations

import json
from urllib.parse import urlencode

import httpx

from tracker.connectors.base import Connector, FetchedEntry


def build_hn_search_url(*, query: str, tags: str = "story", hits_per_page: int = 50) -> str:
    params = {"query": query, "tags": tags, "hitsPerPage": str(hits_per_page)}
    return f"https://hn.algolia.com/api/v1/search_by_date?{urlencode(params)}"


def _hn_item_url(object_id: str | int | None) -> str:
    return f"https://news.ycombinator.com/item?id={object_id}"


class HnAlgoliaConnector(Connector):
    type = "hn_search"

    def __init__(self, *, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds

    async def fetch(self, *, url: str) -> list[FetchedEntry]:
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "tracker/0.1"})
            resp.raise_for_status()
            data = json.loads(resp.text)

        hits = data.get("hits") or []
        entries: list[FetchedEntry] = []
        for hit in hits:
            title = (hit.get("title") or hit.get("story_title") or "").strip()
            object_id = hit.get("objectID")
            link = hit.get("url") or _hn_item_url(object_id)
            published = hit.get("created_at")
            summary = hit.get("story_text") or hit.get("comment_text")
            if link:
                entries.append(
                    FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary)
                )
        return entries

