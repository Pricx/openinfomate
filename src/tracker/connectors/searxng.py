from __future__ import annotations

import json
import re
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from tracker.connectors.base import Connector, FetchedEntry


_HOSTLIKE_RE = re.compile(r"^[A-Za-z0-9.-]+(?::\\d+)?(?:/.*)?$")


def normalize_searxng_base_url(base_url: str) -> str:
    """
    Normalize a SearxNG base URL to an origin(+optional subpath) without `/search`.

    Operators commonly paste the full search endpoint (`.../search`); our callers expect the base.
    Examples:
    - http://127.0.0.1:8888/search -> http://127.0.0.1:8888
    - https://example.com/searxng/search -> https://example.com/searxng
    """
    raw = (base_url or "").strip()
    if not raw:
        return ""

    # Be tolerant when operators omit the scheme.
    if "://" not in raw and _HOSTLIKE_RE.match(raw):
        raw = f"http://{raw}"

    try:
        parts = urlsplit(raw)
    except Exception:
        return (base_url or "").strip().rstrip("/")

    if not parts.scheme or not parts.netloc:
        return (base_url or "").strip().rstrip("/")

    path = (parts.path or "").rstrip("/")
    # Operators/LLMs sometimes paste repeated endpoints like `/search/search`.
    # Strip all trailing `/search` segments to get the real base.
    while path.endswith("/search"):
        path = path[: -len("/search")].rstrip("/")

    return urlunsplit((parts.scheme, parts.netloc, path, "", ""))


def normalize_searxng_search_url(url: str) -> str:
    """
    Normalize a full SearxNG search URL.

    Background:
    - Some historical configs mistakenly store URLs like `/search/search?...`.
    - The base-url normalizer is applied when *building* URLs, but old DB rows may persist.

    We fix the common case by collapsing repeated trailing `search` path segments.
    """
    raw = (url or "").strip()
    if not raw:
        return ""
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw
    if not parts.scheme or not parts.netloc:
        return raw
    segs = [s for s in (parts.path or "").split("/") if s]
    while len(segs) >= 2 and segs[-1] == "search" and segs[-2] == "search":
        segs.pop()
    path = "/" + "/".join(segs) if segs else ""
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query or "", parts.fragment or ""))


def build_searxng_search_url(
    *,
    base_url: str,
    query: str,
    categories: str | None = None,
    time_range: str | None = None,
    language: str | None = None,
    safesearch: int | None = None,
    results: int | None = None,
) -> str:
    base = normalize_searxng_base_url(base_url).rstrip("/") or (base_url or "").rstrip("/")
    params: dict[str, str] = {"q": query, "format": "json"}
    if categories:
        params["categories"] = categories
    if time_range:
        params["time_range"] = time_range
    if language:
        params["language"] = language
    if safesearch is not None:
        params["safesearch"] = str(safesearch)
    if results is not None:
        params["results"] = str(results)
    return f"{base}/search?{urlencode(params)}"


class SearxngConnector(Connector):
    type = "searxng_search"

    def __init__(self, *, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds

    async def fetch(self, *, url: str) -> list[FetchedEntry]:
        url = normalize_searxng_search_url(url) or url
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = await client.get(url, headers={"User-Agent": "tracker/0.1"})
            resp.raise_for_status()
            data = json.loads(resp.text)

        results = data.get("results") or []
        entries: list[FetchedEntry] = []
        for r in results:
            link = (r.get("url") or "").strip()
            title = (r.get("title") or "").strip()
            summary = r.get("content")
            published = r.get("publishedDate") or r.get("published_date")
            if link:
                entries.append(
                    FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary)
                )
        return entries
