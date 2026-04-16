from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Sequence
from urllib.parse import urlencode, urlsplit, urlunsplit

import httpx

from tracker.connectors.base import Connector, FetchedEntry
from tracker.connectors.errors import TemporaryFetchBlockError


_HOSTLIKE_RE = re.compile(r"^[A-Za-z0-9.-]+(?::\\d+)?(?:/.*)?$")
DEFAULT_SEARXNG_BACKEND_PROBE_QUERIES: tuple[str, ...] = (
    "python",
    "linux",
    "open source",
    "artificial intelligence",
)


@dataclass(frozen=True)
class SearxngPayloadState:
    result_count: int
    aux_payload_present: bool
    unresponsive_engines: tuple[tuple[str, str], ...]
    degraded: bool
    degraded_reason: str
    usable: bool


@dataclass(frozen=True)
class SearxngBackendProbeResult:
    healthy: bool
    success_count: int
    total_count: int
    failure_summary: str = ""


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


def _has_nonempty_searxng_payload(value: object) -> bool:
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        return bool(value)
    if isinstance(value, list):
        return bool(value)
    return False


def _normalize_unresponsive_engines(value: object) -> list[tuple[str, str]]:
    if not isinstance(value, list):
        return []
    normalized: list[tuple[str, str]] = []
    for item in value:
        name = ""
        reason = ""
        if isinstance(item, (list, tuple)):
            if item:
                name = str(item[0] or "").strip()
            if len(item) > 1:
                reason = str(item[1] or "").strip()
        elif isinstance(item, dict):
            name = str(item.get("name") or item.get("engine") or item.get("id") or "").strip()
            reason = str(item.get("reason") or item.get("status") or "").strip()
        if name:
            normalized.append((name, reason))
    return normalized


def summarize_searxng_payload(data: dict[str, object]) -> SearxngPayloadState:
    results = data.get("results")
    if isinstance(results, list):
        result_count = len(results)
    elif results:
        result_count = 1
    else:
        result_count = 0

    aux_payload_present = any(
        _has_nonempty_searxng_payload(data.get(key))
        for key in ("answers", "corrections", "infoboxes", "suggestions")
    )
    unresponsive_engines = tuple(_normalize_unresponsive_engines(data.get("unresponsive_engines")))
    degraded_reason = ""
    degraded = False
    if result_count == 0 and not aux_payload_present and unresponsive_engines:
        degraded = True
        details = ", ".join(
            f"{name}:{reason}" if reason else name for name, reason in unresponsive_engines[:6]
        )
        if len(unresponsive_engines) > 6:
            details = f"{details}, +{len(unresponsive_engines) - 6} more"
        degraded_reason = details or "all configured engines unavailable"
    return SearxngPayloadState(
        result_count=result_count,
        aux_payload_present=aux_payload_present,
        unresponsive_engines=unresponsive_engines,
        degraded=degraded,
        degraded_reason=degraded_reason,
        usable=(result_count > 0) or aux_payload_present,
    )


def _raise_for_degraded_searxng_response(*, url: str, data: dict[str, object]) -> None:
    state = summarize_searxng_payload(data)
    if not state.degraded:
        return
    raise TemporaryFetchBlockError(
        url=url,
        reason=f"searxng_upstream_unavailable: {state.degraded_reason}",
    )


async def probe_searxng_backend(
    *,
    base_url: str,
    timeout_seconds: float,
    queries: Sequence[str] = DEFAULT_SEARXNG_BACKEND_PROBE_QUERIES,
    min_successes: int = 2,
) -> SearxngBackendProbeResult:
    base = normalize_searxng_base_url(base_url).rstrip("/") or (base_url or "").rstrip("/")
    if not base:
        return SearxngBackendProbeResult(
            healthy=False,
            success_count=0,
            total_count=0,
            failure_summary="missing base_url",
        )

    probe_queries = tuple(str(q or "").strip() for q in queries if str(q or "").strip())
    if not probe_queries:
        return SearxngBackendProbeResult(
            healthy=False,
            success_count=0,
            total_count=0,
            failure_summary="missing probe queries",
        )

    timeout = max(1.5, min(8.0, float(timeout_seconds or 0.0) or 5.0))
    success_count = 0
    failures: list[str] = []
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        for query in probe_queries:
            probe_url = build_searxng_search_url(
                base_url=base,
                query=query[:120],
                time_range="year",
                results=1,
            )
            try:
                resp = await client.get(probe_url, headers={"User-Agent": "tracker/0.1"})
                status_code = getattr(resp, "status_code", None)
                if status_code is None:
                    try:
                        resp.raise_for_status()
                    except Exception:
                        failures.append(f"{query}: status ?")
                        continue
                elif int(status_code or 0) != 200:
                    failures.append(f"{query}: status {status_code}")
                    continue
                try:
                    data = resp.json()
                except Exception:
                    try:
                        data = json.loads(getattr(resp, "text", ""))
                    except Exception:
                        failures.append(f"{query}: invalid_json")
                        continue
                if not isinstance(data, dict):
                    failures.append(f"{query}: invalid_payload")
                    continue
                state = summarize_searxng_payload(data)
                if state.usable:
                    success_count += 1
                    continue
                failures.append(f"{query}: {state.degraded_reason or 'empty'}")
            except Exception as exc:
                failures.append(f"{query}: {type(exc).__name__}")

    total_count = len(probe_queries)
    threshold = min(total_count, max(1, int(min_successes or 0) or 1))
    failure_summary = "; ".join(failures[:3])
    if len(failures) > 3:
        failure_summary = (
            (failure_summary + "; ") if failure_summary else ""
        ) + f"+{len(failures) - 3} more"
    return SearxngBackendProbeResult(
        healthy=success_count >= threshold,
        success_count=success_count,
        total_count=total_count,
        failure_summary=failure_summary,
    )


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

        _raise_for_degraded_searxng_response(url=url, data=data)
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
