from __future__ import annotations

import re
import warnings
from pathlib import Path
from urllib.parse import urlparse, urlsplit, urlunsplit

import feedparser
import httpx
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

from tracker.connectors.base import Connector, FetchedEntry
from tracker.http_auth import AuthRequiredError, looks_like_login_redirect


_LOCAL_HOSTS = {"localhost", "127.0.0.1", "0.0.0.0", "::1"}
_URL_RE = re.compile(r"https?://[^\s<>\")\]]+", re.IGNORECASE)


def _is_local_url(url: str) -> bool:
    u = (url or "").strip()
    if not u:
        return False
    try:
        host = (urlsplit(u).hostname or "").strip().lower()
    except Exception:
        host = ""
    return host in _LOCAL_HOSTS


def _rewrite_url_host(*, url: str, host: str, scheme: str | None = None) -> str:
    """
    Rewrite the host of an absolute URL, preserving path/query/fragment.

    This is used for feeds that publish placeholder hosts like https://localhost/stream/<id>.
    """
    raw = (url or "").strip()
    h = (host or "").strip()
    if not (raw and h):
        return raw
    try:
        parts = urlsplit(raw)
    except Exception:
        return raw
    sc = (scheme or parts.scheme or "https").strip().lower() or "https"
    if sc not in {"http", "https"}:
        sc = "https"
    return urlunsplit((sc, h, parts.path or "/", parts.query or "", parts.fragment or ""))


def _extract_http_urls_from_html(html: str) -> list[str]:
    """
    Extract http(s) URLs from an HTML-ish snippet, preferring <a href>.
    """
    s = (html or "").strip()
    if not s:
        return []

    out: list[str] = []
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
        soup = BeautifulSoup(s, "html.parser")
    for a in soup.find_all("a"):
        href = ""
        try:
            href = (a.get("href") or "").strip()
        except Exception:
            href = ""
        if href.lower().startswith(("http://", "https://")):
            out.append(href)

    # Also scan plain-text URLs (some feeds provide non-anchor snippets).
    out.extend(_URL_RE.findall(s))
    return out


def _resolve_entry_link(*, link: str, summary_html: str | None, feed_url: str) -> str:
    """
    Resolve broken/placeholder RSS entry links into a user-openable URL.

    Some feeds publish `entry.link` as https://localhost/stream/<id>, which is not
    user-openable. In those cases:
      - Prefer the first unique non-local http(s) URL found in the body.
      - Otherwise fall back to a public permalink by rewriting localhost → feed host.
    """
    raw_link = (link or "").strip()
    if not raw_link:
        return ""
    if not _is_local_url(raw_link):
        return raw_link

    feed_host = ""
    feed_scheme = ""
    try:
        feed_parts = urlsplit((feed_url or "").strip())
        feed_host = (feed_parts.hostname or "").strip()
        feed_scheme = (feed_parts.scheme or "").strip().lower()
    except Exception:
        feed_host = ""
        feed_scheme = ""

    public_permalink = raw_link
    if feed_host and feed_host.lower() not in _LOCAL_HOSTS:
        public_permalink = _rewrite_url_host(url=raw_link, host=feed_host, scheme=feed_scheme or "https")

    # Prefer the first unique non-local URL in the entry body.
    urls = _extract_http_urls_from_html(summary_html or "")
    seen: set[str] = set()
    candidates: list[str] = []
    for u in urls:
        uu = (u or "").strip()
        if not uu:
            continue
        if _is_local_url(uu):
            continue
        if uu in seen:
            continue
        seen.add(uu)
        candidates.append(uu)

    if candidates:
        return candidates[0]

    # Fallback: if we cannot find a usable external URL, try a public permalink by rewriting
    # localhost → feed host.
    if public_permalink and (not _is_local_url(public_permalink)):
        return public_permalink
    return raw_link


def _best_entry_summary(entry: object) -> str | None:
    """
    Prefer richer content fields when available.

    Many RSS feeds (especially forums) put the real body into `content:encoded`,
    which feedparser exposes via `entry.content[0].value`.
    """
    try:
        content = getattr(entry, "content", None)
    except Exception:
        content = None
    if content is None:
        try:
            content = entry.get("content")  # type: ignore[attr-defined]
        except Exception:
            content = None

    if isinstance(content, list) and content:
        first = content[0]
        if isinstance(first, dict):
            value = first.get("value") or ""
        else:
            value = getattr(first, "value", "") or ""
        value = str(value or "").strip()
        if value:
            return value

    for attr in ("summary", "description", "subtitle"):
        try:
            value = getattr(entry, attr, None)
        except Exception:
            value = None
        if not value:
            try:
                value = entry.get(attr)  # type: ignore[attr-defined]
            except Exception:
                value = None
        value = str(value or "").strip()
        if value:
            return value

    return None


class RssConnector(Connector):
    type = "rss"

    def __init__(self, *, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds

    async def fetch_with_state(
        self,
        *,
        url: str,
        etag: str | None,
        last_modified: str | None,
        cookie_header: str | None = None,
    ) -> tuple[list[FetchedEntry], dict[str, str] | None]:
        parsed = urlparse(url)
        if parsed.scheme == "file":
            return await self.fetch(url=url), None

        headers: dict[str, str] = {"User-Agent": "tracker/0.1"}
        cookie = (cookie_header or "").strip()
        if cookie:
            headers["Cookie"] = cookie
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified

        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = await client.get(url, headers=headers)
            status_code = int(getattr(resp, "status_code", 200) or 200)
            if status_code == 304:
                return [], None
            final_url = str(getattr(resp, "url", url) or url)
            if status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=final_url):
                raise AuthRequiredError(url=url, status_code=status_code, final_url=final_url)
            resp.raise_for_status()
            text = resp.text

        feed = feedparser.parse(text)

        update: dict[str, str] = {}
        if resp.headers.get("ETag"):
            update["etag"] = resp.headers["ETag"]
        if resp.headers.get("Last-Modified"):
            update["last_modified"] = resp.headers["Last-Modified"]

        feed_url = final_url or url
        entries: list[FetchedEntry] = []
        for e in feed.entries:
            title = getattr(e, "title", "") or ""
            published = getattr(e, "published", None) or getattr(e, "updated", None)
            summary = _best_entry_summary(e)
            link = _resolve_entry_link(link=(getattr(e, "link", None) or ""), summary_html=summary, feed_url=feed_url)
            if link:
                entries.append(
                    FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary)
                )
        return entries, (update if update else None)

    async def fetch(self, *, url: str) -> list[FetchedEntry]:
        return await self.fetch_with_cookie(url=url, cookie_header=None)

    async def fetch_with_cookie(self, *, url: str, cookie_header: str | None = None) -> list[FetchedEntry]:
        parsed = urlparse(url)
        feed_url = url
        if parsed.scheme == "file":
            feed = feedparser.parse(Path(parsed.path).read_text(encoding="utf-8"))
        else:
            headers: dict[str, str] = {"User-Agent": "tracker/0.1"}
            cookie = (cookie_header or "").strip()
            if cookie:
                headers["Cookie"] = cookie
            async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
                feed_url = str(getattr(resp, "url", url) or url)
                status_code = int(getattr(resp, "status_code", 200) or 200)
                if status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=feed_url):
                    raise AuthRequiredError(url=url, status_code=status_code, final_url=feed_url)
                resp.raise_for_status()
                feed = feedparser.parse(resp.text)

        entries: list[FetchedEntry] = []
        for e in feed.entries:
            title = getattr(e, "title", "") or ""
            published = getattr(e, "published", None) or getattr(e, "updated", None)
            summary = _best_entry_summary(e)
            link = _resolve_entry_link(link=(getattr(e, "link", None) or ""), summary_html=summary, feed_url=feed_url)
            if link:
                entries.append(
                    FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary)
                )
        return entries
