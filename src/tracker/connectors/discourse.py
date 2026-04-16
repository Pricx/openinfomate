from __future__ import annotations

import asyncio
import json
import logging
import re
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit

import feedparser
import httpx
import requests
from bs4 import BeautifulSoup

from tracker.connectors.base import Connector, FetchedEntry
from tracker.connectors.errors import TemporaryFetchBlockError
from tracker.http_auth import AuthRequiredError, looks_like_login_redirect
from tracker.normalize import html_to_text, normalize_text

logger = logging.getLogger(__name__)

# Some Discourse sites place Cloudflare challenges in front of `*.json` endpoints
# but allow RSS feeds. Cache by netloc to avoid the extra (failing) request on
# every fetch in long-running services.
_CF_CHALLENGED_NETLOCS: set[str] = set()
_RSS_OPTIONAL_UNAVAILABLE_URLS: set[str] = set()
_JSON_BLOCKLIKE_STATUS_CODES = {403, 408, 425, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}
_DISCOURSE_BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain;q=0.9, application/xml;q=0.8, text/xml;q=0.8, */*;q=0.7",
    "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

_DISCOURSE_RSS_BOILERPLATE_RE = (
    re.compile(r"(阅读完整话题|read full topic|continue reading|click to expand)", re.IGNORECASE),
)
_HTTPX_REQUESTS_FALLBACK_EXCEPTIONS = (httpx.ConnectError, httpx.ConnectTimeout)


def _retry_after_seconds(headers: dict[str, str] | httpx.Headers | None) -> int | None:
    if not headers:
        return None
    try:
        raw = str(headers.get("Retry-After") or headers.get("retry-after") or "").strip()
    except Exception:
        raw = ""
    if not raw:
        return None
    try:
        return max(0, int(raw))
    except Exception:
        return None


def _looks_like_transient_discourse_block(*, status_code: int, headers: dict[str, str] | httpx.Headers | None, final_url: str) -> bool:
    code = int(status_code or 0)
    if code == 403:
        try:
            if str((headers or {}).get("cf-mitigated") or "").strip().lower() == "challenge":
                return True
        except Exception:
            pass
        return not looks_like_login_redirect(original_url=final_url, final_url=final_url)
    return code in _JSON_BLOCKLIKE_STATUS_CODES


def build_discourse_json_url(*, base_url: str, json_path: str = "/latest.json") -> str:
    base = base_url.rstrip("/")
    path = json_path if json_path.startswith("/") else f"/{json_path}"
    return f"{base}{path}"


def build_discourse_topic_url(*, base_url: str, slug: str, topic_id: int) -> str:
    base = base_url.rstrip("/")
    return f"{base}/t/{slug}/{topic_id}"


def _base_from_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, "", "", ""))


def _normalize_discourse_html_summary(raw: str) -> str | None:
    text = normalize_text(raw or "")
    if not text:
        return None
    if len(text) > 1200:
        text = text[:1200].rstrip() + "…"
    return text or None


class DiscourseConnector(Connector):
    type = "discourse"

    def __init__(
        self,
        *,
        timeout_seconds: int = 20,
        rss_catchup_pages: int = 1,
        cookie: str | None = None,
    ):
        self.timeout_seconds = timeout_seconds
        self.rss_catchup_pages = max(1, int(rss_catchup_pages or 1))
        self.cookie = (cookie or "").strip() or None

    async def _requests_get(self, *, url: str, headers: dict[str, str]):
        def _run():
            return requests.get(
                url,
                headers=headers,
                timeout=self.timeout_seconds,
                allow_redirects=True,
            )

        return await asyncio.to_thread(_run)

    async def _get_with_transport_fallback(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
        headers: dict[str, str],
    ):
        try:
            return await client.get(url, headers=headers)
        except _HTTPX_REQUESTS_FALLBACK_EXCEPTIONS as exc:
            logger.info("discourse httpx get failed; retrying with requests: url=%s err=%r", url, exc)
            return await self._requests_get(url=url, headers=headers)

    def _with_page(self, url: str, page: int) -> str:
        try:
            parts = urlsplit(url)
            q = dict(parse_qsl(parts.query or "", keep_blank_values=True))
            q["page"] = str(int(page))
            query = urlencode(q)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
        except Exception:
            return url

    def _rss_recall_urls(self, url: str, *, include_top_daily: bool = False) -> list[str]:
        """
        Return auxiliary RSS URLs that improve recall for Discourse latest feeds.

        - primary: direct RSS fallback for the requested JSON endpoint
        - new.rss: backstop for newly created topics that may roll off `latest` quickly
        - top.rss?period=daily: optional stale-run backstop for older-but-important posts
        """
        primary = self._rss_fallback_url(url)
        urls: list[str] = [primary]
        try:
            parts = urlsplit(primary)
            if parts.path.rstrip("/") == "/latest.rss":
                urls.append(urlunsplit((parts.scheme, parts.netloc, "/new.rss", "", "")))
                if include_top_daily:
                    urls.append(urlunsplit((parts.scheme, parts.netloc, "/top.rss", "period=daily", "")))
        except Exception:
            pass
        out: list[str] = []
        seen: set[str] = set()
        for u in urls:
            s = (u or "").strip()
            if not s or s in seen:
                continue
            seen.add(s)
            out.append(s)
        return out

    def _rss_fallback_url(self, url: str) -> str:
        parts = urlsplit(url)
        path = parts.path or ""
        if path.endswith(".json"):
            rss_path = path[: -len(".json")] + ".rss"
        else:
            rss_path = "/latest.rss"
        if not rss_path.startswith("/"):
            rss_path = f"/{rss_path}"
        return urlunsplit((parts.scheme, parts.netloc, rss_path, "", ""))

    def _html_fallback_url(self, url: str) -> str:
        parts = urlsplit(url)
        path = parts.path or ""
        if path.endswith(".json"):
            html_path = path[: -len(".json")]
        elif path.endswith(".rss"):
            html_path = path[: -len(".rss")]
        else:
            html_path = path or "/latest"
        if not html_path.startswith("/"):
            html_path = f"/{html_path}"
        return urlunsplit((parts.scheme, parts.netloc, html_path, parts.query, parts.fragment))

    def _effective_rss_catchup_pages(self, *, url: str, include_top_daily: bool) -> int:
        pages = max(1, int(self.rss_catchup_pages or 1))
        try:
            primary = self._rss_fallback_url(url)
            path = (urlsplit(primary).path or "").rstrip("/")
        except Exception:
            path = ""
        if path == "/latest.rss":
            pages = max(pages, 8)
            if include_top_daily:
                pages = max(pages, 12)
        return pages

    def _clean_rss_summary(self, *, raw: str, title: str) -> str | None:
        text = html_to_text(raw or "")
        if not text:
            return None
        for pat in _DISCOURSE_RSS_BOILERPLATE_RE:
            text = pat.sub(" ", text)
        text = normalize_text(text)
        title_norm = normalize_text(title or "")
        if title_norm and text.startswith(title_norm):
            text = normalize_text(text[len(title_norm) :].lstrip(" -:：|·"))
        if not text or text == title_norm:
            return None
        if len(text) > 1200:
            text = text[:1200].rstrip() + "…"
        return text or None

    def _parse_rss(self, *, text: str) -> list[FetchedEntry]:
        feed = feedparser.parse(text)
        entries: list[FetchedEntry] = []
        for e in feed.entries:
            link = getattr(e, "link", None) or ""
            title = getattr(e, "title", "") or ""
            published = getattr(e, "published", None) or getattr(e, "updated", None)
            summary = self._clean_rss_summary(
                raw=str(getattr(e, "summary", None) or getattr(e, "description", None) or ""),
                title=title,
            )
            if link:
                entries.append(FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary))
        return entries

    def _parse_html_latest(self, *, html: str, page_url: str) -> list[FetchedEntry]:
        soup = BeautifulSoup(html or "", "html.parser")
        entries: list[FetchedEntry] = []
        seen: set[str] = set()
        for row in soup.select("tr.topic-list-item"):
            anchor = row.select_one("a.title[href]")
            if anchor is None:
                continue
            href = str(anchor.get("href") or "").strip()
            if not href:
                continue
            link = urljoin(page_url, href)
            if not link or link in seen:
                continue
            seen.add(link)
            title = normalize_text(anchor.get_text(" ", strip=True) or "")
            if not title:
                continue
            excerpt = row.select_one("p.excerpt")
            summary = _normalize_discourse_html_summary(excerpt.get_text(" ", strip=True) if excerpt else "")
            entries.append(FetchedEntry(url=link, title=title, summary=summary))
        return entries

    async def _fetch_html_latest(
        self,
        *,
        client: httpx.AsyncClient,
        url: str,
    ) -> list[FetchedEntry]:
        page_url = self._html_fallback_url(url)
        headers = dict(_DISCOURSE_BROWSER_HEADERS)
        headers["Accept"] = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        if self.cookie:
            headers["Cookie"] = self.cookie
        resp = await self._get_with_transport_fallback(client=client, url=page_url, headers=headers)
        final_url = str(getattr(resp, "url", page_url) or page_url)
        if resp.status_code in {401, 403} or looks_like_login_redirect(original_url=page_url, final_url=final_url):
            raise AuthRequiredError(url=page_url, status_code=resp.status_code, final_url=final_url)
        resp.raise_for_status()
        return self._parse_html_latest(html=resp.text, page_url=page_url)

    async def _fetch_rss_pages(
        self,
        *,
        client: httpx.AsyncClient,
        primary_url: str,
        pages: int,
    ) -> list[FetchedEntry]:
        """
        Fetch a small number of Discourse RSS pages (page=0..N) and merge entries.

        This is used as a catch-up mechanism after downtime on high-volume Discourse sites,
        especially when `*.json` endpoints are blocked (e.g. Cloudflare challenge).
        """
        merged: list[FetchedEntry] = []
        seen: set[str] = set()
        max_pages = max(1, int(pages or 1))
        headers = dict(_DISCOURSE_BROWSER_HEADERS)
        headers["Accept"] = "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8"
        if self.cookie:
            headers["Cookie"] = self.cookie
        transient_status_codes = {408, 425, 429, 500, 502, 503, 504}
        max_consecutive_failures = 3
        consecutive_failures = 0
        skipped_pages = 0
        for i in range(max_pages):
            url = primary_url if i == 0 else self._with_page(primary_url, i)
            try:
                resp = await self._get_with_transport_fallback(client=client, url=url, headers=headers)
            except Exception as exc:
                if i == 0:
                    raise
                skipped_pages += 1
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.info(
                        "discourse rss catchup stopping after request failures: url=%s failures=%s err=%s",
                        url,
                        consecutive_failures,
                        exc,
                    )
                    break
                logger.info("discourse rss page request failed (skipping): url=%s err=%s", url, exc)
                continue
            if resp.status_code == 404 and i > 0:
                break
            final_url = str(getattr(resp, "url", url) or url)
            if resp.status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=final_url):
                raise AuthRequiredError(url=url, status_code=resp.status_code, final_url=final_url)
            if int(getattr(resp, "status_code", 0) or 0) in transient_status_codes and i > 0:
                skipped_pages += 1
                consecutive_failures += 1
                if consecutive_failures >= max_consecutive_failures:
                    logger.info(
                        "discourse rss catchup stopping after transient failures: url=%s status=%s failures=%s",
                        url,
                        resp.status_code,
                        consecutive_failures,
                    )
                    break
                logger.info(
                    "discourse rss page transient error (skipping): url=%s status=%s",
                    url,
                    resp.status_code,
                )
                continue
            resp.raise_for_status()
            consecutive_failures = 0
            entries = self._parse_rss(text=resp.text)
            if not entries and i > 0:
                break
            for e in entries:
                if e.url and e.url not in seen:
                    seen.add(e.url)
                    merged.append(e)
        if skipped_pages:
            logger.info("discourse rss catchup skipped_pages=%s primary=%s", skipped_pages, primary_url)
        return merged

    async def _merge_rss_urls(
        self,
        *,
        client: httpx.AsyncClient,
        urls: list[str],
        seen: set[str],
        entries: list[FetchedEntry],
    ) -> None:
        headers = dict(_DISCOURSE_BROWSER_HEADERS)
        headers["Accept"] = "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8"
        if self.cookie:
            headers["Cookie"] = self.cookie
        for extra in urls:
            if extra in _RSS_OPTIONAL_UNAVAILABLE_URLS:
                continue
            try:
                extra_resp = await self._get_with_transport_fallback(client=client, url=extra, headers=headers)
                if extra_resp.status_code == 404:
                    _RSS_OPTIONAL_UNAVAILABLE_URLS.add(extra)
                    continue
                final_url = str(getattr(extra_resp, "url", extra) or extra)
                if extra_resp.status_code in {401, 403} or looks_like_login_redirect(
                    original_url=extra, final_url=final_url
                ):
                    raise AuthRequiredError(url=extra, status_code=extra_resp.status_code, final_url=final_url)
                extra_resp.raise_for_status()
                for entry in self._parse_rss(text=extra_resp.text):
                    if entry.url and entry.url not in seen:
                        seen.add(entry.url)
                        entries.append(entry)
            except Exception as exc:
                logger.info("discourse rss recall failed: url=%s err=%s", extra, exc)

    async def fetch(self, *, url: str, include_top_daily: bool = False) -> list[FetchedEntry]:
        parts = urlsplit(url)
        netloc = parts.netloc
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            headers = dict(_DISCOURSE_BROWSER_HEADERS)
            if self.cookie:
                headers["Cookie"] = self.cookie
            if netloc and netloc in _CF_CHALLENGED_NETLOCS:
                rss_urls = self._rss_recall_urls(url, include_top_daily=include_top_daily)
                primary = rss_urls[0]
                pages = self._effective_rss_catchup_pages(url=url, include_top_daily=include_top_daily)
                try:
                    entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=pages)
                except Exception as exc:
                    logger.info("discourse rss fetch failed: url=%s err=%s", primary, exc)
                    entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=1)
                if not entries:
                    try:
                        entries = await self._fetch_html_latest(client=client, url=url)
                    except Exception as exc:
                        logger.info("discourse html fetch failed after empty rss fallback: url=%s err=%s", url, exc)
                seen = {e.url for e in entries if e.url}
                await self._merge_rss_urls(client=client, urls=rss_urls[1:], seen=seen, entries=entries)
                return entries

            resp = await self._get_with_transport_fallback(client=client, url=url, headers=headers)
            final_url = str(getattr(resp, "url", url) or url)
            if _looks_like_transient_discourse_block(
                status_code=int(getattr(resp, "status_code", 0) or 0),
                headers=getattr(resp, "headers", None),
                final_url=final_url,
            ):
                if netloc:
                    _CF_CHALLENGED_NETLOCS.add(netloc)
                rss_urls = self._rss_recall_urls(url, include_top_daily=include_top_daily)
                primary = rss_urls[0]
                pages = self._effective_rss_catchup_pages(url=url, include_top_daily=include_top_daily)
                try:
                    entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=pages)
                except Exception as exc:
                    logger.info("discourse rss fetch failed: url=%s err=%s", primary, exc)
                    try:
                        entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=1)
                    except Exception as fallback_exc:
                        raise TemporaryFetchBlockError(
                            url=url,
                            status_code=int(getattr(resp, "status_code", 0) or 0),
                            final_url=final_url,
                            retry_after_seconds=_retry_after_seconds(getattr(resp, "headers", None)),
                            reason=f"json_blocked_and_rss_failed:{fallback_exc}",
                        ) from exc
                if not entries:
                    try:
                        entries = await self._fetch_html_latest(client=client, url=url)
                    except Exception as exc:
                        raise TemporaryFetchBlockError(
                            url=url,
                            status_code=int(getattr(resp, "status_code", 0) or 0),
                            final_url=final_url,
                            retry_after_seconds=_retry_after_seconds(getattr(resp, "headers", None)),
                            reason=f"json_blocked_rss_empty_and_html_failed:{exc}",
                        ) from exc
                seen = {e.url for e in entries if e.url}
                await self._merge_rss_urls(client=client, urls=rss_urls[1:], seen=seen, entries=entries)
                return entries

            if resp.status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=final_url):
                raise AuthRequiredError(url=url, status_code=resp.status_code, final_url=final_url)
            resp.raise_for_status()
            data = json.loads(resp.text)

            base_url = _base_from_url(url)
            topics = (((data.get("topic_list") or {}).get("topics")) or [])
            entries: list[FetchedEntry] = []
            for t in topics:
                topic_id = t.get("id")
                slug = t.get("slug") or str(topic_id)
                title = (t.get("title") or "").strip()
                created_at = t.get("created_at") or t.get("last_posted_at")
                excerpt = (t.get("excerpt") or "").strip() or None
                link = build_discourse_topic_url(base_url=base_url, slug=slug, topic_id=int(topic_id))
                entries.append(FetchedEntry(url=link, title=title, published_at_iso=created_at, summary=excerpt))

            # Optional extra recall (configured by rss_catchup_pages):
            # even when JSON endpoints work, `latest.json` is a moving window and can miss
            # fast-moving topics (or posts created during shorter downtime). When operators
            # set rss_catchup_pages>1, merge a small number of Latest RSS pages (page=0..N)
            # to improve recall while still letting the LLM decide relevance.
            effective_rss_pages = self._effective_rss_catchup_pages(url=url, include_top_daily=include_top_daily)
            if effective_rss_pages > 1:
                rss_urls = self._rss_recall_urls(url, include_top_daily=include_top_daily)
                primary = rss_urls[0]
                try:
                    p_parts = urlsplit(primary)
                except Exception:
                    p_parts = None
                if p_parts and p_parts.path.rstrip("/") == "/latest.rss":
                    seen = {e.url for e in entries if e.url}
                    try:
                        for e in await self._fetch_rss_pages(
                            client=client,
                            primary_url=primary,
                            pages=effective_rss_pages,
                        ):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss recall failed: url=%s err=%s", primary, exc)
                    await self._merge_rss_urls(client=client, urls=rss_urls[1:], seen=seen, entries=entries)

            # Recall backstop after downtime: if the operator requests it, merge a Top Daily RSS feed
            # even when JSON endpoints are accessible (latest.json is a moving window and may miss
            # older-but-important posts when the service was down).
            if include_top_daily:
                rss_urls = self._rss_recall_urls(url, include_top_daily=include_top_daily)
                seen = {e.url for e in entries if e.url}
                # Also merge a bounded number of Latest RSS pages as a stronger catch-up mechanism
                # (Top Daily doesn't always include the post we care about).
                #
                # Note: when rss_catchup_pages>1 we already merged /latest.rss pages above for recall,
                # so avoid duplicating requests here.
                primary = rss_urls[0]
                try:
                    p = urlsplit(primary)
                except Exception:
                    p = None
                already_merged_latest_pages = bool(
                    effective_rss_pages > 1 and p and p.path.rstrip("/") == "/latest.rss"
                )
                if not already_merged_latest_pages:
                    try:
                        for e in await self._fetch_rss_pages(
                            client=client,
                            primary_url=primary,
                            pages=effective_rss_pages,
                        ):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss catchup failed: url=%s err=%s", primary, exc)
                await self._merge_rss_urls(client=client, urls=rss_urls[1:], seen=seen, entries=entries)

            return entries
