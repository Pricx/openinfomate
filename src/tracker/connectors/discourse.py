from __future__ import annotations

import json
import logging
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import feedparser
import httpx

from tracker.connectors.base import Connector, FetchedEntry
from tracker.http_auth import AuthRequiredError, looks_like_login_redirect

logger = logging.getLogger(__name__)

# Some Discourse sites place Cloudflare challenges in front of `*.json` endpoints
# but allow RSS feeds. Cache by netloc to avoid the extra (failing) request on
# every fetch in long-running services.
_CF_CHALLENGED_NETLOCS: set[str] = set()


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

    def _with_page(self, url: str, page: int) -> str:
        try:
            parts = urlsplit(url)
            q = dict(parse_qsl(parts.query or "", keep_blank_values=True))
            q["page"] = str(int(page))
            query = urlencode(q)
            return urlunsplit((parts.scheme, parts.netloc, parts.path, query, parts.fragment))
        except Exception:
            return url

    def _rss_recall_urls(self, url: str) -> list[str]:
        """
        Return RSS URLs to use when JSON endpoints are blocked (e.g. Cloudflare challenge).

        We always include the direct RSS fallback, and (for "latest" feeds) add an
        additional "top daily" RSS feed as a recall backstop so important posts
        aren't missed during service downtime.
        """
        primary = self._rss_fallback_url(url)
        urls: list[str] = [primary]
        try:
            parts = urlsplit(primary)
            if parts.path.rstrip("/") == "/latest.rss":
                top_daily = urlunsplit((parts.scheme, parts.netloc, "/top.rss", "period=daily", ""))
                urls.append(top_daily)
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

    def _parse_rss(self, *, text: str) -> list[FetchedEntry]:
        feed = feedparser.parse(text)
        entries: list[FetchedEntry] = []
        for e in feed.entries:
            link = getattr(e, "link", None) or ""
            title = getattr(e, "title", "") or ""
            published = getattr(e, "published", None) or getattr(e, "updated", None)
            # Discourse RSS descriptions are often long and share boilerplate across topics
            # ("阅读完整话题", participant counts, etc.). Using them as the primary ingest text
            # can cause false-positive simhash near-dup drops. Prefer title-only at ingest;
            # full text can be fetched later via per-topic RSS in `fulltext`.
            summary = None
            if link:
                entries.append(FetchedEntry(url=link, title=title, published_at_iso=published, summary=summary))
        return entries

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
        headers = {"User-Agent": "tracker/0.1"}
        if self.cookie:
            headers["Cookie"] = self.cookie
        for i in range(max_pages):
            url = primary_url if i == 0 else self._with_page(primary_url, i)
            resp = await client.get(url, headers=headers)
            if resp.status_code == 404 and i > 0:
                break
            final_url = str(getattr(resp, "url", url) or url)
            if resp.status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=final_url):
                raise AuthRequiredError(url=url, status_code=resp.status_code, final_url=final_url)
            resp.raise_for_status()
            entries = self._parse_rss(text=resp.text)
            if not entries and i > 0:
                break
            for e in entries:
                if e.url and e.url not in seen:
                    seen.add(e.url)
                    merged.append(e)
        return merged

    async def fetch(self, *, url: str, include_top_daily: bool = False) -> list[FetchedEntry]:
        parts = urlsplit(url)
        netloc = parts.netloc
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            headers = {"User-Agent": "tracker/0.1"}
            if self.cookie:
                headers["Cookie"] = self.cookie
            if netloc and netloc in _CF_CHALLENGED_NETLOCS:
                rss_urls = self._rss_recall_urls(url)
                primary = rss_urls[0]
                pages = self.rss_catchup_pages if include_top_daily else 1
                entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=pages)
                seen = {e.url for e in entries if e.url}
                for extra in rss_urls[1:]:
                    try:
                        extra_resp = await client.get(extra, headers=headers)
                        final_url = str(getattr(extra_resp, "url", extra) or extra)
                        if extra_resp.status_code in {401, 403} or looks_like_login_redirect(
                            original_url=extra, final_url=final_url
                        ):
                            raise AuthRequiredError(url=extra, status_code=extra_resp.status_code, final_url=final_url)
                        extra_resp.raise_for_status()
                        for e in self._parse_rss(text=extra_resp.text):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss recall failed: url=%s err=%s", extra, exc)
                return entries

            resp = await client.get(url, headers=headers)
            if resp.status_code == 403 and resp.headers.get("cf-mitigated") == "challenge":
                if netloc:
                    _CF_CHALLENGED_NETLOCS.add(netloc)
                rss_urls = self._rss_recall_urls(url)
                primary = rss_urls[0]
                pages = self.rss_catchup_pages if include_top_daily else 1
                entries = await self._fetch_rss_pages(client=client, primary_url=primary, pages=pages)
                seen = {e.url for e in entries if e.url}
                for extra in rss_urls[1:]:
                    try:
                        extra_resp = await client.get(extra, headers=headers)
                        final_url = str(getattr(extra_resp, "url", extra) or extra)
                        if extra_resp.status_code in {401, 403} or looks_like_login_redirect(
                            original_url=extra, final_url=final_url
                        ):
                            raise AuthRequiredError(url=extra, status_code=extra_resp.status_code, final_url=final_url)
                        extra_resp.raise_for_status()
                        for e in self._parse_rss(text=extra_resp.text):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss recall failed: url=%s err=%s", extra, exc)
                return entries

            final_url = str(getattr(resp, "url", url) or url)
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
            if self.rss_catchup_pages > 1:
                rss_urls = self._rss_recall_urls(url)
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
                            pages=self.rss_catchup_pages,
                        ):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss recall failed: url=%s err=%s", primary, exc)

            # Recall backstop after downtime: if the operator requests it, merge a Top Daily RSS feed
            # even when JSON endpoints are accessible (latest.json is a moving window and may miss
            # older-but-important posts when the service was down).
            if include_top_daily:
                rss_urls = self._rss_recall_urls(url)
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
                    self.rss_catchup_pages > 1 and p and p.path.rstrip("/") == "/latest.rss"
                )
                if not already_merged_latest_pages:
                    try:
                        for e in await self._fetch_rss_pages(
                            client=client,
                            primary_url=primary,
                            pages=self.rss_catchup_pages,
                        ):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss catchup failed: url=%s err=%s", primary, exc)
                for extra in rss_urls[1:]:
                    try:
                        extra_resp = await client.get(extra, headers=headers)
                        extra_resp.raise_for_status()
                        for e in self._parse_rss(text=extra_resp.text):
                            if e.url and e.url not in seen:
                                seen.add(e.url)
                                entries.append(e)
                    except Exception as exc:
                        logger.info("discourse rss recall failed: url=%s err=%s", extra, exc)

            return entries
