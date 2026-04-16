from __future__ import annotations

import json
import logging
import re
from urllib.parse import urljoin, urlsplit

import feedparser
import httpx
from bs4 import BeautifulSoup

from tracker.normalize import normalize_text
from tracker.http_auth import AuthRequiredError, looks_like_login_redirect

logger = logging.getLogger(__name__)

_DISCOURSE_TOPIC_PATH_RE = re.compile(r"^/t/[^/]+/\d+$")
_ARXIV_HOSTS = {"arxiv.org", "www.arxiv.org", "export.arxiv.org"}


def _is_arxiv_abs_url(url: str) -> bool:
    raw = (url or "").strip()
    if not raw:
        return False
    try:
        parts = urlsplit(raw)
    except Exception:
        return False
    host = (parts.netloc or "").strip().lower()
    path = (parts.path or "").strip()
    return host in _ARXIV_HOSTS and path.startswith("/abs/")


def _extract_text_from_arxiv_abs_html(*, html: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")

    title = ""
    try:
        meta = soup.find("meta", attrs={"name": "citation_title"})
        if meta is not None:
            title = str(meta.get("content") or "").strip()
    except Exception:
        title = ""
    if not title:
        node = soup.select_one("h1.title")
        title = normalize_text(node.get_text(" ", strip=True) if node else "")
        if title.lower().startswith("title:"):
            title = title.split(":", 1)[1].strip()

    abstract = ""
    node = soup.select_one("blockquote.abstract")
    if node is not None:
        abstract = normalize_text(node.get_text(" ", strip=True))
        if abstract.lower().startswith("abstract:"):
            abstract = abstract.split(":", 1)[1].strip()
    if not abstract:
        try:
            meta = soup.find("meta", attrs={"name": "description"})
            if meta is not None:
                abstract = normalize_text(str(meta.get("content") or ""))
        except Exception:
            abstract = ""

    parts: list[str] = []
    if title:
        parts.append(f"Title: {title}")
    if abstract:
        parts.append(f"Abstract: {abstract}")
    return normalize_text(" ".join(parts))


def _extract_text_from_html(*, html: str, url: str) -> str:
    soup = BeautifulSoup(html or "", "html.parser")

    # Remove obvious non-content.
    for t in soup.find_all(["script", "style", "noscript", "svg"]):
        t.decompose()

    parts = urlsplit(url)
    host = (parts.netloc or "").lower()

    # Host-specific best-effort main-content selectors.
    main = None
    if host.endswith("mp.weixin.qq.com"):
        main = soup.select_one("#js_content")
    if main is None:
        main = soup.find("article")
    if main is None:
        main = soup.body or soup

    for t in main.find_all(["nav", "header", "footer", "aside", "form"]):
        t.decompose()

    # Preserve outbound URLs (e.g. Discourse cooked HTML often has primary sources in <a href="...">).
    # Without this, full-text extraction drops hrefs and downstream processing can't "jump" to
    # repos/papers/docs referenced by the seed page.
    outbound: list[str] = []
    seen: set[str] = set()
    for a in main.find_all("a"):
        try:
            href = (a.get("href") or "").strip()
        except Exception:
            href = ""
        if not href:
            continue
        if href.startswith("#") or href.lower().startswith(("javascript:", "mailto:", "tel:")):
            continue
        abs_url = urljoin(url, href)
        try:
            p2 = urlsplit(abs_url)
            host2 = (p2.netloc or "").lower()
        except Exception:
            host2 = ""
        # Keep only outbound http(s) URLs to avoid polluting evidence with site-internal navigation.
        if not abs_url.lower().startswith(("http://", "https://")):
            continue
        if not host2 or host2 == host:
            continue
        # Drop fragments to keep URLs stable for extraction/dedupe.
        abs_url = abs_url.split("#", 1)[0]
        if abs_url in seen:
            continue
        seen.add(abs_url)
        outbound.append(abs_url)
        if len(outbound) >= 30:
            break

    text = normalize_text(main.get_text(" ", strip=True))
    if outbound:
        # Keep it single-line so normalization doesn't destroy formatting; regex URL extractors can still see it.
        text = (text + " Links: " + " ".join(outbound)).strip()
    return text


async def _try_fetch_discourse_topic_json(
    *,
    url: str,
    timeout_seconds: int,
    discourse_cookie: str | None = None,
) -> str | None:
    """
    Best-effort Discourse fulltext fetch via `/t/<slug>/<id>.json`.

    Many Discourse sites expose JSON endpoints even when HTML pages are blocked/challenged.
    """
    parts = urlsplit(url)
    path = parts.path or ""
    if not _DISCOURSE_TOPIC_PATH_RE.match(path):
        return None

    base = url.rstrip("/")
    json_url = base + ".json"
    rss_url = base + ".rss"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    }
    cookie = (discourse_cookie or "").strip()
    if cookie:
        # Optional Discourse cookie (for private categories / Cloudflare clearance).
        # Stored on-server only; never commit/export.
        # Note: this is a raw `Cookie:` header value (e.g. "a=b; c=d").
        headers["Cookie"] = cookie
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        resp = await client.get(json_url, headers=headers)
        if resp.status_code == 403 and (resp.headers.get("cf-mitigated") == "challenge"):
            # Cloudflare-challenged Discourse JSON endpoints often still allow RSS crawlers.
            rss_resp = await client.get(
                rss_url,
                headers={
                    **headers,
                    "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8",
                },
            )
            if rss_resp.status_code >= 400:
                return None
            rss_type = (rss_resp.headers.get("content-type") or "").lower()
            if rss_type and "xml" not in rss_type and "rss" not in rss_type:
                return None
            feed = feedparser.parse(rss_resp.text or "")
            desc = normalize_text(str(getattr(feed, "feed", {}).get("description") or ""))
            return desc or None

        if resp.status_code >= 400:
            # Many Discourse sites block `.json` without explicitly flagging a Cloudflare challenge,
            # but still expose `.rss`. Try RSS as a best-effort fallback before giving up.
            try:
                rss_resp = await client.get(
                    rss_url,
                    headers={
                        **headers,
                        "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8",
                    },
                )
                if rss_resp.status_code >= 400:
                    return None
                feed = feedparser.parse(rss_resp.text or "")
                desc = normalize_text(str(getattr(feed, "feed", {}).get("description") or ""))
                return desc or None
            except Exception:
                return None
        content_type = (resp.headers.get("content-type") or "").lower()
        if content_type and "json" not in content_type:
            return None

        try:
            data = resp.json()
        except Exception:
            data = json.loads(resp.text or "{}")

    posts = (((data.get("post_stream") or {}).get("posts")) or []) if isinstance(data, dict) else []
    if not posts or not isinstance(posts, list):
        # As a fallback, try RSS (some sites return empty JSON bodies to non-browser clients).
        try:
            async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
                rss_resp = await client.get(
                    rss_url,
                    headers={
                        **headers,
                        "Accept": "application/rss+xml, application/xml;q=0.9, text/xml;q=0.9, */*;q=0.8",
                    },
                )
            if rss_resp.status_code >= 400:
                return None
            feed = feedparser.parse(rss_resp.text or "")
            desc = normalize_text(str(getattr(feed, "feed", {}).get("description") or ""))
            return desc or None
        except Exception:
            return None
    first = posts[0] if isinstance(posts[0], dict) else None
    if not first:
        return None
    cooked = (first.get("cooked") or "").strip()
    raw = (first.get("raw") or "").strip()
    html = cooked or raw
    if not html:
        return None

    text = _extract_text_from_html(html=html, url=json_url)
    return text or None


async def fetch_fulltext_for_url(
    *,
    url: str,
    timeout_seconds: int,
    max_chars: int,
    discourse_cookie: str | None = None,
    cookie_header: str | None = None,
) -> str:
    """
    Fetch a webpage and extract best-effort readable text (HTML → plain text).

    Raises on HTTP errors and obviously unsupported content types.
    """
    discourse_text = await _try_fetch_discourse_topic_json(
        url=url,
        timeout_seconds=timeout_seconds,
        discourse_cookie=discourse_cookie,
    )
    if discourse_text:
        max_i = max(1, int(max_chars or 1))
        if len(discourse_text) > max_i:
            return discourse_text[:max_i] + "…"
        return discourse_text

    headers = {
        # A more browser-like UA improves success on some sites that block unknown bots.
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html, text/plain;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
    }
    cookie = (cookie_header or "").strip()
    if cookie:
        headers["Cookie"] = cookie
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True) as client:
        resp = await client.get(url, headers=headers)
        final_url = str(getattr(resp, "url", url) or url)
        if resp.status_code in {401, 403} or looks_like_login_redirect(original_url=url, final_url=final_url):
            raise AuthRequiredError(url=url, status_code=resp.status_code, final_url=final_url)
        resp.raise_for_status()
        content_type = (resp.headers.get("content-type") or "").lower()
        if content_type and ("text/html" not in content_type and "text/plain" not in content_type):
            raise ValueError(f"unsupported content-type: {content_type}")
        html = resp.text
        final_url = str(getattr(resp, "url", url) or url)

    if _is_arxiv_abs_url(final_url or url):
        text = _extract_text_from_arxiv_abs_html(html=html)
        if text:
            max_i = max(1, int(max_chars or 1))
            if len(text) > max_i:
                return text[:max_i] + "…"
            return text

    text = _extract_text_from_html(html=html, url=final_url or url)
    if not text:
        raise ValueError("empty extracted text")

    max_i = max(1, int(max_chars or 1))
    if len(text) > max_i:
        return text[:max_i] + "…"
    return text
