from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import httpx
from bs4 import BeautifulSoup

from tracker.connectors.base import Connector, FetchedEntry
from tracker.http_auth import AuthRequiredError, looks_like_login_redirect


@dataclass(frozen=True)
class HtmlListSpec:
    page_url: str
    item_selector: str
    title_selector: str | None
    summary_selector: str | None
    max_items: int


def build_html_list_url(
    *,
    page_url: str,
    item_selector: str,
    title_selector: str | None = None,
    summary_selector: str | None = None,
    max_items: int = 30,
) -> str:
    params: dict[str, str] = {
        "url": page_url,
        "item_selector": item_selector,
        "max_items": str(max_items),
    }
    if title_selector:
        params["title_selector"] = title_selector
    if summary_selector:
        params["summary_selector"] = summary_selector
    # NOTE: underscores are not valid in URL schemes; use a dash so stdlib parsers work.
    return f"html-list://local?{urlencode(params)}"


def parse_html_list_url(url: str) -> HtmlListSpec:
    if url.startswith("html_list://"):
        # Back-compat for early experiments (invalid URL scheme).
        url = "html-list://" + url[len("html_list://") :]

    parts = urlsplit(url)
    if parts.scheme != "html-list":
        raise ValueError(f"invalid scheme for html_list: {parts.scheme}")
    qs = parse_qs(parts.query)

    page_url = (qs.get("url") or [""])[0].strip()
    if not page_url:
        raise ValueError("missing url param")

    item_selector = (qs.get("item_selector") or [""])[0].strip()
    if not item_selector:
        raise ValueError("missing item_selector param")

    title_selector = (qs.get("title_selector") or [""])[0].strip() or None
    summary_selector = (qs.get("summary_selector") or [""])[0].strip() or None
    max_items_raw = (qs.get("max_items") or ["30"])[0].strip()
    try:
        max_items = int(max_items_raw)
    except ValueError as exc:
        raise ValueError(f"invalid max_items: {max_items_raw!r}") from exc
    max_items = max(1, min(200, max_items))

    return HtmlListSpec(
        page_url=page_url,
        item_selector=item_selector,
        title_selector=title_selector,
        summary_selector=summary_selector,
        max_items=max_items,
    )


class HtmlListConnector(Connector):
    type = "html_list"

    def __init__(self, *, timeout_seconds: int = 20):
        self.timeout_seconds = timeout_seconds

    async def fetch(self, *, url: str, cookie_header: str | None = None) -> list[FetchedEntry]:
        spec = parse_html_list_url(url)
        page_parts = urlsplit(spec.page_url)
        if page_parts.scheme == "file":
            html = Path(page_parts.path).read_text(encoding="utf-8")
        else:
            headers: dict[str, str] = {"User-Agent": "tracker/0.1"}
            cookie = (cookie_header or "").strip()
            if cookie:
                headers["Cookie"] = cookie
            async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
                resp = await client.get(spec.page_url, headers=headers)
                final_url = str(getattr(resp, "url", spec.page_url) or spec.page_url)
                if resp.status_code in {401, 403} or looks_like_login_redirect(
                    original_url=spec.page_url, final_url=final_url
                ):
                    raise AuthRequiredError(
                        url=spec.page_url,
                        status_code=resp.status_code,
                        final_url=final_url,
                    )
                resp.raise_for_status()
                html = resp.text

        soup = BeautifulSoup(html, "html.parser")
        nodes = soup.select(spec.item_selector)

        entries: list[FetchedEntry] = []
        seen: set[str] = set()
        for node in nodes:
            # Prefer the title selector as the canonical link anchor when provided.
            # This avoids picking unrelated anchors inside an item card (e.g., "Sponsor" links on GitHub Trending).
            anchor = None
            if spec.title_selector:
                cand = node.select_one(spec.title_selector)
                if cand is not None and getattr(cand, "name", None) == "a" and cand.get("href"):
                    anchor = cand
                elif cand is not None:
                    anchor = cand.select_one("a[href]") if hasattr(cand, "select_one") else None
            if not anchor:
                anchor = node if getattr(node, "name", None) == "a" else node.select_one("a[href]")
            if not anchor:
                continue

            href = (anchor.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            if href.lower().startswith(("javascript:", "mailto:", "tel:")):
                continue

            link = urljoin(spec.page_url, href)
            if link in seen:
                continue
            seen.add(link)

            title_node = None
            if spec.title_selector:
                title_node = node.select_one(spec.title_selector)
            title = (title_node.get_text(" ", strip=True) if title_node else anchor.get_text(" ", strip=True)).strip()

            summary = None
            if spec.summary_selector:
                s = node.select_one(spec.summary_selector)
                if s:
                    summary = s.get_text(" ", strip=True).strip() or None

            entries.append(FetchedEntry(url=link, title=title, summary=summary))
            if len(entries) >= spec.max_items:
                break

        return entries
