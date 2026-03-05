from __future__ import annotations

import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup


_ABS_URL_RE = re.compile(r"https?://[^\"'\\s<>]+")
_REL_URL_RE = re.compile(r"(?P<q>['\"])(?P<url>/[^'\"]+)(?P=q)")


def _looks_like_api(url: str) -> bool:
    u = (url or "").strip().lower()
    if not u:
        return False
    if "/api/" in u:
        return True
    if "wp-json" in u:
        return True
    if "/graphql" in u or "graphql" in u:
        return True
    if u.endswith(".json") or ".json?" in u:
        return True
    return False


def discover_api_urls_from_html(*, page_url: str, html: str) -> list[str]:
    """
    Best-effort extraction of likely web API endpoints from a HTML document.

    This does *not* call the endpoints; it's an operator helper to find candidates.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    found: list[str] = []
    seen: set[str] = set()

    def _add(raw: str) -> None:
        u = urljoin(page_url, (raw or "").strip())
        if not (u.startswith("http://") or u.startswith("https://")):
            return
        if not _looks_like_api(u):
            return
        if u in seen:
            return
        seen.add(u)
        found.append(u)

    for tag in soup.find_all(["script", "link", "a"]):
        attr = "src" if tag.name == "script" else "href"
        raw = (tag.get(attr) or "").strip()
        if raw:
            _add(raw)

    for script in soup.find_all("script"):
        if script.get("src"):
            continue
        text = script.string or script.get_text() or ""
        for m in _ABS_URL_RE.finditer(text):
            _add(m.group(0))
        for m in _REL_URL_RE.finditer(text):
            _add(m.group("url"))

    return sorted(found)

