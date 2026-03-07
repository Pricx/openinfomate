from __future__ import annotations

from urllib.parse import urljoin, urlsplit
import warnings

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning


def _looks_like_comment_feed(url: str) -> bool:
    parts = urlsplit(url)
    path = (parts.path or "").lower()
    query = (parts.query or "").lower()

    # Common patterns (WordPress, etc.)
    if "/comments/" in path:
        return True
    if "comments/feed" in path:
        return True
    if "comment" in query and "feed" in query:
        return True
    if "withcomments=1" in query:
        return True
    return False


def _looks_like_github_commits_feed(url: str) -> bool:
    parts = urlsplit(url)
    host = (parts.netloc or "").lower()
    path = (parts.path or "").lower()
    if host != "github.com":
        return False
    if "/commits/" not in path:
        return False
    # Typical: /owner/repo/commits/main.atom
    if not path.endswith(".atom"):
        return False
    return True


def looks_like_comment_feed_url(url: str) -> bool:
    """
    Heuristic filter to remove "comment feeds" (usually low-signal for discovery).
    """
    return _looks_like_comment_feed(url)


def _looks_like_comment_title(title: object) -> bool:
    if not title:
        return False
    t = str(title).strip().lower()
    return bool(t) and "comment" in t


def _has_rel_alternate(rel_value) -> bool:  # type: ignore[no-untyped-def]
    if not rel_value:
        return False
    if isinstance(rel_value, str):
        return rel_value.lower() == "alternate"
    try:
        return any(str(v).lower() == "alternate" for v in rel_value)
    except Exception:
        return False


def _looks_like_feed_type(type_value: str | None) -> bool:
    if not type_value:
        return False
    t = type_value.lower()
    return "rss" in t or "atom" in t or t.endswith("+xml")


def _looks_like_xml_document(html: str) -> bool:
    head = (html or "").lstrip()[:256].lower()
    return head.startswith("<?xml") or head.startswith("<rss") or head.startswith("<feed")


def _make_discovery_soup(html: str) -> BeautifulSoup:
    raw = html or ""
    if _looks_like_xml_document(raw):
        try:
            return BeautifulSoup(raw, "xml")
        except Exception:
            pass
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", XMLParsedAsHTMLWarning)
        return BeautifulSoup(raw, "html.parser")


def discover_feed_urls_from_html(*, page_url: str, html: str) -> list[str]:
    """
    Extract RSS/Atom feed URLs from a HTML document.

    This is a best-effort helper for operators; it intentionally stays conservative.
    """
    soup = _make_discovery_soup(html)
    found: list[str] = []
    seen: set[str] = set()

    for link in soup.find_all("link"):
        if not _has_rel_alternate(link.get("rel")):
            continue
        if not _looks_like_feed_type(link.get("type")):
            continue
        if _looks_like_comment_title(link.get("title")):
            continue
        href = (link.get("href") or "").strip()
        if not href:
            continue
        u = urljoin(page_url, href)
        if _looks_like_comment_feed(u):
            continue
        if _looks_like_github_commits_feed(u):
            continue
        if u in seen:
            continue
        seen.add(u)
        found.append(u)

    return sorted(found)
