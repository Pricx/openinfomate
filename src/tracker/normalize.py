from __future__ import annotations

import hashlib
import re
import warnings
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning


_TRACKING_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "gclid",
    "fbclid",
    "yclid",
    "mc_cid",
    "mc_eid",
    "ref",
}


def canonicalize_url(url: str) -> str:
    raw = (url or "").strip()
    if not raw:
        return ""

    parts = urlsplit(raw)

    scheme = (parts.scheme or "https").lower()
    # For dedupe, prefer https when a URL is http(s).
    if scheme in {"http", "https"}:
        scheme = "https"

    host = (parts.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]

    port = parts.port
    netloc = host or (parts.netloc or "").lower()
    # Drop default ports for cleaner canonical URLs.
    if port and port not in {80, 443} and host:
        netloc = f"{host}:{port}"

    path = parts.path or "/"
    # Normalize trivial trailing slash differences (except root).
    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    fragment = ""

    query_pairs = [(k, v) for (k, v) in parse_qsl(parts.query, keep_blank_values=True)]
    filtered = [(k, v) for (k, v) in query_pairs if k.lower() not in _TRACKING_KEYS]
    filtered.sort(key=lambda kv: (kv[0], kv[1]))
    query = urlencode(filtered, doseq=True)

    return urlunsplit((scheme, netloc, path, query, fragment))


_WS_RE = re.compile(r"\s+")


def normalize_text(text: str) -> str:
    cleaned = text.replace("\u00a0", " ").strip()
    cleaned = _WS_RE.sub(" ", cleaned)
    return cleaned


def html_to_text(html: str) -> str:
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
        soup = BeautifulSoup(html or "", "html.parser")
    return normalize_text(soup.get_text(" "))


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
