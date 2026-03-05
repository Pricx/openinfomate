from __future__ import annotations

import base64
import re
from urllib.parse import parse_qs, unquote, urlsplit

_URL_RE = re.compile(r"https?://[^\s)\]]+", re.IGNORECASE)


def unwrap_tracking_url(url: str) -> str:
    """
    Best-effort unwrap common tracking/redirect URLs into their destination URL.

    This is used in report outputs so citations point to primary sources
    (not search engine click trackers like `bing.com/ck/...`).
    """
    raw = (url or "").strip()
    if not raw:
        return ""

    # Strip common trailing punctuation from markdown/prose.
    trimmed = raw.rstrip("]).,;:")

    try:
        parts = urlsplit(trimmed)
    except Exception:
        return trimmed

    host = (parts.netloc or "").lower()
    path = parts.path or ""
    qs = parse_qs(parts.query or "")

    # Bing click tracker: https://www.bing.com/ck/a?...&u=a1aHR0cHM6Ly8...&ntb=1
    if host.endswith("bing.com") and path.startswith("/ck/"):
        enc = ""
        if qs.get("u"):
            enc = str(qs.get("u", [""])[0] or "")
        if enc:
            # Bing uses a "a1" prefix before base64.
            b64 = enc[2:] if enc.startswith("a1") else enc
            pad = "=" * ((4 - (len(b64) % 4)) % 4)
            for decoder in (base64.b64decode, base64.urlsafe_b64decode):
                try:
                    out = decoder(b64 + pad).decode("utf-8", errors="ignore").strip()
                except Exception:
                    continue
                if out.startswith(("http://", "https://")):
                    return out

    # DuckDuckGo redirect: https://duckduckgo.com/l/?uddg=<urlencoded>
    if host.endswith("duckduckgo.com") and path.startswith("/l/"):
        uddg = qs.get("uddg")
        if uddg:
            out = unquote(str(uddg[0] or "")).strip()
            if out.startswith(("http://", "https://")):
                return out

    # Google redirect: https://www.google.com/url?q=<urlencoded>
    if host.endswith("google.com") and path == "/url":
        q = qs.get("q") or qs.get("url")
        if q:
            out = unquote(str(q[0] or "")).strip()
            if out.startswith(("http://", "https://")):
                return out

    return trimmed


def unwrap_urls_in_markdown(markdown: str) -> str:
    """
    Rewrite URLs in markdown text, unwrapping tracking URLs when detected.
    """
    text = markdown or ""
    if not text:
        return text

    def _replace(match: re.Match[str]) -> str:
        u = match.group(0) or ""
        stripped = u.rstrip("]).,;:")
        suffix = u[len(stripped) :]
        unwrapped = unwrap_tracking_url(stripped)
        if unwrapped and unwrapped != stripped:
            return unwrapped + suffix
        return u

    return _URL_RE.sub(_replace, text)
