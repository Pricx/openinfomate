from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

_LOGIN_PATH_RE = re.compile(r"/(login|signin|sign-in|auth|oauth|sso)(/|$)", re.IGNORECASE)


def _norm_host(value: str) -> str:
    h = (value or "").strip().lower()
    if not h:
        return ""
    # Allow operators to paste full URLs.
    if "://" in h:
        try:
            h = urlsplit(h).netloc.lower()
        except Exception:
            pass
    h = h.split("/", 1)[0]
    h = h.split(":", 1)[0]
    h = h.lstrip(".")
    if h.startswith("www."):
        h = h[4:]
    return h


def _host_matches(host: str, pattern: str) -> bool:
    h = _norm_host(host)
    p = _norm_host(pattern)
    if not (h and p):
        return False
    if h == p:
        return True
    return h.endswith("." + p)


def parse_domains_csv(raw: str) -> list[str]:
    text = (raw or "").strip()
    if not text:
        return []
    out: list[str] = []
    for part in text.replace("\n", ",").split(","):
        p = _norm_host(part)
        if p:
            out.append(p)
    return out


def host_matches_any(*, host: str, patterns: list[str]) -> bool:
    h = _norm_host(host)
    if not h or not patterns:
        return False
    return any(_host_matches(h, p) for p in patterns if p)


def _best_cookie_for_host(host: str, jar: dict[str, str]) -> str | None:
    """
    Return the best-matching cookie header for `host` from the jar (longest suffix match).
    """
    h = _norm_host(host)
    if not h or not jar:
        return None
    best_key = ""
    best_val = ""
    for k, v in (jar or {}).items():
        if not v:
            continue
        if not _host_matches(h, k):
            continue
        kk = _norm_host(k)
        if len(kk) > len(best_key):
            best_key = kk
            best_val = str(v or "").strip()
    return best_val or None


def parse_cookie_jar_json(raw: str) -> dict[str, str]:
    """
    Parse TRACKER_COOKIE_JAR_JSON (domain/url -> raw Cookie header value).

    Example:
      {"forum.example.com":"a=b; c=d", "github.com":"logged_in=yes; ..."}
    """
    text = (raw or "").strip()
    if not text:
        return {}
    try:
        obj = json.loads(text)
    except Exception:
        logger.warning("invalid TRACKER_COOKIE_JAR_JSON (expected JSON object)")
        return {}
    if not isinstance(obj, dict):
        logger.warning("invalid TRACKER_COOKIE_JAR_JSON (expected JSON object)")
        return {}
    out: dict[str, str] = {}
    for k, v in obj.items():
        key = _norm_host(str(k or ""))
        val = str(v or "").strip()
        if key and val:
            out[key] = val
    return out


def cookie_header_for_url(*, url: str, cookie_jar: dict[str, str]) -> str | None:
    """
    Find a Cookie header value for a URL based on host suffix matching.
    """
    u = (url or "").strip()
    if not u:
        return None
    try:
        host = (urlsplit(u).netloc or "").strip()
    except Exception:
        host = ""
    if not host:
        return None
    return _best_cookie_for_host(host, cookie_jar)


def looks_like_login_redirect(*, original_url: str, final_url: str) -> bool:
    """
    Heuristic: detect "redirected to login" patterns after follow_redirects=True.
    """
    o = (original_url or "").strip()
    f = (final_url or "").strip()
    if not (o and f):
        return False
    if o == f:
        return False
    try:
        f_parts = urlsplit(f)
    except Exception:
        return False
    path = (f_parts.path or "").strip()
    if not path:
        return False
    return bool(_LOGIN_PATH_RE.search(path))


@dataclass(frozen=True)
class AuthRequiredError(RuntimeError):
    """
    Raised when a fetch likely requires login / cookies.

    We keep this intentionally lightweight; the runner decides whether and how to notify/push.
    """

    url: str
    status_code: int | None = None
    final_url: str | None = None

    @property
    def host(self) -> str:
        try:
            return _norm_host(urlsplit((self.final_url or self.url or "").strip()).netloc or "")
        except Exception:
            return ""

    def meta(self) -> dict[str, str]:
        out: dict[str, str] = {"error_type": "auth_required"}
        h = self.host
        if h:
            out["host"] = h
        if self.url:
            out["url"] = self.url
        if self.final_url:
            out["final_url"] = self.final_url
        if self.status_code is not None:
            out["status_code"] = str(int(self.status_code))
        return out
