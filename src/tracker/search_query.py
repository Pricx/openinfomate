from __future__ import annotations

import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


_SEARCH_WS_RE = re.compile(r"\s+")


def normalize_search_query(query: str) -> str:
    """
    Convert v1 comma-separated keyword strings into a more search-engine-friendly query.
    """
    q = (query or "").strip()
    if "," in q:
        q = " ".join(part.strip() for part in q.split(",") if part.strip())
    q = _SEARCH_WS_RE.sub(" ", q)
    return q.strip()


def rewrite_query_param(*, url: str, param: str) -> str:
    """
    Rewrite a URL's query parameter using `normalize_search_query`.

    Only rewrites when the target param exists and the normalized value differs.
    """
    u = (url or "").strip()
    if not u:
        return url

    parts = urlsplit(u)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    if not pairs:
        return url

    changed = False
    out: list[tuple[str, str]] = []
    for k, v in pairs:
        if k == param:
            nv = normalize_search_query(v)
            if nv != v:
                changed = True
            out.append((k, nv))
        else:
            out.append((k, v))

    if not changed:
        return url

    new_query = urlencode(out)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


def set_query_param(*, url: str, param: str, query: str) -> str:
    """
    Set a URL's query parameter to the normalized `query`.

    - If the param exists, it is rewritten.
    - If the param does not exist, it is added.
    """
    u = (url or "").strip()
    if not u:
        return url

    parts = urlsplit(u)
    pairs = parse_qsl(parts.query, keep_blank_values=True)
    desired = normalize_search_query(query)

    changed = False
    out: list[tuple[str, str]] = []
    found = False
    for k, v in pairs:
        if k == param:
            found = True
            if v != desired:
                changed = True
            out.append((k, desired))
        else:
            out.append((k, v))

    if not found:
        out.append((param, desired))
        changed = True

    if not changed:
        return url

    new_query = urlencode(out)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))
