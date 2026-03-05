from __future__ import annotations

import re

from tracker.normalize import normalize_text


_GITHUB_REPO_RE = re.compile(r"(?:https?://)?github\.com/([A-Za-z0-9_.-]+)/([A-Za-z0-9_.-]+)")
_ARXIV_RE = re.compile(r"https?://arxiv\.org/(abs|pdf)/([0-9]+\.[0-9]+(?:v\d+)?|[a-z-]+/[0-9]+)")
_CVE_RE = re.compile(r"\bCVE-\d{4}-\d{4,7}\b", re.IGNORECASE)
_UPPER_DIGIT_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9-]{3,24}\b")


def _normalize_github_repo(owner: str, repo: str) -> str | None:
    o = (owner or "").strip()
    r = (repo or "").strip()
    if not (o and r):
        return None
    if r.endswith(".git"):
        r = r[: -len(".git")]
    if not (o and r):
        return None
    # Guard against common non-repo path segments.
    if o.lower() in {"topics", "search", "features", "pricing", "login", "signup", "about", "site"}:
        return None
    return f"https://github.com/{o}/{r}"


def extract_notable_links(*, text: str, url: str = "", max_links: int = 6) -> list[str]:
    """
    Extract a small list of "notable links" for LLM curation.

    Goal: preserve key anchor URLs (GitHub repo, arXiv paper) even when the snippet
    itself is truncated.
    """
    hay = "\n".join([(url or "").strip(), (text or "").strip()]).strip()
    if not hay:
        return []
    # Bound scanning cost for very large fulltext extractions.
    if len(hay) > 40_000:
        hay = hay[:20_000] + "\n...\n" + hay[-20_000:]

    out: list[str] = []
    seen: set[str] = set()

    def _add(link: str | None) -> None:
        s = (link or "").strip()
        if not s or s in seen:
            return
        seen.add(s)
        out.append(s)

    for m in _GITHUB_REPO_RE.finditer(hay):
        link = _normalize_github_repo(m.group(1), m.group(2))
        _add(link)
        if len(out) >= max_links:
            return out

    for m in _ARXIV_RE.finditer(hay):
        paper_id = (m.group(2) or "").strip()
        if paper_id:
            _add(f"https://arxiv.org/abs/{paper_id}")
        if len(out) >= max_links:
            return out

    return out


def _extract_anchor_tokens(text: str) -> list[str]:
    raw = (text or "").strip()
    if not raw:
        return []

    out: list[str] = []
    seen: set[str] = set()

    for m in _CVE_RE.finditer(raw):
        tok = (m.group(0) or "").upper().strip()
        if tok and tok not in seen:
            seen.add(tok)
            out.append(tok)
        if len(out) >= 8:
            return out

    # Catch high-salience uppercase tokens that include digits (e.g., HBM4 / GPT-5 / RTX4090).
    for m in _UPPER_DIGIT_TOKEN_RE.finditer(raw.upper()):
        tok = (m.group(0) or "").strip()
        if not tok:
            continue
        if not any(ch.isdigit() for ch in tok):
            continue
        if not any(ch.isalpha() for ch in tok):
            continue
        if tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
        if len(out) >= 8:
            return out

    return out


def story_dedupe_text(*, title: str, url: str = "", snippet: str = "") -> str:
    """
    Small, stable text used for "same story" dedupe across different outlets/URLs.

    We intentionally avoid using the full snippet body (too outlet-specific) and instead
    focus on durable anchors: title + notable links + strong tokens (CVE/HBM4/etc).
    """
    t = normalize_text(title or "")
    if not t:
        t = normalize_text(url or "")

    links = extract_notable_links(text=snippet or "", url=url or "", max_links=4)
    anchors = _extract_anchor_tokens(" ".join([title or "", snippet or ""]))

    # Only enable story-level dedupe when there is at least one non-trivial anchor,
    # otherwise generic titles can over-collapse unrelated items.
    if not links and not anchors:
        return ""

    parts: list[str] = []
    if t:
        parts.append(t)
    if links:
        parts.append(" ".join(links))
    if anchors:
        parts.append(" ".join(anchors))
    return normalize_text("\n".join([p for p in parts if p]).strip())
