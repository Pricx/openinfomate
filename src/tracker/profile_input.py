from __future__ import annotations

from bs4 import BeautifulSoup


def _looks_like_html(text: str) -> bool:
    s = (text or "").strip()
    if not s:
        return False
    low = s.lower()
    if "<html" in low or "<!doctype" in low:
        return True
    # Bookmark exports are often anchor-heavy HTML without full markup.
    if "<a" in low and "href" in low:
        return True
    return False


def normalize_profile_text(*, text: str, max_links: int = 400, max_chars: int = 20_000) -> str:
    """
    Normalize arbitrary profile input into a compact text form suitable for LLM prompting.

    - If the input looks like an HTML bookmarks export, extract anchor titles + URLs.
    - Otherwise, keep the raw text (trimmed).

    Safety: never returns HTML; output is always plain text.
    """
    raw = (text or "").strip()
    if not raw:
        return ""

    out = raw
    if _looks_like_html(raw):
        soup = BeautifulSoup(raw, "html.parser")
        links: list[tuple[str, str]] = []
        seen: set[str] = set()
        for a in soup.select("a[href]"):
            href = (a.get("href") or "").strip()
            if not href or href.startswith("#"):
                continue
            low = href.lower()
            if low.startswith(("javascript:", "mailto:", "tel:")):
                continue
            if href in seen:
                continue
            seen.add(href)
            title = (a.get_text(" ", strip=True) or "").strip()
            links.append((title, href))
            if len(links) >= max_links:
                break

        if links:
            lines = [f"BOOKMARKS ({len(links)} links):"]
            for title, href in links:
                t = " ".join((title or "").split()).strip()
                if not t:
                    t = href
                lines.append(f"- {t} | {href}")
            out = "\n".join(lines)

    out = out.strip()
    if len(out) <= max_chars:
        return out
    return out[:max_chars] + "…"

