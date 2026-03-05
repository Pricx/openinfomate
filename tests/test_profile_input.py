from __future__ import annotations

from tracker.profile_input import normalize_profile_text


def test_normalize_profile_text_extracts_bookmarks_from_html():
    html = """
    <!DOCTYPE html>
    <html><body>
      <a href="https://example.com/a">Example A</a>
      <a href="mailto:test@example.com">Mail</a>
      <a href="https://example.com/b"> Example   B </a>
    </body></html>
    """

    out = normalize_profile_text(text=html, max_links=100, max_chars=10_000)
    assert "BOOKMARKS" in out
    assert "<html" not in out.lower()
    assert "Example A | https://example.com/a" in out
    assert "Example B | https://example.com/b" in out
    assert "mailto:" not in out


def test_normalize_profile_text_keeps_plain_text():
    txt = "  hello\\nworld  "
    out = normalize_profile_text(text=txt)
    assert out == "hello\\nworld"

