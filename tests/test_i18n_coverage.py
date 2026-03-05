from __future__ import annotations

import re
from pathlib import Path

from tracker.i18n import ZH_TRANSLATIONS


# NOTE: use a word boundary so we don't match JS like split(',') or createElement('div').
_T_CALL_RE = re.compile(r"""\bt\(\s*["']([^"']+)["']\s*\)""")


def test_i18n_templates_have_zh_translations():
    """
    Ensure all user-facing template strings have a zh translation entry.

    This prevents UI regressions where new text appears untranslated in 中文 mode.
    """
    root = Path(__file__).resolve().parents[1]
    tpl_dir = root / "src" / "tracker" / "templates"
    assert tpl_dir.is_dir()

    keys: set[str] = set()
    for path in sorted(tpl_dir.glob("*.html")):
        text = path.read_text(encoding="utf-8")
        for m in _T_CALL_RE.finditer(text):
            k = (m.group(1) or "").strip()
            if k:
                keys.add(k)

    missing = sorted([k for k in keys if k not in ZH_TRANSLATIONS])
    assert missing == [], "Missing zh translations: " + ", ".join(missing)
