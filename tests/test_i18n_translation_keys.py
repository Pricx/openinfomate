from __future__ import annotations

import re
from pathlib import Path

from tracker.i18n import ZH_TRANSLATIONS


def test_all_template_translation_keys_exist_in_zh_dict():
    root = Path(__file__).resolve().parents[1]
    template_dir = root / "src" / "tracker" / "templates"
    assert template_dir.is_dir()

    key_re = re.compile(r"\bt\(\s*(['\"])(.+?)\1\s*\)")
    keys: set[str] = set()
    for path in template_dir.glob("*.html"):
        text = path.read_text(encoding="utf-8")
        keys.update(m.group(2) for m in key_re.finditer(text))

    missing = sorted(k for k in keys if k not in ZH_TRANSLATIONS)
    assert missing == []
