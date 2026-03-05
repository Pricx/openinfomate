from __future__ import annotations

import datetime as dt
import re
from zoneinfo import ZoneInfo


_TZ_OFFSET_RE = re.compile(
    r"^\s*(?:UTC|GMT)?\s*([+-])\s*(\d{1,2})(?:(?::?)(\d{2}))?\s*$",
    flags=re.IGNORECASE,
)


def resolve_cron_timezone(name: str) -> tuple[dt.tzinfo, bool]:
    """
    Resolve TRACKER_CRON_TIMEZONE into a tzinfo.

    Supports:
    - IANA names (e.g. "UTC", "Asia/Shanghai")
    - UTC offsets (e.g. "+8", "-8", "UTC+8", "+08:00", "-0530")
    """
    raw = (name or "").strip()
    if not raw or raw.upper() in {"UTC", "Z"}:
        return dt.timezone.utc, True

    m = _TZ_OFFSET_RE.match(raw)
    if m:
        sign = 1 if m.group(1) == "+" else -1
        try:
            hours = int(m.group(2) or "0")
            minutes = int(m.group(3) or "0")
        except Exception:
            hours = 0
            minutes = 0
        if 0 <= hours <= 23 and 0 <= minutes <= 59:
            total_minutes = sign * (hours * 60 + minutes)
            return dt.timezone(dt.timedelta(minutes=total_minutes)), True

    # Also allow bare hour offsets like "8" / "-8".
    try:
        if raw.lstrip("+-").isdigit():
            hours2 = int(raw)
            if -23 <= hours2 <= 23:
                return dt.timezone(dt.timedelta(hours=hours2)), True
    except Exception:
        pass

    try:
        return ZoneInfo(raw), True
    except Exception:
        return dt.timezone.utc, False

