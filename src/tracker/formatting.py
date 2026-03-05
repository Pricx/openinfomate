from __future__ import annotations

import datetime as dt
from tracker.timezones import resolve_cron_timezone
import re
from urllib.parse import urlsplit

from tracker.llm import LlmDigestSummary
from tracker.models import Item, ItemTopic, Topic


_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "how",
    "in",
    "into",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "out",
    "re",
    "s",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "we",
    "were",
    "what",
    "when",
    "where",
    "which",
    "who",
    "will",
    "with",
    "you",
    "your",
}


def _domain(url: str) -> str:
    try:
        parts = urlsplit(url)
        return parts.netloc.lower() or parts.scheme.lower() or "unknown"
    except Exception:
        return "unknown"


_WORD_RE = re.compile(r"[a-zA-Z0-9]{3,}")


def _terms(text: str) -> list[str]:
    terms: list[str] = []
    for raw in _WORD_RE.findall(text or ""):
        t = raw.lower()
        if t in _STOPWORDS:
            continue
        terms.append(t)
    return terms


def extract_llm_summary_why(reason: str) -> tuple[str, str]:
    summary = ""
    why = ""
    for raw in (reason or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        low = line.lower()
        if low.startswith("llm_summary"):
            if ":" in line:
                summary = line.split(":", 1)[1].strip()
            elif "=" in line:
                summary = line.split("=", 1)[1].strip()
        elif low.startswith("llm_why"):
            if ":" in line:
                why = line.split(":", 1)[1].strip()
            elif "=" in line:
                why = line.split("=", 1)[1].strip()
    return summary, why


def format_digest_markdown(
    *,
    topic: Topic,
    items: list[tuple[ItemTopic, Item]],
    since: dt.datetime,
    until: dt.datetime | None = None,
    tz_name: str = "UTC",
    lang: str = "en",
    url_overrides_by_item_id: dict[int, str] | None = None,
    previous_total: int | None = None,
    previous_alerts: int | None = None,
    previous_items: list[tuple[ItemTopic, Item]] | None = None,
    llm_summary: LlmDigestSummary | None = None,
) -> str:
    def _norm_lang(v: str) -> str:
        raw = (v or "").strip()
        s = raw.lower()
        if raw in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语"}:
            return "zh"
        if s in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or s.startswith("zh"):
            return "zh"
        if s in {"en", "en-us", "english", "英文"} or s.startswith("en"):
            return "en"
        return "en"

    def _fmt_ts_local(ts_utc: dt.datetime) -> str:
        """
        Render a local timestamp at minute precision.

        Why: Curated Info is a batch surface; second/millisecond precision is noise for humans.
        """
        try:
            tz_raw = (tz_name or "").strip()
            tz, ok = resolve_cron_timezone(tz_raw)
            if not ok:
                tz = dt.timezone.utc
            local = ts_utc.replace(tzinfo=dt.timezone.utc).astimezone(tz)
            # Minute precision; avoid seconds/microseconds noise.
            return local.replace(second=0, microsecond=0).isoformat(timespec="minutes")
        except Exception:
            # Fallback: still trim to minutes.
            try:
                return ts_utc.replace(second=0, microsecond=0).isoformat(timespec="minutes")
            except Exception:
                return str(ts_utc)

    def _window_line(since_utc: dt.datetime, until_utc: dt.datetime | None) -> str:
        tz_raw = (tz_name or "").strip()
        until2 = until_utc or dt.datetime.utcnow()
        a = _fmt_ts_local(since_utc)
        b = _fmt_ts_local(until2)
        is_zh2 = _norm_lang(lang) == "zh"
        label = "窗口" if is_zh2 else "Window"
        if tz_raw and tz_raw.upper() != "UTC":
            return f"{label}: {a}–{b} ({tz_raw})"
        return f"{label}: {a}–{b} UTC"

    current_total = len(items)
    current_alerts = sum(1 for it, _item in items if it.decision == "alert")
    current_digests = max(0, int(current_total) - int(current_alerts))

    def _short(text: str, limit: int) -> str:
        s = (text or "").strip()
        # Avoid "information -> conclusion" style in pushes; keep outputs as factual sentences.
        s = s.replace("->", "—").replace("→", "—")
        if len(s) <= limit:
            return s
        return s[:limit] + "…"

    def _decision_label(decision: str) -> str:
        d = (decision or "").strip().lower()
        if not is_zh:
            return d or "digest"
        if d == "alert":
            return "告警"
        if d == "digest":
            return "摘要"
        if d:
            return d
        return "摘要"

    # Digest is a de-dupe/aggregation surface. By default it should NOT "interpret" items.
    # Only switch to an interpretive format when an explicit LLM digest summary is present.
    brief = bool(llm_summary is not None)

    is_zh = _norm_lang(lang) == "zh"
    title = "Curated Info" if not is_zh else "参考消息"
    header = f"# {topic.name} — {title}\n\n{_window_line(since, until)}\n"
    items_label = "Items" if not is_zh else "条目"
    alerts_label = "alerts" if not is_zh else "告警"
    digests_label = "digests" if not is_zh else "摘要"
    header += f"{items_label}: {current_total} ({current_alerts} {alerts_label}, {current_digests} {digests_label})\n\n"
    if not items:
        return header + ("_No new items._\n" if not is_zh else "_暂无新条目。_\n")

    lines: list[str] = [header]

    # Curated Info is a de-dupe/batch surface. It should not "interpret" or add meta-analytics.
    # Keep the body as a clean full list.

    if llm_summary is not None:
        lines.append(("## Executive Summary\n" if not is_zh else "## 执行摘要\n"))
        lines.append(llm_summary.summary.strip() + "\n")

        if llm_summary.highlights:
            lines.append("### Highlights" if not is_zh else "### 亮点")
            for h in llm_summary.highlights:
                lines.append(f"- {h}")
            lines.append("")

        if llm_summary.risks:
            lines.append("### Risks" if not is_zh else "### 风险")
            for r in llm_summary.risks:
                lines.append(f"- {r}")
            lines.append("")

    # Prefer ALERT items first in digests (still non-interpretive).
    items = sorted(
        items,
        key=lambda row: (
            0 if row[0].decision == "alert" else 1,
            -(
                int((row[1].published_at or row[1].created_at).timestamp())
                if (row[1].published_at or row[1].created_at)
                else 0
            ),
        ),
    )

    lines.append(("## Items\n" if not is_zh else "## 条目\n"))

    refs: list[tuple[int, str, str]] = []
    for ref_i, (it, item) in enumerate(items, start=1):
        u = (
            (url_overrides_by_item_id.get(int(getattr(item, "id", 0) or 0)) or "").strip()
            if url_overrides_by_item_id
            else ""
        )
        if not u:
            u = (item.canonical_url or "").strip()
        refs.append((ref_i, (item.title or "").strip(), u))
        if not brief:
            cite = f" [{ref_i}]" if refs[-1][2] else ""
            dec = _decision_label(str(it.decision or ""))
            tail = f"（{dec}）" if is_zh else f"({dec})"
            lines.append(f"- {item.title}{cite} {tail}".strip())
            continue

        prefix = "[ALERT] " if it.decision == "alert" else ""
        s, _w = extract_llm_summary_why(it.reason or "")
        s = _short(s, 180)
        extra = ""
        if s:
            extra = f" — {s}"
        cite = f" [{ref_i}]" if refs[-1][2] else ""
        lines.append(f"- {prefix}{item.title}{cite}{extra}".strip())

    if refs:
        lines.append("")
        lines.append("References:")
        for ref_i, title2, url2 in refs:
            if not url2:
                continue
            title3 = title2 if title2 else f"Item {ref_i}"
            lines.append(f"[{ref_i}] {title3} — {url2}")

    return "\n".join(lines).strip() + "\n"


def format_im_text(markdown: str) -> str:
    """
    Best-effort conversion of Markdown-ish tracker output into IM-friendly plain text.

    Telegram pushes use `sendMessage` without `parse_mode`, so headings/tables should be rendered as simple text.
    """
    md = (markdown or "").strip()
    if not md:
        return ""

    out: list[str] = []
    for raw in md.splitlines():
        line = (raw or "").rstrip()
        s = line.strip()
        if s.startswith("## "):
            title = s[3:].strip()
            if title:
                # Add a blank line before a new section (except at the very beginning).
                if out and out[-1].strip():
                    out.append("")
                out.append(f"【{title}】")
            continue
        if s.startswith("### "):
            title = s[4:].strip()
            if title:
                out.append(f"- {title}")
            continue
        out.append(line)

    # Trim leading/trailing blank lines and collapse excessive vertical whitespace.
    cleaned: list[str] = []
    blank_run = 0
    for ln in out:
        if not (ln or "").strip():
            blank_run += 1
            if blank_run <= 1:
                cleaned.append("")
            continue
        blank_run = 0
        cleaned.append(ln.rstrip())

    return "\n".join([ln for ln in cleaned]).strip() + "\n"
