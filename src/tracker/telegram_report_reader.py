from __future__ import annotations

import base64
import re
from dataclasses import dataclass

from tracker.push.telegram import split_telegram_message

_MD_LINK_RE = re.compile(r"\[([^\]\n]+)\]\(([^)\s]+)\)")
_REF_ENTRY_RE = re.compile(r"^\[(\d+)\]\s+(.*?)\s+(?:—|-)\s+(https?://\S+)\s*$")


def encode_reader_callback_key(report_key: str) -> str:
    key = (report_key or "").strip()
    if not key:
        return ""
    token = base64.urlsafe_b64encode(key.encode("utf-8")).decode("ascii").rstrip("=")
    return token if len(token) <= 40 else ""


def decode_reader_callback_key(token: str) -> str:
    raw = (token or "").strip()
    if not raw:
        return ""
    try:
        padded = raw + ("=" * (-len(raw) % 4))
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8").strip()
    except Exception:
        return ""


def reader_callback_data(*, report_key: str, action: str, parts: list[object] | tuple[object, ...] = ()) -> str:
    act = (action or "").strip().lower() or "toc"
    extra = [str(part) for part in (parts or [])]
    token = encode_reader_callback_key(report_key)
    if token:
        return ":".join(["brk", token, act, *extra])
    return ":".join(["br", act, *extra])


def _escape_html(text: str) -> str:
    # Telegram HTML parse mode supports a small subset of tags; escape everything else.
    s = str(text or "")
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _escape_html_attr(text: str) -> str:
    s = str(text or "")
    return (
        s.replace("&", "&amp;")
        .replace('"', "&quot;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _escape_html_with_links(text: str) -> str:
    """
    Escape text for Telegram HTML parse mode, but convert Markdown links into <a href>.

    Why: tracker reports are Markdown-ish, but Telegram doesn't render Markdown by default.
    """
    s = str(text or "")
    if not s:
        return ""
    out: list[str] = []
    last = 0
    for m in _MD_LINK_RE.finditer(s):
        if m.start() > last:
            out.append(_escape_html(s[last : m.start()]))
        label = (m.group(1) or "").strip()
        url = (m.group(2) or "").strip()
        low = url.lower()
        if low.startswith("http://") or low.startswith("https://"):
            out.append(f'<a href="{_escape_html_attr(url)}">{_escape_html(label)}</a>')
        else:
            out.append(_escape_html(m.group(0)))
        last = m.end()
    if last < len(s):
        out.append(_escape_html(s[last:]))
    return "".join(out)


def _short(text: str, n: int) -> str:
    s = " ".join((text or "").strip().split())
    if len(s) <= n:
        return s
    return s[: max(0, n - 1)] + "…"


@dataclass(frozen=True)
class ReportSection:
    title: str
    body: str


@dataclass(frozen=True)
class ReportDoc:
    title: str
    sections: list[ReportSection]
    references: str


def parse_report_markdown(markdown: str) -> ReportDoc:
    md = (markdown or "").strip()
    if not md:
        return ReportDoc(title="", sections=[], references="")

    lines = md.splitlines()
    title = ""
    if lines and lines[0].strip().startswith("# "):
        title = lines[0].strip()[2:].strip()
        lines = lines[1:]
        while lines and not (lines[0] or "").strip():
            lines = lines[1:]

    body = "\n".join(lines).strip()

    # Split trailing References block.
    refs = ""
    if body:
        body_lines = body.splitlines()
        ref_idx = -1
        for i in range(len(body_lines) - 1, -1, -1):
            if (body_lines[i] or "").strip().startswith("References:"):
                ref_idx = i
                break
        if ref_idx >= 0:
            refs = "\n".join(body_lines[ref_idx:]).strip()
            body = "\n".join(body_lines[:ref_idx]).strip()

    sections: list[ReportSection] = []
    cur_title = ""
    cur_lines: list[str] = []
    for raw in (body or "").splitlines():
        s = (raw or "").rstrip()
        st = s.strip()
        if st.startswith("## "):
            if cur_title or any((ln or "").strip() for ln in cur_lines):
                sections.append(ReportSection(title=cur_title.strip(), body="\n".join(cur_lines).strip()))
            cur_title = st[3:].strip()
            cur_lines = []
            continue
        cur_lines.append(s)
    if cur_title or any((ln or "").strip() for ln in cur_lines):
        sections.append(ReportSection(title=cur_title.strip(), body="\n".join(cur_lines).strip()))

    # If there are no explicit sections, treat the whole body as one unnamed section.
    if not sections and body:
        sections = [ReportSection(title="", body=body.strip())]

    return ReportDoc(title=title.strip(), sections=sections, references=refs.strip())


def _im_sanitize_block(text: str) -> str:
    """
    Convert Markdown-ish content into IM-friendly plain text (no `#` headings or tables).
    """
    md = (text or "").strip()
    if not md:
        return ""

    out: list[str] = []
    for raw in md.splitlines():
        line = (raw or "").rstrip()
        s = line.strip()
        if s.startswith(("```", "References:")):
            out.append(line)
            continue
        if s.startswith("### "):
            out.append(f"【{s[4:].strip()}】")
            continue
        if s.startswith("#### "):
            out.append(f"- {s[5:].strip()}")
            continue
        if s.startswith("|") and s.endswith("|"):
            # Tables are painful on mobile; keep as a single line.
            out.append(" ".join([p.strip() for p in s.strip("|").split("|") if p.strip()]))
            continue
        if s.startswith(("- ", "* ")):
            out.append("⦁ " + s[2:].strip())
            continue
        out.append(line)

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

    return "\n".join([ln for ln in cleaned]).strip()


def _extract_takeaways(doc: ReportDoc, *, max_items: int = 3) -> list[str]:
    # Prefer a section that looks like "Key Takeaways".
    idx = 0
    for i, sec in enumerate(doc.sections):
        t = (sec.title or "").casefold()
        if any(k in t for k in ("重点摘要", "key takeaways", "executive summary", "summary")):
            idx = i
            break

    body = doc.sections[idx].body if doc.sections else ""
    body2 = _im_sanitize_block(body)
    items: list[str] = []
    for ln in (body2 or "").splitlines():
        m = re.match(r"^\s*\d+\.\s+(.*)$", (ln or "").strip())
        if not m:
            continue
        v = (m.group(1) or "").strip()
        if v:
            items.append(v)
        if len(items) >= max_items:
            break
    if items:
        return items

    # Fallback: first non-empty lines.
    for ln in (body2 or "").splitlines():
        v = (ln or "").strip()
        if v:
            items.append(v)
        if len(items) >= max_items:
            break
    return items


def _window_hint(*, idempotency_key: str, lang: str) -> str:
    key = (idempotency_key or "").strip()
    parts = [p for p in key.split(":") if p]
    if len(parts) < 3:
        return ""
    if parts[0] != "digest":
        return ""
    day = parts[2]
    suffix = parts[3] if len(parts) >= 4 else ""
    ts = ""
    if suffix and len(suffix) == 4 and suffix.isdigit():
        hh = int(suffix[:2])
        mm = int(suffix[2:])
        if 0 <= hh <= 23 and 0 <= mm <= 59:
            ts = f"{suffix[:2]}:{suffix[2:]}"
    if ts:
        return f"窗口：{day} {ts}" if lang == "zh" else f"Window: {day} {ts}"
    return f"日期：{day}" if lang == "zh" else f"Date: {day}"


def parse_reference_entries(ref_block: str) -> list[tuple[int, str, str]]:
    """
    Parse `References:` entries into `(n, title, url)` tuples.

    Expected format (post-processed by tracker): `[1] Title — https://...`
    """
    refs: list[tuple[int, str, str]] = []
    for raw in (ref_block or "").splitlines():
        s = (raw or "").strip()
        if not s or s.startswith("References:") or s.lower() == "references:":
            continue
        m = _REF_ENTRY_RE.match(s)
        if not m:
            continue
        try:
            n = int(m.group(1))
        except Exception:
            continue
        title = (m.group(2) or "").strip()
        url = (m.group(3) or "").strip()
        if not url:
            continue
        refs.append((n, title, url))
    refs.sort(key=lambda r: r[0])
    return refs


def _page_label(*, page_number: int, current: bool, lang: str) -> str:
    n = max(1, int(page_number))
    if current:
        return f"·{n}·" if lang == "zh" else f"·{n}·"
    return str(n)


def _page_number_rows(
    *,
    total_pages: int,
    current_page: int,
    callback_builder,
    row_size: int = 5,
    min_total_pages: int = 3,
    lang: str,
) -> list[list[dict[str, str]]]:
    total = max(1, int(total_pages or 1))
    page_i = max(0, min(int(current_page or 0), total - 1))
    if total < max(1, int(min_total_pages or 3)):
        return []
    row_size = max(1, min(int(row_size or 5), 8))
    buttons = [
        {
            "text": _page_label(page_number=page + 1, current=(page == page_i), lang=lang),
            "callback_data": str(callback_builder(page)),
        }
        for page in range(total)
    ]
    return [buttons[i : i + row_size] for i in range(0, len(buttons), row_size)]


def _prev_next_buttons(
    *,
    current_page: int,
    total_pages: int,
    previous_callback: str,
    next_callback: str,
    lang: str,
) -> list[dict[str, str]]:
    total = max(1, int(total_pages or 1))
    page_i = max(0, min(int(current_page or 0), total - 1))
    nav: list[dict[str, str]] = []
    if page_i > 0:
        nav.append({"text": ("⬅️ 上一页" if lang == "zh" else "⬅️ Prev"), "callback_data": previous_callback})
    if page_i < total - 1:
        nav.append({"text": ("下一页 ➡️" if lang == "zh" else "Next ➡️"), "callback_data": next_callback})
    return nav


def _toc_keyboard(*, doc: ReportDoc, toc_page: int, lang: str, show_feedback: bool = False, report_key: str = "") -> dict:
    # Hide unnamed/preamble sections from the TOC.
    #
    # Why: Many reports include a short preamble before the first `##` heading (window meta, notes).
    # Telegram TOC would otherwise show a useless "Section 1" button, which is anti-UX on mobile.
    sections = [(i, s) for i, s in enumerate(doc.sections or []) if (s.title or "").strip()]
    page_size = 8
    total = len(sections)
    max_page = max(0, ((total - 1) // page_size) if total else 0)
    page_i = max(0, min(int(toc_page or 0), max_page))
    start = page_i * page_size
    chunk = sections[start : start + page_size]

    rows: list[list[dict[str, str]]] = []
    row: list[dict[str, str]] = []
    for sec_idx, sec in chunk:
        title = _short(sec.title, 18)
        row.append(
            {
                "text": title,
                "callback_data": reader_callback_data(report_key=report_key, action="sec", parts=[sec_idx, 0]),
            }
        )
        if len(row) >= 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    page_rows = _page_number_rows(
        total_pages=max_page + 1,
        current_page=page_i,
        callback_builder=lambda page: reader_callback_data(report_key=report_key, action="toc", parts=[page]),
        lang=lang,
    )
    if page_rows:
        rows.extend(page_rows)
    else:
        nav = _prev_next_buttons(
            current_page=page_i,
            total_pages=max_page + 1,
            previous_callback=reader_callback_data(report_key=report_key, action="toc", parts=[page_i - 1]),
            next_callback=reader_callback_data(report_key=report_key, action="toc", parts=[page_i + 1]),
            lang=lang,
        )
        if nav:
            rows.append(nav)

    extra_row: list[dict[str, str]] = [
        {
            "text": ("📚 References" if lang != "zh" else "📚 引用"),
            "callback_data": reader_callback_data(report_key=report_key, action="refs", parts=[0]),
        },
        {
            "text": ("📄 Full" if lang != "zh" else "📄 全文"),
            "callback_data": reader_callback_data(report_key=report_key, action="full", parts=[0]),
        },
    ]
    if show_feedback:
        extra_row.append(
            {
                "text": ("🗳️ Feedback" if lang != "zh" else "🗳️ 反馈"),
                "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=[0]),
            }
        )
    rows.append(extra_row)
    rows.append(
        [
            {
                "text": ("🔄 Refresh" if lang != "zh" else "🔄 刷新"),
                "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[page_i]),
            }
        ]
    )
    return {"inline_keyboard": rows}


def render_cover_html(
    *,
    markdown: str,
    idempotency_key: str,
    lang: str,
    toc_page: int = 0,
    show_feedback: bool | None = None,
) -> tuple[str, dict]:
    doc = parse_report_markdown(markdown)
    key = (idempotency_key or "").strip()
    fallback_zh = "参考消息" if key.startswith("digest:") else "报告"
    fallback_en = "Curated Info" if key.startswith("digest:") else "Report"
    title = doc.title or (fallback_zh if lang == "zh" else fallback_en)
    # Normalize legacy naming on the reader surface.
    if key.startswith("digest:"):
        try:
            if lang == "zh":
                title = str(title or "").replace("每日 Digest", "参考消息").replace("每日Digest", "参考消息")
            else:
                title = str(title or "").replace("Daily Digest", "Curated Info").replace("Digest", "Curated Info")
        except Exception:
            pass
    if lang == "zh":
        low = (title or "").casefold()
        if "self-test" in low or "self test" in low:
            title = "TG Reader 自测"
    hint = _window_hint(idempotency_key=idempotency_key, lang=lang)

    if show_feedback is None:
        show_feedback = key.startswith("digest:")

    # --- Curated Info (digest): render a paginated item list on the cover.
    if key.startswith("digest:"):
        try:
            # Extract window + counts from the markdown header (human-readable, no extra DB dependency).
            window_line = ""
            counts_line = ""
            for raw in (markdown or "").splitlines()[:30]:
                s = (raw or "").strip()
                if not s:
                    continue
                low = s.lower()
                if low.startswith("window:") or s.startswith("窗口:"):
                    window_line = s
                elif low.startswith("since:") or s.startswith("起始:"):
                    # Legacy digest format used "Since/起始".
                    window_line = s
                elif low.startswith("items:") or s.startswith("条目:"):
                    counts_line = s
                if window_line and counts_line:
                    break

            # Item list: prefer parsing the "Items" section to recover per-item decisions.
            decision_by_n: dict[int, str] = {}
            try:
                sec_items = None
                for sec in doc.sections:
                    t = (sec.title or "").casefold()
                    if t in {"items", "条目"} or ("items" in t) or ("条目" in (sec.title or "")):
                        sec_items = sec
                        break
                if sec_items:
                    for raw in (sec_items.body or "").splitlines():
                        line = (raw or "").strip()
                        if not line:
                            continue
                        m = re.search(r"\[(\d+)\]", line)
                        if not m:
                            continue
                        try:
                            n = int(m.group(1))
                        except Exception:
                            continue
                        tail = ""
                        mt = re.search(r"[\(（]([^\)）]+)[\)）]\s*$", line)
                        if mt:
                            tail = (mt.group(1) or "").strip()
                        if ("告警" in tail) or ("alert" in tail.casefold()):
                            decision_by_n[n] = "alert"
                        elif ("摘要" in tail) or ("digest" in tail.casefold()):
                            decision_by_n[n] = "digest"
            except Exception:
                decision_by_n = {}

            # References define the stable item order for the reader.
            refs = parse_reference_entries(doc.references)
            total = len(refs)
            page_size = 12
            max_page = max(0, ((total - 1) // page_size) if total else 0)
            page_i = max(0, min(int(toc_page or 0), max_page))
            start = page_i * page_size
            chunk = refs[start : start + page_size]

            lines: list[str] = []
            lines.append(f"🧠 <b>{_escape_html(title)}</b>")
            if window_line:
                # Show the extracted window line (already localized by the formatter).
                lines.append(f"⏱ <i>{_escape_html(window_line)}</i>")
            elif hint:
                lines.append(f"⏱ <i>{_escape_html(hint)}</i>")
            if counts_line:
                lines.append(f"📦 <i>{_escape_html(counts_line)}</i>")

            lines.append("")
            counter = f"（{page_i + 1}/{max_page + 1}）" if lang == "zh" else f"({page_i + 1}/{max_page + 1})"
            lines.append(f"📌 <b>{'条目' if lang == 'zh' else 'Items'}</b> {_escape_html(counter)}")
            if not refs:
                lines.append(_escape_html("（本窗口暂无新条目）" if lang == "zh" else "(No new items in this window)"))
            else:
                for n, t, u in chunk:
                    dec = decision_by_n.get(int(n), "")
                    badge = "🚨" if dec == "alert" else "•"
                    title2 = _short((t or "").strip(), 200)
                    if (u or "").strip().lower().startswith(("http://", "https://")):
                        lines.append(
                            f"{badge} {int(n)}) <a href=\"{_escape_html_attr(u)}\">{_escape_html(title2)}</a>"
                        )
                    else:
                        lines.append(f"{badge} {int(n)}) {_escape_html(title2)}")

            text = "\n".join(lines).strip()
            if len(text) > 3900:
                text = text[:3899] + "…"

            # Keyboard: paginate items; no TOC/sections for Curated Info.
            kb_rows: list[list[dict[str, str]]] = []
            page_rows = _page_number_rows(
                total_pages=max_page + 1,
                current_page=page_i,
                callback_builder=lambda page: reader_callback_data(report_key=key, action="toc", parts=[page]),
                lang=lang,
            )
            if page_rows:
                kb_rows.extend(page_rows)
            else:
                nav = _prev_next_buttons(
                    current_page=page_i,
                    total_pages=max_page + 1,
                    previous_callback=reader_callback_data(report_key=key, action="toc", parts=[page_i - 1]),
                    next_callback=reader_callback_data(report_key=key, action="toc", parts=[page_i + 1]),
                    lang=lang,
                )
                if nav:
                    kb_rows.append(nav)
            extra_row: list[dict[str, str]] = [
                {
                    "text": ("📚 引用" if lang == "zh" else "📚 References"),
                    "callback_data": reader_callback_data(report_key=key, action="refs", parts=[0]),
                },
                {
                    "text": ("📄 全文" if lang == "zh" else "📄 Full"),
                    "callback_data": reader_callback_data(report_key=key, action="full", parts=[0]),
                },
            ]
            if bool(show_feedback):
                extra_row.append(
                    {
                        "text": ("🗳️ 反馈" if lang == "zh" else "🗳️ Feedback"),
                        "callback_data": reader_callback_data(report_key=key, action="fb", parts=[0]),
                    }
                )
            kb_rows.append(extra_row)
            kb_rows.append(
                [
                    {
                        "text": ("🔄 再发一份" if lang == "zh" else "🔄 New batch"),
                        "callback_data": reader_callback_data(report_key=key, action="rerun", parts=[0]),
                    }
                ]
            )
            return (text, {"inline_keyboard": kb_rows})
        except Exception:
            # Fall back to the generic cover if parsing fails.
            pass

    takeaways = _extract_takeaways(doc, max_items=3)

    lines: list[str] = []
    lines.append(f"🧠 <b>{_escape_html(title)}</b>")
    if hint:
        lines.append(f"⏱ <i>{_escape_html(hint)}</i>")

    if takeaways:
        lines.append("")
        if key.startswith("digest:"):
            label = ("重点摘要" if lang == "zh" else "Key takeaways")
        else:
            label = ("重点摘要（3条）" if lang == "zh" else "Key takeaways (top 3)")
        lines.append(f"✏️ <b>{_escape_html(label)}</b>")
        for i, t in enumerate(takeaways, start=1):
            lines.append(f"{i}) {_escape_html(_short(t, 320))}")

    lines.append("")
    if lang == "zh":
        lines.append("📖 <b>阅读方式</b>")
        lines.append("<blockquote>")
        lines.append(_escape_html("点按钮翻页：目录 / 章节 / 引用 / 全文"))
        lines.append(_escape_html("全文也在聊天内分页；不会发 txt/附件"))
        lines.append(_escape_html("按钮没反应：先等 3–5 秒（轮询），仍不行发 /status"))
        lines.append("</blockquote>")
    else:
        lines.append("📖 <b>How to read</b>")
        lines.append("<blockquote>")
        lines.append(_escape_html("Tap buttons: TOC / Sections / References / Full"))
        lines.append(_escape_html("Full is paginated in-chat (no file attachments)"))
        lines.append(_escape_html("If unresponsive: wait 3–5s; then try /status"))
        lines.append("</blockquote>")

    kb = _toc_keyboard(doc=doc, toc_page=toc_page, lang=lang, show_feedback=bool(show_feedback), report_key=key)
    text = "\n".join(lines).strip()
    if len(text) > 3900:
        text = text[:3899] + "…"
    return (text, kb)


def render_section_html(
    *, markdown: str, section_index: int, page: int, lang: str, show_feedback: bool = False, report_key: str = ""
) -> tuple[str, dict]:
    doc = parse_report_markdown(markdown)
    idx = int(section_index or 0)
    if idx < 0 or idx >= len(doc.sections):
        return render_cover_html(
            markdown=markdown,
            idempotency_key=report_key,
            lang=lang,
            toc_page=0,
            show_feedback=show_feedback,
        )

    sec = doc.sections[idx]
    body = _im_sanitize_block(sec.body)
    pages = split_telegram_message(body, limit=3400) if body else []
    total = max(1, len(pages)) if body else 1
    page_i = max(0, min(int(page or 0), total - 1))
    chunk = pages[page_i] if pages else ""

    title = sec.title or (f"Section {idx + 1}")
    counter = f"（{page_i + 1}/{total}）" if lang == "zh" else f"({page_i + 1}/{total})"
    header = f"🧩 <b>{_escape_html(title)}</b> {_escape_html(counter)}".strip()
    body_html = _escape_html_with_links(chunk) if chunk else ""
    text = (header + ("\n\n" + body_html if body_html else "")).strip()

    kb_rows: list[list[dict[str, str]]] = []
    page_rows = _page_number_rows(
        total_pages=total,
        current_page=page_i,
        callback_builder=lambda page_no: reader_callback_data(report_key=report_key, action="sec", parts=[idx, page_no]),
        lang=lang,
    )
    if page_rows:
        kb_rows.extend(page_rows)
        kb_rows.append(
            [{"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
        )
    else:
        nav_row: list[dict[str, str]] = [
            {"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}
        ]
        nav_row.extend(
            _prev_next_buttons(
                current_page=page_i,
                total_pages=total,
                previous_callback=reader_callback_data(report_key=report_key, action="sec", parts=[idx, page_i - 1]),
                next_callback=reader_callback_data(report_key=report_key, action="sec", parts=[idx, page_i + 1]),
                lang=lang,
            )
        )
        kb_rows.append(nav_row)
    extra_row: list[dict[str, str]] = [
        {
            "text": ("📚 References" if lang != "zh" else "📚 引用"),
            "callback_data": reader_callback_data(report_key=report_key, action="refs", parts=[0]),
        },
        {
            "text": ("📄 Full" if lang != "zh" else "📄 全文"),
            "callback_data": reader_callback_data(report_key=report_key, action="full", parts=[0]),
        },
    ]
    if show_feedback:
        extra_row.append(
            {
                "text": ("🗳️ Feedback" if lang != "zh" else "🗳️ 反馈"),
                "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=[0]),
            }
        )
    kb_rows.append(extra_row)
    return (text[:4096], {"inline_keyboard": kb_rows})


def render_references_html(
    *, markdown: str, page: int, lang: str, show_feedback: bool = False, report_key: str = ""
) -> tuple[str, dict]:
    doc = parse_report_markdown(markdown)
    refs = _im_sanitize_block(doc.references)
    refs = refs.replace("References:", "References").strip()
    pages = split_telegram_message(refs, limit=3400) if refs else []
    total = max(1, len(pages)) if refs else 1
    page_i = max(0, min(int(page or 0), total - 1))
    chunk = pages[page_i] if pages else ""

    counter = f"（{page_i + 1}/{total}）" if lang == "zh" else f"({page_i + 1}/{total})"
    header = f"📚 <b>{'引用' if lang == 'zh' else 'References'}</b> {_escape_html(counter)}".strip()
    body_html = _escape_html_with_links(chunk) if chunk else ""
    text = (
        header
        + (
            ("\n\n" + body_html)
            if body_html
            else ("\n\n" + _escape_html("（无）" if lang == "zh" else "(none)"))
        )
    ).strip()

    kb_rows: list[list[dict[str, str]]] = []
    page_rows = _page_number_rows(
        total_pages=total,
        current_page=page_i,
        callback_builder=lambda page_no: reader_callback_data(report_key=report_key, action="refs", parts=[page_no]),
        lang=lang,
    )
    if page_rows:
        kb_rows.extend(page_rows)
        kb_rows.append(
            [{"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
        )
    else:
        nav_row: list[dict[str, str]] = [
            {"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}
        ]
        nav_row.extend(
            _prev_next_buttons(
                current_page=page_i,
                total_pages=total,
                previous_callback=reader_callback_data(report_key=report_key, action="refs", parts=[page_i - 1]),
                next_callback=reader_callback_data(report_key=report_key, action="refs", parts=[page_i + 1]),
                lang=lang,
            )
        )
        kb_rows.append(nav_row)
    extra_row: list[dict[str, str]] = [
        {
            "text": ("📄 Full" if lang != "zh" else "📄 全文"),
            "callback_data": reader_callback_data(report_key=report_key, action="full", parts=[0]),
        },
    ]
    if show_feedback:
        extra_row.append(
            {
                "text": ("🗳️ Feedback" if lang != "zh" else "🗳️ 反馈"),
                "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=[0]),
            }
        )
    kb_rows.append(extra_row)
    return (text[:4096], {"inline_keyboard": kb_rows})


def render_full_html(
    *, markdown: str, page: int, lang: str, show_feedback: bool = False, report_key: str = ""
) -> tuple[str, dict]:
    doc = parse_report_markdown(markdown)
    blocks: list[str] = []
    for sec in doc.sections:
        title = (sec.title or "").strip()
        body = _im_sanitize_block(sec.body)
        if title:
            blocks.append(f"【{title}】")
        if body:
            blocks.append(body)
        blocks.append("")
    if doc.references:
        blocks.append(_im_sanitize_block(doc.references))
    full_text = "\n".join(blocks).strip()

    pages = split_telegram_message(full_text, limit=3400) if full_text else []
    total = max(1, len(pages)) if full_text else 1
    page_i = max(0, min(int(page or 0), total - 1))
    chunk = pages[page_i] if pages else ""

    counter = f"（{page_i + 1}/{total}）" if lang == "zh" else f"({page_i + 1}/{total})"
    header = f"📄 <b>{'全文' if lang == 'zh' else 'Full'}</b> {_escape_html(counter)}".strip()
    body_html = _escape_html_with_links(chunk) if chunk else ""
    text = (header + ("\n\n" + body_html if body_html else "")).strip()

    kb_rows: list[list[dict[str, str]]] = []
    page_rows = _page_number_rows(
        total_pages=total,
        current_page=page_i,
        callback_builder=lambda page_no: reader_callback_data(report_key=report_key, action="full", parts=[page_no]),
        lang=lang,
    )
    if page_rows:
        kb_rows.extend(page_rows)
        kb_rows.append(
            [{"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
        )
    else:
        nav_row: list[dict[str, str]] = [
            {"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}
        ]
        nav_row.extend(
            _prev_next_buttons(
                current_page=page_i,
                total_pages=total,
                previous_callback=reader_callback_data(report_key=report_key, action="full", parts=[page_i - 1]),
                next_callback=reader_callback_data(report_key=report_key, action="full", parts=[page_i + 1]),
                lang=lang,
            )
        )
        kb_rows.append(nav_row)
    extra_row: list[dict[str, str]] = [
        {
            "text": ("📚 References" if lang != "zh" else "📚 引用"),
            "callback_data": reader_callback_data(report_key=report_key, action="refs", parts=[0]),
        },
    ]
    if show_feedback:
        extra_row.append(
            {
                "text": ("🗳️ Feedback" if lang != "zh" else "🗳️ 反馈"),
                "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=[0]),
            }
        )
    kb_rows.append(extra_row)
    return (text[:4096], {"inline_keyboard": kb_rows})


def render_digest_full_html(
    *,
    markdown: str,
    page: int,
    lang: str,
    show_feedback: bool = False,
    report_key: str = "",
    page_size: int = 18,
) -> tuple[str, dict]:
    """
    Render a full item list for Curated Info (digest).

    Contract:
    - Curated Info is de-dupe only; full view should show ALL items (paginated),
      without extra meta-analytics or summary sections.
    """
    doc = parse_report_markdown(markdown)
    refs = parse_reference_entries(doc.references)

    total = len(refs)
    if total <= 0:
        # Fall back to the generic renderer (best-effort).
        return render_full_html(markdown=markdown, page=page, lang=lang, show_feedback=show_feedback, report_key=report_key)

    # Try to infer per-item decision from the Items section tail markers.
    decision_by_n: dict[int, str] = {}
    try:
        sec_items = None
        for sec in doc.sections:
            t = (sec.title or "").casefold()
            if t in {"items", "条目"} or ("items" in t) or ("条目" in (sec.title or "")):
                sec_items = sec
                break
        if sec_items:
            for raw in (sec_items.body or "").splitlines():
                line = (raw or "").strip()
                if not line:
                    continue
                m = re.search(r"\[(\d+)\]", line)
                if not m:
                    continue
                try:
                    n = int(m.group(1))
                except Exception:
                    continue
                tail = ""
                mt = re.search(r"[\(（]([^\)）]+)[\)）]\s*$", line)
                if mt:
                    tail = (mt.group(1) or "").strip()
                if ("告警" in tail) or ("alert" in tail.casefold()):
                    decision_by_n[n] = "alert"
                elif ("摘要" in tail) or ("digest" in tail.casefold()):
                    decision_by_n[n] = "digest"
    except Exception:
        decision_by_n = {}

    size = max(6, min(int(page_size or 18), 30))
    max_page = max(0, (total - 1) // size)
    page_i = max(0, min(int(page or 0), max_page))
    start = page_i * size
    chunk = refs[start : start + size]

    counter = f"（{page_i + 1}/{max_page + 1}）" if lang == "zh" else f"({page_i + 1}/{max_page + 1})"
    header = f"📄 <b>{'全文' if lang == 'zh' else 'Full'}</b> {_escape_html(counter)}".strip()

    lines: list[str] = [header, ""]
    for n, t, u in chunk:
        dec = decision_by_n.get(int(n), "")
        badge = "🚨" if dec == "alert" else "•"
        title2 = _short((t or "").strip(), 240) or (f"Item {n}")
        if (u or "").strip().lower().startswith(("http://", "https://")):
            lines.append(f"{badge} {int(n)}) <a href=\"{_escape_html_attr(u)}\">{_escape_html(title2)}</a>")
        else:
            lines.append(f"{badge} {int(n)}) {_escape_html(title2)}")

    text = "\n".join(lines).strip()
    if len(text) > 3900:
        text = text[:3899] + "…"

    kb_rows: list[list[dict[str, str]]] = []
    page_rows = _page_number_rows(
        total_pages=max_page + 1,
        current_page=page_i,
        callback_builder=lambda page_no: reader_callback_data(report_key=report_key, action="full", parts=[page_no]),
        lang=lang,
    )
    if page_rows:
        kb_rows.extend(page_rows)
        kb_rows.append(
            [{"text": ("⬅️ 列表" if lang == "zh" else "⬅️ List"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
        )
    else:
        nav: list[dict[str, str]] = [
            {"text": ("⬅️ 列表" if lang == "zh" else "⬅️ List"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}
        ]
        nav.extend(
            _prev_next_buttons(
                current_page=page_i,
                total_pages=max_page + 1,
                previous_callback=reader_callback_data(report_key=report_key, action="full", parts=[page_i - 1]),
                next_callback=reader_callback_data(report_key=report_key, action="full", parts=[page_i + 1]),
                lang=lang,
            )
        )
        kb_rows.append(nav)

    extra_row: list[dict[str, str]] = [
        {
            "text": ("📚 引用" if lang == "zh" else "📚 References"),
            "callback_data": reader_callback_data(report_key=report_key, action="refs", parts=[0]),
        },
    ]
    if show_feedback:
        extra_row.append(
            {
                "text": ("🗳️ 反馈" if lang == "zh" else "🗳️ Feedback"),
                "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=[0]),
            }
        )
    kb_rows.append(extra_row)
    kb_rows.append(
        [
            {
                "text": ("🔄 再发一份" if lang == "zh" else "🔄 New batch"),
                "callback_data": reader_callback_data(report_key=report_key, action="rerun", parts=[0]),
            }
        ]
    )
    return (text[:4096], {"inline_keyboard": kb_rows})


def render_feedback_html(
    *,
    markdown: str,
    page: int,
    lang: str,
    mute_days: int = 7,
    status: str = "",
    page_size: int = 4,
    report_key: str = "",
) -> tuple[str, dict]:
    """
    Render a compact "per-item feedback" page for Digest Reader.

    This keeps Digest de-duped as ONE Telegram message, while still allowing
    users to like/dislike/mute per item via inline buttons.
    """
    doc = parse_report_markdown(markdown)
    refs = parse_reference_entries(doc.references)

    total = len(refs)
    if total <= 0:
        title = "🗳️ <b>反馈</b>" if lang == "zh" else "🗳️ <b>Feedback</b>"
        body = (
            "（未找到 References；无法对条目逐条反馈。）"
            if lang == "zh"
            else "(No References block found; per-item feedback is unavailable.)"
        )
        text = f"{title}\n\n{_escape_html(body)}"
        kb = {
            "inline_keyboard": [
                [{"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
            ]
        }
        return (text[:4096], kb)

    size = max(1, min(int(page_size or 4), 8))
    max_page = max(0, (total - 1) // size)
    page_i = max(0, min(int(page or 0), max_page))
    start = page_i * size
    chunk = refs[start : start + size]

    counter = f"（{page_i + 1}/{max_page + 1}）" if lang == "zh" else f"({page_i + 1}/{max_page + 1})"
    header = f"🗳️ <b>{'反馈' if lang == 'zh' else 'Feedback'}</b> {_escape_html(counter)}".strip()

    lines: list[str] = [header]
    if status.strip():
        lines.append(f"✅ {_escape_html(status.strip())}")

    lines.append("")
    if lang == "zh":
        lines.append("<blockquote>")
        lines.append(_escape_html("对条目逐条反馈：👍 喜欢；👎 不喜欢；🔕 静音域名"))
        lines.append(_escape_html(f"🔕 默认静音：{int(mute_days)} 天（可在设置里改）"))
        lines.append("</blockquote>")
    else:
        lines.append("<blockquote>")
        lines.append(_escape_html("Per-item feedback: 👍 like; 👎 dislike; 🔕 mute domain"))
        lines.append(_escape_html(f"🔕 default mute: {int(mute_days)} days (configurable)"))
        lines.append("</blockquote>")

    lines.append("")
    for n, title, _url in chunk:
        label = title.strip() or (f"Item {n}")
        lines.append(f"{int(n)}) {_escape_html(_short(label, 220))}")

    kb_rows: list[list[dict[str, str]]] = []
    for n, _title, _url in chunk:
        n2 = int(n)
        kb_rows.append(
            [
                {"text": f"👍{n2}", "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=["like", n2, page_i])},
                {"text": f"👎{n2}", "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=["dislike", n2, page_i])},
                {"text": f"🔕{n2}", "callback_data": reader_callback_data(report_key=report_key, action="fb", parts=["mute", n2, page_i])},
            ]
        )

    page_rows = _page_number_rows(
        total_pages=max_page + 1,
        current_page=page_i,
        callback_builder=lambda page_no: reader_callback_data(report_key=report_key, action="fb", parts=[page_no]),
        lang=lang,
    )
    if page_rows:
        kb_rows.extend(page_rows)
        kb_rows.append(
            [{"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}]
        )
    else:
        nav_row: list[dict[str, str]] = [
            {"text": ("⬅️ 目录" if lang == "zh" else "⬅️ TOC"), "callback_data": reader_callback_data(report_key=report_key, action="toc", parts=[0])}
        ]
        nav_row.extend(
            _prev_next_buttons(
                current_page=page_i,
                total_pages=max_page + 1,
                previous_callback=reader_callback_data(report_key=report_key, action="fb", parts=[page_i - 1]),
                next_callback=reader_callback_data(report_key=report_key, action="fb", parts=[page_i + 1]),
                lang=lang,
            )
        )
        kb_rows.append(nav_row)

    text = "\n".join(lines).strip()
    return (text[:4096], {"inline_keyboard": kb_rows})
