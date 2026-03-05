from __future__ import annotations

import datetime as dt

from tracker.models import Source, SourceHealth, SourceMeta


def format_health_markdown(
    *,
    stats: dict[str, int],
    rows: list[tuple[Source, SourceHealth | None, SourceMeta | None]],
) -> str:
    now = dt.datetime.utcnow()
    lines: list[str] = [f"# OpenInfoMate Health\n\nTime: {now.isoformat()} UTC\n"]

    keys = [
        "topics_total",
        "topics_enabled",
        "sources_total",
        "sources_enabled",
        "bindings_total",
        "items_total",
        "item_topics_total",
        "pushes_total",
        "pushes_failed",
        "sources_in_backoff",
        "sources_with_errors",
    ]
    lines.append("## Stats\n")
    for k in keys:
        if k in stats:
            lines.append(f"- {k}: {stats[k]}")

    failing: list[str] = []
    disabled: list[str] = []

    for s, h, m in rows:
        tags = m.tags if m and m.tags else ""
        tags_txt = f" tags={tags!r}" if tags else ""

        if not s.enabled:
            disabled.append(f"- #{s.id} {s.type}{tags_txt} {s.url}")

        if not h:
            continue
        if h.error_count <= 0 and not (h.next_fetch_at and h.next_fetch_at > now):
            continue
        next_txt = h.next_fetch_at.isoformat() if h.next_fetch_at else ""
        err_txt = (h.last_error or "").replace("\n", " ").strip()
        if len(err_txt) > 200:
            err_txt = err_txt[:200] + "…"
        failing.append(
            f"- #{s.id} {s.type}{tags_txt} errs={h.error_count} next={next_txt} last_err={err_txt!r} {s.url}"
        )

    if failing:
        lines.append("\n## Failing Sources\n")
        lines.extend(failing[:50])
        if len(failing) > 50:
            lines.append(f"- ... ({len(failing) - 50} more)")

    if disabled:
        lines.append("\n## Disabled Sources\n")
        lines.extend(disabled[:50])
        if len(disabled) > 50:
            lines.append(f"- ... ({len(disabled) - 50} more)")

    return "\n".join(lines) + "\n"
