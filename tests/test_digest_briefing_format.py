from __future__ import annotations

import datetime as dt

from tracker.formatting import format_digest_markdown
from tracker.models import Item, ItemTopic, Topic


def _row(*, url: str, title: str, decision: str, reason: str, created_at: dt.datetime) -> tuple[ItemTopic, Item]:
    it = ItemTopic(item_id=1, topic_id=1, decision=decision, reason=reason, created_at=created_at)
    item = Item(
        source_id=1,
        url=url,
        canonical_url=url,
        title=title,
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=created_at,
    )
    return it, item


def test_digest_does_not_interpret_llm_reason_fields():
    topic = Topic(name="T", query="x", digest_cron="0 9 * * *")
    since = dt.datetime(2026, 2, 10, 0, 0, 0)
    now = dt.datetime(2026, 2, 10, 12, 0, 0)

    items = [
        _row(
            url="https://example.com/a",
            title="A",
            decision="digest",
            reason="llm_summary: s1\nllm_why: w1\n",
            created_at=now,
        )
    ]
    previous = [
        _row(
            url="https://example.com/old",
            title="Old",
            decision="digest",
            reason="",
            created_at=now - dt.timedelta(hours=1),
        )
    ]

    md = format_digest_markdown(
        topic=topic,
        items=items,
        since=since,
        previous_total=len(previous),
        previous_alerts=0,
        previous_items=previous,
        llm_summary=None,
    )

    assert "## What Changed" not in md
    assert "— s1" not in md


def test_digest_orders_alerts_first():
    topic = Topic(name="T", query="x", digest_cron="0 9 * * *")
    since = dt.datetime(2026, 2, 10, 0, 0, 0)

    t0 = dt.datetime(2026, 2, 10, 12, 0, 0)
    t1 = dt.datetime(2026, 2, 10, 11, 0, 0)

    digest_row = _row(
        url="https://example.com/d",
        title="Digest",
        decision="digest",
        reason="llm_summary: ds\nllm_why: dw\n",
        created_at=t0,
    )
    alert_row = _row(
        url="https://example.com/a",
        title="Alert",
        decision="alert",
        reason="llm_summary: as\nllm_why: aw\n",
        created_at=t1,
    )

    md = format_digest_markdown(
        topic=topic,
        items=[digest_row, alert_row],
        since=since,
        previous_items=None,
        llm_summary=None,
    )

    lines = [ln for ln in md.splitlines() if ln.startswith("- ")]
    assert lines, "expected at least one bullet line"
    assert "(alert)" in lines[0]
