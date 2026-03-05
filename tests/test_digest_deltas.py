from __future__ import annotations

import datetime as dt

from tracker.formatting import format_digest_markdown
from tracker.models import Item, ItemTopic, Topic


def _it(*, url: str, title: str, decision: str = "digest") -> tuple[ItemTopic, Item]:
    it = ItemTopic(item_id=1, topic_id=1, decision=decision, reason="")
    item = Item(
        source_id=1,
        url=url,
        canonical_url=url,
        title=title,
        content_text="",
        content_hash="",
        simhash64=0,
    )
    return it, item


def test_digest_does_not_include_what_changed_analysis():
    topic = Topic(name="T", query="x", digest_cron="0 9 * * *")
    since = dt.datetime(2026, 2, 10, 0, 0, 0)

    current = [
        _it(url="https://github.com/foo/bar", title="Foo bar release"),
        _it(url="https://github.com/baz/qux", title="Baz qux improves"),
        _it(url="https://arxiv.org/abs/1234.5678", title="New GPU architecture"),
    ]
    previous = [
        _it(url="https://github.com/old/thing", title="Old stuff"),
        _it(url="https://example.com/x", title="Example something"),
    ]

    md = format_digest_markdown(
        topic=topic,
        items=current,
        since=since,
        previous_total=len(previous),
        previous_alerts=0,
        previous_items=previous,
    )

    assert "## What Changed" not in md
