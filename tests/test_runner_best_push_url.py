from __future__ import annotations

from tracker.models import Item, Source
from tracker.runner import _best_push_url_for_item


def test_best_push_url_prefers_external_over_localhost_stream():
    item = Item(
        source_id=1,
        url="https://localhost/stream/abc123",
        canonical_url="https://localhost/stream/abc123",
        title="t",
        published_at=None,
        content_text="see https://target.example/post1 for details",
        content_hash="h",
        simhash64=0,
    )
    source = Source(type="rss", url="https://example.com/feed")
    assert _best_push_url_for_item(item=item, source=source) == "https://target.example/post1"


def test_best_push_url_rewrites_localhost_stream_when_no_external_link():
    item = Item(
        source_id=1,
        url="https://localhost/stream/abc123",
        canonical_url="https://localhost/stream/abc123",
        title="t",
        published_at=None,
        content_text="no links here",
        content_hash="h",
        simhash64=0,
    )
    source = Source(type="rss", url="https://example.com/feed")
    assert _best_push_url_for_item(item=item, source=source) == "https://example.com/stream/abc123"

