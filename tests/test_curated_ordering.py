from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from tracker.db import session_factory
from tracker.formatting import format_digest_markdown
from tracker.models import Base, Item, ItemTopic, Source, Topic
from tracker.repo import Repo
from tracker.runner import run_curated_info
from tracker.settings import Settings


def _row(*, item_id: int, url: str, title: str, decision: str, reason: str, created_at: dt.datetime) -> tuple[ItemTopic, Item]:
    it = ItemTopic(item_id=item_id, topic_id=1, decision=decision, reason=reason, created_at=created_at)
    item = Item(
        id=item_id,
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


def test_format_digest_markdown_orders_by_llm_rank_desc():
    topic = Topic(id=1, name="T", query="x", digest_cron="0 9 * * *")
    since = dt.datetime(2026, 2, 10, 0, 0, 0)
    now = dt.datetime(2026, 2, 10, 12, 0, 0)

    low_alert = _row(
        item_id=1,
        url="https://example.com/a",
        title="Low Alert",
        decision="alert",
        reason="llm_rank: 40\nllm_summary: low\n",
        created_at=now,
    )
    high_digest = _row(
        item_id=2,
        url="https://example.com/b",
        title="High Digest",
        decision="digest",
        reason="llm_rank: 95\nllm_summary: high\n",
        created_at=now - dt.timedelta(hours=1),
    )

    md = format_digest_markdown(
        topic=topic,
        items=[low_alert, high_digest],
        since=since,
        previous_items=None,
        llm_summary=None,
    )

    lines = [ln for ln in md.splitlines() if ln.startswith("- ")]
    assert lines
    assert "High Digest" in lines[0]


def test_run_curated_info_orders_items_by_llm_rank_desc(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
        output_language="zh",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed.xml")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 22, 10, 0, 0)

        item_low = Item(
            source_id=int(source.id),
            url="https://example.com/low",
            canonical_url="https://example.com/low",
            title="低分条目",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now - dt.timedelta(minutes=5),
        )
        item_high = Item(
            source_id=int(source.id),
            url="https://example.com/high",
            canonical_url="https://example.com/high",
            title="高分条目",
            content_text="",
            content_hash="",
            simhash64=0,
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add(item_low)
        session.add(item_high)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item_low.id),
                topic_id=int(topic.id),
                decision="alert",
                reason="llm_rank: 35\nllm_summary: low\nllm_hint: alert",
                created_at=item_low.created_at,
            )
        )
        session.add(
            ItemTopic(
                item_id=int(item_high.id),
                topic_id=int(topic.id),
                decision="digest",
                reason="llm_rank: 92\nllm_summary: high\nllm_hint: digest",
                created_at=item_high.created_at,
            )
        )
        session.commit()

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="rank-order",
                now=dt.datetime(2026, 3, 22, 10, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    lines = [ln for ln in markdown.splitlines() if ln.startswith("- ")]
    assert lines
    assert "高分条目" in lines[0]
