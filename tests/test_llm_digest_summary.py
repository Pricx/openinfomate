from __future__ import annotations

import asyncio
import datetime as dt

from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest
from tracker.settings import Settings


def test_llm_digest_summary_is_not_included_in_digest_output(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/a",
        canonical_url="https://example.com/a",
        title="GPU thing",
        content_text="",
        content_hash="",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="digest", reason="", created_at=now))
    db_session.commit()

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_digest_enabled=True,
    )

    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False))
    md = result.per_topic[0].markdown
    assert "## Executive Summary" not in md
