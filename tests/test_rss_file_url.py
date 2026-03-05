from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.pipeline import ingest_rss_source_for_topic
from tracker.repo import Repo


def test_rss_file_url_ingest(db_session, tmp_path):
    fixture = Path(__file__).with_name("fixtures").joinpath("rss_sample.xml").read_text(encoding="utf-8")
    feed_path = tmp_path / "feed.xml"
    feed_path.write_text(fixture, encoding="utf-8")

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai chips")
    source = repo.add_source(type="rss", url=f"file://{feed_path}")
    repo.bind_topic_source(topic=topic, source=source)

    created = asyncio.run(ingest_rss_source_for_topic(session=db_session, topic=topic, source=source, timeout_seconds=1))
    assert len(created) == 2

