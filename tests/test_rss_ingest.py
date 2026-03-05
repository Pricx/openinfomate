from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.pipeline import ingest_rss_source_for_topic
from tracker.repo import Repo


def test_ingest_rss_creates_items_and_decisions(db_session, monkeypatch):
    xml = Path(__file__).with_name("fixtures").joinpath("rss_sample.xml").read_text(encoding="utf-8")

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            return FakeResp(xml)

    monkeypatch.setattr("tracker.connectors.rss.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai chips", digest_cron="0 9 * * *")
    topic.alert_keywords = "breaking"
    db_session.commit()

    source = repo.add_source(type="rss", url="https://example.com/feed")
    repo.bind_topic_source(topic=topic, source=source)

    created = asyncio.run(
        ingest_rss_source_for_topic(session=db_session, topic=topic, source=source, timeout_seconds=1)
    )
    assert len(created) == 2

    # First entry should be alert due to alert_keywords.
    assert created[0].decision in {"alert", "digest", "ignore"}
    assert any(d.decision == "alert" for d in created)

    # Canonicalization strips utm_*.
    assert any(d.canonical_url == "https://example.com/post1" for d in created)


def test_ingest_rss_respects_exclude_keywords(db_session, monkeypatch):
    xml = Path(__file__).with_name("fixtures").joinpath("rss_sample.xml").read_text(encoding="utf-8")

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            return FakeResp(xml)

    monkeypatch.setattr("tracker.connectors.rss.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="ai chips", digest_cron="0 9 * * *")
    source = repo.add_source(type="rss", url="https://example.com/feed")
    repo.bind_topic_source(topic=topic, source=source)

    created = asyncio.run(
        ingest_rss_source_for_topic(
            session=db_session,
            topic=topic,
            source=source,
            timeout_seconds=1,
            exclude_keywords="roundup",
        )
    )
    roundup = [d for d in created if "roundup" in d.title.lower()]
    assert roundup
    assert all(d.decision == "ignore" for d in roundup)


def test_ingest_rss_prefers_content_encoded_for_matching(db_session, monkeypatch):
    """
    Some feeds provide richer text in `content:encoded` rather than <description>.

    Prefer that for matching so keyword-mode tracking doesn't miss relevant items.
    """
    xml = (
        Path(__file__)
        .with_name("fixtures")
        .joinpath("rss_sample_content_encoded.xml")
        .read_text(encoding="utf-8")
    )

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            return FakeResp(xml)

    monkeypatch.setattr("tracker.connectors.rss.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="uniquekey", digest_cron="0 9 * * *")
    db_session.commit()

    source = repo.add_source(type="rss", url="https://example.com/feed")
    repo.bind_topic_source(topic=topic, source=source)

    created = asyncio.run(
        ingest_rss_source_for_topic(session=db_session, topic=topic, source=source, timeout_seconds=1)
    )
    assert any(d.decision == "digest" for d in created)


def test_ingest_rss_rewrites_localhost_stream_links(db_session, monkeypatch):
    xml = (
        Path(__file__)
        .with_name("fixtures")
        .joinpath("rss_sample_localhost_stream.xml")
        .read_text(encoding="utf-8")
    )

    class FakeResp:
        def __init__(self, text: str):
            self.text = text

        def raise_for_status(self):
            return None

    class FakeClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def get(self, url: str, headers: dict):
            return FakeResp(xml)

    monkeypatch.setattr("tracker.connectors.rss.httpx.AsyncClient", FakeClient)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="target.example", digest_cron="0 9 * * *")
    db_session.commit()

    source = repo.add_source(type="rss", url="https://example.com/feed")
    repo.bind_topic_source(topic=topic, source=source)

    created = asyncio.run(
        ingest_rss_source_for_topic(session=db_session, topic=topic, source=source, timeout_seconds=1)
    )
    assert len(created) == 2
    assert all("localhost" not in (d.canonical_url or "") for d in created)

    # For localhost stream placeholders, prefer the first real external link in the body.
    assert any(d.canonical_url == "https://target.example/post1" for d in created)
    assert any(d.canonical_url == "https://target.example/post2" for d in created)
