from __future__ import annotations

import asyncio
import datetime as dt

from tracker.llm import LlmCurationDecision
from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_digest
from tracker.settings import Settings


def test_run_digest_topic_gate_candidate_min_score_filters_candidates(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")
    source = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="pick signals only")
    repo.patch_topic_gate_policy(topic_id=topic.id, patch={"candidate_min_score": 70})

    now = dt.datetime.utcnow()
    item_ts = now - dt.timedelta(seconds=1)
    high = Item(
        source_id=source.id,
        url="https://example.com/high",
        canonical_url="https://example.com/high",
        title="High score",
        content_text="high",
        created_at=item_ts,
        published_at=item_ts,
    )
    low = Item(
        source_id=source.id,
        url="https://example.com/low",
        canonical_url="https://example.com/low",
        title="Low score",
        content_text="low",
        created_at=item_ts,
        published_at=item_ts,
    )
    db_session.add_all([high, low])
    db_session.flush()
    db_session.add_all(
        [
            ItemTopic(
                item_id=high.id,
                topic_id=topic.id,
                decision="candidate",
                reason="",
                relevance_score=90,
                novelty_score=80,
                quality_score=85,
            ),
            ItemTopic(
                item_id=low.id,
                topic_id=topic.id,
                decision="candidate",
                reason="",
                relevance_score=20,
                novelty_score=30,
                quality_score=40,
            ),
        ]
    )
    db_session.commit()

    async def fake_curate(**kwargs):  # type: ignore[no-untyped-def]
        candidates = kwargs["candidates"]
        assert [int(c["item_id"]) for c in candidates] == [high.id]
        return [LlmCurationDecision(item_id=high.id, decision="digest", why="good", summary="picked")]

    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="https://llm.example.com/v1",
        llm_model="gpt-5.2",
        llm_curation_enabled=True,
    )
    result = asyncio.run(run_digest(session=db_session, settings=settings, hours=24, push=False, now=now))

    assert result.per_topic
    high_row = repo.get_item_topic(item_id=high.id, topic_id=topic.id)
    low_row = repo.get_item_topic(item_id=low.id, topic_id=topic.id)
    assert high_row and high_row.decision == "digest"
    assert low_row and low_row.decision == "ignore"
    assert "candidate_min_score=70" in (low_row.reason or "")


def test_run_digest_topic_gate_push_caps_limit_report_items(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")
    source = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.patch_topic_gate_policy(
        topic_id=topic.id,
        patch={"push_min_score": 60, "max_digest_items": 1, "max_alert_items": 1},
    )

    now = dt.datetime.utcnow()
    rows: list[tuple[str, str, int, int, int]] = [
        ("digest", "Digest A", 90, 90, 90),
        ("digest", "Digest B", 88, 88, 88),
        ("alert", "Alert A", 95, 95, 95),
        ("alert", "Alert B", 94, 94, 94),
        ("digest", "Too low", 10, 10, 10),
    ]
    for idx, (decision, title, rel, nov, qual) in enumerate(rows, start=1):
        item = Item(
            source_id=source.id,
            url=f"https://example.com/{idx}",
            canonical_url=f"https://example.com/{idx}",
            title=title,
            content_text=title,
            created_at=now - dt.timedelta(minutes=idx),
            published_at=now - dt.timedelta(minutes=idx),
        )
        db_session.add(item)
        db_session.flush()
        db_session.add(
            ItemTopic(
                item_id=item.id,
                topic_id=topic.id,
                decision=decision,
                reason="seed",
                relevance_score=rel,
                novelty_score=nov,
                quality_score=qual,
            )
        )
    db_session.commit()

    result = asyncio.run(run_digest(session=db_session, settings=Settings(), hours=24, push=False, now=now))
    markdown = result.per_topic[0].markdown
    assert "Digest A" in markdown
    assert "Alert A" in markdown
    assert "Digest B" not in markdown
    assert "Alert B" not in markdown
    assert "Too low" not in markdown
