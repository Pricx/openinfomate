from __future__ import annotations

import asyncio
import datetime as dt

from tracker.llm import LlmCurationDecision
from tracker.models import Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_priority_lane_promotes_candidate_to_alert(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/spark",
        canonical_url="https://example.com/spark",
        title="Introducing GPT-5.3-Codex-Spark",
        content_text="OpenAI released a new Codex Spark model (major update).",
        content_hash="x",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_triage(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_keep=0, usage_cb=None
    ):
        _ = settings, topic, policy_prompt, recent_sent, max_keep, usage_cb
        return [int(candidates[0]["item_id"])]

    async def fake_curate(  # type: ignore[no-untyped-def]
        *, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest=0, max_alert=0, usage_cb=None
    ):
        _ = settings, topic, policy_prompt, recent_sent, max_digest, max_alert, usage_cb
        cid = int(candidates[0]["item_id"])
        return [LlmCurationDecision(item_id=cid, decision="alert", why="major model release", summary="New model release")]

    monkeypatch.setattr("tracker.runner.llm_triage_topic_items", fake_triage)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        priority_lane_enabled=True,
        priority_lane_hours=72,
        priority_lane_pool_max_candidates=50,
        priority_lane_triage_keep_candidates=5,
        priority_lane_max_alert=2,
    )

    asyncio.run(run_tick(session=db_session, settings=settings, push=False))

    it_row = repo.get_item_topic(item_id=int(item.id), topic_id=int(topic.id))
    assert it_row is not None
    assert it_row.decision == "alert"
    assert "priority_lane" in (it_row.reason or "")




def test_priority_lane_budget_denied_falls_back_to_digest(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")
    source = repo.add_source(type="rss", url="file:///tmp/feed.xml")

    now = dt.datetime.utcnow()
    item = Item(
        source_id=source.id,
        url="https://example.com/spark",
        canonical_url="https://example.com/spark",
        title="Introducing GPT-5.3-Codex-Spark",
        content_text="OpenAI released a new Codex Spark model (major update).",
        content_hash="x",
        simhash64=0,
        created_at=now,
    )
    db_session.add(item)
    db_session.flush()
    db_session.add(ItemTopic(item_id=item.id, topic_id=topic.id, decision="candidate", reason="", created_at=now))
    db_session.commit()

    async def fake_triage(*, settings, topic, policy_prompt, candidates, recent_sent=None, max_keep=0, usage_cb=None):  # type: ignore[no-untyped-def]
        _ = settings, topic, policy_prompt, recent_sent, max_keep, usage_cb
        return [int(candidates[0]["item_id"])]

    async def fake_curate(*, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest=0, max_alert=0, usage_cb=None):  # type: ignore[no-untyped-def]
        _ = settings, topic, policy_prompt, recent_sent, max_digest, max_alert, usage_cb
        cid = int(candidates[0]["item_id"])
        return [LlmCurationDecision(item_id=cid, decision="alert", why="major model release", summary="New model release")]

    pushed: list[dict] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):
        pushed.append(payload)
        return True

    monkeypatch.setattr("tracker.runner.llm_triage_topic_items", fake_triage)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_curate)
    monkeypatch.setattr("tracker.runner.can_send_alert_under_budget", lambda **kwargs: False)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    settings = Settings(
        llm_base_url="http://llm.local",
        llm_model="dummy",
        webhook_url="http://example.invalid/webhook",
        priority_lane_enabled=True,
        priority_lane_hours=72,
        priority_lane_pool_max_candidates=50,
        priority_lane_triage_keep_candidates=5,
        priority_lane_max_alert=2,
    )

    asyncio.run(run_tick(session=db_session, settings=settings, push=True))
    assert pushed == []

    it_row = repo.get_item_topic(item_id=int(item.id), topic_id=int(topic.id))
    assert it_row is not None
    assert it_row.decision == "digest"
    assert "alert_suppressed_by_budget" in (it_row.reason or "")
