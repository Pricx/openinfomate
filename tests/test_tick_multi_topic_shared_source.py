from __future__ import annotations

import asyncio
import json
from pathlib import Path

from tracker.connectors.base import FetchedEntry
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_shared_source_bound_to_two_topics_ingests_for_both(db_session):
    feed_path = Path(__file__).with_name("fixtures").joinpath("rss_sample.xml")
    url = f"file://{feed_path}"

    repo = Repo(db_session)
    t1 = repo.add_topic(name="T1", query="ai chips")
    t2 = repo.add_topic(name="T2", query="ai chips")
    source = repo.add_source(type="rss", url=url)
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=t1, source=source)
    repo.bind_topic_source(topic=t2, source=source)

    result = asyncio.run(run_tick(session=db_session, settings=Settings(), push=False))
    assert result.total_created == 4
    assert len(result.per_source) == 2
    assert {r.topic_name for r in result.per_source} == {"T1", "T2"}
    assert all(r.created == 2 for r in result.per_source)


def test_run_tick_dedupes_alert_push_across_topics(db_session, monkeypatch):
    calls: list[str] = []

    async def fake_push_dingtalk_markdown(*, repo, settings, idempotency_key: str, title: str, markdown: str):  # noqa: ANN001, ARG001
        push = repo.reserve_push_attempt(channel="dingtalk", idempotency_key=idempotency_key, max_attempts=settings.push_max_attempts)
        if not push:
            return False
        repo.mark_push_sent(push)
        calls.append(idempotency_key)
        return True

    async def fake_llm_gate_alert_candidate(**_kwargs):  # noqa: ANN003
        return None

    monkeypatch.setattr("tracker.runner.push_dingtalk_markdown", fake_push_dingtalk_markdown)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", fake_llm_gate_alert_candidate)

    feed_path = Path(__file__).with_name("fixtures").joinpath("rss_sample.xml")
    url = f"file://{feed_path}"

    repo = Repo(db_session)
    t1 = repo.add_topic(name="T1", query="ai chips")
    t2 = repo.add_topic(name="T2", query="ai chips")
    # Force both topics to alert on the same items.
    t1.alert_keywords = "AI chips"
    t2.alert_keywords = "AI chips"
    # Allow multiple alerts in the same tick so we can validate cross-topic de-dupe.
    t1.alert_cooldown_minutes = 0
    t2.alert_cooldown_minutes = 0
    db_session.commit()

    source = repo.add_source(type="rss", url=url)
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=t1, source=source)
    repo.bind_topic_source(topic=t2, source=source)

    settings = Settings(dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example")
    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    assert len(calls) == 2
    topic_ids = {key.rsplit(":", 1)[-1] for key in calls}
    assert topic_ids in ({str(t1.id)}, {str(t2.id)})



def test_run_tick_failed_alert_on_one_topic_does_not_block_sibling_topic(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001, ARG001
        return [
            FetchedEntry(
                url="https://example.com/ai-chip-breakthrough",
                title="AI chips breakthrough",
                summary="AI chips breakthrough",
            )
        ]

    calls: list[str] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):  # noqa: ANN001, ARG001
        calls.append(idempotency_key)
        if len(calls) == 1:
            raise RuntimeError("transient webhook failure")
        return True

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    repo = Repo(db_session)
    t1 = repo.add_topic(name="T1", query="ai chips")
    t2 = repo.add_topic(name="T2", query="ai chips")
    t1.alert_keywords = "AI chips"
    t2.alert_keywords = "AI chips"
    t1.alert_cooldown_minutes = 0
    t2.alert_cooldown_minutes = 0
    db_session.commit()

    source = repo.add_source(type="html_list", url="https://example.com/list")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=t1, source=source)
    repo.bind_topic_source(topic=t2, source=source)

    settings = Settings(webhook_url="https://example.invalid/webhook")
    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    assert len(calls) == 2
    assert any(key.endswith(f":{t1.id}") for key in calls)
    assert any(key.endswith(f":{t2.id}") for key in calls)


def test_run_tick_immediate_alert_rule_bypasses_llm_and_dedupes_across_topics(db_session, monkeypatch):
    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None, **_kwargs):  # noqa: ANN001, ARG001
        return [
            FetchedEntry(
                url="https://linux.do/t/topic/123",
                title="求个冰的邀请码，感谢",
                summary="linux.do post",
            )
        ]

    async def fake_llm_curate_topic_items(**_kwargs):  # noqa: ANN003
        raise AssertionError("immediate alert rule should bypass topic llm curation")

    async def fake_push_dingtalk_markdown(*, repo, settings, idempotency_key: str, title: str, markdown: str):  # noqa: ANN001, ARG001
        push = repo.reserve_push_attempt(channel="dingtalk", idempotency_key=idempotency_key, max_attempts=settings.push_max_attempts)
        if not push:
            return False
        repo.mark_push_sent(push)
        calls.append(idempotency_key)
        return True

    async def fake_llm_gate_alert_candidate(**_kwargs):  # noqa: ANN003
        return None

    calls: list[str] = []
    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_curate_topic_items", fake_llm_curate_topic_items)
    monkeypatch.setattr("tracker.runner.push_dingtalk_markdown", fake_push_dingtalk_markdown)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", fake_llm_gate_alert_candidate)

    repo = Repo(db_session)
    t1 = repo.add_topic(name="T1", query="irrelevant")
    t2 = repo.add_topic(name="T2", query="irrelevant")
    repo.upsert_topic_policy(topic_id=t1.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")
    repo.upsert_topic_policy(topic_id=t2.id, llm_curation_enabled=True, llm_curation_prompt="pick urgent only")

    source = repo.add_source(type="discourse", url="https://linux.do/latest.json")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=t1, source=source)
    repo.bind_topic_source(topic=t2, source=source)

    settings = Settings(
        dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        priority_lane_enabled=False,
        immediate_alert_rules_json=json.dumps(
            [
                {
                    "host": "linux.do",
                    "title_all": ["邀请码", "冰"],
                    "reason": "matched immediate alert rule: linux.do 邀请码+冰",
                }
            ],
            ensure_ascii=False,
        ),
    )

    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    assert len(calls) == 1

    item = repo.get_item_by_canonical_url("https://linux.do/t/topic/123")
    assert item is not None
    row1 = repo.get_item_topic(item_id=item.id, topic_id=t1.id)
    row2 = repo.get_item_topic(item_id=item.id, topic_id=t2.id)
    assert row1 is not None and row1.decision == "alert"
    assert row2 is not None and row2.decision == "alert"
