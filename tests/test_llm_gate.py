from __future__ import annotations

import asyncio

from sqlalchemy import select

from tracker.connectors.base import FetchedEntry
from tracker.llm import LlmGateResult
from tracker.models import ItemTopic
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def _seed_one_alert_binding(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    topic.alert_keywords = "breaking"
    db_session.commit()

    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=x")
    repo.upsert_source_score(source_id=source.id, score=90, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)
    return repo, topic, source


def test_llm_gate_can_downgrade_alert_to_digest(db_session, monkeypatch):
    _repo, _topic, _source = _seed_one_alert_binding(db_session)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="Breaking: something happened",
                summary="snippet text",
            )
        ]

    async def fake_gate(*, repo=None, settings, topic, title: str, url: str, content_text: str, usage_cb=None):  # noqa: ANN001
        return LlmGateResult(decision="digest", reason="noise")

    pushed: list[dict] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):
        pushed.append(payload)
        return True

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", fake_gate)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    settings = Settings(
        webhook_url="http://example.invalid/webhook",
        llm_base_url="http://llm.local",
        llm_model="dummy",
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=True))
    assert result.total_created == 1
    assert result.total_pushed_alerts == 0
    assert pushed == []

    it = db_session.scalar(select(ItemTopic))
    assert it
    assert it.decision == "digest"
    assert it.quality_score == 0
    assert "llm_gate=digest" in it.reason
    assert "llm_reason=noise" in it.reason


def test_llm_gate_allows_alert_and_pushes(db_session, monkeypatch):
    _repo, _topic, _source = _seed_one_alert_binding(db_session)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="Breaking: something happened",
                summary="snippet text",
            )
        ]

    async def fake_gate(*, repo=None, settings, topic, title: str, url: str, content_text: str, usage_cb=None):  # noqa: ANN001
        return LlmGateResult(decision="alert", reason="urgent")

    pushed: list[dict] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):
        pushed.append(payload)
        return True

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", fake_gate)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    settings = Settings(
        webhook_url="http://example.invalid/webhook",
        llm_base_url="http://llm.local",
        llm_model="dummy",
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=True))
    assert result.total_created == 1
    assert result.total_pushed_alerts == 1
    assert len(pushed) == 1

    it = db_session.scalar(select(ItemTopic))
    assert it
    assert it.decision == "alert"
    assert it.quality_score == 0
    assert "llm_gate=alert" in it.reason
    assert "llm_reason=urgent" in it.reason


def test_llm_gate_failure_falls_back_to_alert(db_session, monkeypatch):
    _repo, _topic, _source = _seed_one_alert_binding(db_session)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="Breaking: something happened",
                summary="snippet text",
            )
        ]

    async def boom(*args, **kwargs):
        raise RuntimeError("llm down")

    pushed: list[dict] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):
        pushed.append(payload)
        return True

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", boom)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    settings = Settings(
        webhook_url="http://example.invalid/webhook",
        llm_base_url="http://llm.local",
        llm_model="dummy",
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=True))
    assert result.total_created == 1
    assert result.total_pushed_alerts == 1
    assert len(pushed) == 1

    it = db_session.scalar(select(ItemTopic))
    assert it
    assert it.decision == "alert"
    assert it.quality_score == 0



def test_llm_gate_alert_budget_falls_back_to_digest(db_session, monkeypatch):
    _repo, _topic, _source = _seed_one_alert_binding(db_session)

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None):  # noqa: ANN001
        return [
            FetchedEntry(
                url="https://example.com/a",
                title="Breaking: something happened",
                summary="snippet text",
            )
        ]

    async def fake_gate(*, repo=None, settings, topic, title: str, url: str, content_text: str, usage_cb=None):  # noqa: ANN001
        return LlmGateResult(decision="alert", reason="urgent")

    pushed: list[dict] = []

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):
        pushed.append(payload)
        return True

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.llm_gate_alert_candidate", fake_gate)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)
    monkeypatch.setattr("tracker.runner.can_send_alert_under_budget", lambda **kwargs: False)

    settings = Settings(
        webhook_url="http://example.invalid/webhook",
        llm_base_url="http://llm.local",
        llm_model="dummy",
    )

    result = asyncio.run(run_tick(session=db_session, settings=settings, push=True))
    assert result.total_created == 1
    assert result.total_pushed_alerts == 0
    assert pushed == []

    it = db_session.scalar(select(ItemTopic))
    assert it
    assert it.decision == "digest"
    assert "alert_suppressed_by_budget" in (it.reason or "")
