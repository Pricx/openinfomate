from __future__ import annotations

import asyncio
from pathlib import Path

import httpx

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
    async def fake_post(self: httpx.AsyncClient, url: str, json: dict, **_kwargs):  # noqa: ANN001
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

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

    pushes = repo.list_pushes(channel="dingtalk", status="sent", limit=10)
    assert len(pushes) == 2
    topic_ids = {p.idempotency_key.rsplit(":", 1)[-1] for p in pushes}
    assert topic_ids in ({str(t1.id)}, {str(t2.id)})
