from __future__ import annotations

import asyncio

from sqlalchemy import select

from tracker.models import PushLog
from tracker.repo import Repo
from tracker.runner import run_tick
from tracker.settings import Settings


def test_run_tick_pushes_source_disabled_notice(db_session, monkeypatch):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="x")
    source = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)

    async def fake_fetch_with_state(*_args, **_kwargs):
        raise RuntimeError("boom")

    async def fake_send_markdown(self, *, title: str, markdown: str) -> None:
        return None

    monkeypatch.setattr("tracker.runner.RssConnector.fetch_with_state", fake_fetch_with_state)
    monkeypatch.setattr("tracker.push.dingtalk.DingTalkPusher.send_markdown", fake_send_markdown)

    settings = Settings(
        rss_min_interval_seconds=0,
        source_disable_after_errors=1,
        dingtalk_webhook_url="https://example.invalid",
    )
    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    disabled = repo.get_source_by_id(source.id)
    assert disabled
    assert disabled.enabled is False

    pushes = list(db_session.scalars(select(PushLog).order_by(PushLog.id)))
    assert len(pushes) == 1
    assert pushes[0].channel == "dingtalk"
    assert pushes[0].status == "sent"
    assert pushes[0].idempotency_key.startswith(f"source_disabled:{source.id}:")
