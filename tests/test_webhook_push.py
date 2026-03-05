from __future__ import annotations

import asyncio

from sqlalchemy import select

from tracker.models import PushLog
from tracker.runner import run_health_report
from tracker.settings import Settings


def test_health_report_pushes_generic_webhook_idempotently(db_session, monkeypatch):
    async def fake_send_json(self, payload):  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.push.webhook.WebhookPusher.send_json", fake_send_json)

    settings = Settings(webhook_url="https://example.invalid")
    asyncio.run(run_health_report(session=db_session, settings=settings, push=True))
    asyncio.run(run_health_report(session=db_session, settings=settings, push=True))

    pushes = list(db_session.scalars(select(PushLog).order_by(PushLog.id)))
    webhook_pushes = [p for p in pushes if p.channel == "webhook"]
    assert len(webhook_pushes) == 1
    assert webhook_pushes[0].status == "sent"

