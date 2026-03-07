from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from tracker.models import AlertBudget


def _get_or_create_alert_budget(*, session: Session, topic_id: int, day: str) -> AlertBudget:
    budget = session.scalar(
        select(AlertBudget).where(and_(AlertBudget.topic_id == topic_id, AlertBudget.day == day))
    )
    if not budget:
        budget = AlertBudget(topic_id=topic_id, day=day, sent_count=0)
        session.add(budget)
    return budget


def can_send_alert_under_budget(
    *,
    session: Session,
    topic_id: int,
    daily_cap: int,
    cooldown_minutes: int,
    now: dt.datetime | None = None,
) -> bool:
    if daily_cap <= 0:
        return False

    now = now or dt.datetime.utcnow()
    day = now.date().isoformat()
    budget = _get_or_create_alert_budget(session=session, topic_id=topic_id, day=day)

    if budget.sent_count >= daily_cap:
        return False

    if budget.last_sent_at:
        delta = now - budget.last_sent_at
        if delta < dt.timedelta(minutes=max(0, cooldown_minutes)):
            return False

    return True


def record_alert_delivery(
    *,
    session: Session,
    topic_id: int,
    now: dt.datetime | None = None,
) -> None:
    now = now or dt.datetime.utcnow()
    day = now.date().isoformat()
    budget = _get_or_create_alert_budget(session=session, topic_id=topic_id, day=day)
    budget.sent_count += 1
    budget.last_sent_at = now
    session.commit()


def try_consume_alert_budget(
    *,
    session: Session,
    topic_id: int,
    daily_cap: int,
    cooldown_minutes: int,
    now: dt.datetime | None = None,
) -> bool:
    if not can_send_alert_under_budget(
        session=session,
        topic_id=topic_id,
        daily_cap=daily_cap,
        cooldown_minutes=cooldown_minutes,
        now=now,
    ):
        return False
    record_alert_delivery(session=session, topic_id=topic_id, now=now)
    return True
