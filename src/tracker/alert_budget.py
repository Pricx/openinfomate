from __future__ import annotations

import datetime as dt

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from tracker.models import AlertBudget


def try_consume_alert_budget(
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

    budget = session.scalar(
        select(AlertBudget).where(and_(AlertBudget.topic_id == topic_id, AlertBudget.day == day))
    )
    if not budget:
        budget = AlertBudget(topic_id=topic_id, day=day, sent_count=0)
        session.add(budget)

    if budget.sent_count >= daily_cap:
        return False

    if budget.last_sent_at:
        delta = now - budget.last_sent_at
        if delta < dt.timedelta(minutes=max(0, cooldown_minutes)):
            return False

    budget.sent_count += 1
    budget.last_sent_at = now
    session.commit()
    return True
