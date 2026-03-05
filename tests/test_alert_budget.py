import datetime as dt

from freezegun import freeze_time

from tracker.alert_budget import try_consume_alert_budget
from tracker.repo import Repo


def test_alert_budget_respects_daily_cap(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    topic.alert_daily_cap = 2
    topic.alert_cooldown_minutes = 0
    db_session.commit()

    assert try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=2, cooldown_minutes=0, now=dt.datetime(2026, 2, 10, 0, 0)
    )
    assert try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=2, cooldown_minutes=0, now=dt.datetime(2026, 2, 10, 0, 1)
    )
    assert not try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=2, cooldown_minutes=0, now=dt.datetime(2026, 2, 10, 0, 2)
    )


def test_alert_budget_respects_cooldown(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    topic.alert_daily_cap = 10
    topic.alert_cooldown_minutes = 60
    db_session.commit()

    t0 = dt.datetime(2026, 2, 10, 0, 0)
    assert try_consume_alert_budget(session=db_session, topic_id=topic.id, daily_cap=10, cooldown_minutes=60, now=t0)
    assert not try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=10, cooldown_minutes=60, now=t0 + dt.timedelta(minutes=30)
    )
    assert try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=10, cooldown_minutes=60, now=t0 + dt.timedelta(minutes=61)
    )


@freeze_time("2026-02-11 00:00:00")
def test_alert_budget_resets_each_day(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="")
    topic.alert_daily_cap = 1
    topic.alert_cooldown_minutes = 0
    db_session.commit()

    now = dt.datetime.utcnow()
    assert try_consume_alert_budget(session=db_session, topic_id=topic.id, daily_cap=1, cooldown_minutes=0, now=now)
    assert not try_consume_alert_budget(session=db_session, topic_id=topic.id, daily_cap=1, cooldown_minutes=0, now=now)

    tomorrow = now + dt.timedelta(days=1)
    assert try_consume_alert_budget(
        session=db_session, topic_id=topic.id, daily_cap=1, cooldown_minutes=0, now=tomorrow
    )
