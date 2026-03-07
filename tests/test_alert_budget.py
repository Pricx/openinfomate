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



def test_failed_alert_does_not_burn_budget_for_next_alert(db_session, monkeypatch):
    import asyncio

    from tracker.connectors.base import FetchedEntry
    from tracker.models import AlertBudget
    from tracker.runner import run_tick
    from tracker.settings import Settings

    entries_per_tick = [
        [
            FetchedEntry(
                url="https://example.com/item-1",
                title="Breaking: GPU supply crisis hits datacenters",
                summary="urgent GPU supply crunch affects datacenters",
            )
        ],
        [
            FetchedEntry(
                url="https://example.com/item-2",
                title="Breaking: GPU compiler zero-day patched",
                summary="security update for GPU compiler toolchain",
            )
        ],
    ]
    pushed_keys: list[str] = []

    async def fake_fetch_entries_for_source(*, source, timeout_seconds: int = 20, cookie_header_cb=None, **kwargs):  # noqa: ANN001, ARG001
        assert entries_per_tick
        return entries_per_tick.pop(0)

    async def fake_push_webhook_json(*, repo, settings, idempotency_key: str, payload: dict):  # noqa: ANN001, ARG001
        pushed_keys.append(idempotency_key)
        return len(pushed_keys) > 1

    monkeypatch.setattr("tracker.runner.fetch_entries_for_source", fake_fetch_entries_for_source)
    monkeypatch.setattr("tracker.runner.push_webhook_json", fake_push_webhook_json)

    repo = Repo(db_session)
    topic = repo.add_topic(name="T", query="gpu")
    topic.alert_keywords = "breaking"
    topic.alert_daily_cap = 1
    topic.alert_cooldown_minutes = 0
    db_session.commit()

    source = repo.add_source(type="hn_search", url="https://example.com/hn?q=gpu")
    repo.upsert_source_score(source_id=source.id, score=80, origin="manual")
    repo.bind_topic_source(topic=topic, source=source)

    settings = Settings(webhook_url="https://example.invalid/webhook", hn_min_interval_seconds=0)
    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    row = db_session.query(AlertBudget).filter(AlertBudget.topic_id == topic.id).one_or_none()
    assert row is None or row.sent_count == 0

    asyncio.run(run_tick(session=db_session, settings=settings, push=True))

    row = db_session.query(AlertBudget).filter(AlertBudget.topic_id == topic.id).one()
    assert row.sent_count == 1
    assert len(pushed_keys) == 2
    assert all(key.endswith(f":{topic.id}") for key in pushed_keys)
    assert pushed_keys[0] != pushed_keys[1]
