import datetime as dt

from tracker.models import Item, PushLog, Report
from tracker.repo import Repo


def test_repo_get_topic_by_name_case_insensitive(db_session):
    repo = Repo(db_session)
    repo.add_topic(name="AI Chips", query="gpu,asic")
    assert repo.get_topic_by_name("AI Chips") is not None
    assert repo.get_topic_by_name("ai chips") is not None


def test_repo_activity_snapshot(db_session):
    repo = Repo(db_session)
    s1 = repo.add_source(type="rss", url="https://example.com/feed1")
    s2 = repo.add_source(type="rss", url="https://example.com/feed2")

    now = dt.datetime.utcnow()
    s1.last_checked_at = now - dt.timedelta(minutes=30)
    s2.last_checked_at = now - dt.timedelta(minutes=10)

    item = Item(
        source_id=s1.id,
        url="https://example.com/a",
        canonical_url="https://example.com/a",
        title="A",
    )
    db_session.add(item)
    db_session.flush()

    db_session.add(Report(kind="digest", idempotency_key="d1", markdown="d", updated_at=now - dt.timedelta(minutes=20)))
    db_session.add(Report(kind="health", idempotency_key="h1", markdown="h", updated_at=now - dt.timedelta(minutes=5)))

    db_session.add(PushLog(channel="dingtalk", idempotency_key="k1", status="failed", created_at=now - dt.timedelta(minutes=9)))
    db_session.add(
        PushLog(
            channel="dingtalk",
            idempotency_key="k2",
            status="sent",
            created_at=now - dt.timedelta(minutes=8),
            sent_at=now - dt.timedelta(minutes=7),
        )
    )
    db_session.commit()

    snap = repo.get_activity_snapshot()
    assert snap.last_tick_at is not None
    assert snap.last_digest_report_at is not None
    assert snap.last_health_report_at is not None
    assert snap.last_push_attempt_at is not None
    assert snap.last_push_sent_at is not None
