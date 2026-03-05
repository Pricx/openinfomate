from __future__ import annotations

import datetime as dt

from tracker.repo import Repo


def test_mute_rule_upsert_and_expiry(db_session):
    repo = Repo(db_session)

    now = dt.datetime(2026, 2, 17, 0, 0, 0)
    until = now + dt.timedelta(days=3)

    repo.upsert_mute_rule(scope="domain", key="forum.example.com", muted_until=until, reason="test")
    assert repo.is_muted(scope="domain", key="forum.example.com", when=now) is True
    assert repo.is_muted(scope="domain", key="forum.example.com", when=until + dt.timedelta(seconds=1)) is False

    # Update existing rule.
    until2 = now + dt.timedelta(days=1)
    repo.upsert_mute_rule(scope="domain", key="forum.example.com", muted_until=until2, reason="test2")
    assert repo.is_muted(scope="domain", key="forum.example.com", when=now) is True
    assert repo.is_muted(scope="domain", key="forum.example.com", when=until2 + dt.timedelta(seconds=1)) is False


def test_record_telegram_messages_idempotent(db_session):
    repo = Repo(db_session)
    inserted = repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:2",
        message_ids=[10, 11],
        kind="alert",
        item_id=1,
    )
    assert inserted == 2
    inserted2 = repo.record_telegram_messages(
        chat_id="123",
        idempotency_key="alert:1:2",
        message_ids=[10],
        kind="alert",
        item_id=1,
    )
    assert inserted2 == 0

    tm = repo.get_telegram_message(chat_id="123", message_id=10)
    assert tm is not None
    assert tm.item_id == 1
    assert (tm.idempotency_key or "").startswith("alert:")


def test_feedback_events_pending_and_applied(db_session):
    repo = Repo(db_session)
    ev = repo.add_feedback_event(
        channel="telegram",
        user_id="u1",
        chat_id="c1",
        message_id=1,
        kind="like",
        value_int=0,
        item_id=123,
        url="https://example.com/x",
        domain="example.com",
        note="test",
        raw="{}",
    )
    pending = repo.list_pending_feedback_events(limit=10)
    assert [p.id for p in pending] == [ev.id]

    repo.mark_feedback_events_applied(ids=[ev.id])
    pending2 = repo.list_pending_feedback_events(limit=10)
    assert pending2 == []
