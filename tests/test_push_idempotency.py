from tracker.repo import Repo


def test_push_attempt_reserve_and_sent_is_idempotent(db_session):
    repo = Repo(db_session)
    first = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="k1", max_attempts=3)
    assert first is not None

    repo.mark_push_sent(first)

    second = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="k1", max_attempts=3)
    assert second is None


def test_push_attempt_allows_retry_until_max(db_session):
    repo = Repo(db_session)
    p1 = repo.reserve_push_attempt(channel="email", idempotency_key="k2", max_attempts=2)
    assert p1 is not None
    repo.mark_push_failed(p1, error="fail")

    p2 = repo.reserve_push_attempt(channel="email", idempotency_key="k2", max_attempts=2)
    assert p2 is not None
    assert p2.attempts == 2
    repo.mark_push_failed(p2, error="fail2")

    p3 = repo.reserve_push_attempt(channel="email", idempotency_key="k2", max_attempts=2)
    assert p3 is None


def test_any_push_sent_with_prefix_only_counts_sent(db_session):
    repo = Repo(db_session)
    p1 = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="alert:1:1", max_attempts=3)
    assert p1 is not None
    repo.mark_push_failed(p1, error="fail")
    assert repo.any_push_sent_with_prefix(idempotency_prefix="alert:1:") is False

    p2 = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="alert:1:1", max_attempts=3)
    assert p2 is not None
    repo.mark_push_sent(p2)
    assert repo.any_push_sent_with_prefix(idempotency_prefix="alert:1:") is True


def test_any_push_exists_with_prefix_counts_any_status(db_session):
    repo = Repo(db_session)
    assert repo.any_push_exists_with_prefix(idempotency_prefix="alert:1:") is False

    p1 = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="alert:1:1", max_attempts=3)
    assert p1 is not None
    assert repo.any_push_exists_with_prefix(idempotency_prefix="alert:1:") is True

    repo.mark_push_failed(p1, error="fail")
    assert repo.any_push_exists_with_prefix(idempotency_prefix="alert:1:") is True


def test_push_attempt_can_resend_when_allow_sent_is_true(db_session):
    repo = Repo(db_session)
    first = repo.reserve_push_attempt(channel="telegram", idempotency_key="k3", max_attempts=3)
    assert first is not None
    repo.mark_push_sent(first)

    second = repo.reserve_push_attempt(channel="telegram", idempotency_key="k3", max_attempts=3, allow_sent=True)
    assert second is not None
    assert second.attempts == 2
    assert second.status == "pending"


def test_mark_push_failed_persists_non_empty_detail(db_session):
    repo = Repo(db_session)
    push = repo.reserve_push_attempt(channel="telegram", idempotency_key="k4", max_attempts=3)
    assert push is not None

    repo.mark_push_failed(push, error="")

    latest = repo.list_pushes(channel="telegram", idempotency_key="k4", limit=1)
    assert latest
    assert latest[0].status == "failed"
    assert latest[0].error == "unknown push error"
