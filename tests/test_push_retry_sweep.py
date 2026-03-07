from __future__ import annotations

import asyncio

from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_retry_failed_pushes_retries_digest_keys(tmp_path, monkeypatch):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/test.db",
        dingtalk_webhook_url="https://oapi.dingtalk.com/robot/send?access_token=example",
    )

    # Avoid real network.
    async def noop_send_markdown(self, *, title: str, markdown: str) -> None:  # type: ignore[no-untyped-def]
        return None

    monkeypatch.setattr("tracker.push.dingtalk.DingTalkPusher.send_markdown", noop_send_markdown)

    engine, make_session = session_factory(settings)
    from tracker.models import Base

    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="T", query="ai", digest_cron="0 9 * * *")
        key = f"digest:{topic.id}:2020-01-01"
        repo.upsert_report(kind="digest", idempotency_key=key, topic_id=topic.id, title="Digest: T", markdown="# T\n")

        # Seed a failed push log.
        push = repo.reserve_push_attempt(channel="dingtalk", idempotency_key=key, max_attempts=3)
        assert push is not None
        repo.mark_push_failed(push, error="boom")

        from tracker.push_ops import retry_failed_pushes

        out = asyncio.run(retry_failed_pushes(session=session, settings=settings, max_keys=10))

        assert [r.idempotency_key for r in out] == [key]

        rows = repo.list_pushes(channel="dingtalk", idempotency_key=key, limit=10)
        assert len(rows) == 1
        assert rows[0].status == "sent"
        assert rows[0].attempts == 2


def test_retry_failed_pushes_skips_unsupported_keys(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/test.db")
    engine, make_session = session_factory(settings)
    from tracker.models import Base

    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        # Seed a failed push log with an unsupported idempotency key.
        push = repo.reserve_push_attempt(channel="dingtalk", idempotency_key="push_test:dingtalk:x", max_attempts=3)
        assert push is not None
        repo.mark_push_failed(push, error="boom")

        from tracker.push_ops import retry_failed_pushes

        out = asyncio.run(retry_failed_pushes(session=session, settings=settings, max_keys=10))
        assert out == []



def test_make_manual_key_suffix_is_unique():
    from tracker.push_ops import make_manual_key_suffix

    s1 = make_manual_key_suffix()
    s2 = make_manual_key_suffix()

    assert s1.startswith("manual-")
    assert s2.startswith("manual-")
    assert s1 != s2


def test_push_test_uses_runtime_env_telegram_token(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text('TRACKER_TELEGRAM_BOT_TOKEN=\"ENVTEST\"\n', encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{tmp_path}/push-test.db", env_path=str(env_path))

    engine, make_session = session_factory(settings)
    from tracker.models import Base

    Base.metadata.create_all(engine)

    seen: dict[str, str] = {}

    async def fake_push_telegram_text(*, repo, settings, idempotency_key: str, text: str, disable_preview=True, replace_sent=False):  # noqa: ANN001, ARG001
        seen["token"] = str(settings.telegram_bot_token or "")
        seen["key"] = idempotency_key
        return True

    monkeypatch.setattr("tracker.push_ops.push_telegram_text", fake_push_telegram_text)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")

        from tracker.push_ops import push_test

        out = asyncio.run(push_test(session=session, settings=settings, only="telegram"))

    assert out == [("telegram", "sent")]
    assert seen["token"] == "ENVTEST"
    assert seen["key"].startswith("push_test:telegram:")
