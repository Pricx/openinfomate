from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from sqlalchemy import select

from tracker.db import session_factory
from tracker.models import Base, Item, ItemTopic, Report, Source
from tracker.repo import Repo
from tracker.runner import CuratedInfoResult, DigestResult, run_curated_info
from tracker.settings import Settings


def test_curated_info_attempts_push_even_when_empty(tmp_path, monkeypatch):
    """
    Regression: do not silently skip pushes when the curated window has 0 items.

    "宁可 0 条也不凑数" means the LLM may output an empty selection, but the run
    should still push an explicit empty report (unless the operator disables it).
    """
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        digest_scheduler_enabled=True,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    # Seed at least 1 topic so report title/rendering is stable.
    with make_session() as session:
        Repo(session).add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")

    called: dict[str, int] = {"telegram": 0}

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        called["telegram"] += 1
        return True

    # Monkeypatch only the final dispatch step; we just want to prove the code path
    # executes even when the window is empty.
    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", lambda **_k: False, raising=True)

    async def _run() -> None:
        with make_session() as session:
            await run_curated_info(session=session, settings=settings, hours=2, push=True, key_suffix="test")

    asyncio.run(_run())
    assert called["telegram"] >= 1


def test_curated_info_auto_repairs_stalled_candidates_before_empty_window(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/1",
            canonical_url="https://example.com/p/1",
            title="Recovered item",
            created_at=now - dt.timedelta(hours=1),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(hours=1),
            )
        )
        session.commit()
        item_id = int(item.id)
        topic_id = int(topic.id)

    called = {"digest": 0}

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        called["digest"] += 1
        repo = Repo(session)
        row = repo.get_item_topic(item_id=item_id, topic_id=topic_id)
        assert row is not None
        row.decision = "digest"
        row.reason = "llm_why: repaired\nllm_hint: digest"
        session.commit()
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="repair-test",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert called["digest"] == 1
    assert "Recovered item" in markdown


def test_curated_info_historical_replay_is_bounded_to_target_window(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    window_end = dt.datetime(2026, 3, 7, 8, 0, 0)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()

        in_window = Item(
            source_id=int(source.id),
            url="https://example.com/p/in-window",
            canonical_url="https://example.com/p/in-window",
            title="In-window digest",
            created_at=window_end - dt.timedelta(minutes=30),
        )
        future_digest = Item(
            source_id=int(source.id),
            url="https://example.com/p/future-digest",
            canonical_url="https://example.com/p/future-digest",
            title="Future digest",
            created_at=window_end + dt.timedelta(minutes=10),
        )
        future_candidate = Item(
            source_id=int(source.id),
            url="https://example.com/p/future-candidate",
            canonical_url="https://example.com/p/future-candidate",
            title="Future candidate",
            created_at=window_end + dt.timedelta(minutes=20),
        )
        session.add_all([in_window, future_digest, future_candidate])
        session.flush()

        session.add_all(
            [
                ItemTopic(
                    item_id=int(in_window.id),
                    topic_id=int(topic.id),
                    decision="digest",
                    reason="llm_why: in-window\nllm_hint: digest",
                    created_at=window_end - dt.timedelta(minutes=30),
                ),
                ItemTopic(
                    item_id=int(future_digest.id),
                    topic_id=int(topic.id),
                    decision="digest",
                    reason="llm_why: future\nllm_hint: digest",
                    created_at=window_end + dt.timedelta(minutes=10),
                ),
                ItemTopic(
                    item_id=int(future_candidate.id),
                    topic_id=int(topic.id),
                    decision="candidate",
                    reason="llm curation candidate",
                    created_at=window_end + dt.timedelta(minutes=20),
                ),
            ]
        )
        session.commit()

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="historical-window",
                now=window_end,
                allow_auto_repair=False,
            )

    result = asyncio.run(_run())
    assert result.recovery_pending is False
    assert result.pending_topic_ids == ()
    assert "In-window digest" in result.markdown
    assert "Future digest" not in result.markdown
    assert "Future candidate" not in result.markdown


def test_curated_info_read_only_mode_skips_auto_repair(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/1",
            canonical_url="https://example.com/p/1",
            title="Pending item",
            created_at=now - dt.timedelta(hours=1),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(hours=1),
            )
        )
        session.commit()
        item_id = int(item.id)
        topic_id = int(topic.id)

    called = {"digest": 0}

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        called["digest"] += 1
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="read-only-replay",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
                allow_auto_repair=False,
            )

    result = asyncio.run(_run())
    assert called["digest"] == 0
    assert result.recovery_pending is True
    assert result.pending_topic_ids == (topic_id,)
    assert "暂无新条目" in result.markdown

    with make_session() as session:
        repo = Repo(session)
        row = repo.get_item_topic(item_id=item_id, topic_id=topic_id)
    assert row is not None
    assert row.decision == "candidate"


def test_curated_info_enqueues_recovery_and_skips_empty_push_when_backlog_remains(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
        curated_recovery_auto_enqueue_enabled=True,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/1",
            canonical_url="https://example.com/p/1",
            title="Queued item",
            created_at=now - dt.timedelta(hours=1),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(hours=1),
            )
        )
        session.commit()

    called = {"telegram": 0}

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, **_kwargs):  # noqa: ANN001, ARG001
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        called["telegram"] += 1
        return True

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", lambda **_k: False, raising=True)

    async def _run():
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="queued-test",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    result = asyncio.run(_run())
    assert result.recovery_pending is True
    assert result.pushed == 0
    assert called["telegram"] == 0

    with make_session() as session:
        repo = Repo(session)
        queue_raw = repo.get_app_config("curated_recovery_queue_json") or ""
        reports = list(session.scalars(select(Report)))

    assert "后台恢复队列" in result.markdown
    assert queue_raw
    assert reports == []


def test_curated_info_auto_repairs_stalled_candidates_even_with_existing_rows(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)

        alert_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/alert",
            canonical_url="https://example.com/p/alert",
            title="Existing alert",
            created_at=now - dt.timedelta(minutes=50),
        )
        digest_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/digest",
            canonical_url="https://example.com/p/digest",
            title="Recovered digest",
            created_at=now - dt.timedelta(minutes=40),
        )
        session.add_all([alert_item, digest_item])
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(alert_item.id),
                topic_id=int(topic.id),
                decision="alert",
                reason="llm_why: alert\nllm_hint: alert",
                created_at=now - dt.timedelta(minutes=50),
            )
        )
        session.add(
            ItemTopic(
                item_id=int(digest_item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="",
                created_at=now - dt.timedelta(minutes=40),
            )
        )
        session.commit()
        digest_item_id = int(digest_item.id)
        topic_id = int(topic.id)

    called = {"digest": 0}

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        called["digest"] += 1
        repo = Repo(session)
        row = repo.get_item_topic(item_id=digest_item_id, topic_id=topic_id)
        assert row is not None
        row.decision = "digest"
        row.reason = "llm_why: repaired\nllm_hint: digest"
        session.commit()
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="repair-existing-rows",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert called["digest"] == 1
    assert "Existing alert" in markdown
    assert "Recovered digest" in markdown


def test_curated_info_queues_recovery_when_candidates_remain_after_repair(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
        curated_recovery_auto_enqueue_enabled=True,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/queued",
            canonical_url="https://example.com/p/queued",
            title="Queued recovery item",
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(minutes=30),
            )
        )
        session.commit()

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)

    async def _run() -> None:
        with make_session() as session:
            await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="repair-queue-test",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    asyncio.run(_run())

    with make_session() as session:
        repo = Repo(session)
        raw = repo.get_app_config("curated_recovery_queue_json") or ""
    assert '"hours": 2' in raw
    assert '"push": true' in raw.lower()


def test_curated_info_does_not_auto_enqueue_recovery_by_default(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/no-auto-enqueue",
            canonical_url="https://example.com/p/no-auto-enqueue",
            title="Pending item",
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(minutes=30),
            )
        )
        session.commit()

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="repair-no-auto-enqueue",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    result = asyncio.run(_run())
    assert result.recovery_pending is True
    assert "本轮不会推送" in result.markdown

    with make_session() as session:
        repo = Repo(session)
        raw = repo.get_app_config("curated_recovery_queue_json") or ""

    assert raw == ""


def test_curated_info_skips_partial_push_when_existing_rows_and_pending_backlog_remain(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)

        alert_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/alert",
            canonical_url="https://example.com/p/alert",
            title="Existing alert",
            created_at=now - dt.timedelta(minutes=50),
        )
        pending_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/pending",
            canonical_url="https://example.com/p/pending",
            title="Still pending",
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add_all([alert_item, pending_item])
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(alert_item.id),
                topic_id=int(topic.id),
                decision="alert",
                reason="llm_why: alert\nllm_hint: alert",
                created_at=now - dt.timedelta(minutes=50),
            )
        )
        session.add(
            ItemTopic(
                item_id=int(pending_item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(minutes=30),
            )
        )
        session.commit()

    called = {"telegram": 0}

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None, now=None, **_kwargs):  # noqa: ANN001, ARG001
        return DigestResult(since=dt.datetime.utcnow(), per_topic=[])

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        called["telegram"] += 1
        return True

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "run_digest", _fake_run_digest, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", lambda **_k: False, raising=True)

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="repair-existing-rows-push",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    result = asyncio.run(_run())
    assert result.recovery_pending is True
    assert result.pushed == 0
    assert called["telegram"] == 0
    assert "本轮不会推送" in result.markdown

    with make_session() as session:
        repo = Repo(session)
        queue_raw = repo.get_app_config("curated_recovery_queue_json") or ""
        reports = list(session.scalars(select(Report)))

    assert queue_raw == ""
    assert reports == []


def test_curated_info_skips_terminal_fulltext_failures_after_auto_repair_and_still_pushes(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=True,
        fulltext_max_fetches_per_topic=5,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)

        alert_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/alert",
            canonical_url="https://example.com/p/alert",
            title="Existing alert",
            content_text="already curated",
            created_at=now - dt.timedelta(minutes=50),
        )
        stuck_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/stuck",
            canonical_url="https://example.com/p/stuck",
            title="Stuck candidate",
            content_text="short snippet",
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add_all([alert_item, stuck_item])
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(alert_item.id),
                topic_id=int(topic.id),
                decision="alert",
                reason="llm_why: alert\nllm_hint: alert",
                created_at=now - dt.timedelta(minutes=50),
            )
        )
        session.add(
            ItemTopic(
                item_id=int(stuck_item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(minutes=30),
            )
        )
        session.commit()
        stuck_item_id = int(stuck_item.id)
        topic_id = int(topic.id)

    called = {"telegram": 0, "fulltext": 0, "llm": 0}

    async def _fake_fetch_fulltext_for_url(  # type: ignore[no-untyped-def]
        *, url: str, timeout_seconds: int, max_chars: int, discourse_cookie: str | None = None, cookie_header: str | None = None
    ):
        called["fulltext"] += 1
        assert url == "https://example.com/p/stuck"
        raise RuntimeError()

    async def _fake_llm_curate_topic_items(  # type: ignore[no-untyped-def]
        *, repo, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        called["llm"] += 1
        raise RuntimeError("llm unavailable")

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        called["telegram"] += 1
        return True

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "fetch_fulltext_for_url", _fake_fetch_fulltext_for_url, raising=True)
    monkeypatch.setattr(runner_mod, "llm_curate_topic_items", _fake_llm_curate_topic_items, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", lambda **_k: False, raising=True)

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="terminal-fulltext-skip",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    result = asyncio.run(_run())
    assert called["fulltext"] >= 1
    assert called["llm"] >= 1
    assert called["telegram"] == 1
    assert result.recovery_pending is False
    assert result.pushed == 1

    with make_session() as session:
        repo = Repo(session)
        row = repo.get_item_topic(item_id=stuck_item_id, topic_id=topic_id)

    assert row is not None
    assert row.decision == "ignore"
    assert "autorepair_skip: unresolved after fulltext fetch failure" in row.reason


def test_curated_info_skips_llm_failures_after_auto_repair_and_still_pushes(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        cron_timezone="+8",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_model_reasoning="dummy",
        llm_curation_enabled=True,
        fulltext_enabled=False,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        repo.upsert_topic_policy(topic_id=topic.id, llm_curation_enabled=True, llm_curation_prompt="p")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)

        alert_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/alert-existing",
            canonical_url="https://example.com/p/alert-existing",
            title="Existing alert",
            content_text="already curated",
            created_at=now - dt.timedelta(minutes=50),
        )
        stuck_item = Item(
            source_id=int(source.id),
            url="https://example.com/p/llm-stuck",
            canonical_url="https://example.com/p/llm-stuck",
            title="LLM stuck candidate",
            content_text="enough content to avoid fulltext fetch",
            created_at=now - dt.timedelta(minutes=30),
        )
        session.add_all([alert_item, stuck_item])
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(alert_item.id),
                topic_id=int(topic.id),
                decision="alert",
                reason="llm_why: alert\nllm_hint: alert",
                created_at=now - dt.timedelta(minutes=50),
            )
        )
        session.add(
            ItemTopic(
                item_id=int(stuck_item.id),
                topic_id=int(topic.id),
                decision="candidate",
                reason="llm curation candidate",
                created_at=now - dt.timedelta(minutes=30),
            )
        )
        session.commit()
        stuck_item_id = int(stuck_item.id)
        topic_id = int(topic.id)

    called = {"telegram": 0, "llm": 0}

    async def _fake_llm_curate_topic_items(  # type: ignore[no-untyped-def]
        *, repo, settings, topic, policy_prompt, candidates, recent_sent=None, max_digest, max_alert, usage_cb=None
    ):
        called["llm"] += 1
        raise RuntimeError("LLM response missing text")

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        called["telegram"] += 1
        return True

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "llm_curate_topic_items", _fake_llm_curate_topic_items, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", lambda **_k: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", lambda **_k: False, raising=True)

    async def _run() -> CuratedInfoResult:
        with make_session() as session:
            return await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=True,
                key_suffix="llm-failure-skip",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )

    result = asyncio.run(_run())
    assert called["llm"] >= 1
    assert called["telegram"] == 1
    assert result.recovery_pending is False
    assert result.pushed == 1

    with make_session() as session:
        repo = Repo(session)
        row = repo.get_item_topic(item_id=stuck_item_id, topic_id=topic_id)

    assert row is not None
    assert row.decision == "ignore"
    assert "autorepair_skip: unresolved after llm curation failure" in row.reason


def test_curated_info_localizes_non_target_language_titles(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
        output_language="zh",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_model_mini="mini-dummy",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://example.com/feed")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://example.com/p/1",
            canonical_url="https://example.com/p/1",
            title="Show HN: Contrabass – Go and Charm Stack Implementation of OpenAI's Symphony",
            content_text="A new orchestration CLI implemented in Go and Charm.",
            created_at=now - dt.timedelta(minutes=10),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="digest",
                reason="llm_summary: Contrabass 开源实现了类似 OpenAI Symphony 的编程代理编排器。\nllm_hint: digest",
                created_at=now - dt.timedelta(minutes=10),
            )
        )
        session.commit()

    called = {"target_lang": None}

    async def _fake_localize(*, repo=None, settings, target_lang, items, usage_cb=None):  # noqa: ANN001, ARG001
        called["target_lang"] = target_lang
        return {int(items[0]["item_id"]): "Contrabass：Go/Charm 实现的 OpenAI Symphony 式代理编排器"}

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "llm_localize_item_titles", _fake_localize, raising=True)

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="title-localize",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert called["target_lang"] == "zh"
    assert "Contrabass：Go/Charm 实现的 OpenAI Symphony 式代理编排器" in markdown
    assert "Show HN: Contrabass" not in markdown


def test_curated_info_fetches_fulltext_for_low_signal_titles(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
        output_language="zh",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_model_mini="mini-dummy",
        http_timeout_seconds=5,
        fulltext_timeout_seconds=5,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="", digest_cron="0 9 * * *")
        source = Source(type="rss", url="https://github.com/org/repo/releases.atom")
        session.add(source)
        session.flush()
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://github.com/microsoft/vscode-docs/blob/main/docs/copilot/reference/copilot-vscode-features.md",
            canonical_url="https://github.com/microsoft/vscode-docs/blob/main/docs/copilot/reference/copilot-vscode-features.md",
            title="vscode-docs/docs/copilot/reference/copilot-vscode-features.md ... - GitHub",
            content_text="",
            created_at=now - dt.timedelta(minutes=8),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="digest",
                reason="",
                created_at=now - dt.timedelta(minutes=8),
            )
        )
        session.commit()

    seen = {"fetched": 0, "snippet": ""}

    async def _fake_fetch(*, url, timeout_seconds, max_chars, discourse_cookie=None, cookie_header=None):  # noqa: ANN001, ARG001
        seen["fetched"] += 1
        return "This page documents GitHub Copilot features in VS Code, including Copilot Free limits, model selection, and MCP tool connections."

    async def _fake_localize(*, repo=None, settings, target_lang, items, usage_cb=None):  # noqa: ANN001, ARG001
        seen["snippet"] = str(items[0].get("snippet") or "")
        return {int(items[0]["item_id"]): "VS Code Copilot 功能速查：免费额度、多模型选择与 MCP 工具连接"}

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "fetch_fulltext_for_url", _fake_fetch, raising=True)
    monkeypatch.setattr(runner_mod, "llm_localize_item_titles", _fake_localize, raising=True)

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="title-fulltext",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert seen["fetched"] == 1
    assert "Copilot Free limits" in seen["snippet"]
    assert "VS Code Copilot 功能速查：免费额度、多模型选择与 MCP 工具连接" in markdown



def test_curated_info_applies_domain_down_rank_score_gate(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
        domain_quality_low_domains="dev.to",
        source_quality_min_score=50,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = repo.add_source(type="rss", url="https://dev.to/feed/tag/agents")
        repo.upsert_source_score(source_id=source.id, score=74, origin="manual")
        now = dt.datetime(2026, 3, 7, 8, 0, 0)
        item = Item(
            source_id=int(source.id),
            url="https://dev.to/ghost-task",
            canonical_url="https://dev.to/ghost-task",
            title="Ghost task problem",
            created_at=now - dt.timedelta(minutes=10),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="digest",
                reason="llm_why: field report\nllm_hint: digest",
                created_at=now - dt.timedelta(minutes=10),
            )
        )
        session.commit()

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="domain-soft-downrank",
                now=dt.datetime(2026, 3, 7, 8, 0, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert "Ghost task problem" not in markdown


def test_curated_info_feedback_source_score_does_not_hard_filter_source(tmp_path):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=False,
        cron_timezone="+8",
        source_quality_min_score=50,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="AI Agents", query="agent", digest_cron="0 9 * * *")
        source = repo.add_source(type="discourse", url="https://linux.do/latest.json")
        repo.upsert_source_score(source_id=source.id, score=43, origin="feedback")
        now = dt.datetime(2026, 4, 12, 14, 1, 0)
        item = Item(
            source_id=int(source.id),
            url="https://linux.do/t/topic/1949576",
            canonical_url="https://linux.do/t/topic/1949576",
            title="Anthropic 出了harness 产品",
            created_at=now - dt.timedelta(minutes=10),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=int(topic.id),
                decision="digest",
                reason="llm_why: forum field report\nllm_hint: digest",
                created_at=now - dt.timedelta(minutes=10),
            )
        )
        session.commit()

    async def _run() -> str:
        with make_session() as session:
            result = await run_curated_info(
                session=session,
                settings=settings,
                hours=2,
                push=False,
                key_suffix="feedback-source-soft-signal",
                now=dt.datetime(2026, 4, 12, 14, 1, 0),
            )
            return result.markdown

    markdown = asyncio.run(_run())
    assert "Anthropic 出了harness 产品" in markdown
