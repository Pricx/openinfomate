from __future__ import annotations

import datetime as dt
from pathlib import Path

from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


def test_create_telegram_task_retries_negative_prompt_message_collisions(tmp_path):
    db_path = Path(tmp_path) / "repo-telegram-collision.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        first = repo.create_telegram_task(
            chat_id="123",
            user_id="u1",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=1,
            query="a",
        )
        second = repo.create_telegram_task(
            chat_id="123",
            user_id="u1",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=2,
            query="b",
        )

        assert first.prompt_message_id == -1
        assert second.prompt_message_id < 0
        assert second.prompt_message_id != first.prompt_message_id


def test_claim_next_pending_telegram_task_reclaims_stale_running_rows(tmp_path):
    db_path = Path(tmp_path) / "repo-telegram-stale.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        task = repo.create_telegram_task(
            chat_id="123",
            user_id="u1",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=1,
            query="a",
        )
        task.status = "running"
        task.started_at = dt.datetime.utcnow() - dt.timedelta(hours=2)
        session.commit()

        claimed = repo.claim_next_pending_telegram_task(
            kind="config_agent",
            status="pending",
            mark_running=True,
            stale_running_seconds=60,
        )

        assert claimed is not None
        assert int(claimed.id) == int(task.id)
        assert claimed.status == "running"
        assert claimed.started_at is not None
        assert claimed.started_at > dt.datetime.utcnow() - dt.timedelta(minutes=1)

def test_claim_next_pending_telegram_task_keeps_apply_reclaim_out_of_plan_lane(tmp_path):
    db_path = Path(tmp_path) / "repo-telegram-config-agent-phase.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        task = repo.create_telegram_task(
            chat_id="123",
            user_id="u1",
            kind="config_agent",
            status="pending_apply",
            prompt_message_id=-1,
            request_message_id=1,
            query="apply",
        )
        task.provider = "apply"
        task.status = "running"
        task.started_at = dt.datetime.utcnow() - dt.timedelta(hours=2)
        session.commit()

        plan_claim = repo.claim_next_pending_telegram_task(
            kind="config_agent",
            status="pending",
            mark_running=True,
            stale_running_seconds=60,
            provider="",
            stale_provider="",
        )
        assert plan_claim is None

        apply_claim = repo.claim_next_pending_telegram_task(
            kind="config_agent",
            status="pending_apply",
            mark_running=True,
            stale_running_seconds=60,
            stale_provider="apply",
        )
        assert apply_claim is not None
        assert int(apply_claim.id) == int(task.id)


def test_mark_telegram_task_choice_resets_transient_execution_state(tmp_path):
    db_path = Path(tmp_path) / "repo-telegram-choice-reset.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        task = repo.create_telegram_task(
            chat_id="123",
            user_id="u1",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=1,
            query="draft",
        )
        task.status = "failed"
        task.result_key = "old"
        task.error = "boom"
        task.started_at = dt.datetime.utcnow() - dt.timedelta(minutes=5)
        task.finished_at = dt.datetime.utcnow() - dt.timedelta(minutes=1)
        session.commit()

        updated = repo.mark_telegram_task_choice(
            int(task.id),
            option=2,
            intent="retry",
            budget_seconds=30,
            provider="apply",
        )

        assert updated is not None
        assert updated.status == "pending"
        assert updated.result_key == ""
        assert updated.error == ""
        assert updated.started_at is None
        assert updated.finished_at is None
        assert updated.provider == "apply"

