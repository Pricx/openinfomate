from __future__ import annotations

import asyncio
import json
from pathlib import Path

from tracker.db import session_factory
from tracker.llm import _llm_provider_state_key, _record_llm_provider_result
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


async def _false_async(**_kwargs) -> bool:  # noqa: ANN003
    return False


def test_llm_provider_alerts_after_consecutive_failures(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        telegram_bot_token="token",
        output_language="zh",
        llm_failure_alert_threshold=3,
        llm_failure_alert_min_minutes=0,
        llm_failure_alert_cooldown_minutes=60,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    pushed: list[str] = []

    async def _fake_push_telegram_text(*, repo, settings, idempotency_key: str, text: str, disable_preview=True, replace_sent=False):  # noqa: ANN001, ARG001
        pushed.append(text)
        return True

    monkeypatch.setattr("tracker.llm.push_telegram_text", _fake_push_telegram_text)
    monkeypatch.setattr("tracker.llm.push_dingtalk_markdown", _false_async)
    monkeypatch.setattr("tracker.llm.push_webhook_json", _false_async)
    monkeypatch.setattr("tracker.llm.push_email_text", lambda **_kwargs: False)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        for _ in range(3):
            asyncio.run(
                _record_llm_provider_result(
                    repo=repo,
                    settings=settings,
                    kind="curate_items",
                    base_url="https://llm.example.com/v1",
                    model="gpt-main",
                    ok=False,
                    error_message="All connection attempts failed",
                )
            )
        asyncio.run(
            _record_llm_provider_result(
                repo=repo,
                settings=settings,
                kind="curate_items",
                base_url="https://llm.example.com/v1",
                model="gpt-main",
                ok=False,
                error_message="All connection attempts failed",
            )
        )
        asyncio.run(
            _record_llm_provider_result(
                repo=repo,
                settings=settings,
                kind="curate_items",
                base_url="https://llm.example.com/v1",
                model="gpt-main",
                ok=True,
            )
        )

    assert len(pushed) == 1
    assert "LLM 供应商连续失败" in pushed[0]
    assert "gpt-main" in pushed[0]

    key = _llm_provider_state_key(slot="reasoning", base_url="https://llm.example.com/v1", model="gpt-main")
    with make_session() as session:
        repo = Repo(session)
        state = json.loads(repo.get_app_config(key) or "{}")
    assert state["failure_streak"] == 0
    assert state["last_alert_streak"] == 0
    assert state["last_success_at"]


def test_llm_provider_alerts_track_aux_provider_separately(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        telegram_bot_token="token",
        llm_mini_base_url="https://mini.example.com/v1",
        llm_model_mini="gpt-mini",
        llm_failure_alert_threshold=2,
        llm_failure_alert_min_minutes=0,
        llm_failure_alert_cooldown_minutes=60,
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    pushed: list[str] = []

    async def _fake_push_telegram_text(*, repo, settings, idempotency_key: str, text: str, disable_preview=True, replace_sent=False):  # noqa: ANN001, ARG001
        pushed.append(text)
        return True

    monkeypatch.setattr("tracker.llm.push_telegram_text", _fake_push_telegram_text)
    monkeypatch.setattr("tracker.llm.push_dingtalk_markdown", _false_async)
    monkeypatch.setattr("tracker.llm.push_webhook_json", _false_async)
    monkeypatch.setattr("tracker.llm.push_email_text", lambda **_kwargs: False)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        for _ in range(2):
            asyncio.run(
                _record_llm_provider_result(
                    repo=repo,
                    settings=settings,
                    kind="triage_items",
                    base_url="https://mini.example.com/v1",
                    model="gpt-mini",
                    ok=False,
                    error_message="upstream timeout",
                )
            )

    assert len(pushed) == 1
    assert "Aux LLM" in pushed[0]

    key = _llm_provider_state_key(slot="mini", base_url="https://mini.example.com/v1", model="gpt-mini")
    with make_session() as session:
        repo = Repo(session)
        state = json.loads(repo.get_app_config(key) or "{}")
    assert state["failure_streak"] == 2
    assert state["last_alert_streak"] == 2
