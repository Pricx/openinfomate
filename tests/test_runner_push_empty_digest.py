from __future__ import annotations

import asyncio
from pathlib import Path

from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import run_curated_info
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
