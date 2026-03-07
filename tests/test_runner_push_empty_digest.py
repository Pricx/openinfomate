from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from tracker.db import session_factory
from tracker.models import Base, Item, ItemTopic, Source
from tracker.repo import Repo
from tracker.runner import DigestResult, run_curated_info
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

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None):  # noqa: ANN001, ARG001
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

    async def _fake_run_digest(*, session, settings, hours, push, topic_ids=None, key_suffix=None):  # noqa: ANN001, ARG001
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
