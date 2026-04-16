from __future__ import annotations

import asyncio
import datetime as dt
from pathlib import Path

from tracker.collect_messages import group_collect_message_rules, parse_collect_message_rules
from tracker.db import session_factory
from tracker.models import Base, Item, ItemTopic
from tracker.repo import Repo
from tracker.runner import _localize_item_display_titles, run_collect_message, run_collect_message_batch
from tracker.settings import Settings


def test_parse_collect_message_rules_keeps_unique_source_ids():
    rules = parse_collect_message_rules(
        '[{"name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[123,124,124,125]}]'
    )
    assert len(rules) == 1
    assert rules[0].rule_id == "arxiv"
    assert rules[0].fallback_lookback_hours == 24
    assert rules[0].source_ids == (123, 124, 125)


def test_group_collect_message_rules_merges_same_cron_into_single_batch():
    rules = parse_collect_message_rules(
        """
        [
          {"id":"arxiv","name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[123,124]},
          {"id":"papers","name":"Papers","cron":"0 19 * * *","lookback_hours":48,"source_ids":[125]}
        ]
        """
    )
    groups = group_collect_message_rules(rules)
    assert len(groups) == 1
    assert groups[0].cron == "0 19 * * *"
    assert tuple(rule.rule_id for rule in groups[0].rules) == ("arxiv", "papers")


def test_run_collect_message_batch_dedupes_and_orders_by_priority(tmp_path, monkeypatch):
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
        src1 = repo.add_source(type="rss", url="https://export.arxiv.org/rss/cs.AI")
        src2 = repo.add_source(type="rss", url="https://export.arxiv.org/rss/cs.LG")
        now = dt.datetime(2026, 4, 13, 10, 0, 0)

        item_alert = Item(
            source_id=int(src1.id),
            url="https://arxiv.org/abs/2604.00001",
            canonical_url="https://arxiv.org/abs/2604.00001",
            title="Paper A alert",
            created_at=now - dt.timedelta(hours=2),
        )
        item_digest = Item(
            source_id=int(src2.id),
            url="https://arxiv.org/abs/2604.00002",
            canonical_url="https://arxiv.org/abs/2604.00002",
            title="Paper B digest",
            created_at=now - dt.timedelta(hours=3),
        )
        session.add_all([item_alert, item_digest])
        session.flush()
        session.add_all(
            [
                ItemTopic(
                    item_id=int(item_alert.id),
                    topic_id=1,
                    decision="alert",
                    reason="llm_rank: 95\nllm_why: high",
                    created_at=now - dt.timedelta(hours=2),
                ),
                ItemTopic(
                    item_id=int(item_digest.id),
                    topic_id=3,
                    decision="digest",
                    reason="llm_rank: 80\nllm_why: useful",
                    created_at=now - dt.timedelta(hours=3),
                ),
            ]
        )
        session.commit()

    pushed = {"telegram": 0}

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        pushed["telegram"] += 1
        return True

    def _forbid_other_pushes(**_kwargs):  # noqa: ANN003
        raise AssertionError("collect batch must only push a single Telegram message")

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", _forbid_other_pushes, raising=True)
    monkeypatch.setattr(runner_mod, "push_dingtalk_markdown", _forbid_other_pushes, raising=True)
    monkeypatch.setattr(runner_mod, "push_email_text", _forbid_other_pushes, raising=True)
    monkeypatch.setattr(runner_mod, "push_webhook_json", _forbid_other_pushes, raising=True)

    rules = parse_collect_message_rules(
        """
        [
          {"id":"arxiv","name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[1]},
          {"id":"papers","name":"Papers","cron":"0 19 * * *","lookback_hours":24,"source_ids":[2]}
        ]
        """
    )
    rules = [
        rules[0],
        parse_collect_message_rules(
            '[{"id":"papers","name":"Papers","cron":"0 19 * * *","lookback_hours":24,"source_ids":[1,2]}]'
        )[0],
    ]

    async def _run():
        with make_session() as session:
            return await run_collect_message_batch(
                session=session,
                settings=settings,
                rules=rules,
                push=True,
                now=dt.datetime(2026, 4, 13, 11, 0, 0),
                key_suffix="test",
            )

    result = asyncio.run(_run())
    assert pushed["telegram"] == 1
    assert result.idempotency_key.startswith("digest:collect.batch-")
    assert result.idempotency_key.endswith(":2026-04-13:test")
    assert "Paper A alert" in result.markdown
    assert "Paper B digest" in result.markdown
    body = result.markdown.split("References:", 1)[0]
    assert body.count("- Paper A alert") == 1
    assert body.index("Paper A alert") < body.index("Paper B digest")


def test_run_collect_message_batch_localizes_titles_and_references(tmp_path, monkeypatch):
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
        src = repo.add_source(type="rss", url="https://export.arxiv.org/rss/cs.AI")
        now = dt.datetime(2026, 4, 13, 18, 0, 0)
        item = Item(
            source_id=int(src.id),
            url="https://arxiv.org/abs/2604.12345",
            canonical_url="https://arxiv.org/abs/2604.12345",
            title="Scaling Test-Time Compute with Latent Reasoning",
            content_text="A paper about adaptive inference-time compute allocation.",
            created_at=now - dt.timedelta(hours=2),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=1,
                decision="digest",
                reason="llm_summary: 用潜在推理动态扩展测试时算力，提升复杂任务表现。\nllm_why: 与代理推理优化高度相关",
                created_at=now - dt.timedelta(hours=2),
            )
        )
        session.commit()

    calls = {"count": 0}

    async def _fake_localize(*, repo=None, settings, target_lang, items, usage_cb=None):  # noqa: ANN001, ARG001
        calls["count"] += 1
        assert target_lang == "zh"
        return {int(items[0]["item_id"]): "用潜在推理扩展测试时算力"}

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "llm_localize_item_titles", _fake_localize, raising=True)

    rule = parse_collect_message_rules(
        '[{"id":"arxiv","name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[1]}]'
    )[0]

    async def _run():
        with make_session() as session:
            return await run_collect_message_batch(
                session=session,
                settings=settings,
                rules=[rule],
                push=False,
                now=dt.datetime(2026, 4, 13, 19, 0, 0),
                key_suffix="localized",
            )

    result = asyncio.run(_run())
    assert calls["count"] == 1
    assert "用潜在推理扩展测试时算力" in result.markdown
    assert "Scaling Test-Time Compute with Latent Reasoning" not in result.markdown
    assert "[1] 用潜在推理扩展测试时算力 — https://arxiv.org/abs/2604.12345" in result.markdown


def test_run_collect_message_batch_does_not_push_empty_batch(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        digest_push_enabled=True,
        digest_push_empty=True,
        cron_timezone="+8",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    pushed = {"telegram": 0}

    async def _fake_push_telegram_report_reader(**_kwargs) -> bool:  # noqa: ANN003
        pushed["telegram"] += 1
        return True

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", _fake_push_telegram_report_reader, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", _fake_push_telegram_report_reader, raising=True)

    rule = parse_collect_message_rules(
        '[{"id":"arxiv","name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[1]}]'
    )[0]

    async def _run():
        with make_session() as session:
            return await run_collect_message_batch(
                session=session,
                settings=settings,
                rules=[rule],
                push=True,
                now=dt.datetime(2026, 4, 13, 11, 0, 0),
                key_suffix="empty",
            )

    result = asyncio.run(_run())
    assert result.pushed == 0
    assert pushed["telegram"] == 0
    assert "暂无新条目" in result.markdown


def test_localize_item_display_titles_batches_large_collects(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tracker.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text('TRACKER_API_TOKEN="secret"\n', encoding="utf-8")

    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        output_language="zh",
        llm_base_url="http://llm.local",
        llm_model="dummy",
        llm_model_mini="mini-dummy",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    entries = [
        {
            "item_id": idx,
            "title": f"Paper Title {idx}",
            "url": f"https://arxiv.org/abs/2604.{idx:05d}",
            "summary": "",
            "why": "",
            "content_text": "",
        }
        for idx in range(1, 27)
    ]

    calls = {"sizes": []}

    async def _fake_localize(*, repo=None, settings, target_lang, items, usage_cb=None):  # noqa: ANN001, ARG001
        calls["sizes"].append(len(items))
        return {int(item["item_id"]): f"中文标题 {item['item_id']}" for item in items}

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "llm_localize_item_titles", _fake_localize, raising=True)

    async def _run():
        with make_session() as session:
            repo = Repo(session)
            return await _localize_item_display_titles(
                repo=repo,
                settings=settings,
                entries=entries,
                out_lang="zh",
            )

    localized = asyncio.run(_run())
    assert calls["sizes"] == [24, 2]
    assert localized[1] == "中文标题 1"
    assert localized[26] == "中文标题 26"


def test_run_collect_message_keeps_single_rule_key_shape(tmp_path, monkeypatch):
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
        src1 = repo.add_source(type="rss", url="https://export.arxiv.org/rss/cs.AI")
        now = dt.datetime(2026, 4, 13, 10, 0, 0)
        item = Item(
            source_id=int(src1.id),
            url="https://arxiv.org/abs/2604.00001",
            canonical_url="https://arxiv.org/abs/2604.00001",
            title="Paper A alert",
            created_at=now - dt.timedelta(hours=2),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=1,
                decision="alert",
                reason="llm_rank: 95\nllm_why: high",
                created_at=now - dt.timedelta(hours=2),
            )
        )
        session.commit()

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", lambda **_kwargs: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_kwargs: False, raising=True)

    rule = parse_collect_message_rules(
        '[{"name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"source_ids":[1]}]'
    )[0]

    async def _run():
        with make_session() as session:
            return await run_collect_message(
                session=session,
                settings=settings,
                rule=rule,
                push=False,
                now=dt.datetime(2026, 4, 13, 11, 0, 0),
                key_suffix="test",
            )

    result = asyncio.run(_run())
    assert result.idempotency_key == "digest:collect.arxiv:2026-04-13:test"


def test_run_collect_message_falls_back_to_extended_window_when_primary_empty(tmp_path, monkeypatch):
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
        src1 = repo.add_source(type="rss", url="https://export.arxiv.org/rss/cs.AI")
        now = dt.datetime(2026, 4, 13, 11, 0, 0)
        item = Item(
            source_id=int(src1.id),
            url="https://arxiv.org/abs/2604.00003",
            canonical_url="https://arxiv.org/abs/2604.00003",
            title="Paper C fallback",
            created_at=now - dt.timedelta(hours=36),
        )
        session.add(item)
        session.flush()
        session.add(
            ItemTopic(
                item_id=int(item.id),
                topic_id=1,
                decision="digest",
                reason="llm_rank: 88\nllm_why: useful",
                created_at=now - dt.timedelta(hours=36),
            )
        )
        session.commit()

    import tracker.runner as runner_mod

    monkeypatch.setattr(runner_mod, "push_telegram_report_reader", lambda **_kwargs: False, raising=True)
    monkeypatch.setattr(runner_mod, "push_telegram_text", lambda **_kwargs: False, raising=True)

    rule = parse_collect_message_rules(
        '[{"name":"arXiv","cron":"0 19 * * *","lookback_hours":24,"fallback_lookback_hours":72,"source_ids":[1]}]'
    )[0]

    async def _run():
        with make_session() as session:
            return await run_collect_message(
                session=session,
                settings=settings,
                rule=rule,
                push=False,
                now=now,
                key_suffix="fallback",
            )

    result = asyncio.run(_run())
    assert "Paper C fallback" in result.markdown
    assert "窗口: 2026-04-10T19:00+08:00" in result.markdown
