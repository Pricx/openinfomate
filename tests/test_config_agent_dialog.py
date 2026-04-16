from __future__ import annotations

import json
from pathlib import Path

import pytest

from tracker.config_agent_core.dialog_tools import execute_dialog_tool_calls, serialize_dialog_tool_results
from tracker.config_agent_core.service import plan_config_agent_request
from tracker.db import session_factory
from tracker.models import Base, Item, ItemTopic, Report
from tracker.repo import Repo
from tracker.settings import Settings


def _seed_digest_fixture(*, repo: Repo) -> dict[str, int]:
    topic = repo.add_topic(name="AI", query="agent memory")
    source = repo.add_source(type="rss", url="https://example.com/feed.xml")
    repo.bind_topic_source(topic=topic, source=source)
    item = repo.add_item(
        Item(
            source_id=int(source.id),
            url="https://example.com/posts/alpha",
            canonical_url="https://example.com/posts/alpha",
            title="Alpha Agents Memory",
            content_text="Alpha Agents memory architecture and cache invalidation.",
        )
    )
    repo.upsert_item_content(
        item_id=int(item.id),
        url="https://example.com/posts/alpha",
        content_text="Detailed cached explanation about Alpha Agents memory architecture.",
    )
    repo.add_item_topic(
        ItemTopic(
            item_id=int(item.id),
            topic_id=int(topic.id),
            decision="digest",
            reason="llm_summary: Alpha memory system summary\nllm_why: Shows a robust cache design\nllm_rank: 86",
        )
    )
    report = repo.upsert_report(
        kind="digest",
        idempotency_key="digest:0:2026-04-15:1200",
        topic_id=None,
        title="参考消息",
        markdown=(
            "# 参考消息\n\n"
            "## 重点摘要\n"
            "1. Alpha Agents 的缓存分层值得关注。\n\n"
            "References:\n"
            "[1] Alpha Agents Memory — https://example.com/posts/alpha\n"
        ),
    )
    collect = repo.upsert_report(
        kind="digest",
        idempotency_key="digest:collect.arxiv-daily:2026-04-15:1900",
        topic_id=None,
        title="arXiv 专题",
        markdown=(
            "# arXiv 专题\n\n"
            "## 核心论文\n"
            "1. Alpha Agents 论文强调缓存与记忆分层。\n\n"
            "References:\n"
            "[1] Alpha Agents Memory — https://example.com/posts/alpha\n"
        ),
    )
    return {"item_id": int(item.id), "report_id": int(report.id), "collect_id": int(collect.id)}


@pytest.mark.asyncio
async def test_execute_dialog_tool_calls_reads_recent_reports_and_searches_items(tmp_path):
    db_path = Path(tmp_path) / "dialog-tools.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        _seed_digest_fixture(repo=repo)
        executions, warnings = await execute_dialog_tool_calls(
            repo=repo,
            settings=settings,
            user_prompt="总结最近24小时参考消息，并解释 Alpha Agents Memory",
            tool_calls=[
                {"tool": "mcp.reports.recent", "args": {"hours": 24, "limit": 2, "include_items": True}},
                {"tool": "mcp.items.search", "args": {"query": "Alpha Agents Memory", "hours": 48, "limit": 3}},
                {"tool": "mcp.reports.recent", "args": {"hours": 36, "limit": 1, "only_collect": True, "title_query": "arxiv", "include_items": True}},
            ],
        )

    assert warnings == []
    payloads = serialize_dialog_tool_results(executions)
    digest_reports = payloads[0]["result"]["reports"]
    assert digest_reports[0]["reference_count"] == 1
    assert digest_reports[0]["items"][0]["title"] == "Alpha Agents Memory"
    search_items = payloads[1]["result"]["items"]
    assert search_items[0]["best_summary"] == "Alpha memory system summary"
    collect_reports = payloads[2]["result"]["reports"]
    assert collect_reports[0]["is_collect"] is True
    assert collect_reports[0]["title"] == "arXiv 专题"


@pytest.mark.asyncio
async def test_execute_dialog_tool_calls_external_fetch_requires_explicit_url(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "dialog-external.db"
    settings = Settings(db_url=f"sqlite:///{db_path}")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    async def fake_fetch(**_kwargs):
        return "Live fetched page"

    monkeypatch.setattr("tracker.config_agent_core.dialog_tools.fetch_fulltext_for_url", fake_fetch)

    with make_session() as session:
        repo = Repo(session)
        executions, warnings = await execute_dialog_tool_calls(
            repo=repo,
            settings=settings,
            user_prompt="请访问 https://example.com/live 并解释给我",
            tool_calls=[{"tool": "mcp.external.fetch_url", "args": {"url": "https://example.com/live"}}],
        )
        blocked_executions, blocked_warnings = await execute_dialog_tool_calls(
            repo=repo,
            settings=settings,
            user_prompt="请解释这个页面",
            tool_calls=[{"tool": "mcp.external.fetch_url", "args": {"url": "https://example.com/live"}}],
        )

    assert warnings == []
    assert serialize_dialog_tool_results(executions)[0]["result"]["content_excerpt"] == "Live fetched page"
    assert blocked_executions == []
    assert blocked_warnings and "explicit URL" in blocked_warnings[0]


@pytest.mark.asyncio
async def test_plan_config_agent_request_uses_dialog_route_for_cached_digest_question(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "dialog-plan.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        seed = _seed_digest_fixture(repo=repo)
        item_id = int(seed["item_id"])

    async def fake_route(**_kwargs):
        return {
            "mode": "info_reply",
            "tool_calls": [
                {"tool": "mcp.reports.recent", "args": {"hours": 24, "limit": 2, "include_items": True}},
                {"tool": "mcp.items.explain", "args": {"item_id": item_id}},
            ],
        }

    async def fake_answer(**kwargs):
        tool_results = json.loads(str(kwargs.get("tool_results_json") or "[]"))
        joined = json.dumps(tool_results, ensure_ascii=False)
        assert "Alpha Agents Memory" in joined
        return {
            "assistant_reply": "已按缓存中的参考消息整理：Alpha Agents Memory 值得重点关注。",
            "summary": "缓存总结",
            "questions": [],
            "actions": [],
        }

    async def fail_config_plan(**_kwargs):
        raise AssertionError("config planner should not run for cached info dialog")

    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_route_config_agent_dialog", fake_route)
    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_answer_config_agent_dialog", fake_answer)
    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fail_config_plan)

    with make_session() as session:
        repo = Repo(session)
        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="总结最近24小时参考消息，并解释 Alpha Agents Memory",
            actor="pytest",
            client_host="pytest",
        )

    assert result.run_id == 0
    assert result.preview_markdown == ""
    assert result.plan["actions"] == []
    assert "Alpha Agents Memory" in result.plan["assistant_reply"]


@pytest.mark.asyncio
async def test_plan_config_agent_request_for_arxiv_collect_question_prefers_cached_tools_over_deferring_llm(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "dialog-arxiv.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
        output_language="zh-CN",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        _seed_digest_fixture(repo=repo)

    async def fake_route(**_kwargs):
        return {
            "mode": "info_reply",
            "assistant_reply": "把那期内容贴过来，我再帮你筛。",
            "questions": [],
            "tool_calls": [],
        }

    async def fake_answer(**kwargs):
        tool_results = json.loads(str(kwargs.get("tool_results_json") or "[]"))
        joined = json.dumps(tool_results, ensure_ascii=False)
        assert "Alpha Agents Memory" in joined
        return {
            "assistant_reply": "可以，但我需要先看到“最近那期 arXiv 专题”的候选论文列表，你把那期内容贴过来后我再筛。",
            "summary": "需要更多上下文",
            "questions": [],
            "actions": [],
        }

    async def fail_config_plan(**_kwargs):
        raise AssertionError("config planner should not run for cached arxiv dialog")

    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_route_config_agent_dialog", fake_route)
    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_answer_config_agent_dialog", fake_answer)
    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fail_config_plan)

    with make_session() as session:
        repo = Repo(session)
        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="最近你给我总结的这个arXiv专题有什么是特别有信息量/高价值/反直觉的论文推荐么",
            actor="pytest",
            client_host="pytest",
        )

    assert result.run_id == 0
    assert result.preview_markdown == ""
    assert result.plan["actions"] == []
    assert "Alpha Agents Memory" in result.plan["assistant_reply"]
    assert "贴过来" not in result.plan["assistant_reply"]


@pytest.mark.asyncio
async def test_plan_config_agent_request_for_arxiv_collect_question_ignores_generic_route_and_cache_denial(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "dialog-arxiv-generic-route.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
        output_language="zh-CN",
    )
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        _seed_digest_fixture(repo=repo)

    async def fake_route(**_kwargs):
        return {
            "mode": "info_reply",
            "assistant_reply": "",
            "questions": [],
            "tool_calls": [
                {"tool": "mcp.items.search", "args": {"query": "recent arxiv recommendations", "hours": 168, "limit": 6}},
            ],
        }

    async def fake_answer(**kwargs):
        tool_results = json.loads(str(kwargs.get("tool_results_json") or "[]"))
        joined = json.dumps(tool_results, ensure_ascii=False)
        assert "Alpha Agents Memory" in joined
        assert "mcp.reports.recent" in joined
        return {
            "assistant_reply": "我这边当前并没有看到 arXiv 论文条目，所以没法基于该专题推荐。",
            "summary": "缓存中没有 arxiv",
            "questions": [],
            "actions": [],
        }

    async def fail_config_plan(**_kwargs):
        raise AssertionError("config planner should not run for cached arxiv dialog")

    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_route_config_agent_dialog", fake_route)
    monkeypatch.setattr("tracker.config_agent_core.dialog_service.llm_answer_config_agent_dialog", fake_answer)
    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fail_config_plan)

    with make_session() as session:
        repo = Repo(session)
        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="最近你给我总结的这个arXiv专题有什么是特别有信息量/高价值/反直觉的论文推荐么",
            actor="pytest",
            client_host="pytest",
        )

    assert result.run_id == 0
    assert result.preview_markdown == ""
    assert result.plan["actions"] == []
    assert "Alpha Agents Memory" in result.plan["assistant_reply"]
    assert "没有看到 arXiv" not in result.plan["assistant_reply"]
