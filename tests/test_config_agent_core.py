from __future__ import annotations

from pathlib import Path

import pytest

from tracker.config_agent_core import apply_config_agent_plan, build_config_agent_preview_markdown, validate_config_agent_plan
from tracker.config_agent_core.service import plan_config_agent_request
from tracker.db import session_factory
from tracker.envfile import parse_env_assignments
from tracker.llm import LlmProfileProposal
from tracker.models import Base
from tracker.repo import Repo
from tracker.settings import Settings


def test_validate_config_agent_plan_accepts_tracking_settings_and_profile_ops():
    plan = {
        "summary": "test",
        "questions": ["q1"],
        "actions": [
            {"op": "topic.upsert", "name": "AI Agents", "query": "codex fast", "enabled": True},
            {"op": "mcp.setting.set", "field": "digest_hours", "value": "6"},
            {"op": "mcp.setting.clear", "field": "llm_extra_body_json"},
            {"op": "mcp.profile.set", "profile_text": "我关注开源 Agent 与 MCP", "topic_name": "Profile"},
        ],
    }

    clean, warnings = validate_config_agent_plan(plan)

    assert warnings == []
    assert [a.get("op") for a in clean["actions"]] == [
        "topic.upsert",
        "mcp.setting.set",
        "mcp.setting.clear",
        "mcp.profile.set",
    ]
    assert clean["actions"][1]["value"] == "6"
    assert clean["actions"][3]["topic_name"] == "Profile"


def test_validate_config_agent_plan_accepts_topic_gate_ops():
    plan = {
        "actions": [
            {
                "op": "mcp.topic_gate.patch",
                "scope": "defaults",
                "candidate_convergence": "strict",
                "max_digest_items": 6,
                "push_dedupe_strength": "balanced",
            },
            {"op": "mcp.topic_gate.patch", "scope": "topic", "topic_name": "Profile", "reset_all": True, "push_min_score": 82},
        ]
    }

    clean, warnings = validate_config_agent_plan(plan)

    assert warnings == []
    assert [a.get("op") for a in clean["actions"]] == [
        "mcp.topic_gate.patch",
        "mcp.topic_gate.patch",
    ]
    assert clean["actions"][0]["scope"] == "defaults"
    assert clean["actions"][0]["fields"] == ["candidate_convergence", "max_digest_items", "push_dedupe_strength"]
    assert clean["actions"][1]["topic_name"] == "Profile"
    assert clean["actions"][1]["reset_all"] is True


def test_validate_config_agent_plan_rejects_dangerous_remote_security_fields():
    plan = {
        "actions": [
            {"op": "mcp.setting.set", "field": "bootstrap_allow_no_auth", "value": "true"},
        ]
    }

    with pytest.raises(ValueError):
        validate_config_agent_plan(plan)


@pytest.mark.asyncio
async def test_apply_config_agent_plan_updates_profile_settings_and_tracking(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "config-agent.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path))
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    async def fake_profile_setup(**_kwargs):
        return LlmProfileProposal(
            understanding="Focus on deployable OSS agents.",
            ai_prompt="Use {{profile}} to keep high-signal OSS agent posts.",
            interest_axes=["OSS agents", "MCP"],
            interest_keywords=["Codex CLI", "SearxNG"],
            retrieval_queries=["Codex CLI", "open-source agent memory"],
        )

    monkeypatch.setattr("tracker.config_agent_core.service.llm_propose_profile_setup", fake_profile_setup)

    plan = {
        "actions": [
            {"op": "topic.upsert", "name": "AI Agents", "query": "codex fast", "enabled": True},
            {
                "op": "source.add_rss",
                "url": "https://example.com/feed.xml",
                "bind": {"topic": "AI Agents", "include_keywords": "", "exclude_keywords": ""},
            },
            {"op": "mcp.setting.set", "field": "digest_hours", "value": "6"},
            {"op": "mcp.setting.clear", "field": "llm_extra_body_json"},
            {"op": "mcp.profile.set", "profile_text": "我关注开源 Agent、MCP、代码检索", "topic_name": "Profile"},
        ]
    }

    with make_session() as session:
        result = await apply_config_agent_plan(session=session, settings=settings, plan=plan, run_id=None)
        assert result.restart_required is False
        assert any("settings updated:" in note for note in result.notes)
        assert any("profile updated: Profile" == note for note in result.notes)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.get_topic_by_name("AI Agents")
        assert topic is not None
        assert topic.enabled is True
        src = repo.get_source(type="rss", url="https://example.com/feed.xml")
        assert src is not None
        assert any(row[0].name == "AI Agents" and row[1].url == src.url for row in repo.list_topic_sources())

        profile_topic = repo.get_topic_by_name("Profile")
        assert profile_topic is not None
        policy = repo.get_topic_policy(topic_id=int(profile_topic.id))
        assert policy is not None
        assert "{{profile}}" in (policy.llm_curation_prompt or "")
        assert repo.get_app_config("profile_understanding") == "Focus on deployable OSS agents."
        assert "OSS agents" in (repo.get_app_config("profile_interest_axes") or "")

    env_values = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    assert env_values.get("TRACKER_DIGEST_HOURS") == "6"


@pytest.mark.asyncio
async def test_apply_config_agent_plan_updates_topic_gates(tmp_path):
    db_path = Path(tmp_path) / "config-agent-topic-gates.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path))
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        repo.add_topic(name="Profile", query="")
        plan = {
            "actions": [
                {
                    "op": "mcp.topic_gate.patch",
                    "scope": "defaults",
                    "candidate_convergence": "balanced",
                    "max_digest_items": 8,
                    "push_dedupe_strength": "strict",
                },
                {"op": "mcp.topic_gate.patch", "scope": "topic", "topic_name": "Profile", "reset_all": True, "push_min_score": 84},
            ]
        }
        preview = build_config_agent_preview_markdown(repo=repo, settings=settings, session=session, plan=plan)
        assert "Topic Gates" in preview
        assert "push_min_score" not in preview
        assert "进入推送最低分" in preview

        result = await apply_config_agent_plan(session=session, settings=settings, plan=plan, run_id=None)
        assert "topic gates updated: defaults" in result.notes
        assert "topic gates updated: Profile" in result.notes

    with make_session() as session:
        repo = Repo(session)
        defaults = repo.get_topic_gate_defaults()
        topic = repo.get_topic_by_name("Profile")
        assert topic is not None
        described = repo.describe_topic_gate(topic_id=int(topic.id))
        assert defaults.candidate_convergence == "balanced"
        assert defaults.max_digest_items == 8
        assert defaults.push_dedupe_strength == "strict"
        assert described["override"]["push_min_score"] == 84
        assert described["inherits"]["push_min_score"] is False
        assert described["effective"]["candidate_convergence"] == "balanced"


def test_validate_config_agent_plan_accepts_reply_only_turns():
    clean, warnings = validate_config_agent_plan(
        {
            "assistant_reply": "我可以回答配置问题，也可以帮你改 topics / sources / bindings。",
            "summary": "capabilities",
            "actions": [],
        }
    )

    assert warnings == []
    assert clean["assistant_reply"].startswith("我可以回答配置问题")
    assert clean["actions"] == []


@pytest.mark.asyncio
async def test_plan_config_agent_request_converts_tracking_noop_to_reply_only(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "config-agent-noop.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="")
        source = repo.add_source(type="discourse", url="https://discuss.huggingface.co/latest.json")
        repo.bind_topic_source(topic=topic, source=source)

    async def fake_llm_plan(**_kwargs):
        return {
            "assistant_reply": "已把 Hugging Face 论坛加入来源。",
            "summary": "新增 discuss.huggingface.co 来源",
            "questions": [],
            "actions": [
                {
                    "op": "mcp.source_binding.ensure",
                    "source_type": "discourse",
                    "intent": "site_stream",
                    "site": "discuss.huggingface.co",
                    "topic": "__auto__",
                    "bind": {"topic": "__auto__", "include_keywords": "", "exclude_keywords": ""},
                }
            ],
        }

    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fake_llm_plan)

    with make_session() as session:
        repo = Repo(session)
        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="添加 https://discuss.huggingface.co/latest 作为topics来源",
            actor="test",
            client_host="pytest",
        )

    assert result.run_id == 0
    assert result.preview_markdown == ""
    assert result.plan["actions"] == []
    assert "已经" in result.plan["assistant_reply"]
    assert "没有新的变更需要应用" in result.plan["assistant_reply"]


def test_build_config_agent_preview_markdown_keeps_full_profile_text(tmp_path):
    db_path = Path(tmp_path) / "config-agent-preview.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path))
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    profile_text = """AI 理解（不会用于关键词匹配）：
你强烈关注能落地的 AI Agent 工程化。

安全/漏洞信息偏好（重要）：
- 你不关心 MCP CVE 这类泛安全资讯
- 你只关心被广泛使用基础设施的远程 RCE 漏洞"""

    with make_session() as session:
        repo = Repo(session)
        preview = build_config_agent_preview_markdown(
            repo=repo,
            settings=settings,
            session=session,
            plan={
                "actions": [
                    {"op": "mcp.profile.set", "topic_name": "Profile", "profile_text": profile_text},
                ]
            },
        )

    assert "- Rebuild `Profile` profile from new text:" in preview
    assert "  AI 理解（不会用于关键词匹配）：" in preview
    assert "  安全/漏洞信息偏好（重要）：" in preview
    assert "  - 你不关心 MCP CVE 这类泛安全资讯" in preview
    assert "你不关…" not in preview
    assert "  - 你只关心被广泛使用基础设施的远程 RCE 漏洞" in preview


@pytest.mark.asyncio
async def test_apply_config_agent_plan_rejects_reply_only_turns(tmp_path):
    db_path = Path(tmp_path) / "config-agent-reply-only.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path))
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        with pytest.raises(ValueError, match="no actions"):
            await apply_config_agent_plan(
                session=session,
                settings=settings,
                plan={"assistant_reply": "只是回答问题", "actions": []},
                run_id=None,
            )


@pytest.mark.asyncio
async def test_plan_config_agent_request_converts_idempotent_tracking_plan_to_reply_only(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "config-agent-noop.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), llm_base_url="http://llm.test/v1")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    async def fake_llm_plan(**_kwargs):
        return {
            "assistant_reply": "已把 Hugging Face 论坛的 latest 站点流加入并绑定到最匹配的 topic。",
            "summary": "新增 Hugging Face 论坛来源",
            "questions": [],
            "actions": [
                {
                    "op": "mcp.source_binding.ensure",
                    "source_type": "discourse",
                    "intent": "site_stream",
                    "topic": "__auto__",
                    "site": "discuss.huggingface.co",
                    "bind": {"topic": "__auto__", "include_keywords": "", "exclude_keywords": ""},
                }
            ],
        }

    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fake_llm_plan)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="")
        source = repo.add_source(type="discourse", url="https://discuss.huggingface.co/latest.json")
        repo.bind_topic_source(topic=topic, source=source)

        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="添加 https://discuss.huggingface.co/latest 作为topics来源",
            actor="telegram",
            client_host="telegram",
        )

    assert result.run_id == 0
    assert result.preview_markdown == ""
    assert result.plan["actions"] == []
    assert "已经存在于当前配置中" in result.plan["assistant_reply"]


@pytest.mark.asyncio
async def test_plan_config_agent_request_keeps_material_tracking_plan(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "config-agent-material.db"
    env_path = Path(tmp_path) / ".env"
    env_path.write_text("", encoding="utf-8")
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), llm_base_url="http://llm.test/v1")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    async def fake_llm_plan(**_kwargs):
        return {
            "assistant_reply": "已为你添加 Hugging Face 论坛来源。",
            "summary": "新增 Hugging Face 论坛来源",
            "questions": [],
            "actions": [
                {
                    "op": "mcp.source_binding.ensure",
                    "source_type": "discourse",
                    "intent": "site_stream",
                    "topic": "__auto__",
                    "site": "discuss.huggingface.co",
                    "bind": {"topic": "__auto__", "include_keywords": "", "exclude_keywords": ""},
                }
            ],
        }

    monkeypatch.setattr("tracker.config_agent_core.service.llm_plan_config_agent", fake_llm_plan)

    with make_session() as session:
        repo = Repo(session)
        repo.add_topic(name="Profile", query="")
        result = await plan_config_agent_request(
            repo=repo,
            settings=settings,
            user_prompt="添加 https://discuss.huggingface.co/latest 作为topics来源",
            actor="telegram",
            client_host="telegram",
        )

    assert result.run_id > 0
    assert result.plan["actions"]
    assert "## Sources" in result.preview_markdown
    assert "huggingface" in result.preview_markdown.lower()
