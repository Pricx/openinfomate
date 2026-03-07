from __future__ import annotations

from pathlib import Path

import pytest

from tracker.config_agent_core import apply_config_agent_plan, validate_config_agent_plan
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
            interest_keywords=["Codex CLI", "Playwright"],
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

