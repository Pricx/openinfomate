from __future__ import annotations

import asyncio

from tracker.bridge_contract import BridgeConfigPlanRequest, BridgeProfileProposeRequest, BridgeProfileProposeResponse, BridgeTrackingPlanRequest
from tracker.bridge_service import bridge_config_plan, bridge_profile_propose, bridge_tracking_plan
from tracker.settings import Settings


def test_bridge_tracking_plan_prunes_curation_only_profile_topic(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="agent infra bookmarks",
            understanding="agent infra",
            interest_axes=["Agent Infra"],
            interest_keywords=["agent infra"],
            retrieval_queries=["agent infra"],
            ai_prompt="focus",
        )

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Profile", "query": "", "enabled": True},
                    {"op": "topic.upsert", "name": "Agent Infra", "query": "agent infra", "enabled": True},
                    {
                        "op": "source.add_searxng_search",
                        "base_url": "http://127.0.0.1:8888",
                        "query": "agent infra",
                        "bind": {"topic": "Agent Infra", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="agent infra bookmarks",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    topic_names = [str(action.get("name") or action.get("topic") or "") for action in out.actions if action.get("op") == "topic.upsert"]
    assert topic_names == ["Agent Infra"]
    assert any("pruned curation-only profile topic" in warning for warning in out.warnings)


def test_bridge_tracking_plan_keeps_profile_topic_when_tracking_actions_target_it(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="profile",
            understanding="profile",
            interest_axes=[],
            interest_keywords=[],
            retrieval_queries=[],
            ai_prompt="focus",
        )

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Profile", "query": "", "enabled": True},
                    {
                        "op": "source.add_discourse",
                        "base_url": "https://forum.example.com",
                        "json_path": "/latest.json",
                        "bind": {"topic": "Profile", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="profile",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    topic_names = [str(action.get("name") or action.get("topic") or "") for action in out.actions if action.get("op") == "topic.upsert"]
    assert topic_names == ["Profile"]
    assert not any("pruned curation-only profile topic" in warning for warning in out.warnings)


def test_bridge_tracking_plan_reuses_embedded_profile_state_without_profile_reproposal(monkeypatch):
    async def fail_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        raise AssertionError("bridge_profile_propose should not be called when embedded profile state is provided")

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        assert "embedded profile summary" in user_prompt
        assert "- Reliable Agents" in user_prompt
        assert "- agent memory" in user_prompt
        assert "- agent memory production postmortem" in user_prompt
        assert max_tokens_override is not None
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Reliable Agents", "query": "agent memory", "enabled": True},
                    {
                        "op": "source.add_rss",
                        "url": "https://example.com/feed.xml",
                        "bind": {"topic": "Reliable Agents", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fail_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="agent infra bookmarks",
                profile_topic_name="Profile",
                profile_understanding="embedded profile summary",
                profile_interest_axes=["Reliable Agents"],
                profile_interest_keywords=["agent memory"],
                profile_retrieval_queries=["agent memory production postmortem"],
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    assert out.understanding == "embedded profile summary"
    assert out.interest_axes == ["Reliable Agents"]
    assert out.interest_keywords == ["agent memory"]
    assert out.retrieval_queries == ["agent memory production postmortem"]


def test_bridge_tracking_plan_brief_includes_profile_text_and_user_facing_naming_requirements(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="我关注 AI 基础设施 与 工程化经验",
            understanding="关注 AI 基础设施落地经验",
            interest_axes=["AI 基础设施", "工程化经验"],
            interest_keywords=["推理", "部署"],
            retrieval_queries=["AI infra postmortem"],
            ai_prompt="focus",
        )

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        assert "PROFILE_TEXT:" in user_prompt
        assert "我关注 AI 基础设施 与 工程化经验" in user_prompt
        assert "Keep topic names user-facing and natural" in user_prompt
        assert "Preserve the dominant user language" in user_prompt
        assert max_tokens_override is not None
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "AI 基础设施", "query": "AI 基础设施", "enabled": True},
                    {
                        "op": "source.add_searxng_search",
                        "base_url": "http://127.0.0.1:8888",
                        "query": "AI 基础设施",
                        "bind": {"topic": "AI 基础设施", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="我关注 AI 基础设施与工程化经验",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    assert out.normalized_profile_text == "我关注 AI 基础设施 与 工程化经验"


def test_bridge_tracking_plan_uses_llm_for_structured_profile_brief_with_bridge_budget(monkeypatch):
    async def fail_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        raise AssertionError("bridge_profile_propose should not be called when embedded profile state is provided")

    captured: dict[str, object] = {}

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        captured["user_prompt"] = user_prompt
        captured["max_tokens_override"] = max_tokens_override
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "AI 基础设施", "query": "AI infra postmortem", "enabled": True},
                    {
                        "op": "source.add_searxng_search",
                        "base_url": "http://127.0.0.1:8888",
                        "query": "AI infra postmortem",
                        "bind": {"topic": "AI 基础设施", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fail_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="我关注 AI 基础设施与工程化经验",
                profile_topic_name="Profile",
                profile_understanding="关注 AI 基础设施落地经验",
                profile_interest_axes=["AI 基础设施", "工程化经验"],
                profile_interest_keywords=["推理", "部署"],
                profile_retrieval_queries=["AI infra postmortem"],
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    assert "SMART_CONFIG_INPUT:" in out.input_brief
    assert "SEED_QUERIES:" in str(captured["user_prompt"])
    assert "- AI infra postmortem" in str(captured["user_prompt"])
    assert int(captured["max_tokens_override"] or 0) > 0
    assert int(captured["max_tokens_override"] or 0) < int(getattr(Settings(), "ai_setup_plan_max_tokens", 12_000))
    assert not any("skipped LLM" in warning for warning in out.warnings)
    topic_names = [str(action.get("name") or action.get("topic") or "") for action in out.actions if action.get("op") == "topic.upsert"]
    assert topic_names == ["AI 基础设施"]


def test_bridge_tracking_plan_retries_once_after_validation_error(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="agent infra bookmarks",
            understanding="agent infra",
            interest_axes=["Agent Infra"],
            interest_keywords=["agent infra"],
            retrieval_queries=["agent infra"],
            ai_prompt="focus",
        )

    calls = {"count": 0}

    budgets: list[int] = []

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        budgets.append(int(max_tokens_override or 0))
        if calls["count"] == 1:
            raise ValueError("action[7] missing base_url/query")
        assert "VALIDATION_RETRY_NOTE:" in user_prompt
        assert "ALWAYS include both base_url and query" in user_prompt
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Agent Infra", "query": "agent infra", "enabled": True},
                    {
                        "op": "source.add_searxng_search",
                        "base_url": "http://127.0.0.1:8888",
                        "query": "agent infra",
                        "bind": {"topic": "Agent Infra", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="agent infra bookmarks",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    assert calls["count"] == 2
    assert budgets[0] > 0
    assert budgets[1] >= budgets[0]
    assert any("retry: tracking planner regenerated after validation error" in warning for warning in out.warnings)


def test_bridge_tracking_plan_hydrates_missing_topic_queries_from_bound_searches(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="infra profile",
            understanding="infra profile",
            interest_axes=["推理服务性能与成本优化"],
            interest_keywords=["vLLM"],
            retrieval_queries=["vLLM TensorRT-LLM Triton performance comparison"],
            ai_prompt="focus",
        )

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Profile", "query": "", "enabled": True},
                    {"op": "topic.upsert", "name": "推理服务性能与成本优化", "query": "", "enabled": True},
                    {
                        "op": "source.add_searxng_search",
                        "base_url": "http://127.0.0.1:8888",
                        "query": "vLLM TensorRT-LLM Triton performance comparison",
                        "bind": {"topic": "推理服务性能与成本优化", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="infra profile",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    topic_actions = {
        str(action.get("name") or action.get("topic") or ""): action
        for action in out.actions
        if action.get("op") == "topic.upsert"
    }
    assert topic_actions["推理服务性能与成本优化"]["query"] == "vLLM TensorRT-LLM Triton performance comparison"
    assert "Profile" not in topic_actions
    assert any("hydrated missing topic query" in warning for warning in out.warnings)


def test_bridge_tracking_plan_hydrates_missing_topic_queries_from_topic_name_when_no_search_seed(monkeypatch):
    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        return BridgeProfileProposeResponse(
            normalized_profile_text="community profile",
            understanding="community profile",
            interest_axes=["工程社区一线经验"],
            interest_keywords=["forum"],
            retrieval_queries=[],
            ai_prompt="focus",
        )

    async def fake_tracking_plan(*, settings, user_prompt, tracking_snapshot_text, max_tokens_override=None, usage_cb):  # type: ignore[no-untyped-def]
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "工程社区一线经验", "query": "", "enabled": True},
                    {
                        "op": "source.add_discourse",
                        "base_url": "https://community.example.com",
                        "json_path": "/latest.json",
                        "bind": {"topic": "工程社区一线经验", "include_keywords": "", "exclude_keywords": ""},
                    },
                ]
            },
            [],
        )

    monkeypatch.setattr("tracker.bridge_service.bridge_profile_propose", fake_profile)
    monkeypatch.setattr("tracker.bridge_service.llm_plan_tracking_ai_setup", fake_tracking_plan)
    monkeypatch.setattr("tracker.bridge_service.materialize_ai_setup_mcp_plan", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service.autofix_ai_setup_plan_for_source_expansion", lambda **kwargs: (kwargs["plan"], []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_tracking_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeTrackingPlanRequest(
                text="community profile",
                profile_topic_name="Profile",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    topic_actions = {
        str(action.get("name") or action.get("topic") or ""): action
        for action in out.actions
        if action.get("op") == "topic.upsert"
    }
    assert topic_actions["工程社区一线经验"]["query"] == "工程社区一线经验"
    assert any("hydrated missing topic query" in warning for warning in out.warnings)


def test_bridge_config_plan_avoids_nonexistent_profile_fallback(monkeypatch):
    async def fake_config_plan(  # type: ignore[no-untyped-def]
        *,
        repo,
        settings,
        user_prompt,
        tracking_snapshot_text,
        profile_state_text,
        profile_prompt_text,
        settings_state_text,
        conversation_history_text,
        page_context_text,
        settings_mcp_tools_text,
        usage_cb,
    ):
        return {
            "assistant_reply": "ok",
            "summary": "ok",
            "questions": [],
            "actions": [
                {
                    "op": "mcp.source_binding.ensure",
                    "intent": "site_stream",
                    "source_type": "discourse",
                    "site": "community.openai.com",
                    "topic": "__auto__",
                }
            ],
        }

    monkeypatch.setattr("tracker.bridge_service.llm_plan_config_agent", fake_config_plan)
    monkeypatch.setattr("tracker.bridge_service.validate_config_agent_plan", lambda planned: (planned, []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_config_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeConfigPlanRequest(
                user_prompt="补充论坛来源",
                profile_text="agent infra",
                profile_topic_name="Profile",
                tracking_snapshot={
                    "topics": [
                        {
                            "name": "Agent Engineering",
                            "query": "agent engineering",
                            "enabled": True,
                            "digest_cron": "0 9 * * *",
                            "alert_keywords": "",
                        }
                    ],
                    "sources": [],
                    "bindings": [],
                },
            ),
        )
    )

    topic_names = [
        str(action.get("name") or action.get("topic") or "")
        for action in out.actions
        if action.get("op") == "topic.upsert"
    ]
    bind_topics = [
        str((action.get("bind") or {}).get("topic") or "")
        for action in out.actions
        if isinstance(action, dict) and str(action.get("op") or "").startswith("source.")
    ]
    assert "Profile" not in topic_names
    assert bind_topics == ["Agent Engineering"]


def test_bridge_config_plan_injects_compressed_profile_prompt_text(monkeypatch):
    captured: dict[str, str] = {}

    async def fake_config_plan(  # type: ignore[no-untyped-def]
        *,
        repo,
        settings,
        user_prompt,
        tracking_snapshot_text,
        profile_state_text,
        profile_prompt_text,
        settings_state_text,
        conversation_history_text,
        page_context_text,
        settings_mcp_tools_text,
        usage_cb,
    ):
        captured["profile_prompt_text"] = profile_prompt_text
        captured["profile_state_text"] = profile_state_text
        return {
            "assistant_reply": "ok",
            "summary": "ok",
            "questions": [],
            "actions": [],
        }

    monkeypatch.setattr("tracker.bridge_service.llm_plan_config_agent", fake_config_plan)
    monkeypatch.setattr("tracker.bridge_service.validate_config_agent_plan", lambda planned: (planned, []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_config_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeConfigPlanRequest(
                user_prompt="我的 profile 是什么",
                profile_text="raw bookmarks",
                profile_understanding="关注 AI 编程、智能体和检索增强的真实工程落地。",
                profile_interest_axes=["AI 编程工作流", "智能体框架"],
                profile_interest_keywords=["Codex CLI", "OpenClaw"],
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
            ),
        )
    )

    assert out.summary == "ok"
    assert "关注 AI 编程、智能体和检索增强的真实工程落地。" in captured["profile_prompt_text"]
    assert "AI 编程工作流" in captured["profile_prompt_text"]
    assert "Codex CLI" in captured["profile_prompt_text"]
    assert "{{profile}}" not in captured["profile_prompt_text"]


def test_bridge_config_plan_allows_external_setting_fields(monkeypatch):
    async def fake_config_plan(  # type: ignore[no-untyped-def]
        *,
        repo,
        settings,
        user_prompt,
        tracking_snapshot_text,
        profile_state_text,
        profile_prompt_text,
        settings_state_text,
        conversation_history_text,
        page_context_text,
        settings_mcp_tools_text,
        usage_cb,
    ):
        assert "platform.llm.reasoning.model" in settings_mcp_tools_text
        return {
            "assistant_reply": "已整理好平台配置建议。",
            "summary": "updated settings",
            "questions": [],
            "actions": [
                {
                    "op": "mcp.setting.set",
                    "field": "platform.telegram.bot_token",
                    "value": "token-value",
                },
                {
                    "op": "mcp.setting.set",
                    "field": "platform.llm.reasoning.model",
                    "value": "gpt-5.2",
                },
            ],
        }

    monkeypatch.setattr("tracker.bridge_service.llm_plan_config_agent", fake_config_plan)
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_config_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeConfigPlanRequest(
                user_prompt="配置官方 bot 和主力模型",
                settings_mcp_tools_text="platform.llm.reasoning.model\nplatform.telegram.bot_token",
                allowed_setting_fields=[
                    "platform.telegram.bot_token",
                    "platform.llm.reasoning.model",
                ],
            ),
        )
    )

    assert out.assistant_reply == "已整理好平台配置建议。"
    assert [action["field"] for action in out.actions] == [
        "platform.telegram.bot_token",
        "platform.llm.reasoning.model",
    ]


def test_bridge_profile_propose_applies_llm_override(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_profile_setup(*, settings, profile_text, usage_cb):  # type: ignore[no-untyped-def]
        captured["base_url"] = settings.llm_base_url
        captured["api_key"] = settings.llm_api_key
        captured["model"] = settings.llm_model
        captured["reasoning_model"] = settings.llm_model_reasoning
        captured["compat_mode"] = settings.llm_compat_mode
        captured["timeout_seconds"] = settings.llm_timeout_seconds
        return BridgeProfileProposeResponse(
            normalized_profile_text=profile_text,
            understanding="summary",
            interest_axes=[],
            interest_keywords=[],
            retrieval_queries=[],
            ai_prompt="prompt",
        )

    monkeypatch.setattr("tracker.bridge_service.llm_propose_profile_setup", fake_profile_setup)
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_profile_propose(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeProfileProposeRequest(
                text="agent infra",
                llm_override={
                    "base_url": "https://llm.example.com/v1",
                    "api_key": "sk-tenant",
                    "model": "gpt-5.2",
                    "compat_mode": "responses",
                    "timeout_seconds": 123,
                },
            ),
        )
    )

    assert out.understanding == "summary"
    assert captured == {
        "base_url": "https://llm.example.com/v1",
        "api_key": "sk-tenant",
        "model": "gpt-5.2",
        "reasoning_model": "gpt-5.2",
        "compat_mode": "responses",
        "timeout_seconds": 123,
    }


def test_bridge_config_plan_applies_llm_override(monkeypatch):
    captured: dict[str, object] = {}

    async def fake_config_plan(  # type: ignore[no-untyped-def]
        *,
        repo,
        settings,
        user_prompt,
        tracking_snapshot_text,
        profile_state_text,
        profile_prompt_text,
        settings_state_text,
        conversation_history_text,
        page_context_text,
        settings_mcp_tools_text,
        usage_cb,
    ):
        captured["base_url"] = settings.llm_base_url
        captured["api_key"] = settings.llm_api_key
        captured["model"] = settings.llm_model
        captured["reasoning_model"] = settings.llm_model_reasoning
        captured["compat_mode"] = settings.llm_compat_mode
        captured["timeout_seconds"] = settings.llm_timeout_seconds
        return {
            "assistant_reply": "ok",
            "summary": "ok",
            "questions": [],
            "actions": [],
        }

    monkeypatch.setattr("tracker.bridge_service.llm_plan_config_agent", fake_config_plan)
    monkeypatch.setattr("tracker.bridge_service.validate_config_agent_plan", lambda planned: (planned, []))
    monkeypatch.setattr("tracker.bridge_service._effective_settings", lambda session, settings: settings)
    monkeypatch.setattr("tracker.bridge_service._build_usage_cb", lambda session: None)

    out = asyncio.run(
        bridge_config_plan(
            session=object(),  # type: ignore[arg-type]
            settings=Settings(),
            payload=BridgeConfigPlanRequest(
                user_prompt="我的 profile 是什么",
                profile_text="agent infra",
                tracking_snapshot={"topics": [], "sources": [], "bindings": []},
                llm_override={
                    "base_url": "https://llm.example.com/v1",
                    "api_key": "sk-tenant",
                    "model": "gpt-5.2",
                    "compat_mode": "chat_completions",
                    "timeout_seconds": 77,
                },
            ),
        )
    )

    assert out.summary == "ok"
    assert captured == {
        "base_url": "https://llm.example.com/v1",
        "api_key": "sk-tenant",
        "model": "gpt-5.2",
        "reasoning_model": "gpt-5.2",
        "compat_mode": "chat_completions",
        "timeout_seconds": 77,
    }
