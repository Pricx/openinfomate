from __future__ import annotations

from tracker.config_agent import apply_plan_to_snapshot, materialize_ai_setup_mcp_plan
from tracker.connectors.searxng import build_searxng_search_url


PROFILE_SNAPSHOT = {
    "topics": [
        {"name": "Profile", "query": "", "enabled": True, "digest_cron": "0 9 * * *", "alert_keywords": ""},
        {"name": "AI Agents", "query": "codex cli claude code multi-agent orchestration", "enabled": True, "digest_cron": "0 9 * * *", "alert_keywords": ""},
        {"name": "GPU", "query": "cuda nvidia gpu", "enabled": True, "digest_cron": "0 9 * * *", "alert_keywords": ""},
    ],
    "sources": [],
    "bindings": [],
}


def test_materialize_mcp_site_stream_falls_back_to_profile_for_linux_do():
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "site_stream",
                "source_type": "discourse",
                "site": "linux.do",
                "topic": "__auto__",
            }
        ]
    }
    materialized, warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=PROFILE_SNAPSHOT,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert [a["op"] for a in materialized["actions"]] == ["source.add_discourse"]
    action = materialized["actions"][0]
    assert action["base_url"] == "https://linux.do"
    assert action["json_path"] == "/latest.json"
    assert (action.get("bind") or {}).get("topic") == "Profile"
    assert warnings == []



def test_materialize_mcp_site_stream_avoids_nonexistent_profile_fallback_when_topics_exist():
    snapshot_before = {
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
    }
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "site_stream",
                "source_type": "discourse",
                "site": "community.openai.com",
                "topic": "__auto__",
            }
        ]
    }
    materialized, warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=snapshot_before,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert [a["op"] for a in materialized["actions"]] == ["source.add_discourse"]
    action = materialized["actions"][0]
    assert action["base_url"] == "https://community.openai.com"
    assert (action.get("bind") or {}).get("topic") == "Agent Engineering"
    assert warnings == []


def test_materialize_mcp_explicit_discourse_latest_url_normalizes_to_json():
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "site_stream",
                "source_type": "discourse",
                "url": "https://discuss.huggingface.co/latest",
                "topic": "__auto__",
            }
        ]
    }
    materialized, warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=PROFILE_SNAPSHOT,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert [a["op"] for a in materialized["actions"]] == ["source.add_discourse"]
    action = materialized["actions"][0]
    assert action["base_url"] == "https://discuss.huggingface.co"
    assert action["json_path"] == "/latest.json"
    assert (action.get("bind") or {}).get("topic") == "Profile"
    assert warnings == []


def test_materialize_mcp_search_auto_selects_best_topic_and_site_filters_query():
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "search",
                "source_type": "searxng_search",
                "site": "linux.do",
                "query": "codex fast",
                "topic": "__auto__",
            }
        ]
    }
    materialized, _warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=PROFILE_SNAPSHOT,
        plan=plan,
        searxng_base_url="http://127.0.0.1:8889",
        profile_topic_name="Profile",
    )
    assert [a["op"] for a in materialized["actions"]] == ["source.add_searxng_search"]
    action = materialized["actions"][0]
    assert action["base_url"] == "http://127.0.0.1:8889"
    assert action["query"] == "site:linux.do codex fast"
    assert (action.get("bind") or {}).get("topic") == "AI Agents"



def test_materialize_mcp_creates_missing_explicit_topic_before_binding():
    snapshot_before = {"topics": [{"name": "Profile", "query": "", "enabled": True}], "sources": [], "bindings": []}
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "search",
                "source_type": "searxng_search",
                "query": "zeroclaw rust 重构",
                "topic": "Open Source Infra",
            }
        ]
    }
    materialized, _warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=snapshot_before,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert [a["op"] for a in materialized["actions"]] == ["topic.upsert", "source.add_searxng_search"]
    assert materialized["actions"][0]["name"] == "Open Source Infra"
    assert (materialized["actions"][1].get("bind") or {}).get("topic") == "Open Source Infra"



def test_apply_plan_to_snapshot_materializes_mcp_actions_for_preview():
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "search",
                "source_type": "searxng_search",
                "query": "massgen multi agent scaling",
                "topic": "AI Agents",
            }
        ]
    }
    after = apply_plan_to_snapshot(snapshot=PROFILE_SNAPSHOT, plan=plan)
    assert any(s["type"] == "searxng_search" for s in after["sources"])
    assert any(b["topic"] == "AI Agents" and b["source"]["type"] == "searxng_search" for b in after["bindings"])



def test_materialize_mcp_disable_search_source_reuses_exact_source_url():
    source_url = build_searxng_search_url(
        base_url="http://127.0.0.1:8888",
        query="site:linux.do codex fast",
        time_range="week",
        results=10,
    )
    snapshot_before = {
        **PROFILE_SNAPSHOT,
        "sources": [{"type": "searxng_search", "url": source_url, "enabled": True}],
        "bindings": [{"topic": "AI Agents", "source": {"type": "searxng_search", "url": source_url}}],
    }
    plan = {
        "actions": [
            {
                "op": "mcp.source.disable",
                "source_type": "searxng_search",
                "site": "linux.do",
                "query": "codex fast",
            }
        ]
    }
    materialized, _warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=snapshot_before,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert materialized["actions"] == [{"op": "source.disable", "type": "searxng_search", "url": source_url}]



def test_apply_plan_to_snapshot_mcp_reuses_existing_source_and_updates_binding_filters():
    source_url = build_searxng_search_url(
        base_url="http://127.0.0.1:8888",
        query="site:linux.do codex fast",
        time_range="week",
        results=10,
    )
    snapshot_before = {
        **PROFILE_SNAPSHOT,
        "sources": [{"type": "searxng_search", "url": source_url, "enabled": True}],
        "bindings": [
            {
                "topic": "AI Agents",
                "source": {"type": "searxng_search", "url": source_url},
                "include_keywords": "old",
                "exclude_keywords": "",
            }
        ],
    }
    plan = {
        "actions": [
            {
                "op": "mcp.source_binding.ensure",
                "intent": "search",
                "source_type": "searxng_search",
                "site": "linux.do",
                "query": "codex fast",
                "topic": "AI Agents",
                "include_keywords": "release,benchmark",
                "exclude_keywords": "help",
            }
        ]
    }
    after = apply_plan_to_snapshot(snapshot=snapshot_before, plan=plan)
    assert [s for s in after["sources"] if s["type"] == "searxng_search" and s["url"] == source_url] == [
        {"type": "searxng_search", "url": source_url, "enabled": True}
    ]
    bindings = [b for b in after["bindings"] if b["topic"] == "AI Agents" and b["source"]["url"] == source_url]
    assert len(bindings) == 1
    assert bindings[0]["include_keywords"] == "release,benchmark"
    assert bindings[0]["exclude_keywords"] == "help"



def test_materialize_mcp_binding_remove_auto_selects_existing_topic():
    source_url = build_searxng_search_url(
        base_url="http://127.0.0.1:8888",
        query="site:linux.do codex fast",
        time_range="week",
        results=10,
    )
    snapshot_before = {
        **PROFILE_SNAPSHOT,
        "sources": [{"type": "searxng_search", "url": source_url, "enabled": True}],
        "bindings": [{"topic": "AI Agents", "source": {"type": "searxng_search", "url": source_url}}],
    }
    plan = {
        "actions": [
            {
                "op": "mcp.binding.remove",
                "source_type": "searxng_search",
                "site": "linux.do",
                "query": "codex fast",
                "topic": "__auto__",
            }
        ]
    }
    materialized, _warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=snapshot_before,
        plan=plan,
        profile_topic_name="Profile",
    )
    assert materialized["actions"] == [
        {
            "op": "binding.remove",
            "topic": "AI Agents",
            "source": {"type": "searxng_search", "url": source_url},
        }
    ]
