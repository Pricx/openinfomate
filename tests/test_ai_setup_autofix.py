from __future__ import annotations


def test_ai_setup_autofix_removes_non_explicit_rss_and_adds_searx_seed():
    from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion

    snapshot_before = {"topics": [], "sources": [], "bindings": []}
    plan = {
        "actions": [
            {"op": "topic.upsert", "name": "AI Memory Systems", "query": "ai agent memory", "enabled": True},
            {
                "op": "source.add_rss",
                "url": "https://github.com/cpacker/MemGPT/releases.atom",
                "bind": {"topic": "AI Memory Systems", "include_keywords": "", "exclude_keywords": ""},
            },
        ]
    }

    fixed, warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt="我比较关心 ai memory / agent memory 系统",
        searxng_base_url="http://127.0.0.1:8888",
    )
    ops = [a.get("op") for a in (fixed.get("actions") or [])]
    assert "source.add_rss" not in ops
    assert "source.add_searxng_search" in ops
    assert any("removed" in (w or "") for w in (warnings or []))

    searx = [a for a in (fixed.get("actions") or []) if a.get("op") == "source.add_searxng_search"][0]
    assert searx.get("base_url") == "http://127.0.0.1:8888"
    assert (searx.get("bind") or {}).get("topic") == "AI Memory Systems"


def test_ai_setup_autofix_keeps_rss_when_url_is_explicitly_provided():
    from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion

    snapshot_before = {"topics": [], "sources": [], "bindings": []}
    plan = {
        "actions": [
            {"op": "topic.upsert", "name": "AI Memory Systems", "query": "ai agent memory", "enabled": True},
            {
                "op": "source.add_rss",
                "url": "https://example.com/feed.xml",
                "bind": {"topic": "AI Memory Systems", "include_keywords": "", "exclude_keywords": ""},
            },
        ]
    }

    fixed, _warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt="Add this feed: https://example.com/feed.xml",
        searxng_base_url="http://127.0.0.1:8888",
    )
    ops = [a.get("op") for a in (fixed.get("actions") or [])]
    assert "source.add_rss" in ops


def test_ai_setup_autofix_adds_searx_seed_for_existing_topic_when_missing():
    from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion

    snapshot_before = {
        "topics": [{"name": "AI Memory Systems", "query": "ai memory", "enabled": True}],
        "sources": [],
        "bindings": [],
    }
    plan = {"actions": [{"op": "topic.upsert", "name": "AI Memory Systems", "query": "agent memory long-term", "enabled": True}]}

    fixed, _warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt="我关心 AI memory / agent memory 系统，让 ai 超长连续工作",
        searxng_base_url="http://127.0.0.1:8888",
    )
    ops = [a.get("op") for a in (fixed.get("actions") or [])]
    assert "source.add_searxng_search" in ops


def test_ai_setup_autofix_adds_searx_seed_when_existing_query_differs():
    from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion
    from tracker.connectors.searxng import build_searxng_search_url

    url_old = build_searxng_search_url(base_url="http://127.0.0.1:8888", query="ai memory", results=10)
    snapshot_before = {
        "topics": [{"name": "AI Memory Systems", "query": "ai memory", "enabled": True}],
        "sources": [{"type": "searxng_search", "url": url_old, "enabled": True}],
        "bindings": [{"topic": "AI Memory Systems", "source": {"type": "searxng_search", "url": url_old}}],
    }
    plan = {"actions": [{"op": "topic.upsert", "name": "AI Memory Systems", "query": "agent memory long-term", "enabled": True}]}

    fixed, _warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt="我关心 AI memory / agent memory 系统，让 ai 超长连续工作",
        searxng_base_url="http://127.0.0.1:8888",
    )
    searx = [a for a in (fixed.get("actions") or []) if a.get("op") == "source.add_searxng_search"]
    assert searx, "expected an extra searxng_search seed when query differs"
    assert (searx[0].get("bind") or {}).get("topic") == "AI Memory Systems"


def test_ai_setup_autofix_does_not_add_duplicate_searx_seed_when_query_matches():
    from tracker.config_agent import autofix_ai_setup_plan_for_source_expansion
    from tracker.connectors.searxng import build_searxng_search_url

    url = build_searxng_search_url(base_url="http://127.0.0.1:8888", query="agent memory long-term", results=10)
    snapshot_before = {
        "topics": [{"name": "AI Memory Systems", "query": "ai memory", "enabled": True}],
        "sources": [{"type": "searxng_search", "url": url, "enabled": True}],
        "bindings": [{"topic": "AI Memory Systems", "source": {"type": "searxng_search", "url": url}}],
    }
    plan = {"actions": [{"op": "topic.upsert", "name": "AI Memory Systems", "query": "agent memory long-term", "enabled": True}]}

    fixed, _warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt="我关心 AI memory / agent memory 系统，让 ai 超长连续工作",
        searxng_base_url="http://127.0.0.1:8888",
    )
    ops = [a.get("op") for a in (fixed.get("actions") or [])]
    assert ops.count("source.add_searxng_search") == 0


def test_ai_setup_discover_queue_roundtrip(db_session):
    from tracker.ai_setup_discover_queue import enqueue_ai_setup_discover_job, pop_ai_setup_discover_job
    from tracker.repo import Repo

    repo = Repo(db_session)
    ok = enqueue_ai_setup_discover_job(repo=repo, run_id=7, topic_ids=[1, 2, 2, 3])
    assert ok is True
    job = pop_ai_setup_discover_job(repo=repo)
    assert job is not None
    assert job.run_id == 7
    assert job.topic_ids == [1, 2, 3]
