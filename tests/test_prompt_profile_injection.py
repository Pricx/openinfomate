from __future__ import annotations


def test_profile_placeholder_is_injected_and_rendered(db_session):
    from tracker.prompt_templates import resolve_prompt
    from tracker.repo import Repo
    from tracker.settings import Settings

    repo = Repo(db_session)
    repo.set_app_config("profile_understanding", "Focus on deployable OSS agent stacks.")
    repo.set_app_config("profile_interest_axes", "Agent systems; RAG; observability")
    repo.set_app_config("profile_interest_keywords", "MCP, Codex CLI, SearxNG")
    repo.set_app_config("profile_prompt_delta", "Prefer primary sources; no marketing fluff.")

    res = resolve_prompt(repo=repo, settings=Settings(output_language="en"), slot_id="llm.priority_lane.policy")
    assert "{{profile}}" not in (res.text or "")
    assert "USER_PROFILE:" in (res.text or "")
    assert "Focus on deployable OSS agent stacks." in (res.text or "")


def test_profile_placeholder_is_available_for_triage_and_curate(db_session):
    from tracker.prompt_templates import resolve_prompt
    from tracker.repo import Repo
    from tracker.settings import Settings

    repo = Repo(db_session)
    repo.set_app_config("profile_understanding", "Care about deployable agent engineering.")
    repo.set_app_config("profile_interest_axes", "Agents; search; code intelligence")
    repo.set_app_config("profile_interest_keywords", "Codex CLI, MCP, SearxNG")
    repo.set_app_config("profile_prompt_delta", "Keep high-signal community field reports if relevant.")

    settings = Settings(output_language="en")
    triage = resolve_prompt(repo=repo, settings=settings, slot_id="llm.triage_items.user", context={"topic_name": "AI Agents", "topic_query_keywords": "codex", "topic_alert_keywords": "", "max_keep": 5, "topic_policy_prompt_block": "", "recent_sent_block": "", "candidates_block": "1. item_id=1"})
    curate = resolve_prompt(repo=repo, settings=settings, slot_id="llm.curate_items.user", context={"topic_name": "AI Agents", "topic_query_keywords": "codex", "topic_alert_keywords": "", "max_digest": 5, "max_alert": 2, "topic_policy_prompt_block": "", "recent_sent_block": "", "candidates_block": "1. item_id=1"})

    for res in (triage, curate):
        assert "{{profile}}" not in (res.text or "")
        assert "Care about deployable agent engineering." in (res.text or "")
        assert "Codex CLI, MCP, SearxNG" in (res.text or "")


def test_runtime_curation_prompts_allow_profile_aligned_community_reports(db_session):
    from tracker.prompt_templates import resolve_prompt
    from tracker.repo import Repo
    from tracker.settings import Settings

    repo = Repo(db_session)
    repo.set_app_config("profile_understanding", "Care about deployable AI tooling, quotas, and practical operator reports.")
    settings = Settings(output_language="en")

    triage = resolve_prompt(repo=repo, settings=settings, slot_id="llm.triage_items.system")
    curate = resolve_prompt(repo=repo, settings=settings, slot_id="llm.curate_items.system")

    for res in (triage, curate):
        assert "community/forum field reports" in (res.text or "")
        assert "formal benchmark or full reproduction" in (res.text or "")
        assert "resource-directory roundups" in (res.text or "")
        assert "technical founder" not in (res.text or "")


def test_profile_bootstrap_prompts_do_not_embed_fixed_daily_digest_cap():
    from tracker.prompt_templates import builtin_templates

    profile_setup = builtin_templates()["llm.propose_profile_setup.system"]
    topic_fallback = builtin_templates()["llm.propose_topic_setup.fallback_ai_prompt"]

    for text in (
        profile_setup.text_zh or "",
        profile_setup.text_en or "",
        topic_fallback.text_zh or "",
        topic_fallback.text_en or "",
    ):
        assert "3~5" not in text
        assert "3-5" not in text
        assert "digest max" not in text.lower()
