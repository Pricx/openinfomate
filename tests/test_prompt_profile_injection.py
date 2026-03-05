from __future__ import annotations


def test_profile_placeholder_is_injected_and_rendered(db_session):
    from tracker.prompt_templates import resolve_prompt
    from tracker.repo import Repo
    from tracker.settings import Settings

    repo = Repo(db_session)
    repo.set_app_config("profile_understanding", "Focus on deployable OSS agent stacks.")
    repo.set_app_config("profile_interest_axes", "Agent systems; RAG; observability")
    repo.set_app_config("profile_interest_keywords", "MCP, Codex CLI, Playwright")
    repo.set_app_config("profile_prompt_delta", "Prefer primary sources; no marketing fluff.")

    res = resolve_prompt(repo=repo, settings=Settings(output_language="en"), slot_id="llm.priority_lane.policy")
    assert "{{profile}}" not in (res.text or "")
    assert "USER_PROFILE:" in (res.text or "")
    assert "Focus on deployable OSS agent stacks." in (res.text or "")
