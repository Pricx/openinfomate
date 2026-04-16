from tracker.llm import (
    _kind_uses_mini_provider,
    _select_llm_compat_mode_for_kind,
    _select_model_for_kind,
    _select_timeout_for_kind,
)
from tracker.settings import Settings


def test_config_agent_core_plan_keeps_reasoning_model_and_global_timeout():
    settings = Settings(
        llm_model="base-model",
        llm_model_reasoning="reasoning-model",
        llm_model_mini="mini-model",
        llm_timeout_seconds=300,
    )

    assert _kind_uses_mini_provider("config_agent_core_plan") is False
    assert _select_model_for_kind(settings, kind="config_agent_core_plan") == "reasoning-model"
    assert _select_timeout_for_kind(settings, kind="config_agent_core_plan") == 300.0
    assert _select_timeout_for_kind(settings, kind="curate_items") == 300.0


def test_select_llm_compat_mode_for_kind_prefers_explicit_mini_override():
    settings = Settings(
        llm_compat_mode="responses",
        llm_mini_compat_mode="chat_completions",
    )

    assert _select_llm_compat_mode_for_kind(settings, kind="config_agent_core_plan") == "responses"
    assert _select_llm_compat_mode_for_kind(settings, kind="digest_summary") == "chat_completions"


def test_select_llm_compat_mode_for_kind_returns_none_for_auto():
    settings = Settings(
        llm_compat_mode="auto",
        llm_mini_compat_mode="auto",
    )

    assert _select_llm_compat_mode_for_kind(settings, kind="config_agent_core_plan") is None
    assert _select_llm_compat_mode_for_kind(settings, kind="digest_summary") is None
