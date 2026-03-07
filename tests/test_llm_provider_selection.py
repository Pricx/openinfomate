from tracker.llm import _kind_uses_mini_provider, _select_model_for_kind, _select_timeout_for_kind
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
