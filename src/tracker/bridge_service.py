from __future__ import annotations

from typing import cast
from typing import Any

from sqlalchemy.orm import Session

from tracker.bridge_contract import (
    BridgeConfigPlanRequest,
    BridgeConfigPlanResponse,
    BridgeLlmOverride,
    BridgeProfileProposeRequest,
    BridgeProfileProposeResponse,
    BridgeTopicProposeRequest,
    BridgeTopicProposeResponse,
    BridgeTopicProposeSourceHints,
    BridgeTrackingPlanRequest,
    BridgeTrackingPlanResponse,
)
from tracker.config_agent import (
    AllowedOp as TrackingAllowedOp,
    autofix_ai_setup_plan_for_source_expansion,
    materialize_ai_setup_mcp_plan,
    snapshot_compact_text,
    validate_ai_setup_plan,
)
from tracker.config_agent_core import validate_config_agent_plan
from tracker.dynamic_config import effective_settings
from tracker.integrations.config_settings_mcp import (
    MCP_PROFILE_SET_OP,
    MCP_SETTING_CLEAR_OP,
    MCP_SETTING_SET_OP,
)
from tracker.llm import (
    llm_plan_config_agent,
    llm_plan_tracking_ai_setup,
    llm_propose_profile_setup,
    llm_propose_topic_setup,
)
from tracker.prompt_templates import build_default_profile_text_from_fields
from tracker.llm_usage import make_llm_usage_recorder
from tracker.profile_input import normalize_profile_text
from tracker.repo import Repo
from tracker.settings import Settings

_TRACKING_ALLOWED_OPS: set[str] = set(getattr(TrackingAllowedOp, "__args__", ()))  # type: ignore[attr-defined]


def _norm_text(value: object, max_chars: int | None = None) -> str:
    text = str(value or "").strip()
    if max_chars is not None and len(text) > max_chars:
        return text[: max_chars - 1].rstrip() + "…"
    return text


def _normalize_string_list(value: object, limit: int, item_max_chars: int) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        text = _norm_text(item, max_chars=item_max_chars)
        if not text:
            continue
        key = text.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
        if len(out) >= limit:
            break
    return out


def _normalize_tracking_snapshot(value: object) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"topics": [], "topic_policies": [], "sources": [], "bindings": []}
    return {
        "topics": list(value.get("topics") or []),
        "topic_policies": list(value.get("topic_policies") or []),
        "sources": list(value.get("sources") or []),
        "bindings": list(value.get("bindings") or []),
    }


def _normalize_allowed_setting_fields(value: object) -> set[str]:
    if not isinstance(value, list):
        return set()
    out: set[str] = set()
    for item in value[:400]:
        text = _norm_text(item, max_chars=200)
        if text:
            out.add(text)
    return out


def _validate_bridge_config_plan(
    planned: object,
    *,
    allowed_setting_fields: set[str],
) -> tuple[dict[str, Any], list[str]]:
    if not allowed_setting_fields:
        return validate_config_agent_plan(planned)
    if not isinstance(planned, dict):
        raise ValueError("plan must be a JSON object")

    raw_actions = planned.get("actions", [])
    if not isinstance(raw_actions, list):
        raise ValueError("plan.actions must be a list when provided")

    warnings: list[str] = []
    actions: list[dict[str, Any]] = []
    max_actions = 400
    if len(raw_actions) > max_actions:
        warnings.append(f"plan.actions truncated: {len(raw_actions)} -> {max_actions}")

    for idx, raw in enumerate(raw_actions[:max_actions]):
        if not isinstance(raw, dict):
            raise ValueError(f"action[{idx}] must be an object")
        op = _norm_text(raw.get("op"))
        if op in _TRACKING_ALLOWED_OPS:
            tracking_plan, more = validate_ai_setup_plan({"actions": [raw]})
            actions.extend(cast(list[dict[str, Any]], list(tracking_plan.get("actions") or [])))
            warnings.extend(list(more or []))
            continue

        if op == MCP_SETTING_SET_OP:
            field = _norm_text(raw.get("field"), max_chars=200)
            if not field or field not in allowed_setting_fields:
                raise ValueError(f"action[{idx}] forbidden or unknown field: {field!r}")
            if "value" not in raw:
                raise ValueError(f"action[{idx}] missing value")
            actions.append({"op": op, "field": field, "value": raw.get("value")})
            continue

        if op == MCP_SETTING_CLEAR_OP:
            field = _norm_text(raw.get("field"), max_chars=200)
            if not field or field not in allowed_setting_fields:
                raise ValueError(f"action[{idx}] forbidden or unknown field: {field!r}")
            actions.append({"op": op, "field": field})
            continue

        if op == MCP_PROFILE_SET_OP:
            profile_text = _norm_text(raw.get("profile_text") or raw.get("text"))
            if not profile_text:
                raise ValueError(f"action[{idx}] missing profile_text")
            topic_name = _norm_text(raw.get("topic_name") or raw.get("topic") or "Profile", max_chars=200) or "Profile"
            actions.append({"op": op, "profile_text": profile_text, "topic_name": topic_name})
            continue

        raise ValueError(f"action[{idx}] invalid op: {op!r}")

    questions = planned.get("questions") if isinstance(planned.get("questions"), list) else []
    assistant_reply = _norm_text(planned.get("assistant_reply") or planned.get("reply"), max_chars=2000)
    summary = _norm_text(planned.get("summary"), max_chars=800)
    clean_questions = [str(q or "").strip() for q in questions if str(q or "").strip()][:5]
    if not actions and not assistant_reply and not summary and not clean_questions:
        raise ValueError("plan must include assistant_reply, summary/questions, or actions")

    return {
        "assistant_reply": assistant_reply,
        "summary": summary,
        "questions": clean_questions,
        "actions": actions,
    }, warnings


def _build_profile_state_text(payload: BridgeConfigPlanRequest, normalized_profile_text: str) -> str:
    lines = ["PROFILE_STATE:"]
    pairs = [
        ("profile_topic_name", _norm_text(payload.profile_topic_name or "Profile", max_chars=200)),
        ("profile_text", _norm_text(normalized_profile_text, max_chars=4000)),
        ("profile_understanding", _norm_text(payload.profile_understanding, max_chars=4000)),
        ("profile_interest_axes", "\n".join(_normalize_string_list(payload.profile_interest_axes, 200, 220))),
        ("profile_interest_keywords", "\n".join(_normalize_string_list(payload.profile_interest_keywords, 600, 120))),
        ("profile_retrieval_queries", "\n".join(_normalize_string_list(payload.profile_retrieval_queries, 200, 260))),
    ]
    for key, value in pairs:
        if value:
            lines.append(f"{key}:\n{value}")
    return "\n\n".join(lines)


def _build_profile_prompt_text(payload: BridgeConfigPlanRequest, normalized_profile_text: str) -> str:
    return build_default_profile_text_from_fields(
        understanding=_norm_text(payload.profile_understanding, max_chars=4000),
        interest_axes_text="\n".join(_normalize_string_list(payload.profile_interest_axes, 200, 220)),
        interest_keywords_text="\n".join(_normalize_string_list(payload.profile_interest_keywords, 600, 120)),
        raw_profile_text=_norm_text(normalized_profile_text, max_chars=4000),
    )


def _build_tracking_profile_brief(
    *,
    profile_text: str,
    understanding: str,
    interest_axes: list[str],
    interest_keywords: list[str],
    retrieval_queries: list[str],
) -> str:
    lines = ["SMART_CONFIG_INPUT:"]
    if profile_text:
        lines.extend(["", "PROFILE_TEXT:", profile_text])
    if understanding:
        lines.extend(["", "UNDERSTANDING:", understanding])
    if interest_axes:
        lines.extend(["", "INTEREST_AXES:"])
        lines.extend([f"- {item}" for item in interest_axes])
    if interest_keywords:
        lines.extend(["", "KEYWORDS:"])
        lines.extend([f"- {item}" for item in interest_keywords])
    if retrieval_queries:
        lines.extend(["", "SEED_QUERIES:"])
        lines.extend([f"- {item}" for item in retrieval_queries])
    lines.extend(
        [
            "",
            "REQUIREMENTS:",
            "- Expand sources as much as possible; do not be conservative.",
            "- Split into semantically-orthogonal topics; no preset topic count.",
            "- Generate many short, semantically-orthogonal search seeds; do NOT stuff all keywords into one query.",
            "- Keep topic names user-facing and natural; never use slug/case-convention identifiers such as AI-Infrastructure-Systems or ai_infra_tools.",
            "- Preserve the dominant user language from PROFILE_TEXT / UNDERSTANDING / INTEREST_AXES when naming topics and writing summaries.",
        ]
    )
    return "\n".join(lines).strip()


def _select_bridge_tracking_planner_budgets(
    *,
    settings: Settings,
    brief_text: str,
    interest_axes: list[str],
    interest_keywords: list[str],
    retrieval_queries: list[str],
) -> tuple[int, int | None]:
    try:
        base_tokens = int(getattr(settings, "ai_setup_plan_max_tokens", 12_000) or 12_000)
    except Exception:
        base_tokens = 12_000
    base_tokens = max(1400, min(50_000, base_tokens))

    interactive_cap = max(1600, min(base_tokens, 3600))
    retry_cap = max(interactive_cap, min(base_tokens, 6000))

    complexity = len(interest_axes) + len(retrieval_queries) + int(len(interest_keywords) / 8)
    brief_len = len(brief_text or "")
    if brief_len > 30_000:
        complexity += 60
    elif brief_len > 16_000:
        complexity += 35
    elif brief_len > 8_000:
        complexity += 18
    elif brief_len > 4_000:
        complexity += 8

    if complexity >= 90:
        initial = min(interactive_cap, 3200)
    elif complexity >= 45:
        initial = min(interactive_cap, 2600)
    elif complexity >= 20:
        initial = min(interactive_cap, 2200)
    else:
        initial = min(interactive_cap, 1600)
    initial = max(1400, initial)

    retry_budget = min(retry_cap, max(initial + 1200, int(initial * 1.75)))
    if retry_budget <= initial:
        return initial, None
    return initial, retry_budget


_RETRYABLE_TRACKING_PLAN_ERROR_SNIPPETS = (
    "missing base_url/query",
    "missing base_url",
    "missing query",
    "plan.actions must be a non-empty list",
)


def _is_retryable_tracking_plan_error(exc: Exception) -> bool:
    message = _norm_text(exc, max_chars=500).lower()
    if not message:
        return False
    return any(snippet in message for snippet in _RETRYABLE_TRACKING_PLAN_ERROR_SNIPPETS)


def _build_tracking_retry_brief(*, brief_text: str, error_message: str) -> str:
    normalized_error = _norm_text(error_message, max_chars=240) or "validation failed"
    return "\n".join(
        [
            brief_text.rstrip(),
            "",
            "VALIDATION_RETRY_NOTE:",
            f"- Previous draft failed validation: {normalized_error}",
            "- Return ONLY a valid JSON object with a non-empty actions list.",
            "- If you emit source.add_searxng_search, ALWAYS include both base_url and query.",
            "- If you emit source.add_discourse, ALWAYS include base_url.",
            "- If you emit source.add_hn_search, ALWAYS include query.",
            "- If any source action is underspecified, OMIT that action instead of emitting partial fields.",
        ]
    )


def _tracking_request_has_embedded_profile_state(payload: BridgeTrackingPlanRequest) -> bool:
    return bool(
        _norm_text(payload.profile_understanding)
        or _normalize_string_list(payload.profile_interest_axes, 200, 220)
        or _normalize_string_list(payload.profile_interest_keywords, 600, 120)
        or _normalize_string_list(payload.profile_retrieval_queries, 200, 260)
    )


def _profile_response_from_tracking_payload(payload: BridgeTrackingPlanRequest) -> BridgeProfileProposeResponse:
    normalized_profile_text = normalize_profile_text(text=_norm_text(payload.text))
    if not normalized_profile_text:
        raise RuntimeError("profile text is required")
    return BridgeProfileProposeResponse(
        normalized_profile_text=normalized_profile_text,
        understanding=_norm_text(payload.profile_understanding, max_chars=800),
        interest_axes=_normalize_string_list(payload.profile_interest_axes, 200, 220),
        interest_keywords=_normalize_string_list(payload.profile_interest_keywords, 600, 120),
        retrieval_queries=_normalize_string_list(payload.profile_retrieval_queries, 200, 260),
        ai_prompt="",
    )


def _prune_curation_only_profile_tracking_actions(
    *,
    actions: list[dict[str, Any]],
    profile_topic_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    profile_topic = _norm_text(profile_topic_name, max_chars=200) or "Profile"
    if not actions:
        return [], []

    has_profile_topic = False
    has_explicit_profile_tracking_action = False
    for action in actions:
        if not isinstance(action, dict):
            continue
        op = _norm_text(action.get("op"))
        topic_name = _norm_text(action.get("name") or action.get("topic"))
        bind = action.get("bind")
        bind_topic = _norm_text(bind.get("topic")) if isinstance(bind, dict) else ""
        touches_profile = topic_name == profile_topic or bind_topic == profile_topic
        if not touches_profile:
            continue
        if op in {"topic.upsert", "topic.disable"} and topic_name == profile_topic:
            has_profile_topic = True
            continue
        if op.startswith("source.") or op.startswith("binding.") or op.startswith("mcp.source_") or op.startswith("mcp.binding."):
            has_explicit_profile_tracking_action = True

    if not has_profile_topic or has_explicit_profile_tracking_action:
        return list(actions), []

    pruned = [
        action
        for action in actions
        if not (
            isinstance(action, dict)
            and _norm_text(action.get("op")) in {"topic.upsert", "topic.disable"}
            and _norm_text(action.get("name") or action.get("topic")) == profile_topic
        )
    ]
    return pruned, [f"bridge: pruned curation-only profile topic from tracking plan: {profile_topic}"]


_TRACKING_SEARCH_ACTIONS = {"source.add_searxng_search", "source.add_hn_search"}


def _hydrate_tracking_topic_queries(
    *,
    actions: list[dict[str, Any]],
    profile_topic_name: str,
) -> tuple[list[dict[str, Any]], list[str]]:
    profile_topic = _norm_text(profile_topic_name, max_chars=200) or "Profile"
    if not actions:
        return [], []

    topic_search_queries: dict[str, str] = {}
    for action in actions:
        if not isinstance(action, dict):
            continue
        op = _norm_text(action.get("op"))
        if op not in _TRACKING_SEARCH_ACTIONS:
            continue
        bind = action.get("bind")
        topic_name = _norm_text(bind.get("topic")) if isinstance(bind, dict) else ""
        query = _norm_text(action.get("query"), max_chars=1200)
        if topic_name and query and topic_name not in topic_search_queries:
            topic_search_queries[topic_name] = query

    warnings: list[str] = []
    hydrated: list[dict[str, Any]] = []
    for action in actions:
        if not isinstance(action, dict):
            hydrated.append(action)
            continue
        op = _norm_text(action.get("op"))
        if op != "topic.upsert":
            hydrated.append(action)
            continue
        topic_name = _norm_text(action.get("name") or action.get("topic"), max_chars=200)
        current_query = _norm_text(action.get("query"), max_chars=1200)
        if not topic_name or current_query or topic_name == profile_topic:
            hydrated.append(action)
            continue
        derived_query = topic_search_queries.get(topic_name) or topic_name
        patched = dict(action)
        patched["query"] = derived_query
        hydrated.append(patched)
        warnings.append(f"bridge: hydrated missing topic query for {topic_name}")

    return hydrated, warnings


def _split_tracking_actions(actions: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tracking: list[dict[str, Any]] = []
    non_tracking: list[dict[str, Any]] = []
    for action in actions:
        op = _norm_text(action.get("op"))
        if (
            op.startswith("topic.")
            or op.startswith("source.")
            or op.startswith("binding.")
            or op.startswith("mcp.source_")
            or op.startswith("mcp.binding.")
        ):
            tracking.append(action)
        else:
            non_tracking.append(action)
    return tracking, non_tracking


def _build_usage_cb(session: Session):
    try:
        return make_llm_usage_recorder(session=session)
    except Exception:
        return None


def _effective_settings(session: Session, settings: Settings) -> Settings:
    repo = Repo(session)
    try:
        return effective_settings(repo=repo, settings=settings)
    except Exception:
        return settings


def _apply_bridge_llm_override(settings: Settings, override: BridgeLlmOverride | None) -> Settings:
    if override is None:
        return settings

    compat_mode = _norm_text(override.compat_mode or "auto", max_chars=64).lower()
    if compat_mode not in {"auto", "responses", "chat_completions"}:
        compat_mode = "auto"

    updates: dict[str, object] = {
        "llm_base_url": _norm_text(override.base_url, max_chars=2_000),
        "llm_api_key": _norm_text(override.api_key, max_chars=8_000),
        "llm_model": _norm_text(override.model, max_chars=200),
        "llm_model_reasoning": _norm_text(override.model, max_chars=200),
        "llm_compat_mode": compat_mode,
    }
    if override.timeout_seconds is not None:
        updates["llm_timeout_seconds"] = int(override.timeout_seconds)

    try:
        return settings.model_copy(update=updates)  # type: ignore[attr-defined]
    except Exception:
        return settings


async def bridge_profile_propose(
    *,
    session: Session,
    settings: Settings,
    payload: BridgeProfileProposeRequest,
) -> BridgeProfileProposeResponse:
    settings_eff = _apply_bridge_llm_override(_effective_settings(session, settings), payload.llm_override)
    normalized_profile_text = normalize_profile_text(text=_norm_text(payload.text))
    if not normalized_profile_text:
        raise RuntimeError("profile text is required")
    proposal = await llm_propose_profile_setup(
        settings=settings_eff,
        profile_text=normalized_profile_text,
        usage_cb=_build_usage_cb(session),
    )
    if proposal is None:
        raise RuntimeError("upstream core profile proposal unavailable (LLM not configured)")
    return BridgeProfileProposeResponse(
        normalized_profile_text=normalized_profile_text,
        understanding=_norm_text(proposal.understanding, max_chars=800),
        interest_axes=list(proposal.interest_axes or []),
        interest_keywords=list(proposal.interest_keywords or []),
        retrieval_queries=list(proposal.retrieval_queries or []),
        ai_prompt=_norm_text(proposal.ai_prompt, max_chars=8000),
    )


async def bridge_topic_propose(
    *,
    session: Session,
    settings: Settings,
    payload: BridgeTopicProposeRequest,
) -> BridgeTopicProposeResponse:
    settings_eff = _apply_bridge_llm_override(_effective_settings(session, settings), payload.llm_override)
    proposal = await llm_propose_topic_setup(
        settings=settings_eff,
        topic_name=_norm_text(payload.name) or "New Topic",
        brief=_norm_text(payload.brief),
        usage_cb=_build_usage_cb(session),
    )
    if proposal is None:
        raise RuntimeError("upstream core topic proposal unavailable (LLM not configured)")
    hints = None
    if getattr(proposal, "source_hints", None):
        source_hints = proposal.source_hints
        hints = BridgeTopicProposeSourceHints(
            add_hn=bool(getattr(source_hints, "add_hn", True)),
            add_searxng=bool(getattr(source_hints, "add_searxng", True)),
            add_discourse=bool(getattr(source_hints, "add_discourse", False)),
            discourse_base_url=str(getattr(source_hints, "discourse_base_url", "") or ""),
            discourse_json_path=str(getattr(source_hints, "discourse_json_path", "/latest.json") or "/latest.json"),
            add_nodeseek=bool(getattr(source_hints, "add_nodeseek", False)),
        )
    return BridgeTopicProposeResponse(
        topic_name=_norm_text(proposal.topic_name, max_chars=200),
        query=_norm_text(proposal.query_keywords, max_chars=1200),
        alert_keywords=_norm_text(proposal.alert_keywords, max_chars=400),
        ai_prompt=_norm_text(proposal.ai_prompt, max_chars=8000),
        source_hints=hints,
    )


async def bridge_tracking_plan(
    *,
    session: Session,
    settings: Settings,
    payload: BridgeTrackingPlanRequest,
) -> BridgeTrackingPlanResponse:
    settings_eff = _apply_bridge_llm_override(_effective_settings(session, settings), payload.llm_override)
    if _tracking_request_has_embedded_profile_state(payload):
        profile = _profile_response_from_tracking_payload(payload)
    else:
        profile = await bridge_profile_propose(
            session=session,
            settings=settings_eff,
            payload=BridgeProfileProposeRequest(text=payload.text, llm_override=payload.llm_override),
        )
    snapshot_before = _normalize_tracking_snapshot(payload.tracking_snapshot)
    brief_text = _build_tracking_profile_brief(
        profile_text=_norm_text(profile.normalized_profile_text, max_chars=4000),
        understanding=_norm_text(profile.understanding, max_chars=800),
        interest_axes=_normalize_string_list(profile.interest_axes, 200, 220),
        interest_keywords=_normalize_string_list(profile.interest_keywords, 600, 120),
        retrieval_queries=_normalize_string_list(profile.retrieval_queries, 200, 260),
    )
    usage_cb = _build_usage_cb(session)
    retry_warnings: list[str] = []
    snapshot_text = snapshot_compact_text(snapshot_before)
    profile_topic_name = _norm_text(payload.profile_topic_name or "Profile", max_chars=200) or "Profile"
    initial_budget, retry_budget = _select_bridge_tracking_planner_budgets(
        settings=settings_eff,
        brief_text=brief_text,
        interest_axes=_normalize_string_list(profile.interest_axes, 200, 220),
        interest_keywords=_normalize_string_list(profile.interest_keywords, 600, 120),
        retrieval_queries=_normalize_string_list(profile.retrieval_queries, 200, 260),
    )
    try:
        planned = await llm_plan_tracking_ai_setup(
            settings=settings_eff,
            user_prompt=brief_text,
            tracking_snapshot_text=snapshot_text,
            max_tokens_override=initial_budget,
            usage_cb=usage_cb,
        )
    except Exception as exc:
        if not _is_retryable_tracking_plan_error(exc):
            raise
        retry_brief = _build_tracking_retry_brief(brief_text=brief_text, error_message=str(exc))
        planned = await llm_plan_tracking_ai_setup(
            settings=settings_eff,
            user_prompt=retry_brief,
            tracking_snapshot_text=snapshot_text,
            max_tokens_override=(retry_budget or initial_budget),
            usage_cb=usage_cb,
        )
        retry_warnings.append(
            f"retry: tracking planner regenerated after validation error: {_norm_text(exc, max_chars=160)}"
        )
    if planned is None:
        raise RuntimeError("upstream core tracking planner unavailable (LLM not configured)")
    plan, warnings = planned
    plan, materialize_warnings = materialize_ai_setup_mcp_plan(
        snapshot_before=snapshot_before,
        plan=plan,
        searxng_base_url=str(getattr(settings_eff, "searxng_base_url", "") or ""),
        profile_topic_name=profile_topic_name,
    )
    plan, autofix_warnings = autofix_ai_setup_plan_for_source_expansion(
        snapshot_before=snapshot_before,
        plan=plan,
        user_prompt=brief_text,
        searxng_base_url=str(getattr(settings_eff, "searxng_base_url", "") or ""),
        profile_topic_name=profile_topic_name,
    )
    hydrated_actions, hydrated_query_warnings = _hydrate_tracking_topic_queries(
        actions=list((plan or {}).get("actions") or []),
        profile_topic_name=_norm_text(payload.profile_topic_name or "Profile", max_chars=200) or "Profile",
    )
    profile_pruned_actions, profile_prune_warnings = _prune_curation_only_profile_tracking_actions(
        actions=hydrated_actions,
        profile_topic_name=_norm_text(payload.profile_topic_name or "Profile", max_chars=200) or "Profile",
    )
    return BridgeTrackingPlanResponse(
        normalized_profile_text=profile.normalized_profile_text,
        understanding=profile.understanding,
        interest_axes=profile.interest_axes,
        interest_keywords=profile.interest_keywords,
        retrieval_queries=profile.retrieval_queries,
        ai_prompt=profile.ai_prompt,
        input_brief=brief_text,
        warnings=[
            *list(retry_warnings or []),
            *list(warnings or []),
            *list(materialize_warnings or []),
            *list(autofix_warnings or []),
            *list(hydrated_query_warnings or []),
            *list(profile_prune_warnings or []),
        ],
        actions=profile_pruned_actions,
    )


async def bridge_config_plan(
    *,
    session: Session,
    settings: Settings,
    payload: BridgeConfigPlanRequest,
) -> BridgeConfigPlanResponse:
    settings_eff = _apply_bridge_llm_override(_effective_settings(session, settings), payload.llm_override)
    normalized_profile_text = normalize_profile_text(text=_norm_text(payload.profile_text))
    tracking_snapshot = _normalize_tracking_snapshot(payload.tracking_snapshot)
    allowed_setting_fields = _normalize_allowed_setting_fields(payload.allowed_setting_fields)
    planned = await llm_plan_config_agent(
        repo=None,
        settings=settings_eff,
        user_prompt=_norm_text(payload.user_prompt),
        tracking_snapshot_text=snapshot_compact_text(tracking_snapshot),
        profile_state_text=_build_profile_state_text(payload, normalized_profile_text),
        profile_prompt_text=_build_profile_prompt_text(payload, normalized_profile_text),
        settings_state_text=_norm_text(payload.settings_state_text, max_chars=16_000),
        conversation_history_text=_norm_text(payload.conversation_history_text, max_chars=6_000),
        page_context_text=_norm_text(payload.page_context_text, max_chars=2_000),
        settings_mcp_tools_text=_norm_text(payload.settings_mcp_tools_text, max_chars=16_000),
        usage_cb=_build_usage_cb(session),
    )
    if planned is None:
        raise RuntimeError("upstream core config planner unavailable (LLM not configured)")
    clean_plan, warnings = _validate_bridge_config_plan(
        planned,
        allowed_setting_fields=allowed_setting_fields,
    )
    tracking_actions, non_tracking_actions = _split_tracking_actions(list(clean_plan.get("actions") or []))
    if tracking_actions:
        materialized_tracking, materialize_warnings = materialize_ai_setup_mcp_plan(
            snapshot_before=tracking_snapshot,
            plan={"actions": tracking_actions},
            searxng_base_url=str(getattr(settings_eff, "searxng_base_url", "") or ""),
            profile_topic_name=_norm_text(payload.profile_topic_name or "Profile", max_chars=200) or "Profile",
        )
        warnings = [*list(warnings or []), *list(materialize_warnings or [])]
        actions = [*non_tracking_actions, *list(materialized_tracking.get("actions") or [])]
    else:
        actions = [*non_tracking_actions]
    return BridgeConfigPlanResponse(
        assistant_reply=_norm_text(clean_plan.get("assistant_reply"), max_chars=2000),
        summary=_norm_text(clean_plan.get("summary"), max_chars=800),
        questions=_normalize_string_list(clean_plan.get("questions"), 5, 400),
        warnings=[item for item in warnings if _norm_text(item)],
        actions=actions,
    )
