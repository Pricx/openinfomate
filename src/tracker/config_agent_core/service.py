from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_args

from pydantic import TypeAdapter
from sqlalchemy.orm import Session

from tracker.actions import TopicAiPolicySpec, TopicSpec, create_topic as create_topic_action, upsert_topic_ai_policy
from tracker.admin_settings import build_settings_view
from tracker.config_agent import (
    AllowedOp as TrackingAllowedOp,
    apply_plan_to_db,
    apply_plan_to_snapshot,
    diff_tracking_snapshots,
    export_tracking_snapshot,
    snapshot_compact_text,
    validate_ai_setup_plan,
)
from tracker.dynamic_config import _ENV_ONLY_FIELDS, apply_env_block_updates, effective_settings, env_key_for_field
from tracker.integrations.config_settings_mcp import (
    MCP_PROFILE_SET_OP,
    MCP_SETTING_CLEAR_OP,
    MCP_SETTING_SET_OP,
    build_settings_mcp_catalog,
    is_allowed_remote_setting_field,
)
from tracker.llm import llm_plan_config_agent, llm_propose_profile_setup
from tracker.llm_usage import make_llm_usage_recorder
from tracker.profile_input import normalize_profile_text
from tracker.repo import Repo
from tracker.settings import Settings

_TRACKING_ALLOWED_OPS: set[str] = set(getattr(TrackingAllowedOp, "__args__", ()))  # type: ignore[attr-defined]
_CONFIG_AGENT_ALLOWED_OPS: set[str] = _TRACKING_ALLOWED_OPS | {
    MCP_SETTING_SET_OP,
    MCP_SETTING_CLEAR_OP,
    MCP_PROFILE_SET_OP,
}


@dataclass(frozen=True)
class ConfigAgentPlanResult:
    run_id: int
    plan: dict[str, Any]
    warnings: list[str]
    preview_markdown: str


@dataclass(frozen=True)
class ConfigAgentApplyResult:
    run_id: int
    notes: list[str]
    warnings: list[str]
    restart_required: bool = False


def _norm_text(value: object) -> str:
    return str(value or "").strip()


def _join_nonempty(lines: list[str], *, sep: str = "\n") -> str:
    return sep.join([line for line in lines if _norm_text(line)]).strip()


def _coerce_setting_value(field: str, raw_value: object) -> str:
    ann = Settings.model_fields[field].annotation
    raw = raw_value
    if isinstance(raw_value, (dict, list)):
        raw = json.dumps(raw_value, ensure_ascii=False)
    text = str(raw if raw is not None else "").strip()
    if field == "output_language":
        from tracker.dynamic_config import _normalize_output_language

        return _normalize_output_language(text)
    parsed = TypeAdapter(ann).validate_python(text)
    if isinstance(parsed, bool):
        return "true" if parsed else "false"
    return "" if parsed is None else str(parsed)


def _can_clear_setting_field(field: str) -> bool:
    ann = Settings.model_fields[field].annotation
    return type(None) in get_args(ann) or ann in {str, object}


def _profile_state_text(repo: Repo) -> str:
    keys = {
        "profile_topic_name": repo.get_app_config("profile_topic_name") or "Profile",
        "profile_text": repo.get_app_config("profile_text") or "",
        "profile_understanding": repo.get_app_config("profile_understanding") or "",
        "profile_interest_axes": repo.get_app_config("profile_interest_axes") or "",
        "profile_interest_keywords": repo.get_app_config("profile_interest_keywords") or "",
        "profile_retrieval_queries": repo.get_app_config("profile_retrieval_queries") or "",
    }
    lines = ["PROFILE_STATE:"]
    for key, value in keys.items():
        raw = _norm_text(value)
        if not raw:
            continue
        if len(raw) > 4000:
            raw = raw[:4000].rstrip() + "…"
        lines.append(f"{key}:\n{raw}")
    return _join_nonempty(lines, sep="\n\n")


def _settings_state_text(*, repo: Repo, settings: Settings) -> str:
    rows = build_settings_mcp_catalog(repo=repo, settings=settings)
    lines = ["SETTINGS_STATE:"]
    for row in rows:
        field = str(row.get("field") or "")
        current = str(row.get("current_value") or "").strip() or "<unset>"
        lines.append(f"- {field}: {current}")
    return _join_nonempty(lines)


def export_config_agent_snapshot(*, session: Session, settings: Settings) -> dict[str, Any]:
    repo = Repo(session)
    eff = effective_settings(repo=repo, settings=settings)
    settings_catalog = build_settings_mcp_catalog(repo=repo, settings=eff)
    return {
        "tracking": export_tracking_snapshot(session=session),
        "profile": {
            "profile_topic_name": repo.get_app_config("profile_topic_name") or "Profile",
            "profile_text": repo.get_app_config("profile_text") or "",
            "profile_understanding": repo.get_app_config("profile_understanding") or "",
            "profile_interest_axes": repo.get_app_config("profile_interest_axes") or "",
            "profile_interest_keywords": repo.get_app_config("profile_interest_keywords") or "",
            "profile_retrieval_queries": repo.get_app_config("profile_retrieval_queries") or "",
        },
        "settings": {
            str(row.get("field") or ""): {
                "current_value": str(row.get("current_value") or ""),
                "secret": bool(row.get("secret") or False),
                "restart_required": bool(row.get("restart_required") or False),
            }
            for row in settings_catalog
        },
    }


def validate_config_agent_plan(obj: object) -> tuple[dict[str, Any], list[str]]:
    if not isinstance(obj, dict):
        raise ValueError("plan must be a JSON object")
    raw_actions = obj.get("actions", [])
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
        if op not in _CONFIG_AGENT_ALLOWED_OPS:
            raise ValueError(f"action[{idx}] invalid op: {op!r}")

        if op in _TRACKING_ALLOWED_OPS:
            tracking_plan, more = validate_ai_setup_plan({"actions": [raw]})
            actions.extend(list(tracking_plan.get("actions") or []))
            warnings.extend(list(more or []))
            continue

        if op == MCP_SETTING_SET_OP:
            field = _norm_text(raw.get("field"))
            if not is_allowed_remote_setting_field(field):
                raise ValueError(f"action[{idx}] forbidden or unknown field: {field!r}")
            if "value" not in raw:
                raise ValueError(f"action[{idx}] missing value")
            try:
                value = _coerce_setting_value(field, raw.get("value"))
            except Exception as exc:
                raise ValueError(f"action[{idx}] invalid value for field={field!r}") from exc
            if not value and field in _ENV_ONLY_FIELDS:
                raise ValueError(f"action[{idx}] empty secret value not allowed; use clear op")
            actions.append({"op": op, "field": field, "value": value})
            continue

        if op == MCP_SETTING_CLEAR_OP:
            field = _norm_text(raw.get("field"))
            if not is_allowed_remote_setting_field(field):
                raise ValueError(f"action[{idx}] forbidden or unknown field: {field!r}")
            if not _can_clear_setting_field(field):
                raise ValueError(f"action[{idx}] field does not support clear; use set instead: {field!r}")
            actions.append({"op": op, "field": field})
            continue

        if op == MCP_PROFILE_SET_OP:
            profile_text = _norm_text(raw.get("profile_text") or raw.get("text"))
            if not profile_text:
                raise ValueError(f"action[{idx}] missing profile_text")
            topic_name = _norm_text(raw.get("topic_name") or raw.get("topic") or "Profile") or "Profile"
            actions.append({"op": op, "profile_text": profile_text, "topic_name": topic_name})
            continue

    questions = obj.get("questions") if isinstance(obj.get("questions"), list) else []
    assistant_reply = _norm_text(obj.get("assistant_reply") or obj.get("reply"))
    summary = _norm_text(obj.get("summary"))
    clean_questions = [str(q or "").strip() for q in questions if str(q or "").strip()][:5]
    if not actions and not assistant_reply and not summary and not clean_questions:
        raise ValueError("plan must include assistant_reply, summary/questions, or actions")
    out = {
        "assistant_reply": assistant_reply,
        "summary": summary,
        "questions": clean_questions,
        "actions": actions,
    }
    return out, warnings


def _render_tracking_preview(*, session: Session, plan: dict[str, Any]) -> list[str]:
    tracking_actions = [a for a in (plan.get("actions") or []) if str(a.get("op") or "") in _TRACKING_ALLOWED_OPS]
    if not tracking_actions:
        return ["## Tracking", "- (no tracking changes)"]
    before = export_tracking_snapshot(session=session)
    after = apply_plan_to_snapshot(snapshot=before, plan={"actions": tracking_actions})
    diff_md = diff_tracking_snapshots(before=before, after=after)
    lines = [line for line in str(diff_md or "").splitlines() if line.strip()]
    if lines and lines[0].startswith("# "):
        lines = lines[1:]
    return lines or ["## Tracking", "- (no tracking changes)"]


def build_config_agent_preview_markdown(*, repo: Repo, settings: Settings, session: Session, plan: dict[str, Any]) -> str:
    if not list(plan.get("actions") or []):
        return ""

    eff = effective_settings(repo=repo, settings=settings)
    env_path = Path(str(getattr(settings, "env_path", "") or ".env"))
    settings_view = build_settings_view(repo=repo, settings=eff, env_path=env_path)
    view_map = settings_view.get("views") if isinstance(settings_view, dict) else {}

    lines: list[str] = ["# Config Agent Preview", "", "## Profile"]

    profile_actions = [a for a in (plan.get("actions") or []) if str(a.get("op") or "") == MCP_PROFILE_SET_OP]
    if not profile_actions:
        lines.append("- (no profile changes)")
    else:
        for action in profile_actions[:10]:
            topic_name = _norm_text(action.get("topic_name") or "Profile") or "Profile"
            text = _norm_text(action.get("profile_text"))
            if len(text) > 200:
                text = text[:200].rstrip() + "…"
            lines.append(f"- Rebuild `{topic_name}` profile from new text: {text}")

    lines.extend(["", "## Settings"])
    setting_actions = [a for a in (plan.get("actions") or []) if str(a.get("op") or "") in {MCP_SETTING_SET_OP, MCP_SETTING_CLEAR_OP}]
    if not setting_actions:
        lines.append("- (no settings changes)")
    else:
        for action in setting_actions[:80]:
            field = _norm_text(action.get("field"))
            meta = view_map.get(field) if isinstance(view_map, dict) else None
            if not isinstance(meta, dict):
                meta = {}
            label = str(meta.get("label") or field)
            secret = bool(meta.get("secret") or field in _ENV_ONLY_FIELDS)
            if str(action.get("op") or "") == MCP_SETTING_CLEAR_OP:
                lines.append(f"- `{field}` ({label}) -> clear")
                continue
            value = _norm_text(action.get("value"))
            if secret:
                value = "(secret updated)"
            elif len(value) > 160:
                value = value[:160].rstrip() + "…"
            lines.append(f"- `{field}` ({label}) -> {value}")

    lines.extend([""])
    lines.extend(_render_tracking_preview(session=session, plan=plan))
    return _join_nonempty(lines)


async def plan_config_agent_request(
    *,
    repo: Repo,
    settings: Settings,
    user_prompt: str,
    actor: str = "",
    client_host: str = "",
    conversation_history_text: str = "",
    page_context_text: str = "",
) -> ConfigAgentPlanResult:
    prompt = _norm_text(user_prompt)
    if not prompt:
        raise RuntimeError("missing user_prompt")

    eff = effective_settings(repo=repo, settings=settings)
    if not eff.llm_base_url:
        raise RuntimeError("LLM not configured")

    usage_cb = None
    try:
        usage_cb = make_llm_usage_recorder(session=repo.session)
    except Exception:
        usage_cb = None

    tracking_before = export_tracking_snapshot(session=repo.session)
    planned = await llm_plan_config_agent(
        repo=repo,
        settings=eff,
        user_prompt=prompt,
        tracking_snapshot_text=snapshot_compact_text(tracking_before),
        profile_state_text=_profile_state_text(repo),
        settings_state_text=_settings_state_text(repo=repo, settings=eff),
        conversation_history_text=_norm_text(conversation_history_text),
        page_context_text=_norm_text(page_context_text),
        usage_cb=usage_cb,
    )
    if planned is None:
        raise RuntimeError("LLM not configured")

    plan, warnings = validate_config_agent_plan(planned)
    preview_markdown = build_config_agent_preview_markdown(repo=repo, settings=eff, session=repo.session, plan=plan)

    run_id = 0
    if list(plan.get("actions") or []):
        run = repo.add_config_agent_run(
            kind="config_agent_core",
            status="planned",
            actor=actor,
            client_host=client_host,
            user_prompt=prompt,
            plan_json=json.dumps(plan, ensure_ascii=False),
            preview_markdown=preview_markdown,
            snapshot_before_json=json.dumps(export_config_agent_snapshot(session=repo.session, settings=eff), ensure_ascii=False),
            snapshot_preview_json="",
            snapshot_after_json="",
            error="",
        )
        run_id = int(getattr(run, "id", 0) or 0)

    return ConfigAgentPlanResult(
        run_id=run_id,
        plan=plan,
        warnings=warnings,
        preview_markdown=preview_markdown,
    )


def _set_or_delete_app_config(repo: Repo, key: str, value: str) -> None:
    text = _norm_text(value)
    if text:
        repo.set_app_config(key, text)
    else:
        repo.delete_app_config(key)


async def _apply_profile_action(*, repo: Repo, settings: Settings, action: dict[str, Any]) -> list[str]:
    raw_profile_text = _norm_text(action.get("profile_text"))
    profile_text = normalize_profile_text(text=raw_profile_text)
    topic_name = _norm_text(action.get("topic_name") or repo.get_app_config("profile_topic_name") or "Profile") or "Profile"
    if not profile_text:
        return []

    eff = effective_settings(repo=repo, settings=settings)
    proposal = await llm_propose_profile_setup(repo=repo, settings=eff, profile_text=profile_text)
    if proposal is None or not _norm_text(proposal.ai_prompt):
        raise RuntimeError("LLM not configured for profile apply")

    topic = repo.get_topic_by_name(topic_name)
    if not topic:
        topic = create_topic_action(
            session=repo.session,
            spec=TopicSpec(name=topic_name, query="", digest_cron="0 9 * * *", alert_keywords=""),
        )
    else:
        changed = False
        if topic.query != "":
            topic.query = ""
            changed = True
        if not bool(topic.enabled):
            topic.enabled = True
            changed = True
        if changed:
            repo.session.commit()

    repo.set_app_config("profile_topic_name", topic_name)
    repo.set_app_config("profile_text", profile_text)
    _set_or_delete_app_config(repo, "profile_understanding", proposal.understanding)
    _set_or_delete_app_config(
        repo,
        "profile_interest_axes",
        "\n".join([str(x).strip() for x in (proposal.interest_axes or []) if str(x).strip()]),
    )
    _set_or_delete_app_config(
        repo,
        "profile_interest_keywords",
        ", ".join([str(x).strip() for x in (proposal.interest_keywords or []) if str(x).strip()]),
    )
    _set_or_delete_app_config(
        repo,
        "profile_retrieval_queries",
        "\n".join([str(x).strip() for x in (proposal.retrieval_queries or []) if str(x).strip()]),
    )
    _set_or_delete_app_config(repo, "profile_prompt_core", proposal.ai_prompt)
    upsert_topic_ai_policy(
        session=repo.session,
        spec=TopicAiPolicySpec(topic=topic_name, enabled=True, prompt=proposal.ai_prompt),
    )
    return [f"profile updated: {topic_name}"]


async def apply_config_agent_plan(
    *,
    session: Session,
    settings: Settings,
    plan: dict[str, Any],
    run_id: int | None = None,
) -> ConfigAgentApplyResult:
    repo = Repo(session)
    clean_plan, warnings = validate_config_agent_plan(plan)
    if not list(clean_plan.get("actions") or []):
        raise ValueError("plan has no actions to apply")
    notes: list[str] = []
    restart_required = False

    profile_actions = [a for a in (clean_plan.get("actions") or []) if str(a.get("op") or "") == MCP_PROFILE_SET_OP]
    setting_actions = [a for a in (clean_plan.get("actions") or []) if str(a.get("op") or "") in {MCP_SETTING_SET_OP, MCP_SETTING_CLEAR_OP}]
    tracking_actions = [a for a in (clean_plan.get("actions") or []) if str(a.get("op") or "") in _TRACKING_ALLOWED_OPS]

    for action in profile_actions:
        notes.extend(await _apply_profile_action(repo=repo, settings=settings, action=action))

    if setting_actions:
        env_updates: dict[str, str] = {}
        changed_fields: set[str] = set()
        for action in setting_actions:
            field = _norm_text(action.get("field"))
            if not field:
                continue
            env_key = env_key_for_field(field)
            changed_fields.add(field)
            if str(action.get("op") or "") == MCP_SETTING_CLEAR_OP:
                env_updates[env_key] = ""
            else:
                env_updates[env_key] = str(action.get("value") or "")
        result = apply_env_block_updates(
            repo=repo,
            settings=settings,
            env_path=Path(str(getattr(settings, "env_path", "") or ".env")),
            env_updates=env_updates,
        )
        restart_required = bool(result.restart_required)
        if changed_fields:
            notes.append("settings updated: " + ", ".join(sorted(changed_fields)))

    if tracking_actions:
        notes.extend(apply_plan_to_db(session=session, plan={"actions": tracking_actions}))

    if run_id and int(run_id or 0) > 0:
        repo.update_config_agent_run(
            int(run_id),
            status="applied",
            snapshot_after_json=json.dumps(export_config_agent_snapshot(session=session, settings=settings), ensure_ascii=False),
            error="",
        )

    return ConfigAgentApplyResult(
        run_id=int(run_id or 0),
        notes=notes,
        warnings=warnings,
        restart_required=restart_required,
    )


__all__ = [
    "ConfigAgentApplyResult",
    "ConfigAgentPlanResult",
    "apply_config_agent_plan",
    "build_config_agent_preview_markdown",
    "export_config_agent_snapshot",
    "plan_config_agent_request",
    "validate_config_agent_plan",
]
