from __future__ import annotations

import dataclasses
import json
from pathlib import Path
from typing import Any, Literal
from typing import get_args

from pydantic import TypeAdapter

from tracker.dynamic_config import (
    _ENV_ONLY_FIELDS,
    _RESTART_REQUIRED_FIELDS,
    effective_settings,
    env_key_for_field,
)
from tracker.envfile import parse_env_assignments
from tracker.repo import Repo
from tracker.settings import Settings


SettingSource = Literal["default", "env", "db"]
InputKind = Literal["text", "textarea", "number", "toggle", "password", "select"]


@dataclasses.dataclass(frozen=True)
class SettingFieldDef:
    """
    Registry metadata for a Settings field.

    Notes:
    - label/description are message IDs (English) used with the i18n `t()` helper.
    - We keep this intentionally lightweight: the UI is server-rendered (Jinja).
    """

    field: str
    label: str
    description: str = ""
    example: str = ""
    kind: InputKind = "text"
    options: list[tuple[str, str]] | None = None  # for select: (value, label)
    advanced: bool = False
    autosave_secret: bool = False


@dataclasses.dataclass(frozen=True)
class SettingFieldView:
    field: str
    env_key: str
    kind: InputKind
    options: list[tuple[str, str]] | None
    label: str
    description: str
    example: str
    secret: bool
    restart_required: bool
    source: SettingSource
    # Render-friendly values.
    current_value: Any
    current_value_str: str
    secret_is_set: bool
    autosave_secret: bool


@dataclasses.dataclass(frozen=True)
class SettingsSectionDef:
    id: str
    title: str
    description: str = ""
    fields: list[SettingFieldDef] = dataclasses.field(default_factory=list)


def _title_from_field(field: str) -> str:
    # Reasonable fallback for fields without explicit labels.
    return " ".join((field or "").strip().split("_")).strip().title() or field


_TEXTAREA_FIELDS: set[str] = {
    "llm_extra_body_json",
    "cookie_jar_json",
}


def _kind_for_field(field: str, ann: Any) -> InputKind:
    if field in _TEXTAREA_FIELDS:
        return "textarea"
    if field in _ENV_ONLY_FIELDS:
        # Env-only fields often contain secrets or bootstrap config; never echo.
        return "password"
    # Heuristic based on annotation.
    try:
        if ann is bool:
            return "toggle"
        if ann is int or ann is float:
            return "number"
    except Exception:
        pass
    return "text"


def default_settings_sections(*, settings: Settings | None = None) -> list[SettingsSectionDef]:
    """
    Core sections shown in Web Admin "Config".

    We keep this curated (Claude Relay-style): most operators only need these.
    An "Advanced / All Settings" panel can show everything else.
    """
    secs = [
        SettingsSectionDef(
            id="basics",
            title="Basics",
            description="Timezone used by background jobs (formatting + scheduling). Output language follows UI language (top right).",
            fields=[
                SettingFieldDef(
                    field="cron_timezone",
                    label="cron_timezone",
                    description="Timezone used to interpret cron schedules (UTC offset like +8, or an IANA name).",
                    example="+8",
                ),
                SettingFieldDef(
                    field="ui_theme_follow_system",
                    label="Follow system theme",
                    description="Automatically switch light/dark theme to match your browser or system appearance. If you toggle Theme manually from the top bar, this setting will be turned off until you enable it again here.",
                    kind="toggle",
                ),
            ],
        ),
        SettingsSectionDef(
            id="access",
            title="Access",
            description="Configure Web Admin/API authentication. Secrets are stored in `.env` and take effect immediately.",
            fields=[
                SettingFieldDef(
                    field="admin_username",
                    label="Admin username",
                    description="HTTP Basic username for Web Admin. Takes effect immediately.",
                    example="admin",
                ),
                SettingFieldDef(
                    field="admin_password",
                    label="Admin password",
                    description="HTTP Basic password for Web Admin. Leave blank to keep unchanged. Takes effect immediately.",
                    kind="password",
                ),
                SettingFieldDef(
                    field="api_token",
                    label="API token (optional)",
                    description="Optional token auth for Web Admin and setup routes. Leave blank to keep unchanged. Takes effect immediately.",
                    kind="password",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="admin_allow_remote_env_update",
                    label="Allow remote config writes",
                    description="Allow modifying `.env` via Web Admin from non-localhost clients. Dangerous; restart required.",
                    kind="toggle",
                    advanced=True,
                ),
            ],
        ),
        SettingsSectionDef(
            id="schedule",
            title="Curated Info",
            description="Configure Curated Info (batch) scheduling. Curated Info runs on a cadence derived from the lookback window and only de-dupes (no interpretation).",
            fields=[
                # Curated Info (legacy name: Digest)
                SettingFieldDef(
                    field="digest_push_enabled",
                    label="Curated Info push",
                    description="When scheduled Curated Info runs, whether to push to channels (still archived).",
                    kind="toggle",
                ),
                SettingFieldDef(
                    field="digest_hours",
                    label="Curated Info lookback hours",
                    description="Lookback window size for each Curated Info run. Curated Info also runs on this cadence (e.g. 2h => every 2 hours).",
                    kind="number",
                    example="24",
                ),
                SettingFieldDef(
                    field="telegram_digest_reader_enabled",
                    label="Telegram Curated Info reader",
                    description="Use the Telegram Reader card (single message + inline buttons) for Curated Info batches.",
                    kind="toggle",
                ),
                SettingFieldDef(
                    field="telegram_digest_item_feedback_enabled",
                    label="Telegram Curated Info item feedback",
                    description="Show per-item feedback buttons (👍/👎/🔕) inside the Curated Info reader.",
                    kind="toggle",
                ),
                SettingFieldDef(
                    field="health_report_cron",
                    label="Health report cron",
                    description="Daily health report schedule. Empty disables.",
                    example="0 8 * * *",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="discover_sources_cron",
                    label="Source discovery cron",
                    description="Discover new sources periodically. Empty disables.",
                    example="0 */6 * * *",
                    advanced=True,
                ),
            ],
        ),
        SettingsSectionDef(
            id="filters",
            title="Domain Filters",
            description="Comma-separated hosts. If include is set, only those domains are considered (best-effort). Exclude always wins. Source scoring and domain tiering can be used as hard filters before push and discovery.",
            fields=[
                SettingFieldDef(
                    field="include_domains",
                    label="Include domains",
                    description="Optional allowlist (comma-separated hosts).",
                    example="forum.example.com, github.com, arxiv.org",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="exclude_domains",
                    label="Exclude domains",
                    description="Blocklist (comma-separated hosts). Exclude wins over include.",
                    example="csdn.net, zhihu.com",
                ),
                SettingFieldDef(
                    field="source_quality_min_score",
                    label="Min source score",
                    description="Hard filter: minimum source score allowed in pushed Curated Info and alerts. Also used as the default threshold for auto-discovered sources.",
                    kind="number",
                    example="50",
                ),
                SettingFieldDef(
                    field="domain_quality_low_domains",
                    label="Low-quality domains",
                    description="Comma-separated hosts treated as low tier.",
                    example="csdn.net",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="domain_quality_medium_domains",
                    label="Medium-quality domains",
                    description="Comma-separated hosts treated as medium tier.",
                    example="cnblogs.com",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="domain_quality_high_domains",
                    label="High-quality domains",
                    description="Comma-separated hosts treated as high tier.",
                    example="arxiv.org, github.com, openai.com",
                    advanced=True,
                ),
            ],
        ),
        SettingsSectionDef(
            id="llm",
            title="LLM Providers",
            description="Two models: Primary (better) and Aux (cheaper). Aux is only used for triage/digest summary to keep costs down.",
            fields=[
                SettingFieldDef(
                    field="llm_curation_enabled",
                    label="Curation enabled",
                    description="Enable prompt-driven AI selection for candidate items.",
                    kind="toggle",
                ),
                SettingFieldDef(
                    field="llm_curation_triage_enabled",
                    label="Mini triage enabled",
                    description="Use a cheap mini model to pre-filter candidates before final reasoning selection.",
                    kind="toggle",
                ),
                SettingFieldDef(
                    field="llm_base_url",
                    label="Primary base URL",
                    description="OpenAI-compatible base URL for the primary model/provider. `/v1` is optional: keep it if your provider gives it, or omit it and OpenInfoMate will add it automatically.",
                    example="http://127.0.0.1:8317/v1",
                ),
                SettingFieldDef(
                    field="llm_model_reasoning",
                    label="Primary model",
                    description="Model name used for profile/topic planning and selection/curation.",
                    example="gpt-5.2",
                ),
                SettingFieldDef(field="llm_api_key", label="Primary API key", description="API key for the primary model/provider.", kind="password", autosave_secret=True),
                SettingFieldDef(
                    field="llm_extra_body_json",
                    label="Primary extra request body (optional)",
                    description="JSON merged into primary-model requests (provider-specific knobs like reasoning_effort).",
                    kind="textarea",
                    example='{"reasoning":{"effort":"xhigh"}}',
                    advanced=False,
                ),
                SettingFieldDef(
                    field="llm_mini_base_url",
                    label="Aux base URL (optional)",
                    description="Optional separate base URL for the aux model/provider. If unset, falls back to primary base URL.",
                ),
                SettingFieldDef(
                    field="llm_model_mini",
                    label="Aux model (optional)",
                    description="Aux model name used for triage/compression tasks. If unset, falls back to the primary model.",
                    example="gpt-5.1-mini",
                ),
                SettingFieldDef(field="llm_mini_api_key", label="Aux API key (optional)", description="API key for the aux model/provider (if set).", kind="password", autosave_secret=True),
                SettingFieldDef(
                    field="llm_mini_extra_body_json",
                    label="Aux extra request body (optional)",
                    description="JSON merged into aux-model requests. If empty, falls back to the primary extra body.",
                    kind="textarea",
                    example='{"temperature":0}',
                    advanced=False,
                ),
                SettingFieldDef(field="llm_proxy", label="Primary proxy (optional)", description="Optional HTTP proxy for the primary model/provider.", kind="password", advanced=True),
                SettingFieldDef(field="llm_mini_proxy", label="Aux proxy (optional)", description="Optional HTTP proxy for the aux model/provider.", kind="password", advanced=True),
                SettingFieldDef(
                    field="llm_model",
                    label="Default model (legacy)",
                    description="Legacy fallback model name (if Primary model is empty).",
                    advanced=True,
                ),
            ],
        ),
        SettingsSectionDef(
            id="tracking",
            title="Tracking (Smart Config)",
            description="Budgets and discovery knobs for AI Setup and source expansion (operators review/accept candidates).",
            fields=[
                SettingFieldDef(
                    field="ai_setup_plan_max_tokens",
                    label="AI Setup plan max tokens",
                    description="LLM output budget for Smart Config planning JSON. Higher enables more topics/seeds in one run.",
                    kind="number",
                    example="50000",
                ),
                SettingFieldDef(
                    field="ai_setup_transform_chunk_chars",
                    label="AI Setup transform chunk chars",
                    description="Chunk size (chars) for transforming very large inputs into a structured planning brief.",
                    kind="number",
                    example="20000",
                ),
                SettingFieldDef(
                    field="ai_setup_transform_max_chunks",
                    label="AI Setup transform max chunks",
                    description="Max chunks processed when transforming huge inputs (bounds worst-case work).",
                    kind="number",
                    example="60",
                ),
                SettingFieldDef(
                    field="discover_sources_max_results_per_topic",
                    label="Source discovery max pages per topic",
                    description="Max web pages checked per topic in each discovery run (higher finds more feed candidates).",
                    kind="number",
                    example="50",
                ),
            ],
        ),
        SettingsSectionDef(
            id="priority",
            title="Quick Messages",
            description="Push a few time-sensitive, high-impact signals as single-item alerts quickly.",
            fields=[
                SettingFieldDef(field="priority_lane_enabled", label="Enabled", description="Enable the priority lane scanner.", kind="toggle"),
                SettingFieldDef(field="priority_lane_hours", label="Lookback hours", description="Scan candidates within this lookback window.", kind="number", example="72"),
                SettingFieldDef(
                    field="priority_lane_pool_max_candidates",
                    label="Pool max candidates",
                    description="Max candidates considered before triage.",
                    kind="number",
                    example="200",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="priority_lane_triage_keep_candidates",
                    label="Triage keep candidates",
                    description="How many candidates the triage stage keeps (before final selection).",
                    kind="number",
                    example="20",
                    advanced=True,
                ),
                SettingFieldDef(field="priority_lane_max_alert", label="Max alerts", description="Max alerts produced per scan run.", kind="number", example="2"),
            ],
        ),
        SettingsSectionDef(
            id="push",
            title="Push",
            description="Configure push channels (Telegram / DingTalk / Email / Webhook). Secrets are never echoed back. Source-quality gating is under Domain Filters.",
            fields=[
                SettingFieldDef(field="push_dingtalk_enabled", label="DingTalk", description="Enable DingTalk push.", kind="toggle"),
                SettingFieldDef(field="dingtalk_webhook_url", label="Webhook URL", description="DingTalk robot webhook URL.", kind="password"),
                SettingFieldDef(field="dingtalk_secret", label="Secret", description="DingTalk sign secret (SEC...).", kind="password"),
                SettingFieldDef(field="push_telegram_enabled", label="Telegram", description="Enable Telegram push.", kind="toggle"),
                SettingFieldDef(field="telegram_bot_username", label="Bot username", description="Optional @bot username (for /start links).", advanced=True),
                SettingFieldDef(field="telegram_disable_preview", label="Disable link preview", description="Disable Telegram link previews.", kind="toggle", advanced=True),
                SettingFieldDef(
                    field="telegram_prompt_delta_enabled",
                    label="Telegram prompt delta",
                    description="Enable the “Fix prompt” action in reply menus (auditable; requires Apply/Reject).",
                    kind="toggle",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_prompt_delta_target_slot_id",
                    label="Prompt delta target slot",
                    description="Default prompt slot to update from feedback (e.g. llm.curate_items.system).",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_connect_poll_seconds",
                    label="Telegram poll seconds",
                    description="Polling interval for Telegram updates (inline buttons/reactions). Restart required. 0 disables.",
                    kind="number",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_feedback_reactions_enabled",
                    label="Feedback: reactions",
                    description="Enable reaction-based feedback (👍👎🔕) on pushed messages.",
                    kind="toggle",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_feedback_replies_enabled",
                    label="Feedback: replies",
                    description="Enable reply-based feedback parsing and action menus.",
                    kind="toggle",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_feedback_like_emojis",
                    label="Feedback: like emojis",
                    description="Comma/space-separated emojis mapped to “like”.",
                    example="👍,❤️,🔥,⭐,🌟",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_feedback_dislike_emojis",
                    label="Feedback: dislike emojis",
                    description="Comma/space-separated emojis mapped to “dislike”.",
                    example="👎,💩,😡,🤮,❌",
                    advanced=True,
                ),
                SettingFieldDef(
                    field="telegram_feedback_mute_emojis",
                    label="Feedback: mute emojis",
                    description="Comma/space-separated emojis mapped to “mute domain”.",
                    example="🔕",
                    advanced=True,
                ),
                SettingFieldDef(field="smtp_host", label="SMTP host", description="SMTP hostname.", advanced=True),
                SettingFieldDef(field="smtp_port", label="SMTP port", description="SMTP port (587 STARTTLS / 465 SSL).", kind="number", advanced=True),
                SettingFieldDef(field="smtp_user", label="SMTP user", description="SMTP username.", advanced=True),
                SettingFieldDef(field="smtp_password", label="SMTP password", description="SMTP password.", kind="password", advanced=True),
                SettingFieldDef(field="smtp_starttls", label="SMTP STARTTLS", description="Use STARTTLS (usually on 587).", kind="toggle", advanced=True),
                SettingFieldDef(field="smtp_use_ssl", label="SMTP SSL", description="Use SSL (usually on 465).", kind="toggle", advanced=True),
                SettingFieldDef(field="email_from", label="Email from", description="From address.", advanced=True),
                SettingFieldDef(field="email_to", label="Email to", description="Comma-separated recipients.", advanced=True),
                SettingFieldDef(field="webhook_url", label="Webhook URL (generic)", description="Optional generic webhook push URL.", advanced=True),
            ],
        ),
    ]

    # Order sections by operator mental model.
    order = [
        "basics",
        "llm",
        "tracking",
        "priority",
        "schedule",
        "filters",
        "push",
    ]
    try:
        secs.sort(key=lambda s: (order.index(s.id) if s.id in order else 999))
    except Exception:
        pass
    return secs


def build_settings_view(
    *,
    repo: Repo,
    settings: Settings,
    env_path: Path,
    sections: list[SettingsSectionDef] | None = None,
) -> dict[str, Any]:
    """
    Build a render-friendly settings view-model for the admin UI.
    """
    eff = effective_settings(repo=repo, settings=settings)

    env_assignments: dict[str, str] = {}
    try:
        if env_path.exists():
            env_assignments = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    except Exception:
        env_assignments = {}

    # 1) Curated section fields.
    sections = sections or default_settings_sections(settings=settings)
    curated_fields: list[str] = []
    for sec in sections:
        for f in sec.fields:
            curated_fields.append(f.field)

    # 2) Advanced: include ALL Settings fields as a fallback panel.
    all_fields = list(Settings.model_fields.keys())
    all_fields_sorted = sorted(all_fields)

    def _source_for_field(field: str) -> SettingSource:
        if field in _ENV_ONLY_FIELDS:
            return "env" if env_key_for_field(field) in env_assignments else "default"
        if repo.get_app_config_entry(field) is not None:
            return "db"
        if env_key_for_field(field) in env_assignments:
            return "env"
        return "default"

    views: dict[str, SettingFieldView] = {}
    for field in all_fields_sorted:
        ann = Settings.model_fields[field].annotation
        kind = _kind_for_field(field, ann)

        # Registry label fallback (for "All Settings" panel).
        label = _title_from_field(field)
        desc = ""
        ex = ""
        options: list[tuple[str, str]] | None = None
        autosave_secret = False

        # If the field is part of curated registry, use its richer metadata.
        for sec in sections:
            for f in sec.fields:
                if f.field != field:
                    continue
                label = f.label
                desc = f.description or ""
                ex = f.example or ""
                kind = f.kind
                options = f.options
                autosave_secret = bool(f.autosave_secret)

        env_key = env_key_for_field(field)
        secret = field in _ENV_ONLY_FIELDS or kind == "password"
        restart_required = field in _RESTART_REQUIRED_FIELDS
        source = _source_for_field(field)

        try:
            current_value = getattr(eff, field)
        except Exception:
            current_value = None

        secret_is_set = False
        if secret:
            # Avoid echoing; only expose set/unset.
            #
            # Important: Settings is loaded once at process start, but `.env` can be updated
            # via Web Admin. Use the `.env` assignments (best-effort) so UI reflects changes
            # immediately without requiring restart.
            raw = None
            try:
                raw = env_assignments.get(env_key)
            except Exception:
                raw = None
            if raw is None:
                try:
                    raw = getattr(settings, field)
                except Exception:
                    raw = None
            if isinstance(raw, str):
                secret_is_set = bool(raw.strip())
            else:
                secret_is_set = bool(raw)

        if secret:
            current_value_str = ""
        else:
            if isinstance(current_value, (dict, list)):
                current_value_str = json.dumps(current_value, ensure_ascii=False)
            else:
                current_value_str = "" if current_value is None else str(current_value)

        views[field] = SettingFieldView(
            field=field,
            env_key=env_key,
            kind=kind,
            options=options,
            label=label,
            description=desc,
            example=ex,
            secret=secret,
            restart_required=restart_required,
            source=source,
            current_value=current_value,
            current_value_str=current_value_str,
            secret_is_set=secret_is_set,
            autosave_secret=bool(autosave_secret),
        )

    return {
        "sections": [dataclasses.asdict(s) for s in sections],
        "views": {k: dataclasses.asdict(v) for k, v in views.items()},
        "curated_fields": curated_fields,
        "all_fields": all_fields_sorted,
    }


def parse_settings_patch_form(
    *,
    form: Any,
    repo: Repo,
    settings: Settings,
) -> tuple[dict[str, str], list[str]]:
    """
    Parse a settings patch form submission into dotenv updates.

    Returns: (env_updates, errors)
    """
    eff = effective_settings(repo=repo, settings=settings)
    env_updates: dict[str, str] = {}
    errors: list[str] = []

    for raw_key in form.keys():
        field = str(raw_key or "").strip()
        if field not in Settings.model_fields:
            continue

        v = str(form.get(field) or "").strip()

        # Secrets: blank means "no change".
        if field in _ENV_ONLY_FIELDS and not v:
            continue

        # Validation + diff against effective settings.
        ann = Settings.model_fields[field].annotation
        try:
            # Optional fields: treat blank as None so "unset" fields don't get
            # accidentally written as empty strings on unrelated edits.
            if not v and type(None) in get_args(ann):
                parsed = None
            elif field == "output_language":
                # Allow operators to type "中文/英文" etc in free-form inputs.
                # Keep behavior consistent with env-block parsing.
                from tracker.dynamic_config import _normalize_output_language

                parsed = _normalize_output_language(v)
            else:
                parsed = TypeAdapter(ann).validate_python(v)
        except Exception:
            errors.append(field)
            continue

        try:
            baseline = getattr(eff, field)
        except Exception:
            baseline = None

        # Secrets: always treat non-empty input as an update.
        if field in _ENV_ONLY_FIELDS:
            baseline = object()

        # Optional string hygiene: treat "" and None as equivalent for "no change".
        if type(None) in get_args(ann) and parsed is None and baseline == "":
            continue
        if parsed == baseline:
            continue

        if isinstance(parsed, bool):
            env_updates[env_key_for_field(field)] = "true" if parsed else "false"
        else:
            # For optionals / strings, empty string is a valid "clear/disable" for many fields.
            env_updates[env_key_for_field(field)] = "" if parsed is None else str(parsed)

    return env_updates, errors
