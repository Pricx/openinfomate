from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from pydantic import TypeAdapter

from tracker.envfile import parse_env_assignments, upsert_env_vars
from tracker.repo import Repo
from tracker.settings import Settings


# NOTE: This module provides a small "dynamic config" layer:
# - Non-secret Settings fields can be stored in DB (`app_config`) for runtime overrides.
# - Secrets remain in `.env` only.
# - We keep `.env` and DB best-effort in sync (env mtime vs app_config.updated_at).
#
# Design constraint:
# - We intentionally do NOT try to hot-reload APScheduler triggers. Some settings still require restart.


@dataclass(frozen=True)
class ApplyResult:
    updated_env_keys: list[str]
    updated_db_keys: list[str]
    restart_required: bool


# Fields that MUST stay env-only (secrets or chicken-and-egg config).
_ENV_ONLY_FIELDS: set[str] = {
    # bootstrap / infra
    "db_url",
    "env_path",
    # admin auth
    "admin_password",
    # push secrets / credentials
    "dingtalk_webhook_url",
    "dingtalk_secret",
    "telegram_bot_token",
    "telegram_external_bind_base_url",
    "telegram_external_bind_token",
    "telegram_external_bind_code_prefix",
    "smtp_password",
    "api_token",
    "llm_api_key",
    "llm_proxy",
    "llm_mini_api_key",
    "llm_mini_proxy",
    "discourse_cookie",
    "cookie_jar_json",
}

# Env-only fields that can take effect without restart (read dynamically from `.env`).
_NO_RESTART_ENV_ONLY_FIELDS: set[str] = {
    # Web/Admin auth: we want a brand-new install to set these via Web Admin and
    # have them apply immediately (no restart dance).
    "admin_password",
    "api_token",
    # Push + LLM secrets: apply immediately by re-reading TRACKER_ENV_PATH.
    "dingtalk_webhook_url",
    "dingtalk_secret",
    "telegram_bot_token",
    "telegram_external_bind_base_url",
    "telegram_external_bind_token",
    "telegram_external_bind_code_prefix",
    "smtp_password",
    "llm_api_key",
    "llm_proxy",
    "llm_mini_api_key",
    "llm_mini_proxy",
    # Legacy/compat (hidden in UI but still supported).
    "discourse_cookie",
    "cookie_jar_json",
}

# Best-effort cache to avoid re-reading `.env` on every call.
_ENV_ASSIGNMENTS_CACHE: dict[str, Any] = {
    "path": "",
    "mtime_ns": -1,
    "size": -1,
    "assignments": {},
}


def _invalidate_env_assignments_cache(*, env_path: Path | None = None) -> None:
    cached_path = str(_ENV_ASSIGNMENTS_CACHE.get("path") or "")
    target = str(env_path) if env_path is not None else ""
    if target and cached_path and cached_path != target:
        return
    _ENV_ASSIGNMENTS_CACHE["path"] = ""
    _ENV_ASSIGNMENTS_CACHE["mtime_ns"] = -1
    _ENV_ASSIGNMENTS_CACHE["size"] = -1
    _ENV_ASSIGNMENTS_CACHE["assignments"] = {}


def _load_env_assignments(settings: Settings) -> dict[str, str]:
    env_path = Path(str(getattr(settings, "env_path", "") or ".env"))
    try:
        st = env_path.stat()
    except Exception:
        _invalidate_env_assignments_cache(env_path=env_path)
        return {}

    mtime_ns = int(getattr(st, "st_mtime_ns", int(float(getattr(st, "st_mtime", 0.0) or 0.0) * 1_000_000_000)) or 0)
    size = int(getattr(st, "st_size", -1) or 0)
    if (
        _ENV_ASSIGNMENTS_CACHE.get("path") == str(env_path)
        and int(_ENV_ASSIGNMENTS_CACHE.get("mtime_ns") or -1) == mtime_ns
        and int(_ENV_ASSIGNMENTS_CACHE.get("size") or -1) == size
    ):
        cached = _ENV_ASSIGNMENTS_CACHE.get("assignments")
        if isinstance(cached, dict):
            return cached  # type: ignore[return-value]
        return {}

    try:
        text = env_path.read_text(encoding="utf-8")
    except Exception:
        text = ""
    try:
        assignments = parse_env_assignments(text)
    except Exception:
        assignments = {}

    _ENV_ASSIGNMENTS_CACHE["path"] = str(env_path)
    _ENV_ASSIGNMENTS_CACHE["mtime_ns"] = mtime_ns
    _ENV_ASSIGNMENTS_CACHE["size"] = size
    _ENV_ASSIGNMENTS_CACHE["assignments"] = assignments
    return assignments

# A conservative default allowlist for "remote" env-block imports (Web/TG).
# This prevents accidental self-bricking (db_url/env_path) while still covering most operator needs.
_REMOTE_UPDATE_DENY_FIELDS: set[str] = {
    "db_url",
    "env_path",
    "api_host",
    "api_port",
}

# Settings that are read once at process start / scheduler wiring time.
# When these change, a restart is required for the running services to fully apply them.
_RESTART_REQUIRED_FIELDS: set[str] = {
    # scheduler timing / reliability
    "cron_timezone",
    "cron_misfire_grace_seconds",
    "alert_poll_seconds",
    # cron schedules
    "health_report_cron",
    "backup_cron",
    "prune_ignored_cron",
    "discover_sources_cron",
    "push_retry_cron",
    # scheduler wiring / background workers
    "digest_scheduler_enabled",
    "telegram_connect_poll_seconds",
    # concurrency wiring
    "max_concurrent_digests",
    # API bind / admin
    "api_host",
    "api_port",
    "admin_allow_remote_env_update",
}

# Secrets and other env-only fields are typically read once at process start.
# Keep restart hints conservative, but allow auth secrets to apply without restart.
_RESTART_REQUIRED_FIELDS |= {f for f in _ENV_ONLY_FIELDS if f not in _NO_RESTART_ENV_ONLY_FIELDS}


def env_key_for_field(field_name: str) -> str:
    return f"TRACKER_{(field_name or '').strip().upper()}"


def _field_for_env_key(env_key: str) -> str | None:
    k = (env_key or "").strip()
    if not k.startswith("TRACKER_"):
        return None
    raw = k[len("TRACKER_") :].strip()
    if not raw:
        return None
    field = raw.lower()
    if field not in Settings.model_fields:  # pydantic v2
        return None
    return field


def _is_env_only_field(field: str) -> bool:
    return (field or "").strip() in _ENV_ONLY_FIELDS


def _is_remote_denied_field(field: str) -> bool:
    return (field or "").strip() in _REMOTE_UPDATE_DENY_FIELDS


def _normalize_output_language(raw: str) -> str:
    v = (raw or "").strip()
    low = v.lower()
    if v in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语", "漢語"}:
        return "zh"
    if low in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or low.startswith("zh"):
        return "zh"
    if v in {"英文", "英语", "英語"}:
        return "en"
    if low in {"en", "en-us", "english"} or low.startswith("en"):
        return "en"
    raise ValueError("invalid TRACKER_OUTPUT_LANGUAGE (expected zh|en)")


def _quote_env_value(value: str) -> str:
    """
    Return a dotenv-safe double-quoted value.

    Keep this small and deterministic so exported blocks are copy-paste friendly.
    """
    s = str(value)
    s = s.replace("\\", "\\\\").replace('"', '\\"')
    s = s.replace("\r\n", "\n").replace("\n", "\\n")
    return f'"{s}"'


def _value_to_env_str(value: object) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return ""
    return str(value)


def parse_settings_env_block(
    text: str,
    *,
    allow_remote_updates: bool = True,
    blank_values_mean_no_change: bool = True,
) -> dict[str, str]:
    """
    Parse a dotenv-ish block and return normalized TRACKER_* updates.

    Rules:
    - Only keys that map to Settings fields are accepted.
    - Blank values are treated as "no change" by default.
    - For remote-facing imports (Web/TG), deny a small set of dangerous fields (db_url/env_path/api bind).
    - Validation uses pydantic field types for best-effort safety.
    """
    raw = parse_env_assignments(text)
    updates: dict[str, str] = {}

    for key, value in raw.items():
        if not key.startswith("TRACKER_"):
            continue

        field = _field_for_env_key(key)
        if not field:
            raise ValueError(f"unknown config key: {key}")

        v = (value or "").strip()
        if blank_values_mean_no_change and not v:
            continue

        if allow_remote_updates and _is_remote_denied_field(field):
            raise ValueError(f"forbidden config key: {key}")

        # Special normalization for a few "string enums" that operators enter in Chinese.
        if field == "output_language":
            updates[key] = _normalize_output_language(v)
            continue

        ann = Settings.model_fields[field].annotation
        try:
            parsed = TypeAdapter(ann).validate_python(v)
        except Exception as exc:
            raise ValueError(f"invalid {key}") from exc

        # Canonicalize booleans to dotenv-friendly values.
        if isinstance(parsed, bool):
            updates[key] = "true" if parsed else "false"
        else:
            updates[key] = str(parsed)

    return updates


def export_settings_env_block(
    *,
    repo: Repo,
    settings: Settings,
    env_path: Path,
    include_defaults: bool = False,
    include_env_only: bool = False,
) -> str:
    """
    Export a dotenv block for Settings.

    By default we export only explicitly configured keys:
    - keys present in `.env`, OR
    - keys overridden via `app_config`

    Secrets are excluded by default.
    """
    eff = effective_settings(repo=repo, settings=settings)

    env_assignments: dict[str, str] = {}
    try:
        if env_path.exists():
            env_assignments = parse_env_assignments(env_path.read_text(encoding="utf-8"))
    except Exception:
        env_assignments = {}

    lines: list[str] = []
    for field in Settings.model_fields.keys():
        if not include_env_only and _is_env_only_field(field):
            continue

        env_key = env_key_for_field(field)
        has_db = repo.get_app_config_entry(field) is not None
        has_env = env_key in env_assignments
        if not include_defaults and not (has_db or has_env):
            continue

        try:
            value = getattr(eff, field)
        except Exception:
            continue

        v = _value_to_env_str(value)
        # Always quote to avoid issues with spaces or JSON strings.
        lines.append(f"{env_key}={_quote_env_value(v)}")

    return "\n".join(lines).strip()


def apply_env_block_updates(
    *,
    repo: Repo,
    settings: Settings,
    env_path: Path,
    env_updates: Mapping[str, str],
) -> ApplyResult:
    if not env_updates:
        return ApplyResult(updated_env_keys=[], updated_db_keys=[], restart_required=False)

    updates = dict(env_updates)

    # 1) DB updates (non-secret, non-bootstrap fields).
    db_updates: dict[str, str] = {}
    for env_key, value in updates.items():
        field = _field_for_env_key(env_key)
        if not field:
            continue
        if _is_env_only_field(field):
            continue
        # Store under the Settings field name (stable app_config key).
        db_updates[field] = str(value)

    if db_updates:
        repo.set_app_config_many(db_updates)

    # 2) .env updates (always write, including secrets).
    upsert_env_vars(path=env_path, updates=updates)
    _invalidate_env_assignments_cache(env_path=env_path)

    # 3) Restart hint (best-effort; keep this conservative).
    #
    # - Restart-required fields: cron schedules, scheduler wiring, etc.
    # - Env-only fields: secrets/bootstrap config are read at process start (no DB override),
    #   so they also require restart to take effect.
    restart_required = False
    for k in updates.keys():
        f = (_field_for_env_key(k) or "").strip()
        if not f:
            continue
        if f in _RESTART_REQUIRED_FIELDS:
            restart_required = True
            break
        if f in _ENV_ONLY_FIELDS and f not in _NO_RESTART_ENV_ONLY_FIELDS:
            restart_required = True
            break
    return ApplyResult(
        updated_env_keys=sorted(updates.keys()),
        updated_db_keys=sorted(db_updates.keys()),
        restart_required=restart_required,
    )


def _dt_to_ts_utc(value: dt.datetime | None) -> float:
    if value is None:
        return 0.0
    try:
        # DB timestamps are stored as naive UTC in SQLite by default.
        return value.replace(tzinfo=dt.timezone.utc).timestamp()
    except Exception:
        return 0.0


def sync_env_and_db(
    *,
    repo: Repo,
    settings: Settings,
    env_path: Path,
) -> ApplyResult:
    """
    Best-effort two-way sync between `.env` and `app_config` for non-secret Settings fields.

    Conflict rule (user requirement):
    - If DB updated_at is newer -> overwrite env
    - If env file mtime is newer -> overwrite DB
    - Keep them consistent after sync
    """
    try:
        env_mtime = env_path.stat().st_mtime
    except FileNotFoundError:
        env_mtime = 0.0

    env_assignments: dict[str, str] = {}
    if env_mtime > 0:
        try:
            env_assignments = parse_env_assignments(env_path.read_text(encoding="utf-8"))
        except Exception:
            env_assignments = {}

    env_updates: dict[str, str] = {}
    db_updates: dict[str, str] = {}

    for field in Settings.model_fields.keys():
        if _is_env_only_field(field):
            continue

        env_key = env_key_for_field(field)
        env_val = (env_assignments.get(env_key) or "").strip()

        entry = repo.get_app_config_entry(field)
        db_val = (entry.value or "").strip() if entry else ""
        db_ts = _dt_to_ts_utc(getattr(entry, "updated_at", None) if entry else None)

        if not env_val and not entry:
            continue

        if env_val and not entry:
            # env has a value, DB doesn't -> env wins
            db_updates[field] = env_val
            continue

        if entry and not env_val:
            # DB has a value, env doesn't -> DB wins
            env_updates[env_key] = db_val
            continue

        if env_val == db_val:
            continue

        # Both exist but differ -> pick the newer side.
        if env_mtime > db_ts:
            db_updates[field] = env_val
        else:
            env_updates[env_key] = db_val

    if db_updates:
        repo.set_app_config_many(db_updates)
    if env_updates:
        upsert_env_vars(path=env_path, updates=env_updates)
        _invalidate_env_assignments_cache(env_path=env_path)

    restart_required = False
    return ApplyResult(
        updated_env_keys=sorted(env_updates.keys()),
        updated_db_keys=sorted(db_updates.keys()),
        restart_required=restart_required,
    )


def effective_settings(*, repo: Repo, settings: Settings) -> Settings:
    """
    Return a Settings copy with runtime-effective overrides applied.

    This makes many operator changes effective without restart (where the code path
    calls this function per job/run).
    """
    overrides: dict[str, Any] = {}
    try:
        env_assignments = _load_env_assignments(settings)
    except Exception:
        env_assignments = {}
    if env_assignments:
        for field in _NO_RESTART_ENV_ONLY_FIELDS:
            if field not in _ENV_ONLY_FIELDS:
                continue
            env_key = env_key_for_field(field)
            if env_key not in env_assignments:
                continue
            raw = str(env_assignments.get(env_key) or "").strip()
            overrides[field] = (raw or None)
    for field in Settings.model_fields.keys():
        if _is_env_only_field(field):
            continue
        v = repo.get_app_config(field)
        if v is None:
            continue
        # Validate+cast using the Settings field type.
        ann = Settings.model_fields[field].annotation
        try:
            if field == "output_language":
                overrides[field] = _normalize_output_language(v)
            else:
                overrides[field] = TypeAdapter(ann).validate_python(v)
        except Exception:
            # Never let a bad DB value break the service loop.
            continue

    if not overrides:
        return settings
    try:
        return settings.model_copy(update=overrides)  # type: ignore[attr-defined]
    except Exception:
        return settings
