from __future__ import annotations

from pathlib import Path
from typing import Any

from tracker.admin_settings import build_settings_view
from tracker.dynamic_config import _ENV_ONLY_FIELDS, _REMOTE_UPDATE_DENY_FIELDS
from tracker.repo import Repo
from tracker.settings import Settings

MCP_SETTING_SET_OP = "mcp.setting.set"
MCP_SETTING_CLEAR_OP = "mcp.setting.clear"
MCP_PROFILE_SET_OP = "mcp.profile.set"

_FORBIDDEN_REMOTE_FIELDS: set[str] = set(_REMOTE_UPDATE_DENY_FIELDS) | {
    "db_url",
    "env_path",
    "bootstrap_allow_no_auth",
    "admin_username",
    "admin_password",
    "api_token",
    "admin_allow_remote_env_update",
    "telegram_bot_token",
    "telegram_chat_id",
    "telegram_owner_user_id",
    "telegram_setup_code",
}
_ALLOWED_REMOTE_FIELDS: set[str] = {field for field in Settings.model_fields.keys() if field not in _FORBIDDEN_REMOTE_FIELDS}


def allowed_remote_setting_fields() -> list[str]:
    return sorted(_ALLOWED_REMOTE_FIELDS)


def is_allowed_remote_setting_field(field: str) -> bool:
    return (field or "").strip() in _ALLOWED_REMOTE_FIELDS


def build_settings_mcp_catalog(*, repo: Repo, settings: Settings) -> list[dict[str, Any]]:
    env_path = Path(str(getattr(settings, "env_path", "") or ".env"))
    view = build_settings_view(repo=repo, settings=settings, env_path=env_path)
    rows: list[dict[str, Any]] = []

    views = view.get("views") if isinstance(view, dict) else {}
    sections = view.get("sections") if isinstance(view, dict) else []
    section_by_field: dict[str, str] = {}

    for sec in sections if isinstance(sections, list) else []:
        if not isinstance(sec, dict):
            continue
        sec_id = str(sec.get("id") or "").strip()
        for field_meta in sec.get("fields") or []:
            if not isinstance(field_meta, dict):
                continue
            field = str(field_meta.get("field") or "").strip()
            if field:
                section_by_field[field] = sec_id

    for field in allowed_remote_setting_fields():
        meta = views.get(field) if isinstance(views, dict) else None
        if not isinstance(meta, dict):
            meta = {}
        rows.append(
            {
                "field": field,
                "section": section_by_field.get(field, "advanced"),
                "label": str(meta.get("label") or field),
                "description": str(meta.get("description") or "").strip(),
                "kind": str(meta.get("kind") or "text"),
                "secret": bool(meta.get("secret") or field in _ENV_ONLY_FIELDS),
                "restart_required": bool(meta.get("restart_required") or False),
                "current_value": (
                    "<set>"
                    if bool(meta.get("secret_is_set"))
                    else str(meta.get("current_value_str") or "").strip()
                ),
            }
        )
    return rows


def settings_mcp_tool_catalog_text(*, repo: Repo, settings: Settings, lang: str = "zh") -> str:
    rows = build_settings_mcp_catalog(repo=repo, settings=settings)
    is_zh = (lang or "").strip().lower().startswith("zh") or lang in {"中文", "简体中文", "繁體中文", "繁体中文"}
    lines: list[str] = []

    if is_zh:
        lines.append("MCP 配置动作（用于通过自然语言修改 Web Admin 里的安全配置字段）：")
        lines.append(
            f"1) {MCP_SETTING_SET_OP}：设置某个 Settings 字段。示例："
            + '{"op":"mcp.setting.set","field":"llm_base_url","value":"https://example.com/v1"}'
        )
        lines.append(
            f"2) {MCP_SETTING_CLEAR_OP}：清空/关闭某个可清空字段。示例："
            + '{"op":"mcp.setting.clear","field":"llm_extra_body_json"}'
        )
        lines.append(
            f"3) {MCP_PROFILE_SET_OP}：重建 Profile。示例："
            + '{"op":"mcp.profile.set","profile_text":"我关注开源 Agent、MCP、多代理编排","topic_name":"Profile"}'
        )
        lines.append("规则：")
        lines.append("- 只允许修改安全远程字段；禁止 db_url / env_path / api_host / api_port。")
        lines.append("- 若用户明确要更新 Profile/兴趣画像，优先输出 mcp.profile.set。")
        lines.append("- Topics / Sources / Bindings 仍使用 tracking MCP/source actions，不要误用 setting.set。")
        lines.append("- 密钥字段（token / api key / password）允许设置，但预览里不要回显原值。")
        lines.append("可用字段（field）：")
    else:
        lines.append("MCP config actions (for natural-language updates to safe Web Admin settings):")
        lines.append(
            f"1) {MCP_SETTING_SET_OP}: set a Settings field. Example: "
            + '{"op":"mcp.setting.set","field":"llm_base_url","value":"https://example.com/v1"}'
        )
        lines.append(
            f"2) {MCP_SETTING_CLEAR_OP}: clear/disable a clearable field. Example: "
            + '{"op":"mcp.setting.clear","field":"llm_extra_body_json"}'
        )
        lines.append(
            f"3) {MCP_PROFILE_SET_OP}: rebuild the Profile. Example: "
            + '{"op":"mcp.profile.set","profile_text":"I care about open-source agents, MCP, multi-agent orchestration","topic_name":"Profile"}'
        )
        lines.append("Rules:")
        lines.append("- Only safe remote fields are allowed; db_url / env_path / api_host / api_port are forbidden.")
        lines.append("- If the user is changing Profile/interest understanding, prefer mcp.profile.set.")
        lines.append("- Topics / Sources / Bindings must still use tracking/source MCP actions, not setting.set.")
        lines.append("- Secret fields (token / api key / password) may be set, but previews must not echo the raw value.")
        lines.append("Allowed fields:")

    for row in rows:
        field = str(row.get("field") or "")
        label = str(row.get("label") or field)
        kind = str(row.get("kind") or "text")
        section = str(row.get("section") or "advanced")
        desc = str(row.get("description") or "").strip()
        secret = bool(row.get("secret") or False)
        restart = bool(row.get("restart_required") or False)
        flags: list[str] = [section, kind]
        if secret:
            flags.append("secret")
        if restart:
            flags.append("restart")
        meta = ", ".join(flags)
        if desc:
            lines.append(f"- {field} ({meta}): {label} — {desc}")
        else:
            lines.append(f"- {field} ({meta}): {label}")

    return "\n".join(lines).strip()


__all__ = [
    "MCP_PROFILE_SET_OP",
    "MCP_SETTING_CLEAR_OP",
    "MCP_SETTING_SET_OP",
    "allowed_remote_setting_fields",
    "build_settings_mcp_catalog",
    "is_allowed_remote_setting_field",
    "settings_mcp_tool_catalog_text",
]
