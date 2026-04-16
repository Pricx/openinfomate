from __future__ import annotations

from tracker.repo import Repo
from tracker.topic_gate_config import TopicGateConfig

MCP_TOPIC_GATE_PATCH_OP = "mcp.topic_gate.patch"

TOPIC_GATE_AGENT_FIELDS: tuple[str, ...] = (
    "candidate_min_score",
    "candidate_convergence",
    "push_min_score",
    "max_digest_items",
    "max_alert_items",
    "push_dedupe_strength",
)

_FIELD_LABELS_EN: dict[str, str] = {
    "candidate_min_score": "Initial screening min score",
    "candidate_convergence": "Candidate convergence",
    "push_min_score": "Push min score",
    "max_digest_items": "Max digest items per batch",
    "max_alert_items": "Max alert items per batch",
    "push_dedupe_strength": "Push dedupe strength",
}

_FIELD_LABELS_ZH: dict[str, str] = {
    "candidate_min_score": "初筛最低分",
    "candidate_convergence": "候选收敛强度",
    "push_min_score": "进入推送最低分",
    "max_digest_items": "单次最多摘要条数",
    "max_alert_items": "单次最多告警条数",
    "push_dedupe_strength": "推送去重强度",
}


def _is_zh(lang: str) -> bool:
    raw = str(lang or "").strip().lower()
    return raw.startswith("zh") or raw in {"中文", "简体中文", "繁體中文", "繁体中文"}


def topic_gate_field_label(field: str, *, lang: str = "en") -> str:
    labels = _FIELD_LABELS_ZH if _is_zh(lang) else _FIELD_LABELS_EN
    return labels.get(str(field or "").strip(), str(field or "").strip())


def topic_gate_mcp_tool_catalog_text(*, lang: str = "zh") -> str:
    is_zh = _is_zh(lang)
    lines: list[str] = []
    if is_zh:
        lines.append("Topic Gate 配置动作（用于按 topic 控制初筛 / 候选收敛 / 推送阶段）：")
        lines.append(
            f"1) {MCP_TOPIC_GATE_PATCH_OP}：修改全局默认值或某个 topic 的 gate。"
            + ' 示例：{"op":"mcp.topic_gate.patch","scope":"topic","topic_name":"Profile","candidate_convergence":"strict","max_digest_items":6}'
        )
        lines.append("规则：")
        lines.append("- scope 只能是 defaults 或 topic。")
        lines.append("- scope=topic 时必须提供 topic_name。")
        lines.append("- 只允许以下字段：初筛最低分、候选收敛强度、进入推送最低分、单次最多摘要条数、单次最多告警条数、推送去重强度。")
        lines.append("- 字段省略表示“不改”；字段显式设为空/null 表示“清空该字段，恢复继承/无限制”。")
        lines.append("- reset_all=true 表示先清空当前 scope 下所有 Topic Gate，再应用本动作里显式给出的字段。")
        lines.append("- 默认值代表全局默认；topic override 只对单个 topic 生效。")
        lines.append("可用字段：")
    else:
        lines.append("Topic Gate config action (controls initial screening, candidate convergence, and push stage per topic):")
        lines.append(
            f"1) {MCP_TOPIC_GATE_PATCH_OP}: patch global defaults or one topic gate."
            + ' Example: {"op":"mcp.topic_gate.patch","scope":"topic","topic_name":"Profile","candidate_convergence":"strict","max_digest_items":6}'
        )
        lines.append("Rules:")
        lines.append("- scope must be defaults or topic.")
        lines.append("- topic_name is required when scope=topic.")
        lines.append("- Only these fields are allowed: initial screening min score, candidate convergence, push min score, max digest items, max alert items, push dedupe strength.")
        lines.append("- Omitted fields mean no change; explicit empty/null means clear that field back to inherit/unlimited.")
        lines.append("- reset_all=true clears the selected scope first, then applies the explicit fields from this action.")
        lines.append("- defaults are global; topic overrides only affect that topic.")
        lines.append("Allowed fields:")

    for field in TOPIC_GATE_AGENT_FIELDS:
        lines.append(f"- {field}: {topic_gate_field_label(field, lang=lang)}")
    return "\n".join(lines).strip()


def _value_text(value: object) -> str:
    if value is None:
        return "<unset>"
    text = str(value).strip()
    return text or "<unset>"


def _config_summary(config: TopicGateConfig) -> str:
    parts: list[str] = []
    data = config.to_dict()
    for field in TOPIC_GATE_AGENT_FIELDS:
        value = data.get(field)
        if value is None:
            continue
        parts.append(f"{field}={_value_text(value)}")
    return ", ".join(parts) if parts else "<unset>"


def topic_gate_state_text(*, repo: Repo) -> str:
    lines = ["TOPIC_GATE_STATE:"]
    try:
        defaults = repo.get_topic_gate_defaults()
    except Exception:
        defaults = TopicGateConfig()
    lines.append(f"- defaults: {_config_summary(defaults)}")
    try:
        topics = repo.list_topics()
    except Exception:
        topics = []
    for topic in topics[:200]:
        try:
            override = repo.get_topic_gate_override(topic_id=int(topic.id))
        except Exception:
            continue
        if override.is_empty():
            continue
        lines.append(f"- topic {topic.name}: {_config_summary(override)}")
    return "\n".join(lines).strip()


__all__ = [
    "MCP_TOPIC_GATE_PATCH_OP",
    "TOPIC_GATE_AGENT_FIELDS",
    "topic_gate_field_label",
    "topic_gate_mcp_tool_catalog_text",
    "topic_gate_state_text",
]
