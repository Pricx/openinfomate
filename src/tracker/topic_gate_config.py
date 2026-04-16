from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any, Literal

TOPIC_GATE_DEFAULTS_APP_CONFIG_KEY = "topic_gate_defaults_json"

CandidateConvergence = Literal["loose", "balanced", "strict"]
PushDedupeStrength = Literal["off", "loose", "balanced", "strict"]

_CANDIDATE_CONVERGENCE_ALIASES: dict[str, CandidateConvergence | None] = {
    "": None,
    "relaxed": "loose",
    "loose": "loose",
    "宽松": "loose",
    "balanced": "balanced",
    "平衡": "balanced",
    "strict": "strict",
    "严格": "strict",
}
_PUSH_DEDUPE_ALIASES: dict[str, PushDedupeStrength | None] = {
    "": None,
    "off": "off",
    "关闭": "off",
    "loose": "loose",
    "宽松": "loose",
    "balanced": "balanced",
    "平衡": "balanced",
    "strict": "strict",
    "严格": "strict",
}
TOPIC_GATE_FIELDS = (
    "candidate_min_score",
    "candidate_convergence",
    "push_min_score",
    "max_digest_items",
    "max_alert_items",
    "push_dedupe_strength",
)


@dataclass(frozen=True)
class TopicGateConfig:
    candidate_min_score: int | None = None
    candidate_convergence: CandidateConvergence | None = None
    push_min_score: int | None = None
    max_digest_items: int | None = None
    max_alert_items: int | None = None
    push_dedupe_strength: PushDedupeStrength | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def is_empty(self) -> bool:
        return not any(self.to_dict().values())


def _normalize_score(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    parsed = int(raw)
    return max(0, min(100, parsed))


def _normalize_optional_count(value: object) -> int | None:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    parsed = int(raw)
    if parsed == 0:
        return None
    if parsed < 1:
        raise ValueError("count must be >= 1")
    return parsed


def _normalize_candidate_convergence(value: object) -> CandidateConvergence | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw not in _CANDIDATE_CONVERGENCE_ALIASES:
        raise ValueError(f"invalid candidate_convergence: {value!r}")
    return _CANDIDATE_CONVERGENCE_ALIASES[raw]


def _normalize_push_dedupe_strength(value: object) -> PushDedupeStrength | None:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if raw not in _PUSH_DEDUPE_ALIASES:
        raise ValueError(f"invalid push_dedupe_strength: {value!r}")
    return _PUSH_DEDUPE_ALIASES[raw]


def normalize_topic_gate_config(value: object) -> TopicGateConfig:
    if isinstance(value, TopicGateConfig):
        return value

    obj = value
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return TopicGateConfig()
        obj = json.loads(raw)

    if obj is None:
        return TopicGateConfig()
    if not isinstance(obj, dict):
        raise ValueError("topic gate config must be an object")

    return TopicGateConfig(
        candidate_min_score=_normalize_score(obj.get("candidate_min_score", obj.get("initial_min_score"))),
        candidate_convergence=_normalize_candidate_convergence(
            obj.get("candidate_convergence", obj.get("candidate_convergence_mode"))
        ),
        push_min_score=_normalize_score(obj.get("push_min_score")),
        max_digest_items=_normalize_optional_count(obj.get("max_digest_items", obj.get("push_max_digest_items"))),
        max_alert_items=_normalize_optional_count(obj.get("max_alert_items", obj.get("push_max_alert_items"))),
        push_dedupe_strength=_normalize_push_dedupe_strength(
            obj.get("push_dedupe_strength", obj.get("dedupe_strength"))
        ),
    )


def merge_topic_gate_configs(*, defaults: TopicGateConfig, override: TopicGateConfig) -> TopicGateConfig:
    base = defaults.to_dict()
    top = override.to_dict()
    merged = {field: top[field] if top[field] is not None else base[field] for field in TOPIC_GATE_FIELDS}
    return normalize_topic_gate_config(merged)


def topic_gate_inherits_map(*, override: TopicGateConfig) -> dict[str, bool]:
    data = override.to_dict()
    return {field: data[field] is None for field in TOPIC_GATE_FIELDS}


def patch_topic_gate_config(*, base: TopicGateConfig, patch: dict[str, object]) -> TopicGateConfig:
    current = base.to_dict()
    clean = normalize_topic_gate_config(patch)
    for field in TOPIC_GATE_FIELDS:
        if field in patch:
            current[field] = getattr(clean, field)
    return normalize_topic_gate_config(current)


def dump_topic_gate_config(config: TopicGateConfig) -> str:
    payload = {"version": 1, **config.to_dict()}
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def topic_gate_score(
    *,
    source_score: int = 0,
    relevance_score: int = 0,
    novelty_score: int = 0,
    quality_score: int = 0,
) -> int:
    values: list[int] = []
    for raw in [source_score, relevance_score, novelty_score, quality_score]:
        try:
            value = max(0, min(100, int(raw or 0)))
        except Exception:
            value = 0
        if value > 0:
            values.append(value)
    if not values:
        return 0
    return max(0, min(100, int(round(sum(values) / len(values)))))


def candidate_convergence_keep_ratio(value: CandidateConvergence | None) -> float | None:
    if value == "loose":
        return 1.0
    if value == "balanced":
        return 0.66
    if value == "strict":
        return 0.4
    return None


def candidate_convergence_pool_ratio(value: CandidateConvergence | None) -> float | None:
    if value == "loose":
        return 1.0
    if value == "balanced":
        return 0.8
    if value == "strict":
        return 0.5
    return None


def push_dedupe_story_distance(value: PushDedupeStrength | None) -> int | None:
    if value == "balanced":
        return 8
    if value == "strict":
        return 6
    return None


__all__ = [
    "TOPIC_GATE_DEFAULTS_APP_CONFIG_KEY",
    "TOPIC_GATE_FIELDS",
    "TopicGateConfig",
    "candidate_convergence_keep_ratio",
    "candidate_convergence_pool_ratio",
    "dump_topic_gate_config",
    "merge_topic_gate_configs",
    "normalize_topic_gate_config",
    "patch_topic_gate_config",
    "push_dedupe_story_distance",
    "topic_gate_score",
    "topic_gate_inherits_map",
]
