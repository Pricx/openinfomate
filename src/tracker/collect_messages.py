from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass


_ID_RE = re.compile(r"[^a-z0-9._-]+")


@dataclass(frozen=True)
class CollectMessageRule:
    rule_id: str
    title: str
    cron: str
    lookback_hours: int
    fallback_lookback_hours: int
    source_ids: tuple[int, ...]
    enabled: bool = True


@dataclass(frozen=True)
class CollectMessageRuleGroup:
    group_id: str
    cron: str
    rules: tuple[CollectMessageRule, ...]


def _slug(raw: str) -> str:
    value = _ID_RE.sub("-", (raw or "").strip().lower()).strip("-._")
    return value or "collect"


def parse_collect_message_rules(raw: str | None) -> list[CollectMessageRule]:
    text = (raw or "").strip()
    if not text:
        return []
    try:
        data = json.loads(text)
    except Exception:
        return []
    if not isinstance(data, list):
        return []

    out: list[CollectMessageRule] = []
    seen_ids: set[str] = set()
    for idx, row in enumerate(data, start=1):
        if not isinstance(row, dict):
            continue
        enabled = bool(row.get("enabled", True))
        title = str(row.get("title") or row.get("name") or "").strip()
        if not title:
            title = f"Collect {idx}"
        cron = str(row.get("cron") or "0 19 * * *").strip() or "0 19 * * *"
        try:
            lookback_hours = int(row.get("lookback_hours", 24) or 24)
        except Exception:
            lookback_hours = 24
        lookback_hours = max(1, min(24 * 30, lookback_hours))
        try:
            fallback_lookback_hours = int(
                row.get("fallback_lookback_hours", row.get("max_lookback_hours", lookback_hours)) or lookback_hours
            )
        except Exception:
            fallback_lookback_hours = lookback_hours
        fallback_lookback_hours = max(lookback_hours, min(24 * 30, fallback_lookback_hours))

        source_ids: list[int] = []
        raw_ids = row.get("source_ids")
        if isinstance(raw_ids, list):
            for value in raw_ids:
                try:
                    sid = int(value)
                except Exception:
                    sid = 0
                if sid > 0 and sid not in source_ids:
                    source_ids.append(sid)
        if not source_ids:
            continue

        raw_id = str(row.get("id") or row.get("rule_id") or title).strip()
        rule_id = _slug(raw_id)
        if rule_id in seen_ids:
            suffix = 2
            base = rule_id
            while f"{base}-{suffix}" in seen_ids:
                suffix += 1
            rule_id = f"{base}-{suffix}"
        seen_ids.add(rule_id)
        out.append(
            CollectMessageRule(
                rule_id=rule_id,
                title=title,
                cron=cron,
                lookback_hours=lookback_hours,
                fallback_lookback_hours=fallback_lookback_hours,
                source_ids=tuple(source_ids),
                enabled=enabled,
            )
        )
    return out


def group_collect_message_rules(rules: list[CollectMessageRule]) -> list[CollectMessageRuleGroup]:
    by_cron: dict[str, list[CollectMessageRule]] = {}
    for rule in rules:
        if not rule.enabled:
            continue
        cron = (rule.cron or "").strip() or "0 19 * * *"
        by_cron.setdefault(cron, []).append(rule)

    groups: list[CollectMessageRuleGroup] = []
    for cron, bucket in sorted(by_cron.items(), key=lambda row: row[0]):
        ordered = tuple(sorted(bucket, key=lambda row: (row.rule_id, row.title)))
        if len(ordered) == 1:
            group_id = ordered[0].rule_id
        else:
            raw = ",".join(rule.rule_id for rule in ordered)
            group_id = f"batch-{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]}"
        groups.append(
            CollectMessageRuleGroup(
                group_id=group_id,
                cron=cron,
                rules=ordered,
            )
        )
    return groups
