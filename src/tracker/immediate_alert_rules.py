from __future__ import annotations

import json
from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class ImmediateAlertRule:
    host: str
    title_all: tuple[str, ...]
    reason: str


def _normalize_host(raw: str) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return ""
    if "://" in value:
        try:
            value = (urlsplit(value).hostname or "").strip().lower()
        except Exception:
            value = ""
    value = value.rstrip(".")
    if value.startswith("www."):
        value = value[4:]
    return value


def _normalize_title_keywords(raw: object) -> tuple[str, ...]:
    if isinstance(raw, str):
        parts = raw.replace("；", ",").replace("，", ",").split(",")
    elif isinstance(raw, (list, tuple)):
        parts = [str(x or "") for x in raw]
    else:
        parts = []
    out: list[str] = []
    seen: set[str] = set()
    for part in parts:
        token = str(part or "").strip()
        if not token:
            continue
        folded = token.casefold()
        if folded in seen:
            continue
        seen.add(folded)
        out.append(token)
    return tuple(out)


def parse_immediate_alert_rules(raw: str) -> tuple[ImmediateAlertRule, ...]:
    text = (raw or "").strip()
    if not text:
        return ()
    try:
        payload = json.loads(text)
    except Exception:
        return ()
    items = payload if isinstance(payload, list) else [payload]
    out: list[ImmediateAlertRule] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        host = _normalize_host(str(item.get("host") or item.get("domain") or item.get("site") or ""))
        title_all = _normalize_title_keywords(
            item.get("title_all")
            or item.get("title_all_keywords")
            or item.get("title_keywords_all")
            or item.get("keywords")
            or ""
        )
        if not host or not title_all:
            continue
        reason = str(item.get("reason") or "").strip() or f"matched immediate_alert_rule:{host}"
        out.append(ImmediateAlertRule(host=host, title_all=title_all, reason=reason))
    return tuple(out)


def match_immediate_alert_rule(
    *,
    title: str,
    canonical_url: str,
    rules: tuple[ImmediateAlertRule, ...] | list[ImmediateAlertRule] | None,
) -> str | None:
    if not rules:
        return None
    try:
        host = _normalize_host((urlsplit((canonical_url or "").strip()).hostname or "").strip())
    except Exception:
        host = ""
    if not host:
        return None
    hay = (title or "").casefold()
    for rule in rules:
        rule_host = _normalize_host(getattr(rule, "host", ""))
        if not rule_host:
            continue
        if host != rule_host and not host.endswith(f".{rule_host}"):
            continue
        required = tuple(getattr(rule, "title_all", ()) or ())
        if required and all(str(token or "").casefold() in hay for token in required):
            return str(getattr(rule, "reason", "") or f"matched immediate_alert_rule:{rule_host}")
    return None
