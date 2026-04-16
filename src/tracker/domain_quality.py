from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit

from tracker.http_auth import host_matches_any, parse_domains_csv
from tracker.settings import Settings


Tier = str  # "low" | "medium" | "high" | "unknown"

_BUILTIN_HIGH_PATTERNS: tuple[str, ...] = (
    "arxiv.org",
)


def _tier_rank(tier: Tier) -> int:
    t = (tier or "").strip().lower()
    if t == "low":
        return 0
    if t == "high":
        return 2
    if t in {"unknown", "medium", ""}:
        return 1
    # Be permissive: treat unknown values as medium.
    return 1


def normalize_min_tier(raw: str | None, *, default: str = "medium") -> str:
    v = (raw or "").strip().lower()
    if v in {"low", "medium", "high"}:
        return v
    return (default or "medium").strip().lower() if (default or "").strip().lower() in {"low", "medium", "high"} else "medium"


def _merge_patterns(*groups: list[str] | tuple[str, ...]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw in group:
            value = str(raw or "").strip().lower()
            if not value or value in seen:
                continue
            seen.add(value)
            out.append(value)
    return out


@dataclass(frozen=True)
class DomainQualityPolicy:
    low_patterns: list[str]
    medium_patterns: list[str]
    high_patterns: list[str]
    min_push_rank: int

    def tier_for_host(self, host: str) -> Tier:
        h = (host or "").strip()
        if not h:
            return "unknown"
        if self.low_patterns and host_matches_any(host=h, patterns=self.low_patterns):
            return "low"
        if self.high_patterns and host_matches_any(host=h, patterns=self.high_patterns):
            return "high"
        if self.medium_patterns and host_matches_any(host=h, patterns=self.medium_patterns):
            return "medium"
        return "unknown"

    def tier_for_url(self, url: str) -> Tier:
        u = (url or "").strip()
        if not u:
            return "unknown"
        try:
            host = urlsplit(u).netloc or ""
        except Exception:
            host = ""
        return self.tier_for_host(host)

    def allows_push_url(self, url: str) -> bool:
        tier = self.tier_for_url(url)
        return _tier_rank(tier) >= int(self.min_push_rank)

    def score_adjustment_for_tier(self, tier: Tier) -> int:
        t = (tier or "").strip().lower()
        if t == "high":
            return 5
        if t == "low":
            # Soft down-rank only: low-tier domains should be reviewed more strictly,
            # but not turn into de-facto hard blocks once a source already has a
            # decent operator/LLM score.
            return -10
        return 0

    def score_adjustment_for_url(self, url: str) -> int:
        return self.score_adjustment_for_tier(self.tier_for_url(url))

    def extra_min_score_for_tier(self, tier: Tier) -> int:
        t = (tier or "").strip().lower()
        if t == "low":
            # Keep low-tier domains as "soft" gates (not hard blocks), but require
            # meaningfully higher source quality scores before they appear in
            # pushed/curated outputs.
            return 20
        return 0

    def min_score_threshold_for_url(self, *, base_min_score: int, url: str) -> int:
        threshold = int(base_min_score or 0) + int(self.extra_min_score_for_tier(self.tier_for_url(url)))
        return max(0, min(100, threshold))


def build_domain_quality_policy(*, settings: Settings) -> DomainQualityPolicy:
    low = parse_domains_csv(getattr(settings, "domain_quality_low_domains", "") or "")
    medium = parse_domains_csv(getattr(settings, "domain_quality_medium_domains", "") or "")
    high = _merge_patterns(
        parse_domains_csv(getattr(settings, "domain_quality_high_domains", "") or ""),
        _BUILTIN_HIGH_PATTERNS,
    )

    min_push = normalize_min_tier(getattr(settings, "domain_quality_min_tier_for_push", "medium") or "medium")

    return DomainQualityPolicy(
        low_patterns=low,
        medium_patterns=medium,
        high_patterns=high,
        min_push_rank=_tier_rank(min_push),
    )
