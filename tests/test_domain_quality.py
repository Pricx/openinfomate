from __future__ import annotations

from tracker.domain_quality import build_domain_quality_policy
from tracker.settings import Settings


def test_domain_quality_tiers_and_thresholds():
    settings = Settings(
        domain_quality_low_domains="csdn.net",
        domain_quality_medium_domains="cnblogs.com",
        domain_quality_high_domains="arxiv.org",
        domain_quality_min_tier_for_push="high",
    )
    pol = build_domain_quality_policy(settings=settings)

    assert pol.tier_for_url("https://blog.csdn.net/a/b") == "low"
    assert pol.tier_for_url("https://www.cnblogs.com/x/p/1.html") == "medium"
    assert pol.tier_for_url("https://arxiv.org/abs/1234.5678") == "high"

    # push requires high
    assert pol.allows_push_url("https://arxiv.org/abs/1234.5678") is True
    assert pol.allows_push_url("https://www.cnblogs.com/x/p/1.html") is False
