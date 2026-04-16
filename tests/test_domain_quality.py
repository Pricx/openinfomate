from __future__ import annotations

from tracker.domain_quality import build_domain_quality_policy
from tracker.runner import _should_keep_push_item
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


def test_domain_quality_score_adjustments_are_soft_not_blocks():
    settings = Settings(
        domain_quality_low_domains="dev.to",
        domain_quality_high_domains="github.com",
    )
    pol = build_domain_quality_policy(settings=settings)

    assert pol.score_adjustment_for_url("https://dev.to/p/ghost-task") == -10
    assert pol.score_adjustment_for_url("https://github.com/openai/openai-python") == 5
    assert pol.score_adjustment_for_url("https://example.com/post") == 0


def test_domain_quality_builtin_arxiv_is_high_tier_and_passes_source_score_gate():
    settings = Settings(
        source_quality_min_score=60,
    )
    pol = build_domain_quality_policy(settings=settings)

    assert pol.tier_for_url("https://arxiv.org/abs/1234.5678") == "high"
    assert pol.tier_for_url("https://export.arxiv.org/rss/cs.AI") == "high"
    assert pol.score_adjustment_for_url("https://arxiv.org/abs/1234.5678") == 5

    assert _should_keep_push_item(
        url="https://arxiv.org/abs/1234.5678",
        source_id=1,
        source_url="https://export.arxiv.org/rss/cs.AI",
        active_mute_domains=set(),
        domain_policy=pol,
        min_source_score=60,
        scores_by_source_id={},
    ) is True


def test_domain_quality_operator_low_override_beats_builtin_high():
    settings = Settings(
        domain_quality_low_domains="arxiv.org",
    )
    pol = build_domain_quality_policy(settings=settings)

    assert pol.tier_for_url("https://arxiv.org/abs/1234.5678") == "low"


def test_low_tier_domains_raise_keep_bar_without_hard_blocking():
    settings = Settings(
        domain_quality_low_domains="dev.to",
        source_quality_min_score=50,
    )
    pol = build_domain_quality_policy(settings=settings)

    assert pol.min_score_threshold_for_url(base_min_score=50, url="https://dev.to/post") == 70
    assert pol.min_score_threshold_for_url(base_min_score=50, url="https://example.com/post") == 50

    assert _should_keep_push_item(
        url="https://dev.to/post",
        source_id=1,
        source_url="https://dev.to/feed/tag/agents",
        active_mute_domains=set(),
        domain_policy=pol,
        min_source_score=50,
        scores_by_source_id={1: 60},
    ) is False

    assert _should_keep_push_item(
        url="https://dev.to/post",
        source_id=1,
        source_url="https://dev.to/feed/tag/agents",
        active_mute_domains=set(),
        domain_policy=pol,
        min_source_score=50,
        scores_by_source_id={1: 74},
    ) is False

    assert _should_keep_push_item(
        url="https://dev.to/post",
        source_id=1,
        source_url="https://dev.to/feed/tag/agents",
        active_mute_domains=set(),
        domain_policy=pol,
        min_source_score=50,
        scores_by_source_id={1: 90},
    ) is True
