from tracker.pipeline import decide_item_for_topic
from tracker.models import Topic


def _topic() -> Topic:
    return Topic(name="T", query="ai,agent", alert_keywords="breaking")


def test_decide_item_filters_by_exclude_domains_suffix_match():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="AI agent news",
        content_text="something",
        canonical_url="https://www.facebook.com/some/page",
        exclude_domains="facebook.com",
    )
    assert decision == "ignore"
    assert "exclude_domains" in reason


def test_decide_item_filters_by_include_domains_allowlist():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="AI agent news",
        content_text="something",
        canonical_url="https://example.com/post",
        include_domains="news.ycombinator.com,hnrss.org",
    )
    assert decision == "ignore"
    assert "include_domains" in reason


def test_decide_item_allows_when_domain_matches_include_allowlist():
    topic = _topic()
    decision, _reason = decide_item_for_topic(
        topic=topic,
        title="AI agent breaking",
        content_text="something",
        canonical_url="https://hnrss.org/newest?q=agent",
        include_domains="news.ycombinator.com,hnrss.org",
    )
    assert decision == "alert"


def test_decide_item_ignores_domainless_urls():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="AI agent breaking",
        content_text="something",
        canonical_url="file:///tmp/x.html",
        exclude_domains="facebook.com",
    )
    assert decision in {"alert", "digest", "ignore"}
    assert "domain" not in reason.lower()

