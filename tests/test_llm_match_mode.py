from tracker.immediate_alert_rules import ImmediateAlertRule
from tracker.models import Topic
from tracker.pipeline import decide_item_for_topic


def _topic() -> Topic:
    return Topic(name="T", query="irrelevant", alert_keywords="")


def test_llm_mode_ignores_include_keywords_prefilter():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="Some title",
        content_text="Some body",
        include_keywords="mustmatch",
        match_mode="llm",
    )
    assert decision == "candidate"
    assert "include_keywords" not in reason


def test_llm_mode_ignores_exclude_keywords_prefilter():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="Some title",
        content_text="this contains BANWORD",
        exclude_keywords="banword",
        match_mode="llm",
    )
    assert decision == "candidate"
    assert "exclude_keywords" not in reason


def test_keywords_mode_still_applies_include_keywords_prefilter():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="Some title",
        content_text="Some body",
        include_keywords="mustmatch",
        match_mode="keywords",
    )
    assert decision == "ignore"
    assert "include_keywords" in reason


def test_llm_mode_immediate_alert_rule_overrides_candidate():
    topic = _topic()
    decision, reason = decide_item_for_topic(
        topic=topic,
        title="求个冰的邀请码，感谢",
        content_text="Some body",
        canonical_url="https://linux.do/t/topic/1",
        match_mode="llm",
        immediate_alert_rules=(
            ImmediateAlertRule(
                host="linux.do",
                title_all=("邀请码", "冰"),
                reason="matched immediate alert rule: linux.do 邀请码+冰",
            ),
        ),
    )
    assert decision == "alert"
    assert reason == "matched immediate alert rule: linux.do 邀请码+冰"
