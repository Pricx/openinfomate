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
