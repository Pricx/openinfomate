from __future__ import annotations

from tracker.runner import _format_alert_markdown, _format_alert_text


def test_alert_markdown_prefers_llm_fields_when_present():
    md = _format_alert_markdown(
        topic_name="T",
        title="Title",
        url="https://example.com/x",
        reason="llm_summary: s\nllm_why: w\n",
    )
    assert "# Alert: T" in md
    assert "- s" in md
    assert "Reason:" not in md


def test_alert_markdown_falls_back_to_reason_when_no_llm_fields():
    md = _format_alert_markdown(
        topic_name="T",
        title="Title",
        url="https://example.com/x",
        reason="plain reason",
    )
    assert "Reason: plain reason" in md


def test_alert_text_prefers_llm_fields_when_present():
    text = _format_alert_text(
        title="Title",
        url="https://example.com/x",
        reason="llm_summary: s\nllm_why: w\n",
    )
    assert "s" in text
    assert "Reason:" not in text
