from __future__ import annotations

from tracker.openai_compat import _looks_like_responses_required


def test_looks_like_responses_required_matches_plain_json_message():
    body = (
        '{"error":{"message":"Unsupported legacy protocol: /v1/chat/completions is not supported. '
        'Please use /v1/responses.","type":"invalid_request_error"}}'
    )
    assert _looks_like_responses_required(400, body) is True


def test_looks_like_responses_required_matches_escaped_slashes():
    body = (
        '{"error":{"message":"Unsupported legacy protocol: \\/v1\\/chat\\/completions is not supported. '
        'Please use \\/v1\\/responses.","type":"invalid_request_error"}}'
    )
    assert _looks_like_responses_required(400, body) is True


def test_looks_like_responses_required_false_for_other_errors():
    body = '{"error":{"message":"invalid api key","type":"invalid_request_error"}}'
    assert _looks_like_responses_required(400, body) is False

