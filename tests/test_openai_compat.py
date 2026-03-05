from __future__ import annotations

from tracker.openai_compat import _looks_like_responses_required
from tracker.openai_compat import post_openai_compat_json


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


def test_post_openai_compat_json_falls_back_to_responses_without_hint():
    import asyncio

    import httpx

    calls: list[str] = []

    class FakeClient:
        async def post(self, url: str, headers: dict, json: dict):  # noqa: A002
            calls.append(url)
            req = httpx.Request("POST", url)
            if url.endswith("/v1/chat/completions"):
                # No "responses" hint here: fallback should still try /v1/responses.
                return httpx.Response(
                    400,
                    request=req,
                    json={"error": {"message": "legacy protocol unsupported"}},
                )
            if url.endswith("/v1/responses"):
                return httpx.Response(200, request=req, json={"output_text": "pong"})
            return httpx.Response(404, request=req, json={"error": {"message": "not found"}})

    async def _run():
        data, mode = await post_openai_compat_json(
            repo=None,
            client=FakeClient(),  # type: ignore[arg-type]
            base_url="https://example.com/v1",
            headers={},
            payload_chat={
                "model": "m",
                "messages": [{"role": "user", "content": "ping"}],
                "temperature": 0,
                "max_tokens": 8,
            },
        )
        return data, mode

    data, mode = asyncio.run(_run())
    assert mode == "responses"
    assert data.get("output_text") == "pong"
    assert any(u.endswith("/v1/chat/completions") for u in calls)
    assert any(u.endswith("/v1/responses") for u in calls)
