from __future__ import annotations

import base64
import hashlib
import hmac
from urllib.parse import parse_qsl, urlsplit

from tracker.push.dingtalk import build_signed_webhook_url


def test_build_signed_webhook_url_preserves_access_token_and_adds_sign():
    url = "https://oapi.dingtalk.com/robot/send?access_token=abc"
    secret = "SEC123"
    timestamp_ms = 1700000000000

    out = build_signed_webhook_url(webhook_url=url, secret=secret, timestamp_ms=timestamp_ms)

    parts = urlsplit(out)
    params = dict(parse_qsl(parts.query))
    assert params["access_token"] == "abc"
    assert params["timestamp"] == str(timestamp_ms)

    string_to_sign = f"{timestamp_ms}\n{secret}".encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), string_to_sign, digestmod=hashlib.sha256).digest()
    expected_sign = base64.b64encode(sig).decode("utf-8")
    assert params["sign"] == expected_sign

