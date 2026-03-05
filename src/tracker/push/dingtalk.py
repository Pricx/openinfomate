from __future__ import annotations

import base64
import hashlib
import hmac
import time
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx


def build_signed_webhook_url(*, webhook_url: str, secret: str, timestamp_ms: int | None = None) -> str:
    ts = int(time.time() * 1000) if timestamp_ms is None else int(timestamp_ms)
    string_to_sign = f"{ts}\n{secret}".encode("utf-8")
    key = secret.encode("utf-8")
    signature = hmac.new(key, string_to_sign, digestmod=hashlib.sha256).digest()
    sign_b64 = base64.b64encode(signature).decode("utf-8")

    parts = urlsplit(webhook_url)
    params = dict(parse_qsl(parts.query, keep_blank_values=True))
    params["timestamp"] = str(ts)
    params["sign"] = sign_b64
    new_query = urlencode(params)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, new_query, parts.fragment))


class DingTalkPusher:
    def __init__(self, webhook_url: str, *, secret: str | None = None, timeout_seconds: int = 20):
        self.webhook_url = webhook_url
        self.secret = secret
        self.timeout_seconds = timeout_seconds

    async def send_markdown(self, *, title: str, markdown: str) -> None:
        url = (
            build_signed_webhook_url(webhook_url=self.webhook_url, secret=self.secret)
            if self.secret
            else self.webhook_url
        )
        payload = {"msgtype": "markdown", "markdown": {"title": title, "text": markdown}}
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
            # DingTalk often returns HTTP 200 with an application-level error payload.
            try:
                data = resp.json()
            except Exception:
                return
            if isinstance(data, dict) and "errcode" in data:
                errcode = data.get("errcode")
                if errcode not in (0, "0"):
                    errmsg = data.get("errmsg") or ""
                    raise RuntimeError(f"dingtalk webhook error: errcode={errcode} errmsg={errmsg}")
