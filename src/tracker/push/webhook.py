from __future__ import annotations

import httpx


class WebhookPusher:
    def __init__(self, webhook_url: str, *, timeout_seconds: int = 20):
        self.webhook_url = webhook_url
        self.timeout_seconds = timeout_seconds

    async def send_json(self, payload: dict) -> None:
        async with httpx.AsyncClient(timeout=self.timeout_seconds, follow_redirects=True) as client:
            resp = await client.post(self.webhook_url, json=payload)
            resp.raise_for_status()

