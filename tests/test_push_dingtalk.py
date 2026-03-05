from __future__ import annotations

import asyncio

import httpx
import pytest

from tracker.push.dingtalk import DingTalkPusher


def test_dingtalk_pusher_accepts_ok_payload(monkeypatch):
    async def fake_post(self: httpx.AsyncClient, url: str, json: dict):
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"errcode": 0, "errmsg": "ok"}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    pusher = DingTalkPusher("https://oapi.dingtalk.com/robot/send?access_token=example")
    asyncio.run(pusher.send_markdown(title="t", markdown="m"))


def test_dingtalk_pusher_raises_on_errcode(monkeypatch):
    async def fake_post(self: httpx.AsyncClient, url: str, json: dict):
        req = httpx.Request("POST", url)
        return httpx.Response(200, json={"errcode": 310000, "errmsg": "bad"}, request=req)

    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post)

    pusher = DingTalkPusher("https://oapi.dingtalk.com/robot/send?access_token=example")
    with pytest.raises(RuntimeError) as exc:
        asyncio.run(pusher.send_markdown(title="t", markdown="m"))
    assert "errcode=310000" in str(exc.value)
