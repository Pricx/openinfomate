from __future__ import annotations

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_lang_endpoint_sets_cookie_and_redirects(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_token="secret")
    client = TestClient(create_app(settings))

    resp = client.get("/lang?lang=zh&next=/admin", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin"
    assert "tracker_lang=zh" in (resp.headers.get("set-cookie") or "")


def test_lang_endpoint_sanitizes_next(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_token="secret")
    client = TestClient(create_app(settings))

    resp = client.get("/lang?lang=zh&next=https://evil.example/", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/admin"


def test_accept_language_renders_zh(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_token="secret")
    client = TestClient(create_app(settings))

    resp = client.get(
        "/admin?token=secret",
        headers={"accept-language": "zh-CN,zh;q=0.9,en;q=0.8"},
    )
    assert resp.status_code == 200
    assert "OpenInfoMate 管理端" in resp.text
    assert 'id="trackerLang"' in resp.text
