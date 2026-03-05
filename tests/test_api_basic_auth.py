from __future__ import annotations

import base64

import pytest
from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def _basic_auth_header(user: str, password: str) -> dict[str, str]:
    token = base64.b64encode(f"{user}:{password}".encode("utf-8")).decode("ascii")
    return {"authorization": f"Basic {token}"}


def test_api_refuses_public_bind_without_auth(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", api_host="0.0.0.0")
    with pytest.raises(RuntimeError):
        create_app(settings)


def test_api_basic_auth_required_when_configured(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", admin_password="pw")
    client = TestClient(create_app(settings))

    resp = client.get("/doctor")
    assert resp.status_code == 401


def test_api_basic_auth_allows_access(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", admin_username="admin", admin_password="pw")
    client = TestClient(create_app(settings))

    resp = client.get("/doctor", headers=_basic_auth_header("admin", "pw"))
    assert resp.status_code == 200


def test_api_basic_auth_wrong_password_denied(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", admin_password="pw")
    client = TestClient(create_app(settings))

    resp = client.get("/doctor", headers=_basic_auth_header("admin", "wrong"))
    assert resp.status_code == 401


def test_api_basic_auth_allows_unicode_password(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/api.db", admin_username="admin", admin_password="中文密码")
    client = TestClient(create_app(settings))

    resp = client.get("/doctor", headers=_basic_auth_header("admin", "中文密码"))
    assert resp.status_code == 200
