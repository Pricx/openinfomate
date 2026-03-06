from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_api_topic_crud_and_admin(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    app = create_app(settings)
    client = TestClient(app)

    assert client.get("/health").status_code == 200

    # Token required for management endpoints.
    assert client.get("/topics").status_code == 401

    headers = {"x-tracker-token": "secret"}
    r = client.post(
        "/topics",
        headers=headers,
        json={"name": "T", "query": "ai", "digest_cron": "0 9 * * *", "alert_keywords": "breaking"},
    )
    assert r.status_code == 200

    r = client.get("/topics", headers=headers)
    assert r.status_code == 200
    assert [t["name"] for t in r.json()] == ["T"]

    r = client.post(
        "/sources/hn_search",
        headers=headers,
        json={"query": "gpu", "tags": "story", "hits_per_page": 10, "topic": "T"},
    )
    assert r.status_code == 200
    hn_id = r.json()["id"]

    r = client.post(
        "/sources/searxng_search",
        headers=headers,
        json={"base_url": "http://127.0.0.1:8888", "query": "ai chips", "topic": "T"},
    )
    assert r.status_code == 200
    searx_id = r.json()["id"]

    r = client.post(
        "/sources/discourse",
        headers=headers,
        json={"base_url": "https://forum.example.com", "json_path": "/latest.json", "topic": "T"},
    )
    assert r.status_code == 200
    discourse_id = r.json()["id"]

    r = client.get("/sources", headers=headers)
    assert r.status_code == 200
    assert len(r.json()) == 3

    r = client.get("/bindings", headers=headers)
    assert r.status_code == 200
    assert len(r.json()) == 3

    r = client.patch(f"/bindings/T/{hn_id}", headers=headers, json={"include_keywords": "x"})
    assert r.status_code == 200

    r = client.get("/bindings", headers=headers)
    assert r.status_code == 200
    row = next(b for b in r.json() if b["source_id"] == hn_id)
    assert row["include_keywords"] == "x"

    r = client.delete(f"/bindings/T/{searx_id}", headers=headers)
    assert r.status_code == 200

    r = client.get("/bindings", headers=headers)
    assert r.status_code == 200
    assert {b["source_id"] for b in r.json()} == {hn_id, discourse_id}

    r = client.get("/stats", headers=headers)
    assert r.status_code == 200
    data = r.json()
    assert data["topics_total"] == 1
    assert data["sources_total"] == 3
    assert data["bindings_total"] == 2

    r = client.put(f"/sources/{hn_id}/meta", headers=headers, json={"tags": "hn,ai", "notes": "good"})
    assert r.status_code == 200

    r = client.get(f"/sources/{hn_id}/meta", headers=headers)
    assert r.status_code == 200
    assert r.json()["tags"] == "hn,ai"

    r = client.post("/run/health", headers=headers, params={"push": "false"})
    assert r.status_code == 200
    assert "## Stats" in r.json()["markdown"]

    # Admin page supports token in query string.
    r = client.get("/admin?token=secret")
    assert r.status_code == 200

    # Back-compat: management UI alias.
    assert client.get("/management").status_code == 401
    r = client.get("/management?token=secret")
    assert r.status_code == 200


def test_discourse_linux_do_source_auto_binds_profile(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    app = create_app(settings)
    client = TestClient(app)

    headers = {"x-tracker-token": "secret"}
    r = client.post(
        "/topics",
        headers=headers,
        json={"name": "Profile", "query": "", "digest_cron": "0 9 * * *", "alert_keywords": ""},
    )
    assert r.status_code == 200

    r = client.post(
        "/sources/discourse",
        headers=headers,
        json={"base_url": "https://linux.do", "json_path": "/latest.json"},
    )
    assert r.status_code == 200
    discourse_id = r.json()["id"]

    r = client.get("/bindings", headers=headers)
    assert r.status_code == 200
    rows = [b for b in r.json() if b["source_id"] == discourse_id]
    assert rows == [
        {
            "topic": "Profile",
            "source_id": discourse_id,
            "source_type": "discourse",
            "source_url": "https://linux.do/latest.json",
            "include_keywords": "",
            "exclude_keywords": "",
        }
    ]

    r = client.get("/sources", headers=headers)
    assert r.status_code == 200
    src = next(s for s in r.json() if s["id"] == discourse_id)
    assert src["enabled"] is True
