from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_admin_topic_update_and_source_toggle(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    headers = {"x-tracker-token": "secret"}
    r = client.post(
        "/topics",
        headers=headers,
        json={"name": "T", "query": "ai", "digest_cron": "0 9 * * *", "alert_keywords": ""},
    )
    assert r.status_code == 200

    r = client.post(
        "/sources/hn_search",
        headers=headers,
        json={"query": "gpu", "tags": "story", "hits_per_page": 10, "topic": "T"},
    )
    assert r.status_code == 200
    source_id = r.json()["id"]

    r = client.post(
        "/admin/topic/update?token=secret&section=topics",
        data={
            "name": "T",
            "query": "ai2",
            "digest_cron": "0 8 * * *",
            "alert_keywords": "breaking",
            "alert_cooldown_minutes": "60",
            "alert_daily_cap": "2",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "section=topics" in (r.headers.get("location") or "")

    r = client.get("/topics", headers=headers)
    assert r.status_code == 200
    t = r.json()[0]
    assert t["query"] == "ai2"
    assert t["digest_cron"] == "0 8 * * *"
    assert t["alert_keywords"] == "breaking"
    assert t["alert_cooldown_minutes"] == 60
    assert t["alert_daily_cap"] == 2

    r = client.post(
        "/admin/source/toggle?token=secret&section=sources",
        data={"source_id": str(source_id), "enabled": "false"},
        follow_redirects=False,
    )
    assert r.status_code == 303
    assert "section=sources" in (r.headers.get("location") or "")

    r = client.get("/sources", headers=headers)
    assert r.status_code == 200
    row = next(s for s in r.json() if s["id"] == source_id)
    assert row["enabled"] is False
