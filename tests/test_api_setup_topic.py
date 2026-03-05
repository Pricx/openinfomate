from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.settings import Settings


def test_setup_topic_creates_topic_sources_and_policy(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    r = client.get("/setup/topic?token=secret")
    assert r.status_code == 200

    r = client.post(
        "/setup/topic/apply?token=secret",
        data={
            "name": "T",
            "query": "gpu",
            "digest_cron": "0 9 * * *",
            "alert_keywords": "",
            "add_hn": "true",
            "add_searxng": "true",
            "searxng_base_url": "http://127.0.0.1:8888",
            "add_discourse": "true",
            "discourse_base_url": "https://forum.example.com",
            "discourse_json_path": "/latest.json",
            "add_nodeseek": "true",
            "rss_urls": "https://example.com/feed.xml\n",
            "ai_enabled": "true",
            "ai_prompt": "pick only signals",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    headers = {"x-tracker-token": "secret"}
    topics = client.get("/topics", headers=headers).json()
    assert [t["name"] for t in topics] == ["T"]

    policy = client.get("/topics/T/policy", headers=headers).json()
    assert policy["llm_curation_enabled"] is True
    assert "pick only signals" in policy["llm_curation_prompt"]

    sources = client.get("/sources", headers=headers).json()
    assert any(s["type"] == "hn_search" for s in sources)
    assert any(s["type"] == "searxng_search" for s in sources)
    assert any(s["type"] == "discourse" for s in sources)
    assert any(s["type"] == "rss" and "rss.nodeseek.com" in s["url"] for s in sources)
    assert any(s["type"] == "rss" and "example.com/feed.xml" in s["url"] for s in sources)

    bindings = client.get("/bindings", headers=headers).json()
    assert any(b["topic"] == "T" for b in bindings)
