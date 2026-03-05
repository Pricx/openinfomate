from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.connectors.html_list import parse_html_list_url
from tracker.settings import Settings


def test_setup_profile_creates_topic_sources_and_policy(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        llm_curation_enabled=True,
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
    )
    client = TestClient(create_app(settings))

    r = client.get("/setup/profile?token=secret")
    assert r.status_code == 200

    r = client.post(
        "/setup/profile/apply?token=secret",
        data={
            "name": "Profile",
            "digest_cron": "0 9 * * *",
            "add_hn_rss": "true",
            "add_github_trending_daily": "true",
            "github_languages": "python",
            "add_github_trending_monthly": "true",
            "add_arxiv": "true",
            "arxiv_categories": "cs.AI",
            "add_searxng": "true",
            "searxng_base_url": "http://searx.local",
            "add_discourse": "true",
            "discourse_base_url": "https://forum.example.com",
            "discourse_json_path": "/latest.json",
            "add_nodeseek": "true",
            "rss_urls": "https://example.com/feed.xml\n",
            "profile_text": "my bookmarks...",
            "profile_retrieval_queries": "foo\nbar\n",
            "ai_prompt": "pick only signals",
        },
        follow_redirects=False,
    )
    assert r.status_code == 303

    headers = {"x-tracker-token": "secret"}
    topics = client.get("/topics", headers=headers).json()
    assert [t["name"] for t in topics] == ["Profile"]

    policy = client.get("/topics/Profile/policy", headers=headers).json()
    assert policy["llm_curation_enabled"] is True
    assert "pick only signals" in policy["llm_curation_prompt"]

    sources = client.get("/sources", headers=headers).json()
    assert any(s["type"] == "rss" and "news.ycombinator.com/rss" in s["url"] for s in sources)
    html_specs = [parse_html_list_url(s["url"]) for s in sources if s["type"] == "html_list"]
    assert any(spec.page_url == "https://github.com/trending?since=daily" for spec in html_specs)
    assert any(spec.page_url == "https://github.com/trending/python?since=daily" for spec in html_specs)
    assert any(spec.page_url == "https://github.com/trending?since=monthly" for spec in html_specs)
    assert any(spec.page_url == "https://github.com/trending/python?since=monthly" for spec in html_specs)
    assert any(s["type"] == "rss" and "export.arxiv.org/rss/cs.AI" in s["url"] for s in sources)
    assert any(s["type"] == "searxng_search" and "http://searx.local/search" in s["url"] and "q=foo" in s["url"] for s in sources)
    assert any(s["type"] == "searxng_search" and "http://searx.local/search" in s["url"] and "q=bar" in s["url"] for s in sources)
    assert any(s["type"] == "discourse" for s in sources)
    assert any(s["type"] == "rss" and "rss.nodeseek.com" in s["url"] for s in sources)
    assert any(s["type"] == "rss" and "example.com/feed.xml" in s["url"] for s in sources)

    bindings = client.get("/bindings", headers=headers).json()
    assert any(b["topic"] == "Profile" for b in bindings)
