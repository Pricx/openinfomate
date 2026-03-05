from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.llm import LlmTopicProposal
from tracker.llm import LlmTopicSourceHints
from tracker.settings import Settings


def test_topic_propose_requires_llm_config(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    r = client.post("/topics/propose?token=secret", json={"name": "T", "brief": "b"})
    assert r.status_code == 400
    assert "LLM" in r.json().get("detail", "")


def test_topic_propose_returns_proposal(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
    )
    client = TestClient(create_app(settings))

    async def fake_propose(*, settings, topic_name: str, brief: str, usage_cb=None):  # type: ignore[no-untyped-def]
        assert topic_name
        assert brief
        return LlmTopicProposal(
            topic_name="My Topic",
            query_keywords="a,b,c",
            alert_keywords="zero-day,CVE",
            ai_prompt="pick only signals",
            source_hints=LlmTopicSourceHints(
                add_hn=True,
                add_searxng=True,
                add_discourse=True,
                discourse_base_url="https://forum.example.com",
                discourse_json_path="/latest.json",
                add_nodeseek=True,
            ),
        )

    monkeypatch.setattr("tracker.api.llm_propose_topic_setup", fake_propose)

    r = client.post("/topics/propose?token=secret", json={"name": "", "brief": "track x"})
    assert r.status_code == 200
    data = r.json()
    assert data["topic_name"] == "My Topic"
    assert data["query"] == "a,b,c"
    assert data["alert_keywords"] == "zero-day,CVE"
    assert "pick only signals" in data["ai_prompt"]
    assert data.get("source_hints", {}).get("add_discourse") is True
    assert data.get("source_hints", {}).get("add_nodeseek") is True
