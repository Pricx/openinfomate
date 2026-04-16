from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.bridge_contract import BridgeProfileProposeResponse
from tracker.settings import Settings


def test_profile_propose_requires_llm_config(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    r = client.post("/profile/propose?token=secret", json={"text": "x"})
    assert r.status_code == 400
    assert "LLM" in r.json().get("detail", "")


def test_profile_propose_returns_proposal(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        llm_base_url="http://llm.local",
        llm_model="gpt-5.2",
    )
    client = TestClient(create_app(settings))

    async def fake_propose(*, session, settings, payload):  # type: ignore[no-untyped-def]
        assert payload.text
        return BridgeProfileProposeResponse(
            normalized_profile_text="BOOKMARKS\n- https://example.com",
            understanding="You care about LLM agents and recsys; prefer high-signal briefs.",
            ai_prompt="pick only signals",
        )

    monkeypatch.setattr("tracker.api.bridge_profile_propose", fake_propose)

    r = client.post(
        "/profile/propose?token=secret",
        json={
            "text": "<html><body><a href='https://example.com'>Example</a></body></html>",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert "pick only signals" in data["ai_prompt"]
    assert "LLM agents" in data.get("understanding", "")
