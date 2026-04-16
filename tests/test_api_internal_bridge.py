from __future__ import annotations

from pathlib import Path

import httpx
from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.bridge_contract import (
    BRIDGE_CONTRACT_VERSION,
    BridgeConfigPlanResponse,
    BridgeProfileProposeResponse,
    BridgeTrackingPlanResponse,
)
from tracker.settings import Settings


def test_internal_bridge_meta_exposes_exact_contract_version(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", admin_password="pw")
    client = TestClient(create_app(settings))

    response = client.get("/internal/bridge/v1/meta")

    assert response.status_code == 200
    assert response.json()["contract_version"] == BRIDGE_CONTRACT_VERSION


def test_internal_bridge_profile_propose_returns_public_contract(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    async def fake_profile(*, session, settings, payload):  # type: ignore[no-untyped-def]
        assert payload.text == "bookmarks html"
        return BridgeProfileProposeResponse(
            normalized_profile_text="BOOKMARKS\n- https://example.com",
            understanding="agent tooling summary",
            interest_axes=["Agent Infra"],
            interest_keywords=["agents"],
            retrieval_queries=["agent infra"],
            ai_prompt="focus on agent tooling",
        )

    monkeypatch.setattr("tracker.api.bridge_profile_propose", fake_profile)

    response = client.post("/internal/bridge/v1/profile/propose", json={"text": "bookmarks html"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["normalized_profile_text"].startswith("BOOKMARKS")
    assert payload["understanding"] == "agent tooling summary"


def test_internal_bridge_tracking_plan_returns_actions(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    async def fake_tracking(*, session, settings, payload):  # type: ignore[no-untyped-def]
        assert payload.profile_understanding == "agent infra summary"
        assert payload.profile_interest_axes == ["Agent Infra"]
        assert payload.profile_interest_keywords == ["agents"]
        assert payload.profile_retrieval_queries == ["agent infra"]
        return BridgeTrackingPlanResponse(
            normalized_profile_text="agent infra",
            understanding="agent infra summary",
            interest_axes=["Agent Infra"],
            interest_keywords=["agents"],
            retrieval_queries=["agent infra"],
            ai_prompt="focus",
            input_brief="SMART_CONFIG_INPUT",
            warnings=[],
            actions=[
                {
                    "op": "topic.upsert",
                    "name": "Agent Infra",
                    "query": "agent infra",
                    "enabled": True,
                }
            ],
        )

    monkeypatch.setattr("tracker.api.bridge_tracking_plan", fake_tracking)

    response = client.post(
        "/internal/bridge/v1/tracking/plan",
        json={
            "text": "agent infra bookmarks",
            "profile_understanding": "agent infra summary",
            "profile_interest_axes": ["Agent Infra"],
            "profile_interest_keywords": ["agents"],
            "profile_retrieval_queries": ["agent infra"],
            "tracking_snapshot": {"topics": [], "sources": [], "bindings": []},
        },
    )

    assert response.status_code == 200
    assert response.json()["actions"][0]["op"] == "topic.upsert"


def test_internal_bridge_tracking_plan_returns_structured_timeout(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    async def fake_tracking(*, session, settings, payload):  # type: ignore[no-untyped-def]
        raise httpx.ReadTimeout("planner timed out")

    monkeypatch.setattr("tracker.api.bridge_tracking_plan", fake_tracking)

    response = client.post(
        "/internal/bridge/v1/tracking/plan",
        json={
            "text": "agent infra bookmarks",
            "tracking_snapshot": {"topics": [], "sources": [], "bindings": []},
        },
    )

    assert response.status_code == 504
    assert response.json()["detail"] == "upstream core tracking planner timed out"


def test_internal_bridge_config_plan_returns_questions_without_auth_token(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", admin_password="pw")
    client = TestClient(create_app(settings))

    async def fake_config(*, session, settings, payload):  # type: ignore[no-untyped-def]
        assert payload.user_prompt == "帮我补一个 RSS"
        return BridgeConfigPlanResponse(
            assistant_reply="我需要一个明确的 RSS 地址。",
            summary="Need RSS URL",
            questions=["请直接贴出 RSS 或站点 URL。"],
            warnings=[],
            actions=[],
        )

    monkeypatch.setattr("tracker.api.bridge_config_plan", fake_config)

    response = client.post(
        "/internal/bridge/v1/config/plan",
        json={
            "user_prompt": "帮我补一个 RSS",
            "profile_text": "agent infra",
            "tracking_snapshot": {"topics": [], "sources": [], "bindings": []},
        },
    )

    assert response.status_code == 200
    assert response.json()["questions"] == ["请直接贴出 RSS 或站点 URL。"]
