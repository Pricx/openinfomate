from __future__ import annotations

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.db import session_factory
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import _topic_gate_candidate_runtime
from tracker.settings import Settings
from tracker.topic_gate_config import TopicGateConfig, merge_topic_gate_configs


def test_topic_gate_repo_resolves_defaults_and_override(tmp_path):
    settings = Settings(db_url=f"sqlite:///{tmp_path}/repo.db")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as session:
        repo = Repo(session)
        topic = repo.add_topic(name="Profile", query="")
        repo.set_topic_gate_defaults(
            TopicGateConfig(
                candidate_min_score=55,
                candidate_convergence="balanced",
                push_min_score=70,
                max_digest_items=12,
                push_dedupe_strength="balanced",
            )
        )
        repo.upsert_topic_gate_policy(
            topic_id=int(topic.id),
            config=TopicGateConfig(
                max_digest_items=5,
                max_alert_items=2,
                push_dedupe_strength="strict",
            ),
        )

        defaults = repo.get_topic_gate_defaults()
        override = repo.get_topic_gate_override(topic_id=int(topic.id))
        effective = repo.get_effective_topic_gate_config(topic_id=int(topic.id))
        described = repo.describe_topic_gate(topic_id=int(topic.id))

        assert defaults.candidate_min_score == 55
        assert override.max_digest_items == 5
        assert override.max_alert_items == 2
        assert effective.candidate_min_score == 55
        assert effective.candidate_convergence == "balanced"
        assert effective.push_min_score == 70
        assert effective.max_digest_items == 5
        assert effective.max_alert_items == 2
        assert effective.push_dedupe_strength == "strict"
        assert described["inherits"]["candidate_convergence"] is True
        assert described["inherits"]["max_digest_items"] is False


def test_merge_topic_gate_configs_prefers_override_when_present():
    merged = merge_topic_gate_configs(
        defaults=TopicGateConfig(
            candidate_min_score=60,
            candidate_convergence="balanced",
            max_digest_items=10,
            push_dedupe_strength="balanced",
        ),
        override=TopicGateConfig(
            candidate_convergence="strict",
            max_alert_items=2,
        ),
    )

    assert merged.candidate_min_score == 60
    assert merged.candidate_convergence == "strict"
    assert merged.max_digest_items == 10
    assert merged.max_alert_items == 2
    assert merged.push_dedupe_strength == "balanced"


def test_topic_gate_candidate_runtime_loose_disables_internal_caps():
    runtime = _topic_gate_candidate_runtime(
        settings=Settings(
            llm_curation_max_candidates=30,
            llm_curation_triage_enabled=True,
            llm_curation_history_dedupe_days=7,
            llm_curation_input_dedupe_enabled=True,
            llm_model_mini="mini-model",
        ),
        gate=TopicGateConfig(candidate_convergence="loose"),
    )

    assert runtime["max_candidates"] == 0
    assert runtime["triage_enabled"] is False
    assert runtime["input_dedupe_enabled"] is False
    assert runtime["history_dedupe_days"] == 0


def test_api_topic_gate_defaults_and_override_roundtrip(tmp_path):
    settings = Settings(
        db_url=f"sqlite:///{tmp_path}/api.db",
        api_token="secret",
    )
    client = TestClient(create_app(settings))
    headers = {"x-tracker-token": "secret"}

    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)
    with make_session() as session:
        repo = Repo(session)
        repo.add_topic(name="Profile", query="")

    resp = client.put(
        "/config/topic-gates/defaults",
        headers=headers,
        json={
            "candidate_min_score": 50,
            "candidate_convergence": "balanced",
            "push_min_score": 75,
            "max_digest_items": 8,
            "push_dedupe_strength": "balanced",
        },
    )
    assert resp.status_code == 200
    assert resp.json()["defaults"]["push_min_score"] == 75

    resp = client.put(
        "/topics/Profile/gate-config",
        headers=headers,
        json={
            "candidate_convergence": "strict",
            "max_alert_items": 1,
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["override"]["candidate_convergence"] == "strict"
    assert data["effective"]["candidate_min_score"] == 50
    assert data["effective"]["candidate_convergence"] == "strict"
    assert data["effective"]["push_min_score"] == 75
    assert data["effective"]["max_digest_items"] == 8
    assert data["effective"]["max_alert_items"] == 1
    assert data["inherits"]["candidate_convergence"] is False
    assert data["inherits"]["candidate_min_score"] is True

    resp = client.put("/topics/Profile/gate-config", headers=headers, json={})
    assert resp.status_code == 200
    data = resp.json()
    assert data["override"] == {
        "candidate_min_score": None,
        "candidate_convergence": None,
        "push_min_score": None,
        "max_digest_items": None,
        "max_alert_items": None,
        "push_dedupe_strength": None,
    }
    assert data["effective"]["candidate_min_score"] == 50
    assert data["effective"]["candidate_convergence"] == "balanced"
    assert data["effective"]["max_digest_items"] == 8
