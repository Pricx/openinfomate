from __future__ import annotations

from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.repo import Repo
from tracker.settings import Settings


def test_topic_gate_defaults_api_roundtrip(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    r = client.put(
        "/topic-gates/defaults?token=secret",
        json={
            "candidate_min_score": 65,
            "candidate_convergence": "严格",
            "max_digest_items": 10,
        },
    )
    assert r.status_code == 200
    body = r.json()
    assert body["defaults"]["candidate_min_score"] == 65
    assert body["defaults"]["candidate_convergence"] == "strict"
    assert body["defaults"]["max_digest_items"] == 10

    r = client.get("/topic-gates/defaults?token=secret")
    assert r.status_code == 200
    body = r.json()
    assert body["effective"]["candidate_convergence"] == "strict"


def test_topic_gate_topic_api_returns_defaults_override_effective(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret")
    client = TestClient(create_app(settings))

    with client:
        from tracker.db import session_factory
        from tracker.models import Base

        engine, make_session = session_factory(settings)
        Base.metadata.create_all(engine)
        with make_session() as session:
            repo = Repo(session)
            repo.add_topic(name="Profile", query="")
            repo.patch_topic_gate_defaults({"candidate_min_score": 50, "max_digest_items": 9})

    r = client.put(
        "/topics/Profile/gates?token=secret",
        json={"push_min_score": 85, "candidate_convergence": "balanced"},
    )
    assert r.status_code == 200
    body = r.json()
    assert body["override"]["push_min_score"] == 85
    assert body["effective"]["candidate_min_score"] == 50
    assert body["effective"]["candidate_convergence"] == "balanced"
    assert body["effective"]["max_digest_items"] == 9


def test_admin_topics_page_renders_topic_gate_and_topic_update_persists_override(tmp_path):
    db_path = Path(tmp_path) / "admin-topic-gates.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", admin_password="secret1", api_token="tok")
    client = TestClient(create_app(settings))

    with client:
        from tracker.db import session_factory
        from tracker.models import Base

        engine, make_session = session_factory(settings)
        Base.metadata.create_all(engine)
        with make_session() as session:
            repo = Repo(session)
            repo.add_topic(name="Profile", query="agent")

    resp = client.get("/admin?section=topics&token=tok")
    assert resp.status_code == 200
    assert "Topic Gate defaults" in resp.text
    assert 'name="candidate_min_score"' in resp.text
    assert 'name="push_dedupe_strength"' in resp.text

    resp = client.post(
        "/admin/topic/update",
        params={"token": "tok"},
        data={
            "name": "Profile",
            "query": "agent",
            "digest_cron": "0 9 * * *",
            "alert_keywords": "",
            "alert_cooldown_minutes": "120",
            "alert_daily_cap": "5",
            "candidate_min_score": "55",
            "candidate_convergence": "strict",
            "push_min_score": "80",
            "max_digest_items": "6",
            "max_alert_items": "",
            "push_dedupe_strength": "balanced",
        },
        follow_redirects=False,
    )
    assert resp.status_code in {302, 303}

    with client:
        from tracker.db import session_factory

        _engine, make_session = session_factory(settings)
        with make_session() as session:
            repo = Repo(session)
            topic = repo.get_topic_by_name("Profile")
            assert topic is not None
            described = repo.describe_topic_gate(topic_id=int(topic.id))
            assert described["override"]["candidate_min_score"] == 55
            assert described["override"]["candidate_convergence"] == "strict"
            assert described["override"]["push_min_score"] == 80
            assert described["override"]["max_digest_items"] == 6
            assert described["override"]["push_dedupe_strength"] == "balanced"
