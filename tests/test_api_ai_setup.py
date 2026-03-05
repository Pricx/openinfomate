from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.config_agent import export_tracking_snapshot
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_admin_ai_setup_apply_undo_and_baseline_restore(tmp_path):
    db_path = Path(tmp_path) / "api.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        before = export_tracking_snapshot(session=session)
        plan = {
            "actions": [
                {"op": "topic.upsert", "name": "T1", "query": "mcp report synthesis", "enabled": True},
                {
                    "op": "source.add_rss",
                    "url": "https://example.com/feed.xml",
                    "bind": {"topic": "T1", "include_keywords": "", "exclude_keywords": ""},
                },
            ]
        }
        run = repo.add_config_agent_run(
            kind="tracking_ai_setup",
            status="planned",
            user_prompt="add a topic and bind an rss",
            plan_json=json.dumps(plan, ensure_ascii=False),
            snapshot_before_json=json.dumps(before, ensure_ascii=False),
            snapshot_preview_json="",
            snapshot_after_json="",
        )
        run_id = int(run.id)

    # Apply the planned run.
    r = client.post("/admin/ai-setup/apply?token=secret", data={"run_id": str(run_id)})
    assert r.status_code == 200
    assert r.json().get("ok") is True

    with make_session() as session:
        repo = Repo(session)
        t1 = repo.get_topic_by_name("T1")
        assert t1 and t1.enabled is True
        src = repo.get_source(type="rss", url="https://example.com/feed.xml")
        assert src and src.enabled is True
        assert any(b[0].name == "T1" and b[1].type == "rss" for b in repo.list_topic_sources())

    # Undo should restore snapshot_before (empty) by disabling the added topic/source and removing bindings.
    r = client.post("/admin/ai-setup/undo?token=secret", data={})
    assert r.status_code == 200
    assert r.json().get("ok") is True

    with make_session() as session:
        repo = Repo(session)
        t1 = repo.get_topic_by_name("T1")
        assert t1 and t1.enabled is False
        src = repo.get_source(type="rss", url="https://example.com/feed.xml")
        assert src and src.enabled is False
        assert repo.list_topic_sources() == []

    # Capture baseline, then add another topic, then restore baseline.
    r = client.post("/admin/ai-setup/baseline/set?token=secret", data={})
    assert r.status_code == 200
    assert r.json().get("ok") is True

    with make_session() as session:
        repo = Repo(session)
        repo.add_topic(name="T2", query="x", digest_cron="0 9 * * *")

    r = client.post("/admin/ai-setup/baseline/restore?token=secret", data={})
    assert r.status_code == 200
    assert r.json().get("ok") is True

    with make_session() as session:
        repo = Repo(session)
        t2 = repo.get_topic_by_name("T2")
        assert t2 and t2.enabled is False
