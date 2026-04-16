from __future__ import annotations

import httpx
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



def test_admin_ai_setup_apply_materializes_mcp_source_binding_actions(tmp_path):
    db_path = Path(tmp_path) / "api-mcp.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        repo.add_topic(name="AI Agents", query="codex fast claude code", digest_cron="0 9 * * *")
        before = export_tracking_snapshot(session=session)
        plan = {
            "actions": [
                {
                    "op": "mcp.source_binding.ensure",
                    "intent": "search",
                    "source_type": "searxng_search",
                    "site": "linux.do",
                    "query": "codex fast",
                    "topic": "__auto__",
                }
            ]
        }
        run = repo.add_config_agent_run(
            kind="tracking_ai_setup",
            status="planned",
            user_prompt="add codex fast search on linux.do",
            plan_json=json.dumps(plan, ensure_ascii=False),
            snapshot_before_json=json.dumps(before, ensure_ascii=False),
            snapshot_preview_json="",
            snapshot_after_json="",
        )
        run_id = int(run.id)

    r = client.post("/admin/ai-setup/apply?token=secret", data={"run_id": str(run_id)})
    assert r.status_code == 200
    assert r.json().get("ok") is True

    with make_session() as session:
        repo = Repo(session)
        sources = [s for s in repo.list_sources() if s.type == "searxng_search"]
        assert len(sources) == 1
        assert "site%3Alinux.do+codex+fast" in sources[0].url
        assert any(b[0].name == "AI Agents" and b[1].url == sources[0].url for b in repo.list_topic_sources())



def test_admin_ai_setup_plan_materializes_mcp_actions_in_preview(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-plan-mcp.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    async def _fake_plan(**_kwargs):
        return (
            {
                "actions": [
                    {
                        "op": "mcp.source_binding.ensure",
                        "intent": "site_stream",
                        "source_type": "discourse",
                        "site": "linux.do",
                        "topic": "__auto__",
                    }
                ]
            },
            [],
        )

    import tracker.api as tracker_api

    monkeypatch.setattr(tracker_api, "llm_plan_tracking_ai_setup", _fake_plan)

    app = create_app(settings)
    client = TestClient(app)

    r = client.post("/admin/ai-setup/plan?token=secret", data={"user_prompt": "我要加入 linux do 的 rss"})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    ops = [a.get("op") for a in (body.get("plan") or {}).get("actions") or []]
    assert "source.add_discourse" in ops
    assert "(no source changes)" not in str(body.get("preview_markdown") or "")
    assert "linux.do" in str(body.get("preview_markdown") or "")


def test_admin_ai_setup_apply_returns_error_when_run_finalize_persist_fails(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-finalize-fail.db"
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
                {"op": "topic.upsert", "name": "T1", "query": "codex fast", "enabled": True},
            ]
        }
        run = repo.add_config_agent_run(
            kind="tracking_ai_setup",
            status="planned",
            user_prompt="add a topic",
            plan_json=json.dumps(plan, ensure_ascii=False),
            snapshot_before_json=json.dumps(before, ensure_ascii=False),
            snapshot_preview_json="",
            snapshot_after_json="",
        )
        run_id = int(run.id)

    orig_update = Repo.update_config_agent_run

    def fake_update(self, run_id: int, *, status=None, snapshot_after_json=None, error=None):
        if status == "applied":
            raise RuntimeError("finalize boom")
        return orig_update(self, run_id, status=status, snapshot_after_json=snapshot_after_json, error=error)

    monkeypatch.setattr(Repo, "update_config_agent_run", fake_update)

    r = client.post("/admin/ai-setup/apply?token=secret", data={"run_id": str(run_id)})
    assert r.status_code == 500
    body = r.json()
    assert body.get("ok") is False
    assert body.get("error") == "apply_finalize_failed"
    assert "plan applied" in str(body.get("message") or "")

    with make_session() as session:
        repo = Repo(session)
        assert repo.get_topic_by_name("T1") is not None
        row = repo.get_config_agent_run(run_id)
        assert row is not None
        assert row.status == "planned"
        assert row.snapshot_after_json == ""


def test_admin_ai_setup_plan_returns_error_when_run_persist_fails(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-plan-persist-fail.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    async def _fake_plan(**_kwargs):
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "AI Agents", "query": "codex fast", "enabled": True},
                ]
            },
            [],
        )

    import tracker.api as tracker_api

    monkeypatch.setattr(tracker_api, "llm_plan_tracking_ai_setup", _fake_plan)

    orig_add = Repo.add_config_agent_run

    def fake_add(self, **kwargs):
        if kwargs.get("status") == "planned":
            raise RuntimeError("persist boom")
        return orig_add(self, **kwargs)

    monkeypatch.setattr(Repo, "add_config_agent_run", fake_add)

    app = create_app(settings)
    client = TestClient(app)

    r = client.post("/admin/ai-setup/plan?token=secret", data={"user_prompt": "我要加入 linux do 的 rss"})
    assert r.status_code == 500
    body = r.json()
    assert body.get("ok") is False
    assert body.get("error") == "plan_persist_failed"
    assert "AI Agents" in str(body.get("preview_markdown") or "")


def test_admin_ai_setup_plan_structured_profile_brief_still_calls_llm_planner(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-structured-brief.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    planned_calls: list[str] = []

    async def _fake_plan(**kwargs):
        planned_calls.append(str(kwargs.get("user_prompt") or ""))
        return (
            {
                "actions": [
                    {"op": "topic.upsert", "name": "Profile", "query": "agent infra", "enabled": True},
                ]
            },
            [],
        )

    import tracker.api as tracker_api

    monkeypatch.setattr(tracker_api, "llm_plan_tracking_ai_setup", _fake_plan)

    app = create_app(settings)
    client = TestClient(app)

    structured_brief = "\n".join([
        "UNDERSTANDING:",
        "关注 agent infra / workflow automation。",
        "",
        "INTEREST_AXES:",
        "- Agent infra",
        "- Workflow automation",
        "",
        "SEED_QUERIES:",
        "- agent infra observability",
        "- workflow automation orchestration",
    ])

    r = client.post("/admin/ai-setup/plan?token=secret", data={"user_prompt": structured_brief})
    assert r.status_code == 200
    body = r.json()
    assert body.get("ok") is True
    assert planned_calls == [structured_brief]
    ops = [a.get("op") for a in (body.get("plan") or {}).get("actions") or []]
    assert "topic.upsert" in ops
    assert all("expanded deterministically" not in str(item) for item in (body.get("warnings") or []))


def test_admin_ai_setup_plan_returns_explicit_timeout_error_without_deterministic_fallback(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-plan-timeout.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    async def _fake_plan(**_kwargs):
        raise httpx.ReadTimeout("planner timed out")

    import tracker.api as tracker_api

    monkeypatch.setattr(tracker_api, "llm_plan_tracking_ai_setup", _fake_plan)

    app = create_app(settings)
    client = TestClient(app)

    structured_brief = "\n".join([
        "INTEREST_AXES:",
        "- Agent infra",
        "",
        "SEED_QUERIES:",
        "- agent infra observability",
    ])

    r = client.post("/admin/ai-setup/plan?token=secret", data={"user_prompt": structured_brief})
    assert r.status_code == 504
    body = r.json()
    assert body.get("ok") is False
    assert body.get("error") == "plan_timed_out"
    assert "timed out" in str(body.get("message") or "")
