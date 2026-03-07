from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from tracker.api import create_app
from tracker.config_agent_core import ConfigAgentApplyResult, ConfigAgentPlanResult
from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings


def test_admin_config_agent_plan_endpoint_returns_preview(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-config-agent-plan.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    async def fake_plan(**_kwargs):
        return ConfigAgentPlanResult(
            run_id=7,
            plan={"actions": [{"op": "mcp.setting.set", "field": "digest_hours", "value": "6"}]},
            warnings=["w1"],
            preview_markdown="# Config Agent Preview\n\n## Settings\n- `digest_hours` -> 6",
        )

    monkeypatch.setattr("tracker.api.plan_config_agent_request", fake_plan)

    client = TestClient(create_app(settings))
    resp = client.post("/admin/config-agent/plan?token=secret", data={"user_prompt": "把参考消息改成 6 小时一次"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["run_id"] == 7
    assert body["warnings"] == ["w1"]
    assert "digest_hours" in body["preview_markdown"]


def test_admin_config_agent_apply_endpoint_executes_run(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-config-agent-apply.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", api_token="secret", env_path=str(env_path))
    app = create_app(settings)
    client = TestClient(app)

    _engine, make_session = session_factory(settings)
    with make_session() as session:
        repo = Repo(session)
        run = repo.add_config_agent_run(
            kind="config_agent_core",
            status="planned",
            user_prompt="set digest hours",
            plan_json=json.dumps({"actions": [{"op": "mcp.setting.set", "field": "digest_hours", "value": "6"}]}, ensure_ascii=False),
            preview_markdown="# preview",
            snapshot_before_json="{}",
        )
        run_id = int(run.id)

    async def fake_apply(*, session, settings, plan, run_id=None):  # noqa: ANN001
        assert plan["actions"][0]["field"] == "digest_hours"
        return ConfigAgentApplyResult(run_id=int(run_id or 0), notes=["settings updated: digest_hours"], warnings=[], restart_required=True)

    monkeypatch.setattr("tracker.api.apply_config_agent_plan", fake_apply)

    resp = client.post("/admin/config-agent/apply?token=secret", data={"run_id": str(run_id)})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["run_id"] == run_id
    assert body["restart_required"] is True
    assert body["notes"] == ["settings updated: digest_hours"]

def test_admin_config_agent_plan_endpoint_passes_conversation_context(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-config-agent-chat-context.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )
    seen: dict[str, str] = {}

    async def fake_plan(**kwargs):
        seen["conversation_history_text"] = str(kwargs.get("conversation_history_text") or "")
        seen["page_context_text"] = str(kwargs.get("page_context_text") or "")
        return ConfigAgentPlanResult(
            run_id=9,
            plan={"actions": [{"op": "mcp.setting.set", "field": "digest_hours", "value": "2"}]},
            warnings=[],
            preview_markdown="# preview",
        )

    monkeypatch.setattr("tracker.api.plan_config_agent_request", fake_plan)

    client = TestClient(create_app(settings))
    resp = client.post(
        "/admin/config-agent/plan?token=secret",
        data={
            "user_prompt": "继续，把刚才的计划应用到当前页面",
            "conversation_json": json.dumps([
                {"role": "user", "text": "帮我先加 linux.do 来源"},
                {"role": "assistant", "text": "我已经拟好一份计划"},
            ], ensure_ascii=False),
            "page_id": "admin:sources",
            "page_section": "sources",
        },
    )
    assert resp.status_code == 200
    assert "RECENT_CONVERSATION_HISTORY" in seen["conversation_history_text"]
    assert "USER: 帮我先加 linux.do 来源" in seen["conversation_history_text"]
    assert "section: sources" in seen["page_context_text"]


def test_admin_config_agent_plan_endpoint_supports_reply_only_turns(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "api-config-agent-reply.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        api_token="secret",
        env_path=str(env_path),
        llm_base_url="https://example.com/v1",
        llm_api_key="sk-test",
        llm_model_reasoning="test-model",
    )

    async def fake_plan(**_kwargs):
        return ConfigAgentPlanResult(
            run_id=0,
            plan={"assistant_reply": "我可以回答配置问题，也可以帮你修改 sources。", "actions": []},
            warnings=[],
            preview_markdown="",
        )

    monkeypatch.setattr("tracker.api.plan_config_agent_request", fake_plan)

    client = TestClient(create_app(settings))
    resp = client.post("/admin/config-agent/plan?token=secret", data={"user_prompt": "你能做哪些事情？"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["run_id"] == 0
    assert body["plan"]["assistant_reply"].startswith("我可以回答配置问题")
    assert body["preview_markdown"] == ""

