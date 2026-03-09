from __future__ import annotations

import json
from pathlib import Path

import pytest

from tracker.config_agent_core import ConfigAgentApplyResult, ConfigAgentPlanResult
from tracker.db import session_factory
from tracker.models import Base, TelegramTask
from tracker.repo import Repo
from tracker.service import _run_telegram_config_agent_worker_job
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


@pytest.mark.asyncio
async def test_telegram_plain_message_queues_config_agent_task(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-plan.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    batches = [[{"update_id": 1, "message": {"message_id": 10, "text": "帮我加入 linux.do 的 codex fast 搜索", "chat": {"id": 123}, "from": {"id": 123}}}]]
    sent: list[str] = []
    created_prompt_ids: list[int] = []

    original_create = Repo.create_telegram_task

    def capture_create(self, **kwargs):  # noqa: ANN001
        created_prompt_ids.append(int(kwargs.get("prompt_message_id") or 0))
        return original_create(self, **kwargs)

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        sent.append(text)
        return 222

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.telegram_connect.Repo.create_telegram_task", capture_create)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        await telegram_poll(repo=repo, settings=settings)

    with make_session() as session:
        repo = Repo(session)
        tasks = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=5)
        assert len(tasks) == 1
        assert tasks[0].status == "pending"
        assert tasks[0].prompt_message_id == 222
        assert "linux.do" in (tasks[0].query or "")
    assert created_prompt_ids == [222]
    assert len(sent) == 1
    assert "已等待 0 秒" in sent[0]
    assert "已加入智能配置队列" not in sent[0]


@pytest.mark.asyncio
async def test_telegram_config_agent_reply_refines_existing_task(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-refine.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    batches = [[{"update_id": 1, "message": {"message_id": 11, "text": "再加上 windows codex 环境排障", "chat": {"id": 123}, "from": {"id": 123}, "reply_to_message": {"message_id": 200}}}]]
    created_prompt_ids: list[int] = []

    original_create = Repo.create_telegram_task

    def capture_create(self, **kwargs):  # noqa: ANN001
        created_prompt_ids.append(int(kwargs.get("prompt_message_id") or 0))
        return original_create(self, **kwargs)

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    sent: list[str] = []

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        sent.append(text)
        return 333

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.telegram_connect.Repo.create_telegram_task", capture_create)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        task = repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="awaiting",
            prompt_message_id=200,
            request_message_id=10,
            query="加入 linux.do 的 codex fast 搜索",
        )
        await telegram_poll(repo=repo, settings=settings)
        session.refresh(task)
        assert task.status == "canceled"

    with make_session() as session:
        repo = Repo(session)
        tasks = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=10)
        pending = [t for t in tasks if t.status == "pending"]
        assert len(pending) == 1
        assert pending[0].prompt_message_id == 333
        assert "加入 linux.do 的 codex fast 搜索" in (pending[0].query or "")
        assert "补充/修订" in (pending[0].query or "")
    assert created_prompt_ids[-1:] == [333]
    assert len(sent) == 1
    assert "已等待 0 秒" in sent[0]
    assert "智能配置修订队列" not in sent[0]


@pytest.mark.asyncio
async def test_telegram_config_agent_callback_moves_task_to_pending_apply(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-apply-callback.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    batches = []
    callback_task_id = 0
    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        task = repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="awaiting",
            prompt_message_id=300,
            request_message_id=12,
            query="set digest hours to 6",
        )
        task.intent = json.dumps({"run_id": 9}, ensure_ascii=False)
        session.commit()
        callback_task_id = int(task.id)
        batches.append([
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": f"cfgag:apply:{callback_task_id}",
                    "message": {"message_id": 300, "chat": {"id": 123}},
                },
            }
        ])

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 20):  # noqa: ANN001, ARG001
        return

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)

    with make_session() as session:
        repo = Repo(session)
        await telegram_poll(repo=repo, settings=settings)

    with make_session() as session:
        row = session.get(TelegramTask, callback_task_id)
        assert row.status == "pending_apply"


@pytest.mark.asyncio
async def test_telegram_config_agent_worker_generates_preview_and_apply(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-worker.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("output_language", "zh")
        repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="pending",
            prompt_message_id=555,
            request_message_id=10,
            query="把参考消息窗口改成 6 小时",
        )

    async def fake_plan(**_kwargs):
        return ConfigAgentPlanResult(
            run_id=21,
            plan={"actions": [{"op": "mcp.setting.set", "field": "digest_hours", "value": "6"}]},
            warnings=["w1"],
            preview_markdown="# Config Agent Preview\n\n## Settings\n- `digest_hours` -> 6",
        )

    edited: list[dict] = []

    async def fake_edit_raw_text(self, *, chat_id: str, message_id: int, text: str, parse_mode: str | None = None, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        edited.append({"chat_id": chat_id, "message_id": message_id, "text": text, "reply_markup": reply_markup})
        return True

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        raise AssertionError("send_raw_text should not be called when placeholder message exists")

    monkeypatch.setattr("tracker.service.plan_config_agent_request", fake_plan)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_raw_text", fake_edit_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await _run_telegram_config_agent_worker_job(make_session, settings)

    with make_session() as session:
        repo = Repo(session)
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        assert task.status == "awaiting"
        assert task.prompt_message_id == 555
        payload = json.loads(task.intent or "{}")
        assert payload["run_id"] == 21
    assert edited and edited[0]["chat_id"] == "123"
    assert any("仍在规划中" in row["text"] for row in edited)
    assert any("智能配置计划已生成" in row["text"] for row in edited)

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
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        task.status = "pending_apply"
        task.intent = json.dumps({"run_id": int(run.id)}, ensure_ascii=False)
        session.commit()

    async def fake_apply(*, session, settings, plan, run_id=None):  # noqa: ANN001
        return ConfigAgentApplyResult(run_id=int(run_id or 0), notes=["settings updated: digest_hours"], warnings=[], restart_required=False)

    monkeypatch.setattr("tracker.service.apply_config_agent_plan", fake_apply)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    await _run_telegram_config_agent_worker_job(make_session, settings)

    with make_session() as session:
        repo = Repo(session)
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        assert task.status == "done"
        assert task.result_key.startswith("config_agent_applied:")
        assert task.prompt_message_id == 555
    assert any("智能配置已应用" in row["text"] for row in edited)


@pytest.mark.asyncio
async def test_telegram_config_agent_worker_handles_reply_only_turns(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-worker-reply.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("output_language", "zh")
        repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=10,
            query="你是谁",
        )

    async def fake_plan(**_kwargs):
        return ConfigAgentPlanResult(
            run_id=0,
            plan={"assistant_reply": "我是 OpenInfoMate 的智能配置助手。", "actions": []},
            warnings=[],
            preview_markdown="",
        )

    sent_text: list[str] = []
    edited: list[str] = []

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        sent_text.append(text)
        return 666

    async def fake_edit_raw_text(self, *, chat_id: str, message_id: int, text: str, parse_mode: str | None = None, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        edited.append(text)
        return True

    monkeypatch.setattr("tracker.service.plan_config_agent_request", fake_plan)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_raw_text", fake_edit_raw_text)

    await _run_telegram_config_agent_worker_job(make_session, settings)

    with make_session() as session:
        repo = Repo(session)
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        assert task.status == "done"
        assert task.result_key == "config_agent_reply"
        assert task.prompt_message_id == 666
    assert len(sent_text) == 1
    assert "仍在规划中" in sent_text[0]
    assert edited == ["我是 OpenInfoMate 的智能配置助手。"]


@pytest.mark.asyncio
async def test_telegram_config_agent_worker_notifies_on_plan_failure(tmp_path, monkeypatch):
    db_path = Path(tmp_path) / "tg-config-worker-fail.db"
    env_path = Path(tmp_path) / ".env"
    settings = Settings(db_url=f"sqlite:///{db_path}", env_path=str(env_path), telegram_bot_token="TEST")
    _engine, make_session = session_factory(settings)
    Base.metadata.create_all(_engine)

    with make_session() as session:
        repo = Repo(session)
        repo.set_app_config("output_language", "zh")
        repo.create_telegram_task(
            chat_id="123",
            user_id="123",
            kind="config_agent",
            status="pending",
            prompt_message_id=-1,
            request_message_id=10,
            query="帮我加一条追踪",
        )

    async def fake_plan(**_kwargs):
        raise RuntimeError("llm unavailable")

    sent_text: list[str] = []
    edited: list[str] = []

    async def fake_send_raw_text(self, *, chat_id: str, text: str, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        sent_text.append(text)
        return 666

    async def fake_edit_raw_text(self, *, chat_id: str, message_id: int, text: str, parse_mode: str | None = None, disable_preview: bool = True, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        edited.append(text)
        return True

    monkeypatch.setattr("tracker.service.plan_config_agent_request", fake_plan)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_raw_text", fake_edit_raw_text)

    await _run_telegram_config_agent_worker_job(make_session, settings)

    with make_session() as session:
        repo = Repo(session)
        task = repo.list_telegram_tasks(chat_id="123", kind="config_agent", limit=1)[0]
        assert task.status == "failed"
        assert task.prompt_message_id == 666
        assert "llm unavailable" in (task.error or "")
    assert len(sent_text) == 1
    assert "仍在规划中" in sent_text[0]
    assert any("计划生成失败" in text for text in edited)
