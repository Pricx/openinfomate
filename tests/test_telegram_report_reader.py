from __future__ import annotations

import asyncio
import datetime as dt
from contextlib import contextmanager

import httpx
import pytest

from tracker.db import session_factory
from tracker.models import Base
from tracker.push_dispatch import push_telegram_report_reader
from tracker.repo import Repo
from tracker.runner import CuratedInfoResult
from tracker.settings import Settings
from tracker.telegram_report_reader import render_cover_html, render_section_html
from tracker.telegram_connect import (
    _answer_callback_query_best_effort,
    _telegram_poll_client_timeout_seconds,
    _wait_for_reader_nav_idle_for_tests,
    telegram_get_updates,
    telegram_poll,
)


_SAMPLE_MD = """
# 参考消息

## 重点摘要
1. A
2. B
3. C

## 版图与分类法
- X
- Y

References:
[1] Example — https://example.com
""".strip()


@pytest.fixture(autouse=True)
def _stub_runtime_effective_settings(monkeypatch):
    def _passthrough(*, repo, settings):  # noqa: ARG001
        return settings

    monkeypatch.setattr("tracker.dynamic_config.effective_settings", _passthrough)


@pytest.mark.asyncio
async def test_push_telegram_report_reader_sends_cover_and_records_mapping(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("output_language", "zh")

    settings = Settings(telegram_bot_token="TEST")

    sent: list[dict] = []

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> int:
        sent.append({"chat_id": chat_id, "text": text, "parse_mode": parse_mode, "reply_markup": reply_markup})
        return 200

    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)

    ok = await push_telegram_report_reader(
        repo=repo,
        settings=settings,
        idempotency_key="digest:1:2026-02-26:0900",
        markdown=_SAMPLE_MD,
    )
    assert ok is True
    assert sent and sent[0]["chat_id"] == "123"
    assert sent[0]["parse_mode"] == "HTML"
    assert "<b>" in str(sent[0]["text"] or "")
    assert isinstance(sent[0]["reply_markup"], dict)
    assert "inline_keyboard" in sent[0]["reply_markup"]

    msg = repo.get_telegram_message(chat_id="123", message_id=200)
    assert msg is not None
    assert (msg.idempotency_key or "").startswith("digest:")


@pytest.mark.asyncio
async def test_telegram_report_reader_callback_edits_digest_in_place(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    report_key = "digest:1:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:refs:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    settings = Settings(telegram_bot_token="TEST")
    await telegram_poll(repo=repo, settings=settings)
    assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert edited[0]["chat_id"] == "123"
    assert edited[0]["message_id"] == 200
    assert edited[0]["parse_mode"] == "HTML"
    assert "<b>" in str(edited[0]["text"] or "")
    assert isinstance(edited[0]["reply_markup"], dict)
    assert "inline_keyboard" in edited[0]["reply_markup"]


@pytest.mark.asyncio
async def test_telegram_report_reader_callback_ack_does_not_block_navigation(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    report_key = "digest:1:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:refs:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    ack_started = asyncio.Event()
    release_ack = asyncio.Event()
    ack_timeouts: list[int] = []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        ack_timeouts.append(int(client_timeout_seconds))
        ack_started.set()
        await release_ack.wait()

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    settings = Settings(telegram_bot_token="TEST")
    poll_task = asyncio.create_task(telegram_poll(repo=repo, settings=settings))
    try:
        await asyncio.wait_for(ack_started.wait(), timeout=0.1)
        await asyncio.wait_for(poll_task, timeout=0.2)
        assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200, timeout_seconds=0.5)
    finally:
        release_ack.set()
        await asyncio.sleep(0)
        if not poll_task.done():
            poll_task.cancel()
            try:
                await poll_task
            except asyncio.CancelledError:
                pass

    assert ack_timeouts == [12]
    assert edited
    assert edited[0]["message_id"] == 200


@pytest.mark.asyncio
async def test_telegram_report_reader_callback_recovers_when_active_session_misses_mapping(tmp_path, monkeypatch):
    db_path = tmp_path / "reader-mapping-fallback.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", telegram_bot_token="TEST")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    with make_session() as seed_session:
        repo = Repo(seed_session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        report_key = "digest:1:2026-02-26:0900"
        repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
        repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:refs:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    with make_session() as session:
        repo = Repo(session)
        live_session_id = id(session)
        orig_get_telegram_message = Repo.get_telegram_message

        def fake_get_telegram_message(self, *, chat_id: str, message_id: int):
            if id(self.session) == live_session_id:
                return None
            return orig_get_telegram_message(self, chat_id=chat_id, message_id=message_id)

        monkeypatch.setattr(Repo, "get_telegram_message", fake_get_telegram_message)

        await telegram_poll(repo=repo, settings=settings, make_session=make_session)
        assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert edited[0]["chat_id"] == "123"
    assert edited[0]["message_id"] == 200
    assert "<b>" in str(edited[0]["text"] or "")


@pytest.mark.asyncio
async def test_telegram_report_reader_callback_recovers_when_mapping_is_missing_but_cover_matches_report(tmp_path, monkeypatch):
    db_path = tmp_path / "reader-mapping-cover-recovery.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", telegram_bot_token="TEST")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    report_key = "digest:0:2026-03-30:1200"
    markdown = (
        "# 参考消息\n\n"
        "窗口: 2026-03-30T10:00+08:00–2026-03-30T12:00+08:00 (Asia/Shanghai)\n"
        "条目: 2 (1 告警, 1 摘要)\n\n"
        "## 条目\n\n"
        "- Alpha [1]（告警）\n"
        "- Beta [2]（摘要）\n\n"
        "References:\n"
        "[1] Alpha — https://example.com/a\n"
        "[2] Beta — https://example.com/b\n"
    )
    with make_session() as seed_session:
        repo = Repo(seed_session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        repo.upsert_report(kind="digest", idempotency_key=report_key, title="参考消息", markdown=markdown)

    cover_text, _kb = render_cover_html(
        markdown=markdown,
        idempotency_key=report_key,
        lang="zh",
        toc_page=0,
        show_feedback=True,
    )
    plain_cover = (
        str(cover_text or "")
        .replace("<b>", "")
        .replace("</b>", "")
        .replace("<i>", "")
        .replace("</i>", "")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
    )

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:refs:0",
                    "message": {"message_id": 200, "chat": {"id": 123}, "text": plain_cover},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    with make_session() as session:
        repo = Repo(session)
        await telegram_poll(repo=repo, settings=settings, make_session=make_session)
        assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert edited[0]["chat_id"] == "123"
    assert edited[0]["message_id"] == 200
    with make_session() as verify_session:
        recovered = Repo(verify_session).get_telegram_message(chat_id="123", message_id=200)
        assert recovered is not None
        assert recovered.idempotency_key == report_key


@pytest.mark.asyncio
async def test_telegram_digest_reader_per_item_feedback_buttons_record_feedback(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    # Seed one item that matches the References URL.
    from tracker.models import Item, Source

    src = Source(type="rss", url="https://example.com/rss")
    db_session.add(src)
    db_session.flush()
    item = Item(
        source_id=int(src.id),
        url="https://example.com",
        canonical_url="https://example.com",
        title="Example",
    )
    db_session.add(item)
    db_session.commit()

    report_key = "digest:1:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:fb:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            },
            {
                "update_id": 2,
                "callback_query": {
                    "id": "cq2",
                    "from": {"id": 123},
                    "data": "br:fb:like:1:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            },
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    settings = Settings(telegram_bot_token="TEST")
    await telegram_poll(repo=repo, settings=settings)
    assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert any("🗳️" in str(e.get("text") or "") for e in edited)

    from tracker.models import FeedbackEvent

    evs = list(db_session.query(FeedbackEvent).all())
    assert len(evs) == 1
    assert (evs[0].kind or "") == "like"
    assert int(evs[0].item_id or 0) == int(item.id)
    assert (evs[0].url or "").strip() == "https://example.com"




@pytest.mark.asyncio
async def test_telegram_report_reader_rerun_creates_new_batch(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    report_key = "digest:0:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:rerun:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    answered: list[str] = []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        answered.append(text)
        return

    async def fake_run_curated_info(*, session, settings, hours: int, push: bool, key_suffix: str | None = None):  # noqa: ANN001, ARG001
        return CuratedInfoResult(
            since=dt.datetime.utcnow(),
            pushed=0,
            markdown=_SAMPLE_MD + "\n",
            idempotency_key=f"digest:0:2026-02-26:{key_suffix}",
        )

    reruns: list[tuple[str, str]] = []

    async def fake_push_report_reader(*, repo, settings, idempotency_key: str, markdown: str, disable_preview=None, replace_sent: bool = False):  # noqa: ANN001, ARG001
        reruns.append((idempotency_key, markdown))
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.runner.run_curated_info", fake_run_curated_info)
    monkeypatch.setattr("tracker.push_dispatch.push_telegram_report_reader", fake_push_report_reader)
    monkeypatch.setattr("tracker.telegram_connect.push_telegram_report_reader", fake_push_report_reader, raising=False)

    settings = Settings(telegram_bot_token="TEST")
    await telegram_poll(repo=repo, settings=settings)
    await asyncio.sleep(0)

    assert answered
    assert any("生成" in (x or "") for x in answered)
    assert reruns
    assert reruns[0][0].startswith("digest:0:")
    assert ":manual-" in reruns[0][0]


@pytest.mark.asyncio
async def test_telegram_report_reader_non_stale_edit_error_does_not_send_duplicate(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    report_key = "digest:1:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_SAMPLE_MD + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:refs:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    acks: list[str] = []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        acks.append(text)
        return

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        raise RuntimeError("telegram api timeout")

    async def fake_send_raw_text(
        self,
        *,
        chat_id: str,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> int:
        raise AssertionError("send_raw_text should not be used for non-stale edit errors")

    async def fake_send_text(self, *, chat_id: str, text: str, disable_preview: bool = True, parse_mode: str | None = None, reply_markup: dict | None = None):  # noqa: ANN001, ARG001
        acks.append(text)
        return [301]

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_raw_text", fake_send_raw_text)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.send_text", fake_send_text)

    settings = Settings(telegram_bot_token="TEST")
    await telegram_poll(repo=repo, settings=settings)
    assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert acks
    assert any("Reader 操作失败" in (text or "") for text in acks)


def _make_reader_md(ref_count: int) -> str:
    refs = "\n".join(f"[{i}] Item {i} — https://example.com/{i}" for i in range(1, ref_count + 1))
    items = "\n".join(f"- Item {i} [{i}] · Profile （摘要）" for i in range(1, ref_count + 1))
    return (
        "# 参考消息\n\n"
        "窗口: 2026-03-21T22:00+08:00–2026-03-22T00:00+08:00 (Asia/Shanghai)\n"
        f"条目: {ref_count} (0 告警, {ref_count} 摘要)\n\n"
        "## 条目\n\n"
        f"{items}\n\n"
        "References:\n"
        f"{refs}\n"
    ).strip()


def test_render_cover_html_uses_page_number_buttons_when_pages_ge_three():
    text, kb = render_cover_html(
        markdown=_make_reader_md(89),
        idempotency_key="digest:0:2026-03-22:2000",
        lang="zh",
        toc_page=6,
        show_feedback=True,
    )
    rows = kb["inline_keyboard"]
    assert all(button["callback_data"].startswith("brk:") for row in rows[:2] for button in row)
    assert [button["text"] for button in rows[0]] == ["1", "2", "3", "4", "5"]
    assert [button["text"] for button in rows[1]] == ["6", "·7·", "8"]
    assert rows[0][0]["callback_data"].endswith(":toc:0")
    assert rows[1][1]["callback_data"].endswith(":toc:6")
    assert "（7/8）" in text


def test_render_section_html_keeps_prev_next_for_two_pages():
    md = (
        "# 参考消息\n\n"
        "## 第一节\n\n"
        + ("A" * 3600)
    )
    text, kb = render_section_html(markdown=md, section_index=0, page=1, lang="zh", show_feedback=False)
    rows = kb["inline_keyboard"]
    assert rows[0][0] == {"text": "⬅️ 目录", "callback_data": "br:toc:0"}
    assert any(button["text"] == "⬅️ 上一页" for button in rows[0])
    assert all("·" not in button["text"] for button in rows[0])
    assert "（2/2）" in text


@pytest.mark.asyncio
async def test_telegram_report_reader_coalesces_rapid_navigation_to_latest_page(db_session, monkeypatch):
    repo = Repo(db_session)
    repo.set_app_config("telegram_chat_id", "123")
    repo.set_app_config("telegram_connected_notified", "1")
    repo.set_app_config("output_language", "zh")

    report_key = "digest:0:2026-02-26:0900"
    repo.upsert_report(kind="digest", idempotency_key=report_key, title="t", markdown=_make_reader_md(89) + "\n")
    repo.record_telegram_messages(chat_id="123", idempotency_key=report_key, message_ids=[200], kind="digest")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": "br:toc:0",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            },
            {
                "update_id": 2,
                "callback_query": {
                    "id": "cq2",
                    "from": {"id": 123},
                    "data": "br:toc:1",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            },
            {
                "update_id": 3,
                "callback_query": {
                    "id": "cq3",
                    "from": {"id": 123},
                    "data": "br:toc:6",
                    "message": {"message_id": 200, "chat": {"id": 123}},
                },
            },
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[str] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        await asyncio.sleep(0.02)
        edited.append(text)
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    @contextmanager
    def fake_make_session():
        yield db_session

    settings = Settings(telegram_bot_token="TEST")
    await telegram_poll(repo=repo, settings=settings, make_session=fake_make_session)
    assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert "（7/8）" in edited[-1]
    assert all("（2/8）" not in text for text in edited)
    assert len(edited) <= 2


@pytest.mark.asyncio
async def test_answer_callback_query_best_effort_times_out_quickly(monkeypatch):
    started: list[float] = []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        started.append(asyncio.get_running_loop().time())
        await asyncio.sleep(0.3)

    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)

    loop = asyncio.get_running_loop()
    t0 = loop.time()
    await _answer_callback_query_best_effort(
        bot_token="TEST",
        callback_query_id="cq-slow",
        text="Loading…",
        show_alert=False,
        client_timeout_seconds=1,
    )
    elapsed = loop.time() - t0

    assert started
    assert elapsed < 0.25


def test_telegram_poll_client_timeout_uses_fast_connect_and_poll_sized_read():
    timeout = _telegram_poll_client_timeout_seconds(Settings(telegram_bot_token="TEST"), poll_timeout_seconds=3)

    assert timeout.connect == 5.0
    assert timeout.read == 5.0
    assert timeout.write == 5.0


@pytest.mark.asyncio
async def test_telegram_get_updates_does_not_retry_transport_timeouts(monkeypatch):
    class _FakeClient:
        is_closed = False

        def __init__(self):
            self.calls = 0

        async def request(self, *, method: str, url: str, **kwargs):  # noqa: ANN003, ARG002
            self.calls += 1
            raise httpx.ReadTimeout("boom")

    fake_client = _FakeClient()
    resets: list[str] = []

    async def fake_tg_http_client():
        return fake_client

    async def fake_reset():
        resets.append("reset")

    monkeypatch.setattr("tracker.telegram_connect._tg_http_client", fake_tg_http_client)
    monkeypatch.setattr("tracker.telegram_connect._reset_tg_http_client", fake_reset)

    with pytest.raises(httpx.ReadTimeout):
        await telegram_get_updates(
            bot_token="TEST",
            offset=None,
            timeout_seconds=3,
            client_timeout_seconds=1,
        )

    assert fake_client.calls == 1
    assert resets == []


@pytest.mark.asyncio
async def test_telegram_report_reader_keyed_callback_works_without_message_mapping(tmp_path, monkeypatch):
    db_path = tmp_path / "reader-keyed-callback.db"
    settings = Settings(db_url=f"sqlite:///{db_path}", telegram_bot_token="TEST")
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    report_key = "digest:0:2026-03-30:1200"
    markdown = _make_reader_md(24)
    with make_session() as seed_session:
        repo = Repo(seed_session)
        repo.set_app_config("telegram_chat_id", "123")
        repo.set_app_config("telegram_connected_notified", "1")
        repo.set_app_config("output_language", "zh")
        repo.upsert_report(kind="digest", idempotency_key=report_key, title="参考消息", markdown=markdown)

    _cover_text, kb = render_cover_html(
        markdown=markdown,
        idempotency_key=report_key,
        lang="zh",
        toc_page=0,
        show_feedback=True,
    )
    refs_callback = ""
    for row in kb.get("inline_keyboard", []):
        for button in row:
            if button.get("callback_data", "").endswith(":refs:0"):
                refs_callback = button["callback_data"]
                break
        if refs_callback:
            break
    assert refs_callback.startswith("brk:")

    batches = [
        [
            {
                "update_id": 1,
                "callback_query": {
                    "id": "cq1",
                    "from": {"id": 123},
                    "data": refs_callback,
                    "message": {"message_id": 200, "chat": {"id": 123}, "text": "stale reader body"},
                },
            }
        ]
    ]

    async def fake_delete_webhook(*, bot_token: str, client_timeout_seconds: int) -> None:  # noqa: ARG001
        return

    async def fake_get_updates(*, bot_token: str, offset, timeout_seconds: int, client_timeout_seconds: int):  # noqa: ANN001, ARG001
        return batches.pop(0) if batches else []

    async def fake_answer_callback_query(*, bot_token: str, callback_query_id: str, text: str = "", show_alert: bool = False, client_timeout_seconds: int = 0):  # noqa: ANN001, ARG001
        return

    edited: list[dict] = []

    async def fake_edit_text(
        self,
        *,
        chat_id: str,
        message_id: int,
        text: str,
        parse_mode: str | None = None,
        disable_preview: bool = True,  # noqa: ARG001
        reply_markup: dict | None = None,
    ) -> bool:
        edited.append(
            {
                "chat_id": chat_id,
                "message_id": message_id,
                "text": text,
                "parse_mode": parse_mode,
                "reply_markup": reply_markup,
            }
        )
        return True

    monkeypatch.setattr("tracker.telegram_connect.telegram_delete_webhook", fake_delete_webhook)
    monkeypatch.setattr("tracker.telegram_connect.telegram_get_updates", fake_get_updates)
    monkeypatch.setattr("tracker.telegram_connect.telegram_answer_callback_query", fake_answer_callback_query)
    monkeypatch.setattr("tracker.push.telegram.TelegramPusher.edit_text", fake_edit_text)

    with make_session() as session:
        repo = Repo(session)
        await telegram_poll(repo=repo, settings=settings, make_session=make_session)
        assert await _wait_for_reader_nav_idle_for_tests(chat_id="123", message_id=200)

    assert edited
    assert edited[0]["chat_id"] == "123"
    assert edited[0]["message_id"] == 200
    assert "📚" in str(edited[0]["text"] or "")
