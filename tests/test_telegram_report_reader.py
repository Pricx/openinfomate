from __future__ import annotations

import datetime as dt

import pytest

from tracker.push_dispatch import push_telegram_report_reader
from tracker.repo import Repo
from tracker.runner import CuratedInfoResult
from tracker.settings import Settings
from tracker.telegram_connect import telegram_poll


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

    assert edited
    assert edited[0]["chat_id"] == "123"
    assert edited[0]["message_id"] == 200
    assert edited[0]["parse_mode"] == "HTML"
    assert "<b>" in str(edited[0]["text"] or "")
    assert isinstance(edited[0]["reply_markup"], dict)
    assert "inline_keyboard" in edited[0]["reply_markup"]


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

    assert answered
    assert any("生成" in (x or "") for x in answered)
    assert reruns
    assert reruns[0][0].startswith("digest:0:")
    assert ":manual-" in reruns[0][0]
