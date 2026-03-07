from __future__ import annotations

import logging

from tracker.push import DingTalkPusher, EmailPusher, TelegramPusher, WebhookPusher
from tracker.push.telegram import TelegramPartialDeliveryError, is_stale_telegram_edit_error, split_telegram_message
from tracker.repo import Repo
from tracker.settings import Settings


logger = logging.getLogger(__name__)


def _classify_telegram_push_key(key: str) -> tuple[str, int | None]:
    raw = (key or "").strip()
    kind = ""
    item_id = None
    if raw.startswith("alert:"):
        kind = "alert"
        try:
            item_id = int(raw.split(":", 2)[1])
        except Exception:
            item_id = None
    elif raw.startswith("digest:"):
        kind = "digest"
    return kind, item_id


def _persist_telegram_messages_strict(*, repo: Repo, chat_id: str, idempotency_key: str, message_ids: list[int]) -> None:
    kind, item_id = _classify_telegram_push_key(idempotency_key)
    repo.ensure_telegram_messages_recorded(
        chat_id=chat_id,
        idempotency_key=idempotency_key,
        message_ids=message_ids,
        kind=kind,
        item_id=item_id,
    )


async def _send_telegram_raw_text_guarded(
    *,
    pusher: TelegramPusher,
    chat_id: str,
    text: str,
    disable_preview: bool,
    delivered_new_message_ids: list[int],
    parse_mode: str | None = None,
    reply_markup: dict | None = None,
    context: str = "telegram send failed",
) -> int:
    try:
        mid = await pusher.send_raw_text(
            chat_id=chat_id,
            text=text,
            disable_preview=disable_preview,
            parse_mode=parse_mode,
            reply_markup=reply_markup,
        )
    except Exception as exc:
        surviving = [int(mid) for mid in delivered_new_message_ids if int(mid or 0) > 0]
        if surviving:
            raise TelegramPartialDeliveryError(
                f"{context}; partial messages remain: {surviving}",
                message_ids=surviving,
            ) from exc
        raise
    return int(mid or 0)


async def _delete_telegram_message_if_remote_deleted(
    *, repo: Repo, pusher: TelegramPusher, chat_id: str, message_id: int
) -> bool:
    mid = int(message_id or 0)
    if int(mid or 0) <= 0:
        return False
    deleted = await pusher.delete_message(chat_id=chat_id, message_id=mid)
    if not deleted:
        return False
    try:
        repo.delete_telegram_message(chat_id=chat_id, message_id=mid)
    except Exception as exc:
        logger.warning(
            "telegram message mapping delete failed: chat_id=%s message_id=%s err=%s",
            chat_id,
            mid,
            exc,
        )
    return True


def _split_csv(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split(",") if v.strip()]


async def push_dingtalk_markdown(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    title: str,
    markdown: str,
) -> bool:
    if not bool(getattr(settings, "push_dingtalk_enabled", True)):
        return False
    if not settings.dingtalk_webhook_url:
        return False

    push = repo.reserve_push_attempt(
        channel="dingtalk",
        idempotency_key=idempotency_key,
        max_attempts=settings.push_max_attempts,
    )
    if not push:
        return False

    try:
        await DingTalkPusher(
            settings.dingtalk_webhook_url,
            secret=settings.dingtalk_secret,
            timeout_seconds=settings.http_timeout_seconds,
        ).send_markdown(title=title, markdown=markdown)
        repo.mark_push_sent(push)
        return True
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise


def push_email_text(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    subject: str,
    text: str,
) -> bool:
    if not (
        settings.smtp_host
        and settings.email_from
        and settings.email_to
    ):
        return False

    push = repo.reserve_push_attempt(
        channel="email",
        idempotency_key=idempotency_key,
        max_attempts=settings.push_max_attempts,
    )
    if not push:
        return False

    try:
        EmailPusher(
            host=settings.smtp_host,
            port=settings.smtp_port,
            user=settings.smtp_user,
            password=settings.smtp_password,
            email_from=settings.email_from,
            email_to=_split_csv(settings.email_to),
            starttls=settings.smtp_starttls,
            use_ssl=settings.smtp_use_ssl,
            timeout_seconds=settings.http_timeout_seconds,
        ).send(subject=subject, text=text)
        repo.mark_push_sent(push)
        return True
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise


async def push_webhook_json(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    payload: dict,
) -> bool:
    if not settings.webhook_url:
        return False

    push = repo.reserve_push_attempt(
        channel="webhook",
        idempotency_key=idempotency_key,
        max_attempts=settings.push_max_attempts,
    )
    if not push:
        return False

    try:
        await WebhookPusher(
            settings.webhook_url,
            timeout_seconds=settings.http_timeout_seconds,
        ).send_json(payload)
        repo.mark_push_sent(push)
        return True
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise


def _telegram_chat_id(*, repo: Repo, settings: Settings) -> str | None:
    chat_id = (repo.get_app_config("telegram_chat_id") or "").strip()
    if chat_id:
        return chat_id
    fallback = (settings.telegram_chat_id or "").strip()
    return fallback or None


async def push_telegram_text(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    text: str,
    disable_preview: bool | None = None,
    replace_sent: bool = False,
) -> bool:
    if not bool(getattr(settings, "push_telegram_enabled", True)):
        return False
    token = (settings.telegram_bot_token or "").strip()
    chat_id = _telegram_chat_id(repo=repo, settings=settings)
    if not (token and chat_id):
        return False

    orig_key = (idempotency_key or "").strip()
    key = orig_key

    push = repo.reserve_push_attempt(
        channel="telegram",
        idempotency_key=key,
        max_attempts=settings.push_max_attempts,
        allow_sent=bool(replace_sent),
    )
    if not push:
        return False

    try:
        if disable_preview is None:
            disable_preview = bool(getattr(settings, "telegram_disable_preview", True))
        pusher = TelegramPusher(token, timeout_seconds=settings.http_timeout_seconds)

        message_ids: list[int] = []
        new_message_ids: list[int] = []
        existing_ids = repo.list_telegram_message_ids_by_key(chat_id=chat_id, idempotency_key=key, limit=50)
        use_replace_sent = bool(replace_sent or existing_ids)
        if use_replace_sent:
            parts = split_telegram_message(text)
            if not parts:
                raise ValueError("empty text")
            if len(parts) == 1:
                payload_parts = [parts[0]]
            else:
                total = len(parts)
                payload_parts = [f"[{i}/{total}]\n{part}" for i, part in enumerate(parts, start=1)]

            if existing_ids:
                keep = min(len(existing_ids), len(payload_parts))
                for i in range(keep):
                    old_mid = int(existing_ids[i])
                    try:
                        await pusher.edit_text(
                            chat_id=chat_id,
                            message_id=old_mid,
                            text=payload_parts[i],
                            disable_preview=disable_preview,
                        )
                        message_ids.append(old_mid)
                        continue
                    except Exception as exc:
                        # Stale mapping: message was deleted or is no longer editable.
                        # Fall back to sending a new part and drop the bad mapping so future edits converge.
                        if is_stale_telegram_edit_error(exc):
                            try:
                                repo.delete_telegram_message(chat_id=chat_id, message_id=old_mid)
                            except Exception:
                                pass
                            mid = await _send_telegram_raw_text_guarded(
                                pusher=pusher,
                                chat_id=chat_id,
                                text=payload_parts[i],
                                disable_preview=disable_preview,
                                delivered_new_message_ids=new_message_ids,
                                context="telegram replace send failed",
                            )
                            if mid > 0:
                                new_message_ids.append(mid)
                                message_ids.append(mid)
                            continue
                        raise
                # Delete extra old parts if the new message became shorter.
                for mid in existing_ids[keep:]:
                    try:
                        await _delete_telegram_message_if_remote_deleted(
                            repo=repo,
                            pusher=pusher,
                            chat_id=chat_id,
                            message_id=int(mid),
                        )
                    except Exception:
                        continue
                # Send extra parts if the new message became longer.
                for part in payload_parts[keep:]:
                    mid = await _send_telegram_raw_text_guarded(
                        pusher=pusher,
                        chat_id=chat_id,
                        text=part,
                        disable_preview=disable_preview,
                        delivered_new_message_ids=new_message_ids,
                        context="telegram replace send failed",
                    )
                    if mid > 0:
                        new_message_ids.append(mid)
                        message_ids.append(mid)
            else:
                message_ids = await pusher.send_text(chat_id=chat_id, text=text, disable_preview=disable_preview)
        else:
            message_ids = await pusher.send_text(chat_id=chat_id, text=text, disable_preview=disable_preview)
        _persist_telegram_messages_strict(repo=repo, chat_id=chat_id, idempotency_key=key, message_ids=message_ids)

        repo.mark_push_sent(push)
        return True
    except TelegramPartialDeliveryError as exc:
        if exc.message_ids:
            try:
                _persist_telegram_messages_strict(
                    repo=repo,
                    chat_id=chat_id,
                    idempotency_key=key,
                    message_ids=exc.message_ids,
                )
            except Exception as map_exc:
                logger.warning(
                    "telegram partial-delivery mapping persist failed: key=%s chat_id=%s err=%s",
                    key,
                    chat_id,
                    map_exc,
                )
        repo.mark_push_failed(push, error=str(exc))
        raise
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise


async def push_telegram_text_card(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    text: str,
    reply_markup: dict | None = None,
    disable_preview: bool | None = None,
    replace_sent: bool = False,
) -> bool:
    """
    Push ONE Telegram message with an optional inline keyboard.

    Use this when you need callback_query buttons (e.g., operator workflows) without
    dumping a long report.
    """
    if not bool(getattr(settings, "push_telegram_enabled", True)):
        return False
    token = (settings.telegram_bot_token or "").strip()
    chat_id = _telegram_chat_id(repo=repo, settings=settings)
    if not (token and chat_id):
        return False

    key = (idempotency_key or "").strip()
    if not key:
        return False

    push = repo.reserve_push_attempt(
        channel="telegram",
        idempotency_key=key,
        max_attempts=settings.push_max_attempts,
        allow_sent=bool(replace_sent),
    )
    if not push:
        return False

    try:
        if disable_preview is None:
            disable_preview = bool(getattr(settings, "telegram_disable_preview", True))
        pusher = TelegramPusher(token, timeout_seconds=settings.http_timeout_seconds)

        message_ids: list[int] = []
        existing_ids = repo.list_telegram_message_ids_by_key(chat_id=chat_id, idempotency_key=key, limit=50)
        use_replace_sent = bool(replace_sent or existing_ids)
        if use_replace_sent:
            if existing_ids:
                mid0 = int(existing_ids[0])
                try:
                    await pusher.edit_text(
                        chat_id=chat_id,
                        message_id=mid0,
                        text=text,
                        disable_preview=bool(disable_preview),
                        reply_markup=reply_markup,
                    )
                    message_ids.append(mid0)
                except Exception as exc:
                    if is_stale_telegram_edit_error(exc):
                        try:
                            repo.delete_telegram_message(chat_id=chat_id, message_id=mid0)
                        except Exception:
                            pass
                        mid_new = await pusher.send_raw_text(
                            chat_id=chat_id,
                            text=text,
                            disable_preview=bool(disable_preview),
                            reply_markup=reply_markup,
                        )
                        if int(mid_new or 0) > 0:
                            message_ids.append(int(mid_new))
                    else:
                        raise

                # Delete legacy extra parts (multi-part mappings) so the card converges to one message.
                for mid in existing_ids[1:]:
                    try:
                        await _delete_telegram_message_if_remote_deleted(
                            repo=repo,
                            pusher=pusher,
                            chat_id=chat_id,
                            message_id=int(mid),
                        )
                    except Exception:
                        continue
            else:
                mid = await pusher.send_raw_text(
                    chat_id=chat_id,
                    text=text,
                    disable_preview=bool(disable_preview),
                    reply_markup=reply_markup,
                )
                if int(mid or 0) > 0:
                    message_ids.append(int(mid))
        else:
            mid = await pusher.send_raw_text(
                chat_id=chat_id,
                text=text,
                disable_preview=bool(disable_preview),
                reply_markup=reply_markup,
            )
            if int(mid or 0) > 0:
                message_ids.append(int(mid))
        _persist_telegram_messages_strict(repo=repo, chat_id=chat_id, idempotency_key=key, message_ids=message_ids)

        repo.mark_push_sent(push)
        return True
    except TelegramPartialDeliveryError as exc:
        if exc.message_ids:
            try:
                _persist_telegram_messages_strict(
                    repo=repo,
                    chat_id=chat_id,
                    idempotency_key=key,
                    message_ids=exc.message_ids,
                )
            except Exception as map_exc:
                logger.warning(
                    "telegram partial-delivery mapping persist failed: key=%s chat_id=%s err=%s",
                    key,
                    chat_id,
                    map_exc,
                )
        repo.mark_push_failed(push, error=str(exc))
        raise
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise


async def push_telegram_report_reader(
    *,
    repo: Repo,
    settings: Settings,
    idempotency_key: str,
    markdown: str,
    disable_preview: bool | None = None,
    replace_sent: bool = False,
) -> bool:
    """
    Push a Telegram-native "reader card" for report-style markdown (digest).

    Instead of dumping the full markdown (which Telegram doesn't render well on mobile),
    we send ONE cover message with inline buttons. Button clicks (callback_query) can
    edit the same message to show sections/pages on demand.
    """
    if not bool(getattr(settings, "push_telegram_enabled", True)):
        return False
    token = (settings.telegram_bot_token or "").strip()
    chat_id = _telegram_chat_id(repo=repo, settings=settings)
    if not (token and chat_id):
        return False

    orig_key = (idempotency_key or "").strip()
    key = orig_key

    push = repo.reserve_push_attempt(
        channel="telegram",
        idempotency_key=key,
        max_attempts=settings.push_max_attempts,
        allow_sent=bool(replace_sent),
    )
    if not push:
        return False

    try:
        if disable_preview is None:
            disable_preview = bool(getattr(settings, "telegram_disable_preview", True))

        out_lang_raw = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
        low = out_lang_raw.lower()
        lang = "zh" if (out_lang_raw in {"中文", "简体中文", "繁體中文", "繁体中文"} or low.startswith("zh")) else "en"

        from tracker.telegram_report_reader import render_cover_html

        show_feedback = bool(
            orig_key.startswith("digest:")
            and bool(getattr(settings, "telegram_digest_item_feedback_enabled", True))
        )
        text_html, kb = render_cover_html(
            markdown=markdown,
            idempotency_key=orig_key,
            lang=lang,
            toc_page=0,
            show_feedback=show_feedback,
        )

        pusher = TelegramPusher(token, timeout_seconds=settings.http_timeout_seconds)

        message_ids: list[int] = []
        existing_ids = repo.list_telegram_message_ids_by_key(chat_id=chat_id, idempotency_key=key, limit=50)
        use_replace_sent = bool(replace_sent or existing_ids)
        if use_replace_sent:
            if existing_ids:
                mid0 = int(existing_ids[0])
                try:
                    await pusher.edit_text(
                        chat_id=chat_id,
                        message_id=mid0,
                        text=text_html,
                        parse_mode="HTML",
                        disable_preview=disable_preview,
                        reply_markup=kb,
                    )
                    message_ids.append(mid0)
                except Exception as exc:
                    if is_stale_telegram_edit_error(exc):
                        try:
                            repo.delete_telegram_message(chat_id=chat_id, message_id=mid0)
                        except Exception:
                            pass
                        mid_new = await pusher.send_raw_text(
                            chat_id=chat_id,
                            text=text_html,
                            parse_mode="HTML",
                            disable_preview=disable_preview,
                            reply_markup=kb,
                        )
                        if int(mid_new or 0) > 0:
                            message_ids.append(int(mid_new))
                    else:
                        raise

                # Delete extra old parts (legacy multi-part mapping) so the reader converges to one message.
                for mid in existing_ids[1:]:
                    try:
                        await _delete_telegram_message_if_remote_deleted(
                            repo=repo,
                            pusher=pusher,
                            chat_id=chat_id,
                            message_id=int(mid),
                        )
                    except Exception:
                        continue
            else:
                mid = await pusher.send_raw_text(
                    chat_id=chat_id,
                    text=text_html,
                    parse_mode="HTML",
                    disable_preview=disable_preview,
                    reply_markup=kb,
                )
                if int(mid or 0) > 0:
                    message_ids.append(int(mid))
        else:
            mid = await pusher.send_raw_text(
                chat_id=chat_id,
                text=text_html,
                parse_mode="HTML",
                disable_preview=disable_preview,
                reply_markup=kb,
            )
            if int(mid or 0) > 0:
                message_ids.append(int(mid))
        _persist_telegram_messages_strict(repo=repo, chat_id=chat_id, idempotency_key=key, message_ids=message_ids)

        repo.mark_push_sent(push)
        return True
    except TelegramPartialDeliveryError as exc:
        if exc.message_ids:
            try:
                _persist_telegram_messages_strict(
                    repo=repo,
                    chat_id=chat_id,
                    idempotency_key=key,
                    message_ids=exc.message_ids,
                )
            except Exception as map_exc:
                logger.warning(
                    "telegram partial-delivery mapping persist failed: key=%s chat_id=%s err=%s",
                    key,
                    chat_id,
                    map_exc,
                )
        repo.mark_push_failed(push, error=str(exc))
        raise
    except Exception as exc:
        repo.mark_push_failed(push, error=str(exc))
        raise
