from __future__ import annotations

import datetime as dt
import time
from dataclasses import dataclass

import httpx
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from tracker.push_dispatch import (
    push_dingtalk_markdown,
    push_email_text,
    push_telegram_report_reader,
    push_telegram_text,
    push_webhook_json,
)
from tracker.repo import Repo
from tracker.settings import Settings


@dataclass(frozen=True)
class PushRetryResult:
    idempotency_key: str
    results: list[tuple[str, str]]  # (channel, status)


def make_manual_key_suffix(prefix: str = "manual") -> str:
    raw = (prefix or "manual").strip() or "manual"
    safe = "".join(ch if (ch.isalnum() or ch in {"-", "_"}) else "-" for ch in raw).strip("-_") or "manual"
    stamp = dt.datetime.utcnow().strftime("%H%M%S%f")
    # Add a tiny monotonic tail so repeated force-runs in the same microsecond still diverge.
    tail = f"{time.time_ns() % 1000:03d}"
    return f"{safe}-{stamp}{tail}"


def _parse_key(idempotency_key: str) -> tuple[str, list[str]]:
    raw = (idempotency_key or "").strip()
    if not raw:
        raise ValueError("empty idempotency_key")
    parts = raw.split(":")
    return parts[0], parts


def _effective_push_settings(*, repo: Repo, settings: Settings) -> Settings:
    """Resolve runtime-effective settings for operator push/test/retry flows."""
    try:
        from tracker.dynamic_config import effective_settings

        return effective_settings(repo=repo, settings=settings)
    except Exception:
        return settings


async def retry_failed_pushes(
    *,
    session: Session,
    settings: Settings,
    max_keys: int = 20,
) -> list[PushRetryResult]:
    """
    Best-effort sweep: retry failed pushes (digest/alert/health) up to max_keys.

    Notes:
    - Only retries keys supported by `retry_push_key`.
    - Respects `settings.push_max_attempts` via `Repo.reserve_push_attempt`.
    """
    repo = Repo(session)
    settings = _effective_push_settings(repo=repo, settings=settings)
    max_keys = max(1, min(200, int(max_keys)))

    rows = repo.list_pushes(status="failed", limit=max(50, max_keys * 10))
    keys: list[str] = []
    seen: set[str] = set()
    for p in rows:
        if p.attempts >= settings.push_max_attempts:
            continue
        key = (p.idempotency_key or "").strip()
        if not key:
            continue
        prefix = key.split(":", 1)[0]
        if prefix not in {"alert", "digest", "health"}:
            continue
        if key in seen:
            continue
        seen.add(key)
        keys.append(key)
        if len(keys) >= max_keys:
            break

    out: list[PushRetryResult] = []
    for key in keys:
        try:
            out.append(await retry_push_key(session=session, settings=settings, idempotency_key=key))
        except ValueError:
            continue
    return out


async def push_test(
    *,
    session: Session,
    settings: Settings,
    only: str | None = None,
) -> list[tuple[str, str]]:
    """
    Send a small test message to the configured push channels.

    Returns a list of (channel, status).
    """
    if only and only not in {"dingtalk", "email", "telegram", "webhook"}:
        raise ValueError("invalid only (expected dingtalk|email|telegram|webhook)")

    repo = Repo(session)
    settings = _effective_push_settings(repo=repo, settings=settings)
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")

    md = (
        "# OpenInfoMate Push Test\n\n"
        f"- time_utc: {dt.datetime.utcnow().isoformat()}Z\n"
        f"- db: {settings.db_url}\n"
    )

    results: list[tuple[str, str]] = []

    if only in (None, "dingtalk"):
        if not settings.dingtalk_webhook_url:
            results.append(("dingtalk", "skip (not configured)"))
        else:
            try:
                ok = await push_dingtalk_markdown(
                    repo=repo,
                    settings=settings,
                    idempotency_key=f"push_test:dingtalk:{ts}",
                    title="OpenInfoMate Push Test",
                    markdown=md,
                )
                results.append(("dingtalk", "sent" if ok else "skip (idempotent)"))
            except Exception as exc:
                if isinstance(exc, OperationalError):
                    raise
                results.append(("dingtalk", f"error: {exc}"))

    if only in (None, "telegram"):
        if not settings.telegram_bot_token:
            results.append(("telegram", "skip (missing bot token)"))
        else:
            chat_id = (repo.get_app_config("telegram_chat_id") or settings.telegram_chat_id or "").strip()
            if not chat_id:
                # Token-only sanity check (cannot send without a chat_id).
                try:
                    token = (settings.telegram_bot_token or "").strip()
                    url = f"https://api.telegram.org/bot{token}/getMe"
                    async with httpx.AsyncClient(timeout=float(settings.http_timeout_seconds or 20), follow_redirects=True) as client:
                        resp = await client.get(url)
                        resp.raise_for_status()
                        data = resp.json()
                    if not isinstance(data, dict) or not data.get("ok"):
                        desc = (data.get("description") if isinstance(data, dict) else None) or "telegram api error"
                        results.append(("telegram", f"token error: {desc}"))
                    else:
                        res = data.get("result") if isinstance(data, dict) else None
                        uname = (str(res.get("username") or "").strip() if isinstance(res, dict) else "")
                        results.append(("telegram", f"token ok{(' @' + uname) if uname else ''} (chat not connected)"))
                except Exception as exc:
                    results.append(("telegram", f"token test error: {exc}"))
            else:
                try:
                    ok = await push_telegram_text(
                        repo=repo,
                        settings=settings,
                        idempotency_key=f"push_test:telegram:{ts}",
                        text=md,
                    )
                    results.append(("telegram", "sent" if ok else "skip (idempotent)"))
                except Exception as exc:
                    if isinstance(exc, OperationalError):
                        raise
                    results.append(("telegram", f"error: {exc}"))

    if only in (None, "email"):
        if not (settings.smtp_host and settings.email_from and settings.email_to):
            results.append(("email", "skip (not configured)"))
        else:
            try:
                ok = push_email_text(
                    repo=repo,
                    settings=settings,
                    idempotency_key=f"push_test:email:{ts}",
                    subject="[OpenInfoMate] Push Test",
                    text=md,
                )
                results.append(("email", "sent" if ok else "skip (idempotent)"))
            except Exception as exc:
                if isinstance(exc, OperationalError):
                    raise
                results.append(("email", f"error: {exc}"))

    if only in (None, "webhook"):
        if not settings.webhook_url:
            results.append(("webhook", "skip (not configured)"))
        else:
            try:
                ok = await push_webhook_json(
                    repo=repo,
                    settings=settings,
                    idempotency_key=f"push_test:webhook:{ts}",
                    payload={"type": "push_test", "time_utc": dt.datetime.utcnow().isoformat() + "Z"},
                )
                results.append(("webhook", "sent" if ok else "skip (idempotent)"))
            except Exception as exc:
                if isinstance(exc, OperationalError):
                    raise
                results.append(("webhook", f"error: {exc}"))

    return results


async def retry_push_key(
    *,
    session: Session,
    settings: Settings,
    idempotency_key: str,
    only: str | None = None,
) -> PushRetryResult:
    """
    Re-send an existing alert/digest/health push by its idempotency key.

    Supported keys:
      - alert:<item_id>:<topic_id>
      - digest:<topic_id>:<YYYY-MM-DD>
      - health:<YYYY-MM-DD>
    """
    if only and only not in {"dingtalk", "email", "telegram", "webhook"}:
        raise ValueError("invalid only (expected dingtalk|email|telegram|webhook)")

    repo = Repo(session)
    settings = _effective_push_settings(repo=repo, settings=settings)
    prefix, parts = _parse_key(idempotency_key)

    title = ""
    markdown = ""
    subject = ""
    webhook_payload: dict | None = None

    if prefix == "digest":
        report = repo.get_report_by_key(kind="digest", idempotency_key=idempotency_key)
        if not report:
            raise ValueError("digest report not found for this key (run a digest first)")
        from tracker.models import Topic

        topic = session.get(Topic, report.topic_id) if report.topic_id else None
        topic_name = topic.name if topic else ""
        title = report.title or (f"Digest: {topic_name}" if topic_name else "Digest")
        markdown = report.markdown
        subject = f"[Digest] {topic_name}" if topic_name else "[Digest] OpenInfoMate"
        day = parts[2] if len(parts) >= 3 else dt.datetime.utcnow().date().isoformat()
        webhook_payload = {
            "type": "digest",
            "topic": topic_name or None,
            "topic_id": report.topic_id,
            "date": day,
            "markdown": markdown,
        }

    elif prefix == "health":
        report = repo.get_report_by_key(kind="health", idempotency_key=idempotency_key)
        if not report:
            raise ValueError("health report not found for this key (run a health report first)")
        title = report.title or "OpenInfoMate Health"
        markdown = report.markdown
        subject = "[Health] OpenInfoMate"
        day = parts[1] if len(parts) >= 2 else dt.datetime.utcnow().date().isoformat()
        webhook_payload = {"type": "health", "date": day, "markdown": markdown}

    elif prefix == "alert":
        if len(parts) < 3:
            raise ValueError("invalid alert key format (expected alert:<item_id>:<topic_id>)")
        try:
            item_id = int(parts[1])
            topic_id = int(parts[2])
        except Exception as exc:
            raise ValueError("invalid alert key format (expected alert:<item_id>:<topic_id>)") from exc

        from tracker.models import Topic

        item = repo.get_item_by_id(item_id)
        topic = session.get(Topic, topic_id)
        it = repo.get_item_topic(item_id=item_id, topic_id=topic_id)
        if not item or not topic or not it:
            raise ValueError("alert item/topic not found for this key")

        title = f"Alert: {topic.name}"
        final_reason = (it.reason or "").strip() or "manual retry"
        markdown = (
            f"# Alert: {topic.name}\n\n"
            f"- [{item.title}]({item.canonical_url})\n\n"
            f"Reason: {final_reason}\n"
        )
        subject = f"[Alert] {topic.name}: {item.title}"
        webhook_payload = {
            "type": "alert",
            "topic": topic.name,
            "topic_id": topic.id,
            "item_id": item.id,
            "title": item.title,
            "url": item.canonical_url,
            "reason": final_reason,
        }

    else:
        raise ValueError(f"unsupported idempotency_key prefix: {prefix}")

    results: list[tuple[str, str]] = []

    if only in (None, "dingtalk"):
        try:
            ok = await push_dingtalk_markdown(
                repo=repo,
                settings=settings,
                idempotency_key=idempotency_key,
                title=title,
                markdown=markdown,
            )
            results.append(("dingtalk", "sent" if ok else "skip (not configured/idempotent)"))
        except Exception as exc:
            results.append(("dingtalk", f"error: {exc}"))

    if only in (None, "telegram"):
        try:
            use_reader = bool(prefix == "digest") and bool(getattr(settings, "telegram_digest_reader_enabled", True))
            if use_reader:
                ok = await push_telegram_report_reader(
                    repo=repo,
                    settings=settings,
                    idempotency_key=idempotency_key,
                    markdown=markdown,
                )
            else:
                ok = await push_telegram_text(
                    repo=repo,
                    settings=settings,
                    idempotency_key=idempotency_key,
                    text=markdown,
                )
            results.append(("telegram", "sent" if ok else "skip (not configured/idempotent)"))
        except Exception as exc:
            results.append(("telegram", f"error: {exc}"))

    if only in (None, "email"):
        try:
            ok = push_email_text(
                repo=repo,
                settings=settings,
                idempotency_key=idempotency_key,
                subject=subject,
                text=markdown,
            )
            results.append(("email", "sent" if ok else "skip (not configured/idempotent)"))
        except Exception as exc:
            results.append(("email", f"error: {exc}"))

    if only in (None, "webhook"):
        if webhook_payload is None:
            results.append(("webhook", "skip (not supported for this key)"))
        else:
            try:
                ok = await push_webhook_json(
                    repo=repo,
                    settings=settings,
                    idempotency_key=idempotency_key,
                    payload=webhook_payload,
                )
                results.append(("webhook", "sent" if ok else "skip (not configured/idempotent)"))
            except Exception as exc:
                results.append(("webhook", f"error: {exc}"))

    return PushRetryResult(idempotency_key=idempotency_key, results=results)
