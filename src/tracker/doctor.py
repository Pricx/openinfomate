from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

import httpx

from tracker.repo import ActivitySnapshot
from tracker.settings import Settings
from tracker.timezones import resolve_cron_timezone


@dataclass(frozen=True)
class DoctorReport:
    db_ok: bool
    db_error: str | None
    api_host: str
    api_port: int
    api_bind_public: bool
    api_auth_configured: bool
    cron_timezone: str
    cron_timezone_ok: bool
    cron_now_iso: str
    profile_configured: bool
    push_dingtalk_configured: bool
    push_telegram_configured: bool
    push_email_configured: bool
    push_webhook_configured: bool
    push_missing_env: dict[str, list[str]]
    stats: dict[str, int]
    recommendations: list[str]
    last_tick_at: str | None = None
    last_digest_report_at: str | None = None
    last_health_report_at: str | None = None
    last_push_attempt_at: str | None = None
    last_push_sent_at: str | None = None
    last_digest_sync_at: str | None = None
    last_curated_sync_at: str | None = None
    next_health_report_at: str | None = None
    next_discover_sources_at: str | None = None


def _looks_like_loopback_host(host: str) -> bool:
    h = (host or "").strip().lower()
    if not h:
        return True
    if h in {"127.0.0.1", "::1", "localhost"}:
        return True
    return False


def build_doctor_report(
    *,
    settings: Settings,
    stats: dict[str, int],
    db_ok: bool,
    db_error: str | None,
    profile_configured: bool = False,
    telegram_chat_configured: bool = False,
    activity: ActivitySnapshot | None = None,
) -> DoctorReport:
    tz, tz_ok = resolve_cron_timezone(settings.cron_timezone)
    now_iso = dt.datetime.now(tz=tz).isoformat()
    raw_lang = (getattr(settings, "output_language", "") or "").strip().lower()
    is_zh = raw_lang.startswith("zh") or raw_lang in {"cn"} or (getattr(settings, "output_language", "") in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语", "漢語"})

    prof_ok = bool(profile_configured)
    dingtalk_enabled = bool(getattr(settings, "push_dingtalk_enabled", True))
    push_dingtalk = bool(dingtalk_enabled and settings.dingtalk_webhook_url)
    telegram_enabled = bool(getattr(settings, "push_telegram_enabled", True))
    push_telegram = bool(telegram_enabled and settings.telegram_bot_token)
    push_email = bool(settings.smtp_host and settings.email_from and settings.email_to)
    push_webhook = bool(settings.webhook_url)

    api_host = str(getattr(settings, "api_host", "") or "").strip() or "127.0.0.1"
    api_port = int(getattr(settings, "api_port", 0) or 0) or 0
    api_bind_public = not _looks_like_loopback_host(api_host)
    api_auth_configured = bool(getattr(settings, "api_token", None) or getattr(settings, "admin_password", None))

    missing: dict[str, list[str]] = {}
    if dingtalk_enabled and not push_dingtalk:
        missing["dingtalk"] = ["TRACKER_DINGTALK_WEBHOOK_URL"]
    if telegram_enabled and not push_telegram:
        req_tg: list[str] = []
        if not settings.telegram_bot_token:
            req_tg.append("TRACKER_TELEGRAM_BOT_TOKEN")
        if not telegram_chat_configured:
            req_tg.append("connect Telegram chat via /setup/push")
        missing["telegram"] = req_tg or ["TRACKER_TELEGRAM_BOT_TOKEN", "connect Telegram chat via /setup/push"]
    if not push_email:
        req: list[str] = []
        if not settings.smtp_host:
            req.append("TRACKER_SMTP_HOST")
        if not settings.email_from:
            req.append("TRACKER_EMAIL_FROM")
        if not settings.email_to:
            req.append("TRACKER_EMAIL_TO")
        missing["email"] = req or ["TRACKER_SMTP_HOST", "TRACKER_EMAIL_FROM", "TRACKER_EMAIL_TO"]
    if not push_webhook:
        missing["webhook"] = ["TRACKER_WEBHOOK_URL"]

    recs: list[str] = []
    if not prof_ok:
        recs.append(
            "Profile is not set. Create an AI-native Profile first (it drives LLM curation without keyword matching). "
            "Open `/admin` → Profile Setup (or `/setup/profile`)."
        )
    if not push_dingtalk and not push_telegram and not push_email and not push_webhook:
        recs.append(
            "Configure at least one push channel (DingTalk webhook, Telegram bot, and/or SMTP email). "
            "Then run `openinfomate push test` to verify. "
            "Tip: use `/management` → Doctor → Quick Config (localhost-only) or `openinfomate env set` to write `.env`."
        )
    if stats.get("topics_total", 0) == 0:
        recs.append("Seed topics (e.g. `openinfomate topic bootstrap-file --in topics.txt ...`).")
    if stats.get("sources_total", 0) == 0:
        recs.append("Add sources (bootstrap topics will seed default sources).")
    if stats.get("bindings_total", 0) == 0 and stats.get("topics_total", 0) > 0 and stats.get("sources_total", 0) > 0:
        recs.append("Bind topics to sources (or re-run topic bootstrap).")
    if not tz_ok:
        recs.append(f"Invalid TRACKER_CRON_TIMEZONE={settings.cron_timezone!r}; service falls back to UTC.")

    if api_bind_public and not api_auth_configured:
        recs.append(
            ("API 绑定到非 localhost，但未配置 API token 或 admin password；服务将拒绝启动。请先配置鉴权再绑定 0.0.0.0。")
            if is_zh
            else (
                "API is bound to a non-loopback host but no auth is configured; the service will refuse to start. "
                "Set TRACKER_API_TOKEN or TRACKER_ADMIN_PASSWORD before binding to 0.0.0.0."
            )
        )
    if api_auth_configured and (not api_bind_public):
        recs.append(
            ("已配置鉴权，但 API 仍绑定在 localhost。若你想从外部访问 Web Admin，请设置 TRACKER_API_HOST=0.0.0.0（并确保防火墙/反代正确）。")
            if is_zh
            else (
                "Auth is configured but the API is still bound to localhost. "
                "If you want remote Web Admin, set TRACKER_API_HOST=0.0.0.0 (and ensure firewall/proxy allows it)."
            )
        )

    def _iso(value: dt.datetime | None) -> str | None:
        return value.isoformat() if value else None

    last_tick_at = _iso(activity.last_tick_at) if activity else None
    last_digest_report_at = _iso(activity.last_digest_report_at) if activity else None
    last_health_report_at = _iso(activity.last_health_report_at) if activity else None
    last_push_attempt_at = _iso(activity.last_push_attempt_at) if activity else None
    last_push_sent_at = _iso(activity.last_push_sent_at) if activity else None
    last_digest_sync_at = _iso(activity.last_digest_sync_at) if activity else None
    last_curated_sync_at = _iso(activity.last_curated_sync_at) if activity else None

    if stats.get("sources_total", 0) > 0 and activity:
        if activity.last_tick_at is None:
            recs.append("No tick has run yet. Start the scheduler: `tracker service run` (or systemd service).")
        else:
            age = dt.datetime.utcnow() - activity.last_tick_at
            expected = max(10, int(getattr(settings, "alert_poll_seconds", 900) or 900))
            if age.total_seconds() > max(2 * expected, 3600):
                mins = int(age.total_seconds() // 60)
                recs.append(f"No tick in ~{mins} minutes. Scheduler may be stopped (check systemd / logs).")

    if (push_dingtalk or push_telegram or push_email or push_webhook) and activity:
        if activity.last_push_attempt_at is None:
            recs.append("Push is configured but no push attempts recorded yet. Run `tracker push test`.")
        elif activity.last_push_sent_at is None:
            recs.append("Push attempts exist but no successful pushes yet. Check `tracker push list --status failed`.")
        else:
            pass

    if activity and bool(getattr(settings, 'digest_scheduler_enabled', False)):
        now_utc = dt.datetime.utcnow()
        stale_after = dt.timedelta(minutes=15)
        if activity.last_digest_sync_at is None or activity.last_curated_sync_at is None:
            recs.append(
                'Digest pipeline scheduler heartbeat is missing. Keep both per-topic digest progression and cross-topic curated sync installed; otherwise candidate items can stall before Curated Info.'
            )
        else:
            if now_utc - activity.last_digest_sync_at > stale_after:
                mins = int((now_utc - activity.last_digest_sync_at).total_seconds() // 60)
                recs.append(
                    f'Digest progression scheduler heartbeat is stale (~{mins} minutes). Check digest:sync job installation before candidate backlog dries up Curated Info.'
                )
            if now_utc - activity.last_curated_sync_at > stale_after:
                mins = int((now_utc - activity.last_curated_sync_at).total_seconds() // 60)
                recs.append(
                    f'Curated sync heartbeat is stale (~{mins} minutes). Cross-topic Curated Info may stop running.'
                )
        if activity.digest_sync_enabled_topics > 0 and activity.digest_sync_scheduled_topics <= 0:
            recs.append(
                'Enabled topics exist but zero per-topic digest progression jobs are scheduled. Candidate -> digest promotion can stall completely.'
            )
        elif activity.digest_sync_enabled_topics > 0 and activity.digest_sync_scheduled_topics < activity.digest_sync_enabled_topics:
            recs.append(
                f'Only {activity.digest_sync_scheduled_topics}/{activity.digest_sync_enabled_topics} enabled topics currently have per-topic digest progression jobs.'
            )
        if not activity.curated_sync_job_present:
            recs.append('Cross-topic Curated Info job heartbeat reports missing runtime job `digest:curated`.')

    def _next_cron(cron_expr: str) -> str | None:
        raw = (cron_expr or "").strip()
        if not raw:
            return None
        try:
            from apscheduler.triggers.cron import CronTrigger

            trigger = CronTrigger.from_crontab(raw, timezone=tz)
            nxt = trigger.get_next_fire_time(None, dt.datetime.now(tz=tz))
            return nxt.isoformat() if nxt else None
        except Exception:
            return None

    return DoctorReport(
        db_ok=db_ok,
        db_error=db_error,
        api_host=api_host,
        api_port=api_port,
        api_bind_public=api_bind_public,
        api_auth_configured=api_auth_configured,
        cron_timezone=settings.cron_timezone,
        cron_timezone_ok=tz_ok,
        cron_now_iso=now_iso,
        profile_configured=prof_ok,
        push_dingtalk_configured=push_dingtalk,
        push_telegram_configured=push_telegram,
        push_email_configured=push_email,
        push_webhook_configured=push_webhook,
        push_missing_env=missing,
        stats=stats,
        recommendations=recs,
        last_tick_at=last_tick_at,
        last_digest_report_at=last_digest_report_at,
        last_health_report_at=last_health_report_at,
        last_push_attempt_at=last_push_attempt_at,
        last_push_sent_at=last_push_sent_at,
        last_digest_sync_at=last_digest_sync_at,
        last_curated_sync_at=last_curated_sync_at,
        next_health_report_at=_next_cron(getattr(settings, "health_report_cron", "") or ""),
        next_discover_sources_at=_next_cron(getattr(settings, "discover_sources_cron", "") or ""),
    )


def try_fetch_url(*, url: str, timeout_seconds: int) -> tuple[bool, str]:
    """
    Best-effort connectivity check for operator diagnostics.
    """
    try:
        resp = httpx.get(url, headers={"User-Agent": "tracker/0.1"}, timeout=timeout_seconds, follow_redirects=True)
        resp.raise_for_status()
        return True, f"HTTP {resp.status_code}"
    except Exception as exc:
        return False, str(exc)
