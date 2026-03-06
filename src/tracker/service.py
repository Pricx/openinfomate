from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from tracker.db import session_factory
from tracker.job_lock import job_lock_async
from tracker.logging_config import configure_logging
from tracker.maintenance import run_backup, run_prune_ignored
from tracker.models import Base
from tracker.repo import Repo
from tracker.runner import run_digest, run_discover_sources, run_health_report, run_tick
from tracker.push_ops import retry_failed_pushes
from tracker.settings import get_settings
from tracker.timezones import resolve_cron_timezone

logger = logging.getLogger(__name__)


def _cron_timezone(settings) -> dt.tzinfo:
    name = (getattr(settings, "cron_timezone", None) or "").strip()
    tz, ok = resolve_cron_timezone(name)
    return tz if ok else dt.timezone.utc


def _misfire_grace_seconds(settings) -> int | None:
    """
    Misfire grace time for cron-like jobs.

    If the service is down during a scheduled run, APScheduler can "catch up" by
    running the job once on resume, but only within this grace window.
    """
    try:
        v = int(getattr(settings, "cron_misfire_grace_seconds", 0) or 0)
    except Exception:
        v = 0
    return v if v > 0 else None


def _norm_output_language(raw: str) -> str:
    s = (raw or "").strip().lower()
    raw2 = (raw or "").strip()
    if raw2 in {"中文", "简体中文", "繁体中文", "繁體中文", "汉语"}:
        return "zh"
    if s in {"zh", "zh-cn", "zh-hans", "zh-hant", "cn"} or s.startswith("zh"):
        return "zh"
    if s in {"en", "en-us", "english", "英文"} or s.startswith("en"):
        return "en"
    return "en"


async def _maybe_notify_source_candidates_batch(make_session, settings) -> None:
    """
    Notify operators via Telegram when unreviewed SourceCandidates reach a batch size.

    This is global (not per AI Setup run): candidates are a review queue, and operators
    accept them in Web Admin.
    """
    try:
        with make_session() as session:
            repo = Repo(session)

            raw_on = (repo.get_app_config("ai_setup_candidates_notify_telegram_enabled") or "").strip().lower()
            # Default OFF: avoid noisy operator pushes by default.
            if not raw_on:
                enabled = False
            else:
                enabled = False if raw_on in {"0", "false", "off", "no"} else True
            raw_bs = (repo.get_app_config("ai_setup_candidates_notify_batch_size") or "").strip()
            batch = int(raw_bs or 10)
            batch = max(1, min(500, batch))
            if not enabled or batch <= 0:
                return
            # If auto-accept is enabled, candidates should not bother operators.
            try:
                raw_aa = (repo.get_app_config("discover_sources_auto_accept_enabled") or "").strip().lower()
                auto_accept_enabled = False if raw_aa in {"0", "false", "off", "no"} else True
            except Exception:
                auto_accept_enabled = bool(getattr(settings, "discover_sources_auto_accept_enabled", True))
            if auto_accept_enabled:
                return

            from sqlalchemy import func, select
            from tracker.models import SourceCandidate

            total_new = int(
                session.scalar(select(func.count()).select_from(SourceCandidate).where(SourceCandidate.status == "new"))
                or 0
            )

            # Load state.
            last_total = 0
            try:
                st_raw = (repo.get_app_config("source_candidates_notify_state_json") or "").strip()
                st_obj = json.loads(st_raw) if st_raw else {}
                if isinstance(st_obj, dict) and int(st_obj.get("batch") or 0) == int(batch):
                    last_total = int(st_obj.get("last_notified_total_new") or 0)
            except Exception:
                last_total = 0

            # Reset when backlog is cleared.
            if total_new < batch:
                last_total = 0

            if total_new < (last_total + batch) or total_new < batch:
                return

            new_floor = int(total_new // batch) * int(batch)
            if new_floor <= last_total:
                return

            out_lang_raw = (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
            out_lang = _norm_output_language(out_lang_raw)
            # Use DB-backed toggle (applies without restart) if present.
            try:
                raw_disc = (repo.get_app_config("discover_sources_enabled") or "").strip().lower()
                discover_enabled = False if raw_disc in {"0", "false", "off", "no"} else True
            except Exception:
                discover_enabled = bool(getattr(settings, "discover_sources_enabled", True))

            # Stable cutoff: tie TG actions to the "floor milestone" so repeated clicks are idempotent.
            cutoff_id = 0
            try:
                ids = list(
                    session.scalars(
                        select(SourceCandidate.id)
                        .where(SourceCandidate.status == "new")
                        .order_by(SourceCandidate.id.asc())
                        .limit(int(new_floor))
                    )
                    or []
                )
                if ids:
                    cutoff_id = int(ids[-1] or 0)
            except Exception:
                cutoff_id = 0
            if cutoff_id <= 0:
                return

            if out_lang == "zh":
                text = (
                    f"候选源已累计到 {total_new}（批量阈值={batch}）。\n\n"
                    "请在 Web 管理后台的「追踪 → 智能配置」里审核候选源。\n"
                    "也可以用下面按钮快速接受/忽略本批次。"
                )
                kb = {
                    "inline_keyboard": [
                        [
                            {"text": "✅ 同意（接受本批）", "callback_data": f"cands:accept:{cutoff_id}"},
                            {"text": "🚫 拒绝（忽略本批）", "callback_data": f"cands:ignore:{cutoff_id}"},
                        ],
                        [
                            {
                                "text": ("⏸️ 停止扩源" if discover_enabled else "▶️ 恢复扩源"),
                                "callback_data": ("cands:discover:off" if discover_enabled else "cands:discover:on"),
                            }
                        ],
                    ]
                }
            else:
                text = (
                    f"Source candidates reached {total_new} (batch={batch}).\n\n"
                    "Review them in Web Admin → Tracking → AI Setup.\n"
                    "Or use the buttons below to accept/ignore this batch."
                )
                kb = {
                    "inline_keyboard": [
                        [
                            {"text": "✅ Accept batch", "callback_data": f"cands:accept:{cutoff_id}"},
                            {"text": "🚫 Ignore batch", "callback_data": f"cands:ignore:{cutoff_id}"},
                        ],
                        [
                            {
                                "text": ("⏸️ Pause discovery" if discover_enabled else "▶️ Resume discovery"),
                                "callback_data": ("cands:discover:off" if discover_enabled else "cands:discover:on"),
                            }
                        ],
                    ]
                }

            # Idempotent per batch milestone.
            id_key = f"source_candidates:batch:n{int(new_floor)}"
            try:
                from tracker.push_dispatch import push_telegram_text_card

                sent = await push_telegram_text_card(
                    repo=repo,
                    settings=settings,
                    idempotency_key=id_key,
                    text=text,
                    reply_markup=kb,
                )
                if not sent:
                    return
            except Exception:
                # If push fails, don't advance state.
                return

            try:
                repo.set_app_config(
                    "source_candidates_notify_state_json",
                    json.dumps(
                        {
                            "version": 1,
                            "batch": int(batch),
                            "last_notified_total_new": int(new_floor),
                            "last_notified_cutoff_candidate_id": int(cutoff_id),
                            "updated_at": dt.datetime.utcnow().isoformat() + "Z",
                        },
                        ensure_ascii=False,
                    ),
                )
            except Exception:
                pass
    except Exception:
        return

def _last_fire_time_within(*, trigger: CronTrigger, now: dt.datetime, lookback_seconds: int) -> dt.datetime | None:
    """
    Best-effort "last scheduled time" finder for an APScheduler CronTrigger.

    Why we need this:
    - Our scheduler uses in-memory jobs (no persistent job store).
    - If the service is down during a scheduled cron time, APScheduler won't "remember" the missed run on restart.
    - `misfire_grace_time` only helps when a scheduled run is late while the scheduler is alive.

    So on startup we compute whether a fire time within the grace window was missed and needs a catch-up run.
    """
    try:
        lookback_seconds = int(lookback_seconds)
    except Exception:
        return None
    if lookback_seconds <= 0:
        return None

    start = now - dt.timedelta(seconds=lookback_seconds + 60)  # small buffer
    prev: dt.datetime | None = None
    nxt = trigger.get_next_fire_time(None, start)
    # Iterate bounded by lookback window size. We only need the most recent fire time.
    for _ in range(2000):
        if not nxt or nxt > now:
            break
        prev = nxt
        # Advance at least 1 second to avoid returning the same time repeatedly.
        nxt = trigger.get_next_fire_time(prev, prev + dt.timedelta(seconds=1))

    if prev is None:
        return None
    try:
        if (now - prev).total_seconds() > float(lookback_seconds):
            return None
    except Exception:
        return None
    return prev


async def _run_tick_job(make_session, settings):
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            with make_session() as session:
                await run_tick(session=session, settings=settings, push=True)
    except TimeoutError as exc:
        logger.warning("job lock busy (tick skipped): %s", exc)


async def _run_config_sync_job(make_session, settings) -> None:
    """
    Best-effort env↔DB sync for non-secret settings.

    This is intentionally lightweight: it keeps `.env` and `app_config` consistent so:
    - Web/TG changes (DB) can be exported back to `.env`
    - manual `.env` edits can be reflected into DB-backed overrides
    """
    try:
        async with job_lock_async(name="jobs", timeout_seconds=5):
            with make_session() as session:
                repo = Repo(session)
                from pathlib import Path

                from tracker.dynamic_config import sync_env_and_db

                res = sync_env_and_db(repo=repo, settings=settings, env_path=Path(settings.env_path or ".env"))
                if res.updated_db_keys or res.updated_env_keys:
                    logger.info(
                        "config sync: env_keys=%d db_keys=%d",
                        len(res.updated_env_keys),
                        len(res.updated_db_keys),
                    )
    except TimeoutError:
        return
    except Exception:
        return


async def _run_health_job(make_session, settings):
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            with make_session() as session:
                await run_health_report(session=session, settings=settings, push=True)
    except TimeoutError as exc:
        logger.warning("job lock busy (health skipped): %s", exc)


async def _run_backup_job(settings) -> None:
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            out = run_backup(settings=settings)
            if out:
                logger.info("backup ok: %s", out)
    except TimeoutError as exc:
        logger.warning("job lock busy (backup skipped): %s", exc)
    except Exception as exc:
        logger.warning("backup failed: %s", exc)


async def _run_prune_job(settings) -> None:
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            res = run_prune_ignored(settings=settings)
            logger.info("prune ignored: %s", res)
    except TimeoutError as exc:
        logger.warning("job lock busy (prune skipped): %s", exc)
    except Exception as exc:
        logger.warning("prune failed: %s", exc)


async def _run_discover_sources_job(make_session, settings):
    ok = False
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            with make_session() as session:
                await run_discover_sources(session=session, settings=settings)
                ok = True
    except TimeoutError as exc:
        logger.warning("job lock busy (discover-sources skipped): %s", exc)
    except Exception as exc:
        logger.warning("discover-sources failed: %s", exc)

    # Notify outside the global `jobs` lock (push can take network time).
    if ok:
        try:
            await _maybe_notify_source_candidates_batch(make_session, settings)
        except Exception:
            pass


async def _run_ai_setup_discover_queue_job(make_session, settings):
    """
    Background helper for Web Admin "AI Setup".

    When an operator clicks Apply during a busy period, the API enqueues a discover-sources job
    instead of running it synchronously. This worker drains that queue when the global `jobs`
    lock is available, so candidates eventually appear without manual retries.
    """
    try:
        # Fast-path: avoid taking the global `jobs` lock if there's no queue.
        with make_session() as session:
            repo = Repo(session)
            if not (repo.get_app_config("tracking_ai_setup_discover_queue_json") or "").strip():
                return
    except Exception:
        return

    ok_for_notify = False
    try:
        async with job_lock_async(name="jobs", timeout_seconds=0.0):
            with make_session() as session:
                repo = Repo(session)
                try:
                    from tracker.ai_setup_discover_queue import pop_ai_setup_discover_job, record_ai_setup_discover_status

                    job = pop_ai_setup_discover_job(repo=repo)
                    if not job:
                        return
                except Exception:
                    return

                try:
                    record_ai_setup_discover_status(
                        repo=repo,
                        run_id=int(job.run_id),
                        ok=False,
                        queued=False,
                        running=True,
                        error="",
                        per_topic=[],
                    )
                except Exception:
                    pass

                try:
                    result = await run_discover_sources(
                        session=session,
                        settings=settings,
                        topic_ids=job.topic_ids,
                    )
                    per_topic = []
                    for r in getattr(result, "per_topic", []) or []:
                        per_topic.append(
                            {
                                "topic": str(getattr(r, "topic_name", "") or ""),
                                "pages_checked": int(getattr(r, "pages_checked", 0) or 0),
                                "candidates_created": int(getattr(r, "candidates_created", 0) or 0),
                                "candidates_found": int(getattr(r, "candidates_found", 0) or 0),
                                "errors": int(getattr(r, "errors", 0) or 0),
                            }
                        )
                    try:
                        record_ai_setup_discover_status(
                            repo=repo,
                            run_id=int(job.run_id),
                            ok=True,
                            queued=False,
                            running=False,
                            error="",
                            per_topic=per_topic,
                        )
                    except Exception:
                        pass
                    ok_for_notify = True
                except Exception as exc:
                    try:
                        record_ai_setup_discover_status(
                            repo=repo,
                            run_id=int(job.run_id),
                            ok=False,
                            queued=False,
                            running=False,
                            error=str(exc),
                            per_topic=[],
                        )
                    except Exception:
                        pass
    except TimeoutError:
        return

    # Notify outside the global `jobs` lock (push can take network time).
    try:
        if ok_for_notify:
            await _maybe_notify_source_candidates_batch(make_session, settings)
    except Exception:
        pass


async def _run_source_candidates_notify_job(make_session, settings) -> None:
    """
    Periodic notifier for the SourceCandidate review queue.

    Important: do NOT take the global `jobs` lock here, otherwise notifications would be delayed
    while discover-sources is running (the whole point is to keep operators informed mid-run).
    """
    try:
        await _maybe_notify_source_candidates_batch(make_session, settings)
    except Exception:
        return


async def _run_push_retry_job(make_session, settings):
    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            with make_session() as session:
                await retry_failed_pushes(
                    session=session,
                    settings=settings,
                    max_keys=settings.push_retry_max_keys,
                )
    except TimeoutError as exc:
        logger.warning("job lock busy (push retry skipped): %s", exc)


async def _run_telegram_connect_poll_job(make_session, settings):
    """
    Background helper: bind Telegram chat_id after the operator clicks the connect link.

    This avoids needing to manually hit the "Poll" button in /setup/push.
    """
    if not (getattr(settings, "telegram_bot_token", None) or ""):
        return
    try:
        # Keep Telegram inline buttons responsive: do NOT serialize with the global `jobs` lock.
        # SQLite has WAL + busy_timeout; best-effort concurrent access is OK here.
        async with job_lock_async(name="telegram_poll", timeout_seconds=1):
            with make_session() as session:
                repo = Repo(session)
                try:
                    # Apply DB-backed dynamic overrides for non-secret Settings fields so Web Admin
                    # changes take effect without requiring a restart.
                    try:
                        from tracker.dynamic_config import effective_settings

                        settings = effective_settings(repo=repo, settings=settings)
                    except Exception:
                        pass

                    poll_seconds = int(getattr(settings, "telegram_connect_poll_seconds", 0) or 0)
                    if poll_seconds <= 0:
                        return

                    from tracker.telegram_connect import telegram_poll

                    # If already connected, telegram_poll will send a one-time welcome (if not notified).
                    #
                    # Note: some operators configure `TRACKER_TELEGRAM_CHAT_ID` via `.env` instead of
                    # the /start connect flow (DB app_config). We still want polling enabled for
                    # inline buttons + replies in that mode.
                    if not (repo.get_app_config("telegram_setup_code") or "").strip() and not (
                        (repo.get_app_config("telegram_chat_id") or "").strip()
                        or (getattr(settings, "telegram_chat_id", "") or "").strip()
                    ):
                        return
                    await telegram_poll(repo=repo, settings=settings)
                except Exception as exc:
                    logger.warning("telegram poll job failed: %s", exc)
                    return
    except TimeoutError:
        return


async def _telegram_connect_long_poll_loop(make_session, settings):
    """
    Always-on Telegram polling loop (long-poll).

    Why:
    - APScheduler interval polling adds avoidable latency for inline buttons/reactions.
    - Long-poll keeps the chat UI responsive without hammering Telegram.
    """
    backoff_seconds = 1.0
    while True:
        try:
            # Fast-path disable/unconfigured checks (avoid busy-loop).
            if not (getattr(settings, "telegram_bot_token", None) or ""):
                await asyncio.sleep(2.0)
                continue

            # Avoid busy-loop when polling is disabled or Telegram is not connected.
            try:
                with make_session() as session:
                    repo = Repo(session)
                    try:
                        from tracker.dynamic_config import effective_settings

                        eff = effective_settings(repo=repo, settings=settings)
                    except Exception:
                        eff = settings

                    poll_seconds = int(getattr(eff, "telegram_connect_poll_seconds", 0) or 0)
                    if poll_seconds <= 0:
                        await asyncio.sleep(2.0)
                        continue

                    has_code = bool((repo.get_app_config("telegram_setup_code") or "").strip())
                    has_chat = bool(
                        (repo.get_app_config("telegram_chat_id") or "").strip()
                        or (getattr(eff, "telegram_chat_id", "") or "").strip()
                    )
                    if not (has_code or has_chat):
                        await asyncio.sleep(2.0)
                        continue
            except Exception:
                pass

            # `_run_telegram_connect_poll_job` applies dynamic overrides and blocks on getUpdates
            # (long-poll) when enabled and connected.
            await _run_telegram_connect_poll_job(make_session, settings)
            backoff_seconds = 1.0
            # Small yield: if the poll returns immediately (e.g., lock contention), avoid a tight loop.
            await asyncio.sleep(0.05)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("telegram long-poll loop failed: %s", exc)
            await asyncio.sleep(backoff_seconds)
            backoff_seconds = min(10.0, backoff_seconds * 2.0)

async def _run_telegram_profile_delta_worker_job(make_session, settings):
    """
    Background worker for Telegram-driven, feedback-based Profile delta updates.

    Design:
    - Telegram polling runs under the global `jobs` lock; it must stay fast.
    - Profile delta updates use a reasoning model; this worker runs the LLM call outside `jobs`.
    - Output must be confirmable (avoid profile drift): the worker sends a proposal with inline buttons.
    """
    if not (getattr(settings, "telegram_bot_token", None) or ""):
        return
    if not (
        getattr(settings, "llm_base_url", None)
        and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
    ):
        return

    # Dedicated lock: keep this isolated from portfolio/deep-research locks.
    try:
        async with job_lock_async(name="profile_interactive", timeout_seconds=1):
            # Claim a task quickly under the DB lock.
            try:
                async with job_lock_async(name="jobs", timeout_seconds=5):
                    with make_session() as session:
                        repo = Repo(session)
                        task = repo.claim_next_pending_telegram_task(kind="profile_delta", status="pending", mark_running=True)
                        if not task:
                            return
                        task_id = int(task.id)
                        chat_id = (task.chat_id or "").strip()
                        out_lang = _norm_output_language(
                            (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
                        )

                        # Resolve profile topic/policy.
                        profile_topic_name = (repo.get_app_config("profile_topic_name") or "Profile").strip() or "Profile"
                        topic = repo.get_topic_by_name(profile_topic_name)
                        pol = repo.get_topic_policy(topic_id=int(topic.id)) if topic else None
                        if not (topic and pol and (pol.llm_curation_prompt or "").strip()):
                            repo.mark_telegram_task_failed(task_id, error="profile policy not configured")
                            return

                        # Bootstrap core/delta prompt state.
                        core = (repo.get_app_config("profile_prompt_core") or "").strip()
                        if not core:
                            core = (pol.llm_curation_prompt or "").strip()
                            if core:
                                repo.set_app_config("profile_prompt_core", core)
                        delta = (repo.get_app_config("profile_prompt_delta") or "").strip()

                        # Pick pending feedback events (optionally bounded by a cutoff time).
                        #
                        # NOTE: We primarily consume the pending queue (applied_at is None).
                        # For backward compatibility, if the task includes explicit feedback_ids,
                        # we will union them into the set (best-effort).
                        cutoff_dt: dt.datetime | None = None
                        explicit_ids: list[int] = []
                        try:
                            obj = json.loads((task.query or "").strip() or "{}")
                        except Exception:
                            obj = {}
                        if isinstance(obj, dict):
                            raw_cutoff = str(obj.get("cutoff_utc") or "").strip()
                            if raw_cutoff:
                                try:
                                    cutoff_dt = dt.datetime.fromisoformat(raw_cutoff.replace("Z", "+00:00"))
                                    if cutoff_dt.tzinfo is not None:
                                        cutoff_dt = cutoff_dt.astimezone(dt.timezone.utc).replace(tzinfo=None)
                                except Exception:
                                    cutoff_dt = None
                            raw_ids = obj.get("feedback_ids")
                            if isinstance(raw_ids, list):
                                for x in raw_ids:
                                    try:
                                        n = int(x)
                                    except Exception:
                                        n = 0
                                    if n > 0:
                                        explicit_ids.append(n)

                        pending = repo.list_pending_feedback_events(
                            limit=200,
                            kinds=["like", "dislike", "rate", "profile_note"],
                        )
                        if cutoff_dt is not None:
                            pending = [e for e in pending if getattr(e, "created_at", None) is not None and e.created_at <= cutoff_dt]

                        if (not pending and not explicit_ids) or not core:
                            repo.mark_telegram_task_failed(task_id, error="no feedback events or empty core")
                            return

                        events_rows = [e for e in pending if getattr(e, "applied_at", None) is None]
                        have_ids = {int(getattr(e, "id", 0) or 0) for e in events_rows if int(getattr(e, "id", 0) or 0) > 0}
                        if explicit_ids:
                            try:
                                from tracker.models import FeedbackEvent
                            except Exception:
                                FeedbackEvent = None  # type: ignore[assignment]
                            if FeedbackEvent is not None:
                                for ev_id in explicit_ids:
                                    if ev_id in have_ids:
                                        continue
                                    row = repo.session.get(FeedbackEvent, int(ev_id))
                                    if row is None:
                                        continue
                                    if getattr(row, "applied_at", None) is not None:
                                        continue
                                    if cutoff_dt is not None and getattr(row, "created_at", None) is not None:
                                        if row.created_at > cutoff_dt:
                                            continue
                                    events_rows.append(row)

                        allowed_kinds = {"like", "dislike", "rate", "profile_note"}

                        def _event_text(ev) -> str:  # noqa: ANN001
                            raw = str(getattr(ev, "raw", "") or "").strip()
                            if not raw:
                                return ""
                            try:
                                obj2 = json.loads(raw)
                            except Exception:
                                return ""
                            if not isinstance(obj2, dict):
                                return ""
                            t = obj2.get("text")
                            return str(t or "").strip() if isinstance(t, str) else ""

                        safe_events = []
                        used_ids: list[int] = []
                        for e in events_rows[:50]:
                            if str(getattr(e, "kind", "") or "").strip() not in allowed_kinds:
                                continue
                            used_ids.append(int(e.id))
                            safe_events.append(
                                {
                                    "id": int(e.id),
                                    "kind": str(e.kind or ""),
                                    "value_int": int(e.value_int or 0),
                                    "domain": str(e.domain or ""),
                                    "url": str(e.url or ""),
                                    "note": str(e.note or ""),
                                    "text": _event_text(e),
                                    "created_at": e.created_at.isoformat(),
                                }
                            )
                        if not safe_events:
                            repo.mark_telegram_task_failed(task_id, error="no usable feedback events")
                            return
            except TimeoutError:
                return

            # Run LLM outside the DB lock.
            try:
                from tracker.llm import llm_update_profile_delta_from_feedback
            except Exception as exc:
                err = str(exc) or exc.__class__.__name__
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=f"llm import failed: {err}")
                except Exception:
                    pass
                return

            try:
                s2 = settings.model_copy(update={"output_language": out_lang})  # type: ignore[attr-defined]
            except Exception:
                s2 = settings

            update = None
            err = ""
            try:
                with make_session() as session:
                    repo_llm = Repo(session)
                    update = await llm_update_profile_delta_from_feedback(
                        repo=repo_llm,
                        settings=s2,
                        core_prompt=core,
                        delta_prompt=delta,
                        feedback_events=safe_events,
                        usage_cb=None,
                    )
            except Exception as exc:
                err = str(exc) or exc.__class__.__name__
                update = None

            if not update or not str(getattr(update, "delta_prompt", "") or "").strip():
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=(err or "empty delta"))
                except Exception:
                    pass
                return

            new_delta = str(update.delta_prompt or "").strip()
            if len(new_delta) > 2000:
                new_delta = new_delta[:2000] + "…"
            note = str(getattr(update, "note", "") or "").strip()

            # Compose a compact, confirmable proposal message.
            if out_lang == "zh":
                lines = [
                    "🧠 Profile 更新提案（delta）",
                    f"- 本轮反馈：{len(used_ids)} 条",
                    "",
                    "建议 delta_prompt（将替换现有 delta）：",
                    "--------------------",
                    new_delta,
                    "--------------------",
                ]
                if note:
                    lines += ["", f"note: {note}"]
                lines += [
                    "",
                    "操作：点按钮 Apply / Reject；Edit 需要你回复这条消息粘贴新的 delta（回复 0 取消）。",
                ]
            else:
                lines = [
                    "🧠 Profile delta proposal",
                    f"- feedback events: {len(used_ids)}",
                    "",
                    "Proposed delta_prompt (replaces current delta):",
                    "--------------------",
                    new_delta,
                    "--------------------",
                ]
                if note:
                    lines += ["", f"note: {note}"]
                lines += ["", "Actions: tap Apply/Reject; Edit requires replying with a replacement delta (reply 0 to cancel)."]
            text = "\n".join([ln for ln in lines if ln is not None]).strip()
            if len(text) > 3800:
                text = text[:3790] + "…"

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("✅ Apply" if out_lang != "zh" else "✅ 应用"), "callback_data": f"pd:apply:{task_id}"},
                        {"text": ("✏️ Edit" if out_lang != "zh" else "✏️ 编辑"), "callback_data": f"pd:edit:{task_id}"},
                        {"text": ("❌ Reject" if out_lang != "zh" else "❌ 拒绝"), "callback_data": f"pd:reject:{task_id}"},
                    ]
                ]
            }

            # Send proposal to Telegram (network IO outside DB lock).
            try:
                from tracker.push.telegram import TelegramPusher

                p = TelegramPusher(settings.telegram_bot_token, timeout_seconds=int(getattr(settings, "http_timeout_seconds", 20) or 20))
                prompt_mid = int(
                    await p.send_raw_text(chat_id=chat_id, text=text, disable_preview=True, reply_markup=kb) or 0
                )
            except Exception as exc:
                err2 = str(exc) or exc.__class__.__name__
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=f"telegram send failed: {err2}")
                except Exception:
                    pass
                return

            # Persist proposal under the DB lock.
            try:
                async with job_lock_async(name="jobs", timeout_seconds=60):
                    with make_session() as session:
                        repo = Repo(session)
                        from tracker.models import TelegramTask

                        row = repo.session.get(TelegramTask, int(task_id))
                        if not row:
                            return
                        row.status = "awaiting"
                        if prompt_mid > 0:
                            row.prompt_message_id = int(prompt_mid)
                        row.intent = json.dumps(
                            {"delta_prompt": new_delta, "note": note, "feedback_ids": used_ids},
                            ensure_ascii=False,
                        )
                        row.error = ""
                        repo.session.commit()
            except TimeoutError:
                return
            except Exception:
                return
    except TimeoutError:
        return


async def _run_telegram_prompt_delta_worker_job(make_session, settings):
    """
    Background worker for Telegram-driven, feedback-based Prompt delta updates.

    Scope (v1):
    - Update `research.engine.synth.operator_delta` (small, auditable tail appended to synthesis prompt).
    - Only runs when a `telegram_tasks(kind="prompt_delta")` task exists (explicit operator intent).
    """
    if not (getattr(settings, "telegram_bot_token", None) or ""):
        return
    if not (
        getattr(settings, "llm_base_url", None)
        and (getattr(settings, "llm_model_reasoning", None) or getattr(settings, "llm_model", None))
    ):
        return

    try:
        async with job_lock_async(name="prompt_interactive", timeout_seconds=1):
            # Claim a task quickly under the DB lock.
            try:
                async with job_lock_async(name="jobs", timeout_seconds=5):
                    with make_session() as session:
                        repo = Repo(session)
                        task = repo.claim_next_pending_telegram_task(kind="prompt_delta", status="pending", mark_running=True)
                        if not task:
                            return
                        task_id = int(task.id)
                        chat_id = (task.chat_id or "").strip()
                        out_lang = _norm_output_language(
                            (repo.get_app_config("output_language") or getattr(settings, "output_language", "") or "").strip()
                        )

                        # Parse task payload.
                        try:
                            obj = json.loads((task.query or "").strip() or "{}")
                        except Exception:
                            obj = {}
                        target_slot_id = "research.engine.synth.operator_delta"
                        fb_ids: list[int] = []
                        if isinstance(obj, dict):
                            ts = str(obj.get("target_slot_id") or "").strip()
                            if ts:
                                target_slot_id = ts
                            raw_ids = obj.get("feedback_ids")
                            if isinstance(raw_ids, list):
                                for x in raw_ids:
                                    try:
                                        n = int(x)
                                    except Exception:
                                        n = 0
                                    if n > 0:
                                        fb_ids.append(n)

                        # Load current operator delta (best-effort).
                        target_template_id = ""
                        try:
                            from tracker.prompt_templates import resolve_prompt_best_effort

                            resolved = resolve_prompt_best_effort(
                                repo=repo,
                                settings=settings,
                                slot_id=target_slot_id,
                                language=out_lang,  # type: ignore[arg-type]
                            )
                            cur_delta = resolved.text
                            target_template_id = resolved.template_id
                        except Exception:
                            cur_delta = ""
                            target_template_id = ""

                        # Pick feedback ids if omitted.
                        if not fb_ids:
                            pending = repo.list_pending_feedback_events(limit=20, kinds=["prompt_note"])
                            fb_ids = [int(e.id) for e in pending if int(getattr(e, "id", 0) or 0) > 0]

                        if not fb_ids:
                            repo.mark_telegram_task_failed(task_id, error="no prompt feedback events")
                            return

                        # Load events.
                        try:
                            from tracker.models import FeedbackEvent
                        except Exception:
                            FeedbackEvent = None  # type: ignore[assignment]
                        events_rows = []
                        if FeedbackEvent is not None:
                            for ev_id in fb_ids:
                                row = repo.session.get(FeedbackEvent, int(ev_id))
                                if row is None:
                                    continue
                                if getattr(row, "applied_at", None) is not None:
                                    continue
                                events_rows.append(row)

                        def _event_text(ev) -> str:  # noqa: ANN001
                            raw = str(getattr(ev, "raw", "") or "").strip()
                            if not raw:
                                return ""
                            try:
                                obj2 = json.loads(raw)
                            except Exception:
                                return ""
                            if not isinstance(obj2, dict):
                                return ""
                            t = obj2.get("text")
                            return str(t or "").strip() if isinstance(t, str) else ""

                        safe_events = []
                        used_ids: list[int] = []
                        for e in events_rows[:50]:
                            if str(getattr(e, "kind", "") or "").strip() != "prompt_note":
                                continue
                            used_ids.append(int(e.id))
                            safe_events.append(
                                {
                                    "id": int(e.id),
                                    "kind": str(e.kind or ""),
                                    "value_int": int(e.value_int or 0),
                                    "domain": str(e.domain or ""),
                                    "url": str(e.url or ""),
                                    "note": str(e.note or ""),
                                    "text": _event_text(e),
                                    "created_at": e.created_at.isoformat(),
                                }
                            )
                        if not safe_events:
                            repo.mark_telegram_task_failed(task_id, error="no usable prompt_note events")
                            return
            except TimeoutError:
                return

            # Run LLM outside the DB lock.
            try:
                from tracker.llm import llm_update_prompt_delta_from_feedback
            except Exception as exc:
                err = str(exc) or exc.__class__.__name__
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=err)
                except Exception:
                    pass
                return

            update = None
            err = ""
            try:
                # Force output language so prompt templates pick the correct zh/en variant.
                try:
                    s2 = settings.model_copy(update={"output_language": out_lang})  # type: ignore[attr-defined]
                except Exception:
                    s2 = settings
                with make_session() as session:
                    repo_llm = Repo(session)
                    update = await llm_update_prompt_delta_from_feedback(
                        repo=repo_llm,
                        settings=s2,
                        target_slot_id=target_slot_id,
                        current_delta_prompt=cur_delta,
                        feedback_events=safe_events,
                        usage_cb=None,
                    )
            except Exception as exc:
                err = str(exc) or exc.__class__.__name__
                update = None

            if not update or not str(getattr(update, "delta_prompt", "") or "").strip():
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=(err or "empty delta"))
                except Exception:
                    pass
                return

            new_delta = str(update.delta_prompt or "").strip()
            if len(new_delta) > 2000:
                new_delta = new_delta[:2000] + "…"
            note = str(getattr(update, "note", "") or "").strip()

            is_zh = out_lang == "zh"
            cur_short = (cur_delta or "").strip()
            if len(cur_short) > 800:
                cur_short = cur_short[:800] + "…"
            new_short = new_delta
            if len(new_short) > 800:
                new_short = new_short[:800] + "…"

            if is_zh:
                lines = [
                    "🧩 提示词更新提案（delta）",
                    f"- target: {target_slot_id}",
                    f"- 本轮反馈：{len(used_ids)} 条",
                    "",
                    "当前 delta（将被替换）：",
                    "--------------------",
                    (cur_short or "（空）"),
                    "--------------------",
                    "",
                    "建议 delta（可编辑）：",
                    "--------------------",
                    new_short,
                    "--------------------",
                ]
                if note:
                    lines += ["", f"note: {note}"]
                lines += ["", "操作：点按钮 Apply / Reject；Edit 需要你回复这条消息粘贴新的 delta（回复 0 取消）。"]
            else:
                lines = [
                    "🧩 Prompt delta proposal",
                    f"- target: {target_slot_id}",
                    f"- feedback events: {len(used_ids)}",
                    "",
                    "Current delta (will be replaced):",
                    "--------------------",
                    (cur_short or "(empty)"),
                    "--------------------",
                    "",
                    "Proposed delta (editable):",
                    "--------------------",
                    new_short,
                    "--------------------",
                ]
                if note:
                    lines += ["", f"note: {note}"]
                lines += ["", "Actions: tap Apply/Reject; Edit requires replying with a replacement delta (reply 0 to cancel)."]
            text = "\n".join([ln for ln in lines if ln is not None]).strip()
            if len(text) > 3800:
                text = text[:3790] + "…"

            kb = {
                "inline_keyboard": [
                    [
                        {"text": ("✅ Apply" if not is_zh else "✅ 应用"), "callback_data": f"td:apply:{task_id}"},
                        {"text": ("✏️ Edit" if not is_zh else "✏️ 编辑"), "callback_data": f"td:edit:{task_id}"},
                        {"text": ("❌ Reject" if not is_zh else "❌ 拒绝"), "callback_data": f"td:reject:{task_id}"},
                    ]
                ]
            }

            # Send proposal to Telegram (network IO outside DB lock).
            try:
                from tracker.push.telegram import TelegramPusher

                p = TelegramPusher(
                    settings.telegram_bot_token,
                    timeout_seconds=int(getattr(settings, "http_timeout_seconds", 20) or 20),
                )
                prompt_mid = int(
                    await p.send_raw_text(chat_id=chat_id, text=text, disable_preview=True, reply_markup=kb) or 0
                )
            except Exception as exc:
                err2 = str(exc) or exc.__class__.__name__
                try:
                    async with job_lock_async(name="jobs", timeout_seconds=30):
                        with make_session() as session:
                            repo = Repo(session)
                            repo.mark_telegram_task_failed(task_id, error=f"telegram send failed: {err2}")
                except Exception:
                    pass
                return

            # Persist proposal under the DB lock.
            try:
                async with job_lock_async(name="jobs", timeout_seconds=60):
                    with make_session() as session:
                        repo = Repo(session)
                        from tracker.models import TelegramTask

                        row = repo.session.get(TelegramTask, int(task_id))
                        if not row:
                            return
                        row.status = "awaiting"
                        if prompt_mid > 0:
                            row.prompt_message_id = int(prompt_mid)
                        row.intent = json.dumps(
                            {
                                "target_slot_id": target_slot_id,
                                "target_template_id": target_template_id,
                                "lang": out_lang,
                                "current_delta_prompt": cur_delta,
                                "delta_prompt": new_delta,
                                "note": note,
                                "feedback_ids": used_ids,
                            },
                            ensure_ascii=False,
                        )
                        row.error = ""
                        repo.session.commit()
            except TimeoutError:
                return
            except Exception:
                return
    except TimeoutError:
        return


async def _run_curated_job(make_session, settings, digest_sem: asyncio.Semaphore):
    """
    Run ONE cross-topic Curated Info batch.

    This is the primary "batch noise reduction" surface:
    - De-dupe only (no interpretation)
    - Stable snapshot (new message per run on Telegram)
    """
    async with digest_sem:
        # Curated window/push settings should honor DB-backed overrides without restart,
        # so compute an effective Settings snapshot per run.
        eff_settings = settings
        try:
            with make_session() as session:
                repo0 = Repo(session)
                from tracker.dynamic_config import effective_settings

                eff_settings = effective_settings(repo=repo0, settings=settings)
        except Exception:
            eff_settings = settings

        try:
            async with job_lock_async(name="jobs", timeout_seconds=300):
                with make_session() as session:
                    # Guard against stringy values like "0" coming from dynamic config.
                    hours = 24
                    try:
                        hours = int(getattr(eff_settings, "digest_hours", 24))
                    except Exception:
                        hours = 24
                    if hours <= 0:
                        hours = 24
                    from tracker.runner import run_curated_info

                    await run_curated_info(
                        session=session,
                        settings=eff_settings,
                        hours=hours,
                        push=bool(getattr(eff_settings, "digest_push_enabled", True)),
                    )
        except TimeoutError as exc:
            logger.warning("job lock busy (curated skipped): %s", exc)


async def _run_digest_job(make_session, settings, topic_id: int):
    # Digest window/push settings should honor DB-backed overrides without restart,
    # so compute an effective Settings snapshot per run.
    eff_settings = settings
    try:
        with make_session() as session:
            repo0 = Repo(session)
            from tracker.dynamic_config import effective_settings

            eff_settings = effective_settings(repo=repo0, settings=settings)
    except Exception:
        eff_settings = settings

    try:
        async with job_lock_async(name="jobs", timeout_seconds=300):
            with make_session() as session:
                # Guard against stringy values like "0" coming from dynamic config.
                hours = 24
                try:
                    hours = int(getattr(eff_settings, "digest_hours", 24))
                except Exception:
                    hours = 24
                if hours <= 0:
                    hours = 24
                await run_digest(
                    session=session,
                    settings=eff_settings,
                    hours=hours,
                    push=bool(getattr(eff_settings, "digest_push_enabled", True)),
                    topic_ids=[topic_id],
                )
    except TimeoutError as exc:
        logger.warning("job lock busy (digest skipped): %s", exc)


async def _run_digest_job_limited(make_session, settings, topic_id: int, digest_sem: asyncio.Semaphore):
    async with digest_sem:
        await _run_digest_job(make_session, settings, topic_id)


async def _sync_digest_jobs(
    scheduler: AsyncIOScheduler,
    make_session,
    settings,
    digest_cron_map: dict[str, str],
    digest_sem: asyncio.Semaphore,
):
    tz = _cron_timezone(settings)
    misfire = _misfire_grace_seconds(settings)
    desired: dict[str, tuple[int, str]] = {}
    with make_session() as session:
        repo = Repo(session)
        for topic in repo.list_topics():
            if not topic.enabled:
                continue
            job_id = f"digest:{topic.id}"
            desired[job_id] = (topic.id, topic.digest_cron)

    # Remove obsolete jobs.
    for job_id in list(digest_cron_map.keys()):
        if job_id not in desired:
            if scheduler.get_job(job_id):
                scheduler.remove_job(job_id)
            digest_cron_map.pop(job_id, None)

    # Add/update desired jobs.
    for job_id, (topic_id, cron) in desired.items():
        if digest_cron_map.get(job_id) == cron and scheduler.get_job(job_id):
            continue
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)
        try:
            trigger = CronTrigger.from_crontab(cron, timezone=tz)
        except Exception:
            digest_cron_map.pop(job_id, None)
            continue
        scheduler.add_job(
            _run_digest_job_limited,
            trigger=trigger,
            args=[make_session, settings, topic_id, digest_sem],
            id=job_id,
            max_instances=1,
            coalesce=True,
            misfire_grace_time=misfire,
        )
        digest_cron_map[job_id] = cron


def _curated_cron_for_hours(hours: int) -> str:
    """
    Derive Curated Info schedule from the lookback window (hours).

    Example:
    - 2  -> run at minute 0 every 2 hours (00:00, 02:00, ...)
    - 24+ -> run daily at 00:00
    """
    try:
        h = int(hours or 0)
    except Exception:
        h = 24
    if h <= 0:
        h = 24
    if h >= 24:
        return "0 0 * * *"
    return f"0 */{h} * * *"


async def _sync_curated_job_from_digest_hours(
    scheduler: AsyncIOScheduler,
    make_session,
    settings,
    curated_cron_map: dict[str, str],
    digest_sem: asyncio.Semaphore,
):
    """
    Schedule ONE cross-topic Curated Info batch.

    Cadence is derived from Settings.digest_hours (no separate cron knob).
    """
    tz = _cron_timezone(settings)
    misfire = _misfire_grace_seconds(settings)

    eff = settings
    try:
        with make_session() as session:
            repo0 = Repo(session)
            from tracker.dynamic_config import effective_settings

            eff = effective_settings(repo=repo0, settings=settings)
    except Exception:
        eff = settings

    try:
        hours = int(getattr(eff, "digest_hours", 24) or 24)
    except Exception:
        hours = 24
    cron = _curated_cron_for_hours(hours)

    job_id = "digest:curated"
    if curated_cron_map.get(job_id) == cron and scheduler.get_job(job_id):
        return
    if scheduler.get_job(job_id):
        scheduler.remove_job(job_id)
    try:
        trigger = CronTrigger.from_crontab(cron, timezone=tz)
    except Exception:
        curated_cron_map.pop(job_id, None)
        return
    scheduler.add_job(
        _run_curated_job,
        trigger=trigger,
        args=[make_session, settings, digest_sem],
        id=job_id,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=misfire,
    )
    curated_cron_map[job_id] = cron


async def _install_digest_scheduler_jobs(
    scheduler: AsyncIOScheduler,
    make_session,
    settings,
    *,
    misfire: int,
) -> asyncio.Semaphore:
    digest_sem = asyncio.Semaphore(max(1, settings.max_concurrent_digests))
    if not settings.digest_scheduler_enabled:
        return digest_sem

    digest_cron_map: dict[str, str] = {}
    curated_cron_map: dict[str, str] = {}

    await _sync_digest_jobs(scheduler, make_session, settings, digest_cron_map, digest_sem)
    scheduler.add_job(
        _sync_digest_jobs,
        "interval",
        seconds=300,
        args=[scheduler, make_session, settings, digest_cron_map, digest_sem],
        id="digest:sync",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=misfire,
    )

    await _sync_curated_job_from_digest_hours(scheduler, make_session, settings, curated_cron_map, digest_sem)
    scheduler.add_job(
        _sync_curated_job_from_digest_hours,
        "interval",
        seconds=300,
        args=[scheduler, make_session, settings, curated_cron_map, digest_sem],
        id="curated:sync",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=misfire,
    )
    return digest_sem


async def serve_forever() -> None:
    settings = get_settings()
    configure_logging(level=settings.log_level)
    engine, make_session = session_factory(settings)
    Base.metadata.create_all(engine)

    # Best-effort: keep `.env` and DB settings overrides consistent across deploys/edits.
    try:
        with make_session() as session:
            repo = Repo(session)
            from pathlib import Path

            from tracker.dynamic_config import sync_env_and_db

            sync_env_and_db(repo=repo, settings=settings, env_path=Path(settings.env_path or ".env"))
    except Exception:
        pass

    tz = _cron_timezone(settings)
    misfire = _misfire_grace_seconds(settings)
    scheduler = AsyncIOScheduler(timezone=tz)

    if settings.health_report_cron:
        try:
            trigger = CronTrigger.from_crontab(settings.health_report_cron, timezone=tz)
        except Exception:
            trigger = None
        if trigger:
            scheduler.add_job(
                _run_health_job,
                trigger=trigger,
                args=[make_session, settings],
                id="health:daily",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
            )
    if settings.backup_cron:
        try:
            trigger = CronTrigger.from_crontab(settings.backup_cron, timezone=tz)
        except Exception:
            trigger = None
        if trigger:
            scheduler.add_job(
                _run_backup_job,
                trigger=trigger,
                args=[settings],
                id="db:backup",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
            )

    if settings.prune_ignored_cron:
        try:
            trigger = CronTrigger.from_crontab(settings.prune_ignored_cron, timezone=tz)
        except Exception:
            trigger = None
        if trigger:
            scheduler.add_job(
                _run_prune_job,
                trigger=trigger,
                args=[settings],
                id="db:prune_ignored",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
            )

    if settings.discover_sources_cron:
        try:
            trigger = CronTrigger.from_crontab(settings.discover_sources_cron, timezone=tz)
        except Exception:
            trigger = None
        if trigger:
            scheduler.add_job(
                _run_discover_sources_job,
                trigger=trigger,
                args=[make_session, settings],
                id="sources:discover",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
            )

    if settings.push_retry_cron:
        try:
            trigger = CronTrigger.from_crontab(settings.push_retry_cron, timezone=tz)
        except Exception:
            trigger = None
        if trigger:
            scheduler.add_job(
                _run_push_retry_job,
                trigger=trigger,
                args=[make_session, settings],
                id="push:retry_failed",
                max_instances=1,
                coalesce=True,
                misfire_grace_time=misfire,
            )
    scheduler.add_job(
        _run_tick_job,
        "interval",
        seconds=settings.alert_poll_seconds,
        args=[make_session, settings],
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _run_config_sync_job,
        "interval",
        seconds=60,
        args=[make_session, settings],
        id="config:sync_env_db",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _run_source_candidates_notify_job,
        "interval",
        seconds=60,
        args=[make_session, settings],
        id="candidates:notify_batch",
        max_instances=1,
        coalesce=True,
    )

    # Telegram polling is handled by an always-on long-poll loop (started after scheduler.start()).

    # Background worker for feedback-driven Profile delta proposals (Telegram).
    # Runs fast when idle (no tasks), and keeps polling responsive by moving reasoning calls off `jobs`.
    scheduler.add_job(
        _run_telegram_profile_delta_worker_job,
        "interval",
        seconds=10,
        args=[make_session, settings],
        id="telegram:profile_delta_worker",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        _run_telegram_prompt_delta_worker_job,
        "interval",
        seconds=10,
        args=[make_session, settings],
        id="telegram:prompt_delta_worker",
        max_instances=1,
        coalesce=True,
    )

    # Background worker: drain the AI Setup discover-sources queue.
    # Runs only when there is work queued and the global `jobs` lock is available.
    scheduler.add_job(
        _run_ai_setup_discover_queue_job,
        "interval",
        seconds=10,
        args=[make_session, settings],
        id="tracking:ai_setup_discover_queue",
        max_instances=1,
        coalesce=True,
    )

    await _install_digest_scheduler_jobs(
        scheduler,
        make_session,
        settings,
        misfire=misfire,
    )

    scheduler.start()

    # Always-on Telegram long-poll loop (inline buttons/reactions/replies).
    try:
        asyncio.create_task(_telegram_connect_long_poll_loop(make_session, settings))
    except Exception:
        pass

    await asyncio.Event().wait()
