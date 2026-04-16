from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any

from tracker.repo import Repo


QUEUE_KEY = "curated_recovery_queue_json"
STATUS_KEY = "curated_recovery_last_json"


@dataclass(frozen=True)
class CuratedRecoveryJob:
    window_end_utc: str
    hours: int
    push: bool
    pending_topic_ids: list[int]
    created_at: str
    attempts: int = 0
    last_error: str = ""
    last_attempt_at: str = ""


def _iso_utc(value: dt.datetime | None = None) -> str:
    ts = value or dt.datetime.utcnow()
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts.replace(microsecond=0).isoformat() + "Z"


def parse_iso_utc(value: object) -> dt.datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        ts = dt.datetime.fromisoformat(raw)
    except Exception:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return ts


def recovery_key_suffix(*, window_end_utc: str) -> str:
    ts = parse_iso_utc(window_end_utc) or dt.datetime.utcnow()
    return f"recovery-{ts.strftime('%Y%m%d%H%M%S')}"


def _normalize_window_end(value: object) -> str:
    ts = parse_iso_utc(value)
    if ts is None:
        return _iso_utc()
    return _iso_utc(ts)


def _normalize_hours(hours: int | None) -> int:
    try:
        value = int(hours or 0)
    except Exception:
        value = 0
    if value <= 0:
        return 24
    return min(168, value)


def _normalize_topic_ids(topic_ids: list[int] | None) -> list[int]:
    out: list[int] = []
    for value in topic_ids or []:
        try:
            item = int(value or 0)
        except Exception:
            continue
        if item <= 0 or item in out:
            continue
        out.append(item)
        if len(out) >= 200:
            break
    return out


def _load_queue_obj(raw: str) -> dict[str, Any]:
    text = (raw or "").strip()
    if not text:
        return {"version": 1, "queue": []}
    try:
        obj = json.loads(text)
    except Exception:
        return {"version": 1, "queue": []}
    if isinstance(obj, list):
        return {"version": 1, "queue": obj}
    return obj if isinstance(obj, dict) else {"version": 1, "queue": []}


def _dump_queue_obj(queue: list[dict[str, Any]]) -> str:
    return json.dumps({"version": 1, "queue": queue}, ensure_ascii=False, sort_keys=True)


def _job_matches(payload: dict[str, Any], *, window_end_utc: str, hours: int, push: bool) -> bool:
    return (
        str(payload.get("window_end_utc") or "").strip() == window_end_utc
        and int(payload.get("hours") or 0) == hours
        and bool(payload.get("push")) is push
    )


def has_curated_recovery_jobs(*, repo: Repo) -> bool:
    return bool((repo.get_app_config(QUEUE_KEY) or "").strip())


def enqueue_curated_recovery_job(
    *,
    repo: Repo,
    window_end_utc: str,
    hours: int,
    push: bool,
    pending_topic_ids: list[int] | None,
    last_error: str = "",
) -> bool:
    norm_window_end = _normalize_window_end(window_end_utc)
    norm_hours = _normalize_hours(hours)
    norm_push = bool(push)
    norm_topics = _normalize_topic_ids(pending_topic_ids)
    if not norm_topics:
        return False

    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    queue0 = obj.get("queue")
    queue: list[dict[str, Any]] = queue0 if isinstance(queue0, list) else []

    updated = False
    for item in queue[:200]:
        if not isinstance(item, dict):
            continue
        if not _job_matches(item, window_end_utc=norm_window_end, hours=norm_hours, push=norm_push):
            continue
        merged_topics = list(dict.fromkeys([*_normalize_topic_ids(item.get("pending_topic_ids")), *norm_topics]))[:200]
        if merged_topics != _normalize_topic_ids(item.get("pending_topic_ids")):
            item["pending_topic_ids"] = merged_topics
            updated = True
        if last_error and str(item.get("last_error") or "").strip() != last_error.strip():
            item["last_error"] = last_error.strip()[:1000]
            updated = True
        break
    else:
        queue.append(
            {
                "window_end_utc": norm_window_end,
                "hours": norm_hours,
                "push": norm_push,
                "pending_topic_ids": norm_topics,
                "created_at": _iso_utc(),
                "attempts": 0,
                "last_error": last_error.strip()[:1000],
                "last_attempt_at": "",
            }
        )
        updated = True

    if updated:
        repo.set_app_config(QUEUE_KEY, _dump_queue_obj(queue))
    return updated


def peek_curated_recovery_job(*, repo: Repo) -> CuratedRecoveryJob | None:
    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    queue0 = obj.get("queue")
    queue: list[dict[str, Any]] = queue0 if isinstance(queue0, list) else []
    if not queue:
        return None
    first = queue[0]
    if not isinstance(first, dict):
        return None
    window_end_utc = _normalize_window_end(first.get("window_end_utc"))
    hours = _normalize_hours(int(first.get("hours") or 0))
    push = bool(first.get("push"))
    pending_topic_ids = _normalize_topic_ids(first.get("pending_topic_ids"))
    if not pending_topic_ids:
        return None
    try:
        attempts = int(first.get("attempts") or 0)
    except Exception:
        attempts = 0
    return CuratedRecoveryJob(
        window_end_utc=window_end_utc,
        hours=hours,
        push=push,
        pending_topic_ids=pending_topic_ids,
        created_at=str(first.get("created_at") or "").strip() or _iso_utc(),
        attempts=max(0, attempts),
        last_error=str(first.get("last_error") or "").strip()[:1000],
        last_attempt_at=str(first.get("last_attempt_at") or "").strip(),
    )


def complete_curated_recovery_job(*, repo: Repo, job: CuratedRecoveryJob) -> bool:
    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    queue0 = obj.get("queue")
    queue: list[dict[str, Any]] = queue0 if isinstance(queue0, list) else []
    if not queue:
        return False
    new_queue: list[dict[str, Any]] = []
    removed = False
    for item in queue:
        if removed or not isinstance(item, dict):
            new_queue.append(item)
            continue
        if _job_matches(item, window_end_utc=job.window_end_utc, hours=job.hours, push=job.push):
            removed = True
            continue
        new_queue.append(item)
    if not removed:
        return False
    if new_queue:
        repo.set_app_config(QUEUE_KEY, _dump_queue_obj(new_queue))
    else:
        repo.delete_app_config(QUEUE_KEY)
    return True


def complete_curated_recovery_job_for_window(
    *,
    repo: Repo,
    window_end_utc: str,
    hours: int,
    push: bool,
) -> bool:
    job = CuratedRecoveryJob(
        window_end_utc=_normalize_window_end(window_end_utc),
        hours=_normalize_hours(hours),
        push=bool(push),
        pending_topic_ids=[],
        created_at=_iso_utc(),
    )
    return complete_curated_recovery_job(repo=repo, job=job)


def record_curated_recovery_attempt(
    *,
    repo: Repo,
    job: CuratedRecoveryJob,
    error: str,
    pending_topic_ids: list[int] | None = None,
) -> CuratedRecoveryJob:
    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    queue0 = obj.get("queue")
    queue: list[dict[str, Any]] = queue0 if isinstance(queue0, list) else []
    updated_job = job
    for item in queue:
        if not isinstance(item, dict):
            continue
        if not _job_matches(item, window_end_utc=job.window_end_utc, hours=job.hours, push=job.push):
            continue
        attempts = max(0, int(item.get("attempts") or 0)) + 1
        topics = _normalize_topic_ids(pending_topic_ids or job.pending_topic_ids)
        item["attempts"] = attempts
        item["pending_topic_ids"] = topics
        item["last_error"] = str(error or "").strip()[:1000]
        item["last_attempt_at"] = _iso_utc()
        repo.set_app_config(QUEUE_KEY, _dump_queue_obj(queue))
        updated_job = CuratedRecoveryJob(
            window_end_utc=job.window_end_utc,
            hours=job.hours,
            push=job.push,
            pending_topic_ids=topics,
            created_at=str(item.get("created_at") or "").strip() or job.created_at,
            attempts=attempts,
            last_error=str(item.get("last_error") or "").strip(),
            last_attempt_at=str(item.get("last_attempt_at") or "").strip(),
        )
        break
    return updated_job


def record_curated_recovery_status(
    *,
    repo: Repo,
    job: CuratedRecoveryJob | None,
    ok: bool,
    queued: bool = False,
    running: bool = False,
    error: str = "",
) -> None:
    payload = {
        "version": 1,
        "ok": bool(ok),
        "queued": bool(queued),
        "running": bool(running),
        "error": str(error or "").strip()[:1000],
        "updated_at": _iso_utc(),
    }
    if job is not None:
        payload.update(
            {
                "window_end_utc": job.window_end_utc,
                "hours": int(job.hours or 24),
                "push": bool(job.push),
                "pending_topic_ids": list(job.pending_topic_ids),
                "attempts": int(job.attempts or 0),
            }
        )
    repo.set_app_config(STATUS_KEY, json.dumps(payload, ensure_ascii=False, sort_keys=True))
