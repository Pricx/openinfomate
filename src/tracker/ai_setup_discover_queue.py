from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass
from typing import Any

from tracker.repo import Repo


QUEUE_KEY = "tracking_ai_setup_discover_queue_json"
STATUS_KEY = "tracking_ai_setup_discover_last_json"


@dataclass(frozen=True)
class AiSetupDiscoverJob:
    run_id: int
    topic_ids: list[int]
    created_at: str
    attempts: int = 0


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
    return json.dumps({"version": 1, "queue": queue}, ensure_ascii=False)


def _normalize_topic_ids(topic_ids: list[int] | None) -> list[int]:
    ids = [int(x) for x in (topic_ids or []) if int(x or 0) > 0]
    return list(dict.fromkeys(ids))[:200]


def enqueue_ai_setup_discover_job(*, repo: Repo, run_id: int, topic_ids: list[int]) -> bool:
    """
    Enqueue a discover-sources job for the tracker service to run soon.

    This is used by Web Admin "AI Setup" Apply when the global `jobs` lock is busy.
    """
    rid = int(run_id or 0)
    ids = _normalize_topic_ids(topic_ids)
    if rid <= 0 or not ids:
        return False

    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    q0 = obj.get("queue")
    queue: list[dict[str, Any]] = q0 if isinstance(q0, list) else []

    updated = False
    for it in queue[:200]:
        if not isinstance(it, dict):
            continue
        try:
            if int(it.get("run_id") or 0) != rid:
                continue
        except Exception:
            continue
        prev = _normalize_topic_ids(it.get("topic_ids"))  # type: ignore[arg-type]
        merged = list(dict.fromkeys([*prev, *ids]))[:200]
        if merged != prev:
            it["topic_ids"] = merged
            updated = True
        break
    else:
        queue.append(
            {
                "run_id": rid,
                "topic_ids": ids,
                "created_at": dt.datetime.utcnow().isoformat() + "Z",
                "attempts": 0,
            }
        )
        updated = True

    if updated:
        repo.set_app_config(QUEUE_KEY, _dump_queue_obj(queue))
    return updated


def pop_ai_setup_discover_job(*, repo: Repo) -> AiSetupDiscoverJob | None:
    raw = repo.get_app_config(QUEUE_KEY) or ""
    obj = _load_queue_obj(raw)
    q0 = obj.get("queue")
    queue: list[dict[str, Any]] = q0 if isinstance(q0, list) else []
    if not queue:
        return None

    first = queue.pop(0)
    if queue:
        repo.set_app_config(QUEUE_KEY, _dump_queue_obj(queue))
    else:
        # Keep the key empty to avoid diff churn in exports.
        repo.delete_app_config(QUEUE_KEY)

    if not isinstance(first, dict):
        return None
    try:
        rid = int(first.get("run_id") or 0)
    except Exception:
        rid = 0
    ids = _normalize_topic_ids(first.get("topic_ids"))  # type: ignore[arg-type]
    created_at = str(first.get("created_at") or "").strip() or (dt.datetime.utcnow().isoformat() + "Z")
    try:
        attempts = int(first.get("attempts") or 0)
    except Exception:
        attempts = 0
    if rid <= 0 or not ids:
        return None
    return AiSetupDiscoverJob(run_id=rid, topic_ids=ids, created_at=created_at, attempts=max(0, attempts))


def record_ai_setup_discover_status(
    *,
    repo: Repo,
    run_id: int,
    ok: bool,
    queued: bool = False,
    running: bool = False,
    error: str = "",
    per_topic: list[dict[str, Any]] | None = None,
) -> None:
    payload = {
        "version": 1,
        "run_id": int(run_id or 0),
        "ok": bool(ok),
        "queued": bool(queued),
        "running": bool(running),
        "error": str(error or "")[:2000],
        "per_topic": list(per_topic or [])[:200],
        "updated_at": dt.datetime.utcnow().isoformat() + "Z",
    }
    repo.set_app_config(STATUS_KEY, json.dumps(payload, ensure_ascii=False))
