from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager, contextmanager
from pathlib import Path

try:
    import fcntl  # type: ignore[attr-defined]
except Exception:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]


def _sanitize_lock_name(name: str) -> str:
    s = (name or "").strip() or "lock"
    out: list[str] = []
    for ch in s:
        if ch.isalnum() or ch in {"-", "_", "."}:
            out.append(ch)
        else:
            out.append("_")
    return ("".join(out)[:120] or "lock").strip("._") or "lock"


def job_lock_path(*, name: str) -> Path:
    # Keep the lock under the working directory so systemd user services
    # (tracker + tracker-api) share it via WorkingDirectory=%h/tracker.
    safe = _sanitize_lock_name(name)
    return Path(".tracker_locks") / f"{safe}.lock"


@contextmanager
def job_lock(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):
    """
    Inter-process exclusive lock (best-effort) to avoid SQLite write contention.

    - Uses POSIX flock via fcntl when available.
    - On non-POSIX platforms, becomes a no-op (still correct for dev, but weaker).
    """
    if fcntl is None:  # pragma: no cover
        yield
        return

    path = job_lock_path(name=name)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        start = time.monotonic()
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if timeout_seconds and (time.monotonic() - start) >= float(timeout_seconds):
                    raise TimeoutError(f"job lock busy: {path}")
                time.sleep(max(0.05, float(poll_seconds)))
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(fd)
        except Exception:
            pass


@asynccontextmanager
async def job_lock_async(*, name: str, timeout_seconds: float = 0.0, poll_seconds: float = 0.2):
    """
    Async version of job_lock().
    """
    if fcntl is None:  # pragma: no cover
        yield
        return

    path = job_lock_path(name=name)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(path), os.O_CREAT | os.O_RDWR, 0o600)
    try:
        start = time.monotonic()
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if timeout_seconds and (time.monotonic() - start) >= float(timeout_seconds):
                    raise TimeoutError(f"job lock busy: {path}")
                await asyncio.sleep(max(0.05, float(poll_seconds)))
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(fd)
        except Exception:
            pass
