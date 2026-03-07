from __future__ import annotations

import asyncio
import hashlib
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


def _lock_scope_basis() -> str:
    parts: list[str] = []

    instance = (os.environ.get("OPENINFOMATE_INSTANCE") or "").strip()
    if instance:
        parts.append(f"instance={instance}")

    env_path = (os.environ.get("TRACKER_ENV_PATH") or "").strip()
    if env_path:
        try:
            env_path = str(Path(env_path).expanduser().resolve(strict=False))
        except Exception:
            env_path = str(Path(env_path).expanduser())
        parts.append(f"env={env_path}")

    db_url = (os.environ.get("TRACKER_DB_URL") or "").strip()
    if db_url:
        if db_url.startswith("sqlite:///"):
            raw_path = db_url[len("sqlite:///") :]
            try:
                raw_path = str(Path(raw_path).expanduser().resolve(strict=False))
            except Exception:
                raw_path = str(Path(raw_path).expanduser())
            db_url = f"sqlite:///{raw_path}"
        parts.append(f"db={db_url}")

    if not parts:
        try:
            parts.append(f"cwd={Path.cwd().resolve()}")
        except Exception:
            parts.append(f"cwd={Path.cwd()}")
    return "|".join(parts)


def job_lock_path(*, name: str) -> Path:
    safe = _sanitize_lock_name(name)
    scope = _lock_scope_basis().encode("utf-8", errors="ignore")
    digest = hashlib.sha1(scope).hexdigest()[:12]
    root = Path.home() / ".local" / "state" / "openinfomate" / "locks" / digest
    return root / f"{safe}.lock"


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
