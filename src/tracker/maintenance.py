from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
import sqlite3

from sqlalchemy.engine.url import make_url

from tracker.db import session_factory
from tracker.repo import Repo
from tracker.settings import Settings

logger = logging.getLogger(__name__)


def sqlite_db_path_from_url(*, db_url: str, cwd: Path | None = None) -> Path | None:
    """
    Return a filesystem path for file-based SQLite URLs, else None.

    Supported:
      - sqlite:///relative.db
      - sqlite:////abs/path.db
    Not supported:
      - :memory:
      - non-sqlite URLs
    """
    try:
        url = make_url(db_url)
    except Exception:
        return None
    if url.drivername != "sqlite":
        return None
    if not url.database or url.database == ":memory:":
        return None
    p = Path(url.database)
    if not p.is_absolute():
        p = (cwd or Path.cwd()) / p
    return p


def backup_sqlite_db(*, db_path: Path, backup_path: Path) -> None:
    backup_path.parent.mkdir(parents=True, exist_ok=True)
    if backup_path.exists():
        backup_path.unlink()

    with sqlite3.connect(db_path) as src:
        with sqlite3.connect(backup_path) as dst:
            src.backup(dst)
            dst.commit()


def prune_backups(*, backup_dir: Path, keep_days: int, now: dt.datetime | None = None) -> int:
    if keep_days <= 0:
        return 0

    cutoff = (now or dt.datetime.utcnow()) - dt.timedelta(days=keep_days)
    cutoff_ts = cutoff.timestamp()

    removed = 0
    for p in backup_dir.glob("tracker-backup-*.db"):
        try:
            if p.stat().st_mtime < cutoff_ts:
                p.unlink()
                removed += 1
        except FileNotFoundError:
            continue
    return removed


def run_backup(*, settings: Settings, now: dt.datetime | None = None) -> Path | None:
    now = now or dt.datetime.utcnow()
    db_path = sqlite_db_path_from_url(db_url=settings.db_url)
    if not db_path:
        logger.warning("backup skipped: unsupported db_url=%r", settings.db_url)
        return None

    backup_dir = Path(settings.backup_dir)
    if not backup_dir.is_absolute():
        backup_dir = Path.cwd() / backup_dir
    backup_dir.mkdir(parents=True, exist_ok=True)

    ts = now.strftime("%Y%m%d-%H%M%S")
    out = backup_dir / f"tracker-backup-{ts}.db"
    backup_sqlite_db(db_path=db_path, backup_path=out)
    prune_backups(backup_dir=backup_dir, keep_days=settings.backup_keep_days, now=now)
    return out


def run_prune_ignored(*, settings: Settings, now: dt.datetime | None = None) -> dict[str, int]:
    now = now or dt.datetime.utcnow()
    engine, make_session = session_factory(settings)

    older_than = now - dt.timedelta(days=max(1, settings.prune_ignored_days))
    with make_session() as session:
        repo = Repo(session)
        result = repo.prune_ignored(
            older_than=older_than,
            delete_orphan_items=not settings.prune_keep_items,
            dry_run=False,
        )

    if settings.prune_vacuum and settings.db_url.startswith("sqlite:"):
        with engine.begin() as conn:
            conn.exec_driver_sql("VACUUM")
    return result
