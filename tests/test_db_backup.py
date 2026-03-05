from __future__ import annotations

import datetime as dt
import os
from pathlib import Path
import sqlite3

from tracker.maintenance import prune_backups, run_backup
from tracker.settings import Settings


def test_run_backup_creates_sqlite_copy(tmp_path: Path):
    db_path = tmp_path / "tracker.db"
    with sqlite3.connect(db_path) as conn:
        conn.execute("create table t(x integer)")
        conn.execute("insert into t(x) values (1)")
        conn.commit()

    backup_dir = tmp_path / "backups"
    settings = Settings(
        db_url=f"sqlite:///{db_path}",
        backup_dir=str(backup_dir),
        backup_keep_days=7,
        backup_cron="",
    )

    now = dt.datetime(2026, 2, 10, 0, 0, 0)
    out = run_backup(settings=settings, now=now)
    assert out is not None
    assert out.exists()

    with sqlite3.connect(out) as conn:
        (count,) = conn.execute("select count(*) from t").fetchone()
    assert count == 1


def test_prune_backups_removes_old_files(tmp_path: Path):
    backup_dir = tmp_path / "backups"
    backup_dir.mkdir()

    old_file = backup_dir / "tracker-backup-old.db"
    old_file.write_text("x", encoding="utf-8")
    old_ts = dt.datetime(2026, 1, 1, 0, 0, 0).timestamp()
    os.utime(old_file, (old_ts, old_ts))

    new_file = backup_dir / "tracker-backup-new.db"
    new_file.write_text("y", encoding="utf-8")

    removed = prune_backups(backup_dir=backup_dir, keep_days=7, now=dt.datetime(2026, 2, 10, 0, 0, 0))
    assert removed == 1
    assert not old_file.exists()
    assert new_file.exists()

