from __future__ import annotations

import sqlite3

from sqlalchemy import create_engine
from sqlalchemy import event
from sqlalchemy.orm import Session

from tracker.settings import Settings


def create_engine_from_settings(settings: Settings):
    connect_args = {}
    if settings.db_url.startswith("sqlite:"):
        # `timeout` (seconds) controls how long SQLite will wait for locks.
        connect_args = {"check_same_thread": False, "timeout": 30}

    engine = create_engine(settings.db_url, connect_args=connect_args)

    # SQLite concurrency hardening for long-running services (systemd) where the API
    # and scheduler may touch the DB at the same time.
    if settings.db_url.startswith("sqlite:"):

        @event.listens_for(engine, "connect")
        def _set_sqlite_pragmas(dbapi_connection, _connection_record):  # type: ignore[no-redef]
            if not isinstance(dbapi_connection, sqlite3.Connection):
                return
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute("PRAGMA journal_mode=WAL;")
                cursor.execute("PRAGMA synchronous=NORMAL;")
                cursor.execute("PRAGMA busy_timeout=30000;")  # ms
            finally:
                cursor.close()

    return engine


def session_factory(settings: Settings):
    engine = create_engine_from_settings(settings)

    def _session() -> Session:
        # Avoid "detached instance" surprises when CLI code prints ORM objects
        # after the session closes (common in short-lived commands).
        return Session(engine, expire_on_commit=False)

    return engine, _session
