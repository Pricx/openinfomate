from __future__ import annotations

import datetime as dt

from tracker.doctor import build_doctor_report
from tracker.repo import ActivitySnapshot
from tracker.settings import Settings


def test_build_doctor_report_flags_digest_pipeline_guardrail_failures():
    settings = Settings()
    activity = ActivitySnapshot(
        last_tick_at=dt.datetime.utcnow(),
        last_digest_report_at=None,
        last_health_report_at=None,
        last_push_attempt_at=None,
        last_push_sent_at=None,
        last_digest_sync_at=None,
        last_curated_sync_at=None,
        digest_sync_enabled_topics=3,
        digest_sync_scheduled_topics=1,
        curated_sync_job_present=False,
    )

    report = build_doctor_report(
        settings=settings,
        stats={
            "topics_total": 3,
            "sources_total": 5,
            "bindings_total": 3,
        },
        db_ok=True,
        db_error=None,
        profile_configured=True,
        telegram_chat_configured=False,
        activity=activity,
    )

    assert any("scheduler heartbeat is missing" in rec for rec in report.recommendations)
    assert any("Only 1/3 enabled topics" in rec for rec in report.recommendations)
    assert any("Cross-topic Curated Info job heartbeat reports missing" in rec for rec in report.recommendations)
