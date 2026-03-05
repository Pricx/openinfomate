from __future__ import annotations

import asyncio
import datetime as dt

from tracker.models import SourceHealth
from tracker.repo import Repo
from tracker.runner import run_health_report
from tracker.settings import Settings


def test_health_report_includes_failing_sources(db_session):
    repo = Repo(db_session)
    source = repo.add_source(type="rss", url="https://example.com/feed")
    health = SourceHealth(
        source_id=source.id,
        error_count=2,
        last_error="boom",
        last_error_at=dt.datetime.utcnow(),
        next_fetch_at=dt.datetime.utcnow() + dt.timedelta(hours=1),
    )
    db_session.add(health)
    db_session.commit()

    result = asyncio.run(run_health_report(session=db_session, settings=Settings(), push=False))
    assert "## Failing Sources" in result.markdown
    assert f"#{source.id}" in result.markdown
    assert "errs=2" in result.markdown

