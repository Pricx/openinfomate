from __future__ import annotations

import datetime as dt

from tracker.llm_usage import estimate_llm_cost_usd
from tracker.repo import Repo


def test_estimate_llm_cost_usd_none_when_unset():
    assert (
        estimate_llm_cost_usd(
            prompt_tokens=100,
            completion_tokens=200,
            input_per_million_usd=0.0,
            output_per_million_usd=0.0,
        )
        is None
    )


def test_estimate_llm_cost_usd_math():
    # 1M input @ $1.00 + 2M output @ $2.00 = $5.00
    cost = estimate_llm_cost_usd(
        prompt_tokens=1_000_000,
        completion_tokens=2_000_000,
        input_per_million_usd=1.0,
        output_per_million_usd=2.0,
    )
    assert cost is not None
    assert abs(cost - 5.0) < 1e-9


def test_repo_summarize_llm_usage(db_session):
    repo = Repo(db_session)
    repo.add_llm_usage(
        kind="curate_items",
        model="gpt-5.2",
        topic="t1",
        prompt_tokens=10,
        completion_tokens=5,
        total_tokens=15,
    )
    repo.add_llm_usage(
        kind="digest_summary",
        model="gpt-5.2",
        topic="",
        prompt_tokens=7,
        completion_tokens=3,
        total_tokens=10,
    )
    db_session.commit()

    summary = repo.summarize_llm_usage(since=dt.datetime.utcnow() - dt.timedelta(hours=1))
    assert summary["calls"] == 2
    assert summary["prompt_tokens"] == 17
    assert summary["completion_tokens"] == 8
    assert summary["total_tokens"] == 25

    by_kind = {row["kind"]: row for row in summary["by_kind"]}
    assert by_kind["curate_items"]["calls"] == 1
    assert by_kind["curate_items"]["total_tokens"] == 15
    assert by_kind["digest_summary"]["calls"] == 1
    assert by_kind["digest_summary"]["total_tokens"] == 10

    by_model = {row["model"]: row for row in summary["by_model"]}
    assert by_model["gpt-5.2"]["calls"] == 2
    assert by_model["gpt-5.2"]["total_tokens"] == 25
