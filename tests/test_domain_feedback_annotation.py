from __future__ import annotations

from tracker.repo import Repo
from tracker.runner import _annotate_candidates_domain_feedback


def test_annotate_candidates_domain_feedback_counts_like_dislike(db_session):
    repo = Repo(db_session)

    # Seed historical feedback for the same domain.
    repo.add_feedback_event(
        channel="telegram",
        user_id="u",
        chat_id="c",
        message_id=1,
        kind="like",
        item_id=None,
        url="https://example.com/a",
        domain="example.com",
        raw="{}",
    )
    repo.add_feedback_event(
        channel="telegram",
        user_id="u",
        chat_id="c",
        message_id=2,
        kind="dislike",
        item_id=None,
        url="https://example.com/b",
        domain="example.com",
        raw="{}",
    )

    candidates = [{"url": "https://www.example.com:443/x", "title": "t", "item_id": 1, "snippet": "s"}]
    _annotate_candidates_domain_feedback(repo=repo, candidates=candidates, days=365)

    assert candidates[0]["domain"] == "example.com"
    assert int(candidates[0]["domain_likes"]) == 1
    assert int(candidates[0]["domain_dislikes"]) == 1


def test_annotate_candidates_domain_feedback_sets_zero_when_no_stats(db_session):
    repo = Repo(db_session)
    candidates = [{"url": "https://no-stats.example/x"}]
    _annotate_candidates_domain_feedback(repo=repo, candidates=candidates, days=365)

    assert candidates[0]["domain"] == "no-stats.example"
    assert int(candidates[0]["domain_likes"]) == 0
    assert int(candidates[0]["domain_dislikes"]) == 0

