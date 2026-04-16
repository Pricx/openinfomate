from __future__ import annotations

from tracker.repo import Repo


def test_topic_gate_defaults_and_override_resolution(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")

    defaults = repo.patch_topic_gate_defaults(
        {
            "candidate_min_score": 60,
            "candidate_convergence": "平衡",
            "max_digest_items": 8,
        }
    )
    assert defaults.candidate_min_score == 60
    assert defaults.candidate_convergence == "balanced"
    assert defaults.max_digest_items == 8

    override = repo.patch_topic_gate_policy(
        topic_id=topic.id,
        patch={
            "candidate_min_score": 75,
            "push_min_score": 80,
        },
    )
    assert override.candidate_min_score == 75
    assert override.push_min_score == 80

    resolved = repo.describe_topic_gate(topic_id=topic.id)
    assert resolved["defaults"]["candidate_min_score"] == 60
    assert resolved["override"]["candidate_min_score"] == 75
    assert resolved["effective"]["candidate_convergence"] == "balanced"
    assert resolved["effective"]["candidate_min_score"] == 75
    assert resolved["effective"]["push_min_score"] == 80
    assert resolved["effective"]["max_digest_items"] == 8


def test_topic_gate_empty_override_deletes_row(db_session):
    repo = Repo(db_session)
    topic = repo.add_topic(name="Profile", query="")
    repo.patch_topic_gate_policy(topic_id=topic.id, patch={"candidate_min_score": 50})
    assert repo.get_topic_gate_policy(topic_id=topic.id) is not None

    cleared = repo.patch_topic_gate_policy(
        topic_id=topic.id,
        patch={
            "candidate_min_score": None,
            "candidate_convergence": None,
            "push_min_score": None,
            "max_digest_items": None,
            "max_alert_items": None,
        },
    )
    assert cleared.is_empty()
    assert repo.get_topic_gate_policy(topic_id=topic.id) is None
