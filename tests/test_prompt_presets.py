from __future__ import annotations

from tracker.prompt_presets import topic_policy_presets


def _assert_unique_ids(presets):
    ids = [p.id for p in presets]
    assert ids
    assert len(ids) == len(set(ids))


def test_topic_policy_presets_have_unique_ids():
    _assert_unique_ids(topic_policy_presets())
