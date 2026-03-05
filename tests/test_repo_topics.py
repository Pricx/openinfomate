import pytest

from tracker.repo import Repo


def test_repo_add_and_list_topics(db_session):
    repo = Repo(db_session)
    repo.add_topic(name="T1", query="a,b")
    names = [t.name for t in repo.list_topics()]
    assert names == ["T1"]


def test_repo_add_topic_duplicate_raises(db_session):
    repo = Repo(db_session)
    repo.add_topic(name="T1", query="")
    with pytest.raises(ValueError):
        repo.add_topic(name="T1", query="")

