import pytest

from raftkv.models import Command, LogEntry
from raftkv.storage import Storage


def entry(term: int, key: str = "k", value: str = "v", rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key=key, value=value, request_id=rid))


@pytest.fixture
def store(tmp_path):
    s = Storage(str(tmp_path / "test.db"))
    yield s
    s.close()


def test_fresh_storage_defaults(store):
    assert store.load() == (0, None, 0)
    assert store.last_log_index() == 0
    assert store.term_at(0) == 0  # implicit sentinel (MIT hint: 1-indexed log)
    assert store.term_at(1) is None


def test_term_and_vote_survive_reopen(tmp_path):
    path = str(tmp_path / "t.db")
    s = Storage(path)
    s.save_term_and_vote(5, "node-2")
    s.close()
    assert Storage(path).load() == (5, "node-2", 0)


def test_append_and_read_back(store):
    store.append([entry(1, rid="a"), entry(1, rid="b")])
    assert store.last_log_index() == 2
    assert store.term_at(2) == 1
    assert store.entry(2).command.request_id == "b"
    assert [e.command.request_id for e in store.entries_from(1)] == ["a", "b"]
    assert store.entries_from(3) == []


def test_truncate_from(store):
    store.append([entry(1), entry(1), entry(2)])
    store.truncate_from(2)
    assert store.last_log_index() == 1
    assert store.term_at(2) is None


def test_apply_is_atomic_with_last_applied(store):
    store.append([entry(1, key="temp", value="72", rid="a")])
    store.apply(1, store.entry(1).command)
    assert store.kv_get("temp") == "72"
    assert store.load()[2] == 1  # last_applied advanced in the same transaction


def test_apply_set_then_delete(store):
    store.apply(1, Command(op="set", key="k", value="v", request_id="a"))
    store.apply(2, Command(op="delete", key="k", request_id="b"))
    assert store.kv_get("k") is None
    assert store.kv_all() == {}


def test_log_survives_reopen(tmp_path):
    path = str(tmp_path / "t.db")
    s = Storage(path)
    s.append([entry(3, key="x", value="1", rid="a")])
    s.close()
    reopened = Storage(path)
    assert reopened.last_log_index() == 1
    assert reopened.term_at(1) == 3
    reopened.close()
