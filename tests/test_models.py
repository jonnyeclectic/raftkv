import pytest
from pydantic import ValidationError

from raftkv import raft as raft_module
from raftkv.models import (
    MAX_ENTRIES_PER_APPEND,
    AppendEntriesRequest,
    Command,
    LogEntry,
    RequestVoteRequest,
    Role,
)


def test_command_round_trip():
    cmd = Command(op="set", key="temp", value="72", request_id="r1")
    assert Command.model_validate(cmd.model_dump()) == cmd


def test_command_rejects_unknown_op():
    with pytest.raises(ValidationError):
        Command(op="increment", key="k", value="1", request_id="r1")


def test_command_delete_needs_no_value():
    cmd = Command(op="delete", key="temp", request_id="r2")
    assert cmd.value is None


def test_command_set_requires_value():
    with pytest.raises(ValidationError):
        Command(op="set", key="temp", request_id="r3")  # set without a value is invalid


def test_append_entries_round_trip_nested():
    req = AppendEntriesRequest(
        term=3,
        leader_id="node-1",
        prev_log_index=1,
        prev_log_term=1,
        entries=[LogEntry(term=3, command=Command(op="set", key="k", value="v", request_id="r"))],
        leader_commit=1,
    )
    again = AppendEntriesRequest.model_validate(req.model_dump())
    assert again.entries[0].command.key == "k"


def test_request_vote_defaults_nothing():
    with pytest.raises(ValidationError):
        RequestVoteRequest(term=1, candidate_id="node-1")  # missing log position


def test_role_is_string():
    assert Role.LEADER == "leader"


def _entries(n: int) -> list[LogEntry]:
    return [LogEntry(term=1, command=Command(op="set", key="k", value="v", request_id="r"))] * n


def test_append_entries_accepts_a_full_batch_but_refuses_a_larger_one():
    """`/raft/append-entries` is unauthenticated, so this bound is the only thing standing
    between an anonymous POST and an arbitrarily large decode.

    Every entry was already bounded (`Command.key` 256, `Command.value` 4096) and the LIST
    was not, so one request's size was capped only by how many entries the caller chose to
    send — 50,000 of them decode to roughly 205 MB. The asymmetry was the tell: every admin
    model in this file is bounded, and the two models reachable without credentials were
    the ones that were not.

    Exactly at the cap must still pass. That is not symmetry for its own sake — the leader
    sends exactly `MAX_ENTRIES_PER_APPEND` on a full batch, so a cap that rejected its own
    maximum would break replication for every follower behind by more than one batch, which
    is the ordinary catch-up path rather than an edge case.
    """
    def build(n: int) -> AppendEntriesRequest:
        return AppendEntriesRequest(
            term=1, leader_id="node-1", prev_log_index=0, prev_log_term=0,
            entries=_entries(n), leader_commit=0,
        )

    assert len(build(MAX_ENTRIES_PER_APPEND).entries) == MAX_ENTRIES_PER_APPEND
    with pytest.raises(ValidationError):
        build(MAX_ENTRIES_PER_APPEND + 1)


def test_the_send_cap_and_the_wire_cap_are_the_same_object():
    """Two constants that merely happen to be equal drift. Raising the send cap while a
    smaller wire cap stayed behind would make every follower reject a well-formed append
    from its own leader — a cluster-wide replication stall produced by a one-line tuning
    change, and one no existing test would catch."""
    assert raft_module.MAX_ENTRIES_PER_APPEND is MAX_ENTRIES_PER_APPEND
