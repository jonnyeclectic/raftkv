import pytest
from pydantic import ValidationError

from raftkv.models import (
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
