"""The two unauthenticated RPC endpoints, treated as the trust boundary they are.

`/raft/append-entries` and `/raft/request-vote` take no credentials — that is stated in
`models.py` as the reason `AppendEntriesRequest.entries` is bounded, and the reasoning was
applied to exactly one field. The numbers next to it were not bounded, and they are the
ones that reach SQLite.

The concrete failure is not a large allocation. SQLite stores INTEGER as **signed 64-bit**
and its Python driver raises `OverflowError` for anything past that — so a term of 2**63
is accepted by pydantic, compared against `current_term` successfully (Python ints are
arbitrary precision), assigned to `self.current_term`, and only THEN fails on the way to
disk. The node is left with a term in memory that is not on stable storage, which is the
one thing Figure 2's persistent-state box exists to forbid, and it answers every
subsequent RPC with it.

From there it stops being one node's problem: the leader reads that term off the reply,
tries to persist it, and raises inside `_append_to_peer` — outside the `except
TransportError` — which propagates through `asyncio.gather` and kills the replication
loop. One request, and the cluster stops replicating.

Two independent defences, and both are tested here because either alone leaves a hole:
the wire rejects the value (a 422, before any Raft rule runs), and `_observe_term`
persists BEFORE it mutates memory, so a write that fails for any other reason cannot
diverge the two either.
"""

import pytest
from pydantic import ValidationError

from conftest import make_cfg
from raftkv.models import (
    MAX_WIRE_INT,
    AppendEntriesRequest,
    Command,
    RequestVoteRequest,
)
from raftkv.raft import RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport


def node_with(tmp_path):
    cfg = make_cfg("node-1", peers={"node-2": "node-2", "node-3": "node-3"})
    return RaftNode(cfg, Storage(str(tmp_path / "n.db")), MemoryTransport())


# ---- the wire refuses what the database cannot hold --------------------------


@pytest.mark.parametrize("field", ["term", "prev_log_index", "prev_log_term", "leader_commit"])
def test_append_entries_rejects_an_integer_sqlite_cannot_store(field):
    """2**63 is one past SQLite's signed-64-bit INTEGER. Pydantic must refuse it, so the
    value never reaches a rule method, let alone a transaction."""
    args = dict(term=1, leader_id="node-2", prev_log_index=0, prev_log_term=0,
                entries=[], leader_commit=0)
    args[field] = 2**63
    with pytest.raises(ValidationError):
        AppendEntriesRequest(**args)


@pytest.mark.parametrize("field", ["term", "last_log_index", "last_log_term"])
def test_request_vote_rejects_an_integer_sqlite_cannot_store(field):
    args = dict(term=1, candidate_id="node-2", last_log_index=0, last_log_term=0)
    args[field] = 2**63
    with pytest.raises(ValidationError):
        RequestVoteRequest(**args)


@pytest.mark.parametrize("field", ["term", "prev_log_index", "prev_log_term", "leader_commit"])
def test_append_entries_rejects_a_negative_integer(field):
    """Terms and indices are counts. A negative one has no meaning in Figure 2 and would
    walk `next_index` off the bottom of the log."""
    args = dict(term=1, leader_id="node-2", prev_log_index=0, prev_log_term=0,
                entries=[], leader_commit=0)
    args[field] = -1
    with pytest.raises(ValidationError):
        AppendEntriesRequest(**args)


def test_the_largest_storable_integer_is_still_accepted():
    """The bound is the database's, not an arbitrary ceiling: the largest value SQLite
    can hold must still go through, or the bound would itself be a bug."""
    req = AppendEntriesRequest(
        term=MAX_WIRE_INT, leader_id="node-2", prev_log_index=0,
        prev_log_term=0, entries=[], leader_commit=0,
    )
    assert req.term == MAX_WIRE_INT == 2**63 - 1


def test_identifiers_and_request_ids_are_bounded():
    """`Command.key` (256) and `Command.value` (4096) were bounded and `request_id` was
    not — and `request_id` is the one that gets PERSISTED into every follower's log, 512
    entries at a time."""
    with pytest.raises(ValidationError):
        Command(op="set", key="k", value="v", request_id="x" * 10_000)
    with pytest.raises(ValidationError):
        RequestVoteRequest(term=1, candidate_id="x" * 10_000,
                           last_log_index=0, last_log_term=0)
    with pytest.raises(ValidationError):
        AppendEntriesRequest(term=1, leader_id="x" * 10_000, prev_log_index=0,
                             prev_log_term=0, entries=[], leader_commit=0)


# ---- and the rule method persists before it believes anything ----------------


def test_observe_term_leaves_memory_and_disk_agreeing_when_the_write_fails(tmp_path):
    """The second defence, independent of the first.

    Figure 2: currentTerm is "updated on stable storage BEFORE responding to RPCs". If the
    write is attempted after the assignment, then any failing write — a full disk, a
    revoked file handle, an out-of-range value that slipped past the wire — leaves this
    node answering RPCs with a term no restart will remember. It would then be free to
    vote a second time in a term it had already voted in, which is two leaders in one term.

    So the write goes first and the assignment is its consequence. This drives the failure
    directly rather than through a magic number, because the ordering is the property —
    it must hold for every reason a write can fail, not just for the one that motivated it.
    """
    node = node_with(tmp_path)
    node._observe_term(5)  # legitimately advance: memory AND disk are now 5
    assert node.current_term == 5 and node.storage.load()[0] == 5

    def boom(term, voted_for):
        raise OSError("disk full")

    node.storage.save_term_and_vote = boom

    with pytest.raises(OSError):
        node._observe_term(9)

    assert node.current_term == 5, "adopted a term it failed to persist"
    assert node.storage.load()[0] == 5
    assert node.current_term == node.storage.load()[0]


def test_a_granted_vote_is_on_disk_before_it_is_in_memory(tmp_path):
    """Same ordering, on the path where forgetting it elects two leaders. A vote held only
    in memory is a vote a restart forgets, and rule 6 (one vote per term) is enforced from
    `voted_for`."""
    node = node_with(tmp_path)
    node._observe_term(3)  # memory and disk at term 3, no vote yet
    assert node.current_term == 3 and node.storage.load()[1] is None

    def boom(term, voted_for):
        raise OSError("disk full")

    node.storage.save_term_and_vote = boom

    with pytest.raises(OSError):
        node.handle_request_vote(
            RequestVoteRequest(term=3, candidate_id="node-2",
                               last_log_index=0, last_log_term=0)
        )

    assert node.voted_for is None, "recorded a vote it failed to persist"
    assert node.storage.load()[1] is None
