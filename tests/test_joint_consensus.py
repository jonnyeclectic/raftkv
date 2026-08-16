"""Joint consensus (§6): promoting a learner to a voting member.

The failure this guards against is not a crash. It is two leaders in one term, which
looks like nothing at all until committed data disappears. So every test here attacks
the same property from a different side: at no instant may the old and the new
configuration each be able to decide something on their own.
"""

import asyncio
import re
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from raftkv.app import create_app
from raftkv.models import ClusterConfig, Command, LogEntry, NoOp, Role
from raftkv.raft import NotLeaderError, RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport
from test_api import build_cfg, wait_for_leader

RAFT_SRC = Path(__file__).resolve().parents[1] / "src" / "raftkv" / "raft.py"


def leader_with(tmp_path, voters=("node-2", "node-3"), learners=()):
    cfg = build_cfg(tmp_path, node_id="node-1", peers={p: p for p in voters},
                    db_path=str(tmp_path / "node-1.db"))
    node = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    node.current_term = 1
    node._become_leader()
    node.commit_index = node.storage.last_log_index()  # the term's no-op is committed
    for learner in learners:
        node.add_learner(learner, learner)
        node.commit_index = node.storage.last_log_index()
    # Caught up, which is the only state a learner is promotable from. Set after the
    # loop, not inside it: each add_learner advances commit_index, so a match_index
    # written mid-loop is stale by the time the next learner is added. The gate itself
    # is tested directly further down.
    return caught_up(node, *learners)


def caught_up(node, *peer_ids):
    """Stand in for what real replication does: the peer has acked the committed
    prefix. Unit tests here drive the state machine by hand, so nothing else moves
    match_index."""
    for peer_id in peer_ids:
        node.match_index[peer_id] = node.commit_index
    return node


# ---------------------------------------------------------------- the predicate

def test_joint_agreement_needs_a_majority_of_BOTH_configurations(tmp_path):
    """The single most important assertion in this file. A union majority would pass
    with {node-4, node-5, node-1}; separate majorities must not."""
    node = leader_with(tmp_path)
    node.config = ClusterConfig(
        voters={n: n for n in ["node-1", "node-2", "node-3", "node-4", "node-5"]},
        old_voters={n: n for n in ["node-1", "node-2", "node-3"]},
    )

    # 3 of C-new (majority) but only 1 of C-old (not a majority of 3)
    assert not node._has_agreement({"node-1", "node-4", "node-5"})
    # 2 of C-old (majority) but only 2 of C-new (not a majority of 5)
    assert not node._has_agreement({"node-1", "node-2"})
    # majority of both
    assert node._has_agreement({"node-1", "node-2", "node-3"})
    node.storage.close()


def test_outside_a_joint_phase_the_old_half_is_vacuous(tmp_path):
    node = leader_with(tmp_path)
    assert node.config.old_voters == {}
    assert node._has_agreement({"node-1", "node-2"})     # 2 of 3
    assert not node._has_agreement({"node-1"})           # 1 of 3
    node.storage.close()


def test_an_empty_voter_set_is_satisfied_not_impossible(tmp_path):
    """Guards the branch that lets joint and non-joint share one code path."""
    node = leader_with(tmp_path)
    assert node._is_majority({}, set())
    node.storage.close()


# ---------------------------------------------------------------- the transition

def test_promotion_appends_a_joint_entry_that_takes_effect_immediately(tmp_path):
    node = leader_with(tmp_path, learners=["node-4"])
    assert node.quorum == 2  # 3 voters

    node.promote_learner("node-4")

    assert node.config.joint, "C-old,new must be active the moment it is appended"
    assert set(node.config.old_voters) == {"node-1", "node-2", "node-3"}
    assert set(node.config.voters) == {"node-1", "node-2", "node-3", "node-4"}
    assert "node-4" not in node.config.learners  # promoted out of the learner set
    assert node.quorum == 3  # majority of C-new, and C-old still needs 2

    # committed-ness is irrelevant to adoption
    entry = node.storage.entry(node.storage.last_log_index())
    assert isinstance(entry.command, ClusterConfig)
    assert node.commit_index < node.storage.last_log_index()
    node.storage.close()


def test_leaving_the_joint_phase_is_triggered_only_by_the_joint_entry_committing(tmp_path):
    node = leader_with(tmp_path, learners=["node-4"])
    node.promote_learner("node-4")
    joint_index = node.storage.last_log_index()

    node._maybe_leave_joint()  # not committed yet
    assert node.config.joint, "left the joint phase before C-old,new committed"

    node.commit_index = joint_index
    node._maybe_leave_joint()

    assert not node.config.joint
    assert set(node.config.voters) == {"node-1", "node-2", "node-3", "node-4"}
    final = node.storage.entry(node.storage.last_log_index())
    assert isinstance(final.command, ClusterConfig) and not final.command.joint
    node.storage.close()


def test_growing_three_to_four_to_five_one_at_a_time(tmp_path):
    """Growth 3 -> 4 -> 5. Quorum goes 2 -> 3 -> 3, which is the whole argument for
    odd cluster sizes -- four voters cost an extra machine and tolerate no more
    failures than three."""
    node = leader_with(tmp_path, learners=["node-4", "node-5"])
    assert (len(node.config.voters), node.quorum) == (3, 2)

    node.promote_learner("node-4")
    node.commit_index = node.storage.last_log_index()
    node._maybe_leave_joint()
    assert (len(node.config.voters), node.quorum) == (4, 3)
    tolerated_at_four = len(node.config.voters) - node.quorum

    node.commit_index = node.storage.last_log_index()
    caught_up(node, "node-5")  # it kept replicating through node-4's transition
    node.promote_learner("node-5")
    node.commit_index = node.storage.last_log_index()
    node._maybe_leave_joint()
    assert (len(node.config.voters), node.quorum) == (5, 3)
    tolerated_at_five = len(node.config.voters) - node.quorum

    assert tolerated_at_four == 1, "4 voters tolerate only 1 failure, same as 3"
    assert tolerated_at_five == 2, "5 voters tolerate 2 -- the reason to prefer odd"
    node.storage.close()


# ---------------------------------------------------------------- preconditions

def test_promotion_requires_leadership(tmp_path):
    node = leader_with(tmp_path, learners=["node-4"])
    node.role = Role.FOLLOWER
    with pytest.raises(NotLeaderError):
        node.promote_learner("node-4")
    node.storage.close()


def test_only_one_configuration_change_at_a_time(tmp_path):
    node = leader_with(tmp_path, learners=["node-4", "node-5"])
    node.promote_learner("node-4")
    with pytest.raises(ValueError, match="already in flight"):
        node.promote_learner("node-5")
    node.storage.close()


def test_previous_change_must_be_committed_first(tmp_path):
    node = leader_with(tmp_path, learners=["node-4", "node-5"])
    node.promote_learner("node-4")
    node.commit_index = node.storage.last_log_index()
    node._maybe_leave_joint()  # appends C-new, which is NOT yet committed
    with pytest.raises(ValueError, match="not committed"):
        node.promote_learner("node-5")
    node.storage.close()


def test_leader_must_commit_an_entry_from_its_own_term_first(tmp_path):
    """The post-publication erratum. A leader inheriting an uncommitted configuration
    entry from an earlier term cannot tell whether it is committed; acting on the
    guess is how a C-new outlives the joint phase it was supposed to follow."""
    node = leader_with(tmp_path, learners=["node-4"])
    node.commit_index = 0  # nothing from this term committed yet
    with pytest.raises(ValueError, match="own term"):
        node.promote_learner("node-4")
    node.storage.close()


def test_cannot_promote_a_node_that_is_not_a_learner(tmp_path):
    node = leader_with(tmp_path, learners=["node-4"])
    with pytest.raises(ValueError, match="not a learner"):
        node.promote_learner("node-2")  # already a voter
    with pytest.raises(ValueError, match="not a learner"):
        node.promote_learner("nobody")
    node.storage.close()


# ---------------------------------------------------------------- log-derived config

def test_configuration_is_recovered_from_the_log_after_restart(tmp_path):
    """Membership survives a process restart because it is IN the log, not beside it."""
    node = leader_with(tmp_path, learners=["node-4"])
    node.promote_learner("node-4")
    node.commit_index = node.storage.last_log_index()
    node._maybe_leave_joint()
    expected = set(node.config.voters)
    node.storage.close()

    cfg = build_cfg(tmp_path, node_id="node-1", peers={"node-2": "node-2", "node-3": "node-3"},
                    db_path=str(tmp_path / "node-1.db"))
    reborn = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    assert set(reborn.config.voters) == expected, "config must come from the log"
    assert reborn.quorum == 3
    reborn.storage.close()


def test_truncating_a_configuration_entry_reverts_the_configuration(tmp_path):
    """Append-time adoption is only safe because truncation undoes it. A config cached
    independently of the log would leave this node counting a roster that exists
    nowhere."""
    node = leader_with(tmp_path, learners=["node-4"])
    before = set(node.config.voters)
    node.promote_learner("node-4")
    joint_index = node.storage.last_log_index()
    assert node.config.joint

    node.storage.truncate_from(joint_index)
    node._reload_config()

    assert not node.config.joint
    assert set(node.config.voters) == before
    node.storage.close()


def test_learner_membership_survives_a_leader_change(tmp_path):
    """It used to be leader-local state; putting it in the log is what fixed that."""
    node = leader_with(tmp_path, learners=["node-4"])
    assert "node-4" in node.config.learners
    node.storage.close()

    cfg = build_cfg(tmp_path, node_id="node-1", peers={"node-2": "node-2", "node-3": "node-3"},
                    db_path=str(tmp_path / "node-1.db"))
    reborn = RaftNode(cfg, Storage(cfg.db_path), MemoryTransport())
    assert "node-4" in reborn.config.learners
    reborn.storage.close()


def test_every_log_mutation_site_recomputes_the_configuration(tmp_path):
    """A grep-level invariant, because a single missed call site is a node whose quorum
    math disagrees with its own log -- and that is invisible until it is catastrophic."""
    src = RAFT_SRC.read_text()
    methods = re.split(r"\n    (?=(?:async )?def )", src)
    for method in methods:
        mutates = "self.storage.append(" in method or "self.storage.truncate_from(" in method
        if not mutates:
            continue
        name = method.split("(")[0].split()[-1]
        assert "_reload_config()" in method, f"{name} mutates the log without reloading config"


# ---------------------------------------------------------------- the endpoint

def test_promote_endpoint_round_trip(tmp_path):
    app = create_app(build_cfg(tmp_path))
    with TestClient(app) as c:
        wait_for_leader(c)
        c.post("/admin/add-learner", json={"node_id": "node-4", "addr": "127.0.0.1:8004"})
        # a solo cluster commits instantly, so the learner entry is already committed.
        # Nothing listens at that address, so nothing will ever ack for node-4: stand
        # in for the replication a real node-4 does, or the catch-up gate refuses.
        caught_up(app.state.node, "node-4")
        r = c.post("/admin/promote", json={"node_id": "node-4"})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["joint"] is True
        assert body["old_voters"] == ["solo"]
        assert body["new_voters"] == ["node-4", "solo"]

        state = c.get("/state").json()
        assert state["config"]["old_voters"] != {} or state["config"]["voters"]


def test_promote_endpoint_rejects_a_non_learner(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        assert c.post("/admin/promote", json={"node_id": "ghost"}).status_code == 409


def test_promote_is_refused_while_crashed(tmp_path):
    with TestClient(create_app(build_cfg(tmp_path))) as c:
        wait_for_leader(c)
        c.post("/admin/crash")
        assert c.post("/admin/promote", json={"node_id": "x"}).status_code == 503


# ---------------------------------------------------------------- entry types

def test_config_and_noop_entries_round_trip_through_storage(tmp_path):
    s = Storage(str(tmp_path / "n.db"))
    s.append([
        LogEntry(term=1, command=NoOp()),
        LogEntry(term=1, command=Command(op="set", key="k", value="v", request_id="r")),
        LogEntry(term=1, command=ClusterConfig(voters={"a": "x"}, old_voters={"b": "y"})),
    ])
    assert isinstance(s.entry(1).command, NoOp)
    assert isinstance(s.entry(2).command, Command)
    third = s.entry(3).command
    assert isinstance(third, ClusterConfig) and third.joint
    idx, cfg = s.last_config()
    assert idx == 3 and cfg.old_voters == {"b": "y"}
    s.close()


def test_the_applier_advances_past_entries_with_no_state_machine_effect(tmp_path):
    """A config or no-op entry must not stall the single applier, and must not be
    written into the KV table."""
    async def scenario():
        node = leader_with(tmp_path, learners=["node-4"])
        node.promote_learner("node-4")
        node.start()
        node.commit_index = node.storage.last_log_index()
        node._apply_ready.set()
        for _ in range(50):
            await asyncio.sleep(0.01)
            if node.last_applied == node.commit_index:
                break
        assert node.last_applied == node.commit_index, "applier stalled on a config entry"
        assert node.storage.kv_all() == {}, "a config entry leaked into the state machine"
        await node.stop()
        node.storage.close()

    asyncio.run(scenario())


# ------------------------------------------------------- the catch-up gate (§4.2.1)

def test_promotion_is_refused_while_the_learner_is_behind(tmp_path):
    """Thesis §4.2.1, and an availability bug found by adversarial review.

    A learner's lag is free: nobody counts it. The instant it becomes a voter that lag
    is subtracted from the cluster's fault tolerance, because commitment now needs a
    majority that includes nodes able to ack. Promoting one that is far behind stops
    the cluster committing until it finishes catching up -- availability lost by an
    administrative action that looked instantaneous.
    """
    node = leader_with(tmp_path, learners=("node-4",))
    node.storage.append([LogEntry(term=1, command=NoOp())])
    node.commit_index = node.storage.last_log_index()
    node.match_index["node-4"] = node.commit_index - 3  # three committed entries behind

    with pytest.raises(ValueError, match="3 committed entries behind"):
        node.promote_learner("node-4")
    assert not node.config.joint, "a refused promotion must not open a joint phase"
    assert "node-4" in node.config.learners, "it is still a learner"
    node.storage.close()


def test_promotion_is_allowed_the_moment_the_committed_prefix_is_covered(tmp_path):
    """The gate is the COMMITTED prefix, not the whole log: the uncommitted tail is
    bounded by in-flight writes, so waiting for it would mean never promoting a busy
    cluster."""
    node = leader_with(tmp_path, learners=("node-4",))
    node.storage.append([LogEntry(term=1, command=NoOp())])  # uncommitted tail
    node.match_index["node-4"] = node.commit_index

    node.promote_learner("node-4")

    assert node.config.joint
    assert "node-4" in node.config.voters
    node.storage.close()


def test_the_catch_up_gate_reports_the_gap_so_a_409_can_say_wait(tmp_path):
    """"Denied" and "not yet" are different answers, and a caller that can retry needs
    the second."""
    node = leader_with(tmp_path, learners=("node-4",))
    node.match_index["node-4"] = 0
    with pytest.raises(ValueError) as caught:
        node.promote_learner("node-4")
    assert re.search(r"\d+ committed entries behind", str(caught.value))
    node.storage.close()


def test_promote_endpoint_answers_409_while_the_learner_is_catching_up(tmp_path):
    """The gate has to be visible to the operator, or "nothing happened" is the only
    feedback. 409 plus the gap is what turns it into "wait"."""
    app = create_app(build_cfg(tmp_path))
    with TestClient(app) as c:
        wait_for_leader(c)
        c.post("/admin/add-learner", json={"node_id": "node-4", "addr": "127.0.0.1:8004"})
        r = c.post("/admin/promote", json={"node_id": "node-4"})
        assert r.status_code == 409
        assert "behind" in r.text
