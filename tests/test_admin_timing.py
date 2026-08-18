"""`POST /admin/timing` and `POST /admin/campaign`: the runtime tempo controls.

Debugging a node under breakpoints needs elections STEERED, not hoped for: node-1 must win
while its leader paths are being stepped, and must lose — and stay lost — while a
breakpoint sits in its follower paths. Two controls, and the properties that make them
safe to expose to an operator against a running cluster:

  - a timing update is validated as a WHOLE config before any field is touched, so the
    startup ratio rules (§5.2) hold mid-flight too. The knob exists to steer elections,
    not to configure the pathological cluster those rules prevent booting.
  - campaign elects exactly the node asked, by the ordinary election rules — it burns a
    term like any early timeout and changes nothing about election safety. A sitting
    leader is refused (a no-op wearing a button), and a learner is refused for the same
    reason the election timer loop skips learners: a node outside every voter set can
    only waste a term it cannot win.

Neither control is persisted. A restart restores the env-derived config, which is what
a reset wants.
"""

import pytest
from fastapi.testclient import TestClient

from conftest import SimCluster, eventually, make_cfg
from raftkv.app import create_app
from raftkv.raft import Role


def solo_cfg(tmp_path, **overrides):
    return make_cfg(
        db_path=str(tmp_path / "solo.db"), log_dir=str(tmp_path / "logs"), **overrides
    )


# ---- /admin/timing --------------------------------------------------------------------


def test_a_timing_update_reaches_the_live_timer_and_merges_partially(tmp_path):
    """The update lands on the SHARED config object, so the next timer arm draws from
    the new range — no restart, which is the whole point. Fields not sent stay put."""
    with TestClient(create_app(solo_cfg(tmp_path))) as c:
        node = c.app.state.node
        heartbeat_before = node.cfg.heartbeat_interval
        r = c.post(
            "/admin/timing",
            json={"election_timeout_min": 30, "election_timeout_max": 31},
        )
        assert r.status_code == 200
        assert r.json()["timing"]["election_timeout_min"] == 30
        assert r.json()["timing"]["heartbeat_interval"] == heartbeat_before
        assert node.cfg.election_timeout_min == 30
        node._reset_election_timer()
        assert 30 <= node._election_timeout <= 31, "the live timer missed the update"


def test_a_ratio_breaking_update_is_refused_whole(tmp_path):
    """heartbeat >= election_min is the config that elects around its own leader
    forever. NodeConfig refuses it at boot; the runtime knob must refuse it too, and
    must leave EVERY field untouched — a partially applied bad update is worse than
    either outcome."""
    with TestClient(create_app(solo_cfg(tmp_path))) as c:
        node = c.app.state.node
        before = node.cfg.model_dump()
        r = c.post("/admin/timing", json={"heartbeat_interval": 40})
        assert r.status_code == 409
        assert "election_timeout_min" in r.json()["detail"]
        assert node.cfg.model_dump() == before, "a refused update still changed the config"


def test_an_empty_timing_body_is_a_422(tmp_path):
    with TestClient(create_app(solo_cfg(tmp_path))) as c:
        assert c.post("/admin/timing", json={}).status_code == 422


# ---- /admin/campaign ------------------------------------------------------------------


def test_the_campaign_endpoint_elects_a_solo_follower_on_the_spot(tmp_path):
    """Election timers stretched to 30 s so the node is still a follower when the
    request lands — the press, not the timer, is what elects it. The second press
    then hits the sitting-leader refusal."""
    cfg = solo_cfg(tmp_path, election_timeout_min=30, election_timeout_max=31)
    with TestClient(create_app(cfg)) as c:
        r = c.post("/admin/campaign")
        assert r.status_code == 200
        assert r.json()["role"] == "leader"
        second = c.post("/admin/campaign")
        assert second.status_code == 409
        assert "already the leader" in second.json()["detail"]


async def test_campaign_makes_the_asked_for_follower_the_leader(tmp_path):
    """The follower's term bump deposes the sitting leader through the ordinary
    observe-higher-term path — deterministic over MemoryTransport, no timer waits."""
    cluster = SimCluster(tmp_path, n=3)
    cluster.start()
    try:
        await eventually(lambda: cluster.leader() is not None)
        old_leader = cluster.leader()
        follower = next(n for n in cluster.nodes.values() if n is not old_leader)
        await follower.campaign()
        await eventually(lambda: cluster.leader() is follower)
        assert old_leader.role is not Role.LEADER, "the deposed leader never stepped down"
    finally:
        await cluster.stop()


async def test_a_learner_refuses_to_campaign(make_node):
    """Same guard the election timer loop applies, reachable by button: a node outside
    every voter set can only waste a term it cannot win."""
    node = make_node("node-9", peer_ids=(), learner=True)
    assert not node.is_voter, "precondition: this node must boot as a learner"
    with pytest.raises(ValueError, match="learner"):
        await node.campaign()
