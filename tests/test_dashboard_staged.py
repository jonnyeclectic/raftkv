"""The dashboard's member-vs-staged split, executed rather than grepped.

Same technique as tests/test_dashboard_leader.py, for the same reason: slice the shipped
block out of the HTML, run it under node against hand-built /state payloads, and assert
on return values. A test that re-implements `isStaged` would pass forever no matter what
the browser loads.

Why this block earns a test of its own. Membership is a view over the log (§6), so the
only honest way to ask "is this process in the cluster?" is to read its own configuration
-- and a process started with RAFT_LEARNER=1 and no peers has an empty voter set, because
_bootstrap_config() deliberately refuses to put a lone node in its own voter set. That one
condition decides which row a node is drawn in, whether it counts toward the quorum
arithmetic in the health strip, and whether it appears in the topology at all.

Both directions of getting it wrong are quiet:

  - too eager, and a CRASHED node reclassifies itself out of the cluster the instant it
    stops answering -- disappearing from the topology and shrinking the quorum denominator
    at exactly the moment that denominator is what you need to read. It would also hand a
    null to stagedCard(), which reads s.node_id unguarded, so every tick would throw and
    take the whole render loop down.
  - too shy, and a node in nobody's configuration is drawn as an ordinary member with a
    term of 0 and an empty log, which reads as a broken cluster rather than as the two-step
    join that Raft actually requires.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- node classification"
END = "/* --- end node classification"

CLUSTER = ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
STAGING = ["127.0.0.1:8004", "127.0.0.1:8005"]


def classification_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(nodes: list[str], states: dict) -> dict:
    """Classify `nodes` against `states` and report both rows."""
    script = f"""
const NODES = {json.dumps(nodes)};
const states = {json.dumps(states)};
{classification_source()}
console.log(JSON.stringify({{members: memberAddrs(), staged: stagedAddrs()}}));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def addr_of(node_id: str) -> str:
    return f"127.0.0.1:800{node_id[-1]}"


def member(node_id, *, role="follower", term=7, voters=("node-1", "node-2", "node-3"),
           old_voters=(), learners=()):
    """A node whose own log holds a configuration -- whatever its role in it."""
    return {
        "node_id": node_id, "role": role, "term": term,
        "learner": node_id in learners,
        "config": {
            "voters": {v: addr_of(v) for v in voters},
            "old_voters": {v: addr_of(v) for v in old_voters},
            "learners": {v: addr_of(v) for v in learners},
        },
    }


def unattached(node_id):
    """Exactly what `RAFT_LEARNER=1` with no peers reports: it is up and answering, it
    has never seen a configuration entry, and _bootstrap_config() puts it in its own
    LEARNER set rather than its own voter set, so it never campaigns either."""
    return {
        "node_id": node_id, "role": "follower", "term": 0, "learner": True,
        "config": {"voters": {}, "old_voters": {}, "learners": {node_id: addr_of(node_id)}},
    }


def test_a_process_in_no_configuration_is_staged_not_a_member():
    """The distinction this split exists to draw: `docker compose up` provisions a process,
    and a process is not a cluster member until a leader appends a configuration entry
    naming it."""
    got = run(CLUSTER + STAGING,
              {**{a: member(f"node-{a[-1]}") for a in CLUSTER},
               **{a: unattached(f"node-{a[-1]}") for a in STAGING}})
    assert got["members"] == CLUSTER
    assert got["staged"] == STAGING


def test_attaching_moves_a_node_out_of_the_staged_row():
    """One press of "attach as learner", seen from node-4's side: the replicated config
    entry lands, its voter set stops being empty, and the same address changes rows. The
    node is still non-voting -- membership, not suffrage, is what this split is about."""
    nodes = CLUSTER + ["127.0.0.1:8004"]
    before = {**{a: member(f"node-{a[-1]}") for a in CLUSTER},
              "127.0.0.1:8004": unattached("node-4")}
    assert run(nodes, before)["staged"] == ["127.0.0.1:8004"]

    attached = member("node-4", learners=("node-4",))
    after = {**{a: member(f"node-{a[-1]}", learners=("node-4",)) for a in CLUSTER},
             "127.0.0.1:8004": attached}
    got = run(nodes, after)
    assert got["staged"] == [], "an attached learner was still drawn as unattached"
    assert got["members"] == nodes
    assert attached["learner"] is True, "the fixture stopped being a learner; test is moot"


def test_a_crashed_member_stays_a_member():
    """A killed node polls as null. Counting it out of the cluster would shrink the
    quorum denominator in the health strip to fit whatever is currently reachable, which
    is the one number that must not move under a partition: 2 of 3 is the point."""
    got = run(CLUSTER, {CLUSTER[0]: None,
                        CLUSTER[1]: member("node-2"),
                        CLUSTER[2]: member("node-3", role="leader")})
    assert got["members"] == CLUSTER
    assert got["staged"] == []


def test_a_crashed_node_can_never_reach_the_staged_card():
    """stagedCard() reads s.node_id with no null guard, and is allowed to only because
    this classification never hands it one. Relaxing isStaged to `s?.config` would throw
    on every tick and stop the entire render loop -- including the cards of the nodes
    that are still healthy. Whole cluster dark: everything is null."""
    got = run(CLUSTER + STAGING, {a: None for a in CLUSTER + STAGING})
    assert got["staged"] == [], "a null state reached the card that dereferences it"
    assert got["members"] == CLUSTER + STAGING


def test_a_node_mid_joint_consensus_is_a_member():
    """During C-old,new `voters` carries C-new and `old_voters` carries C-old. Reading
    the wrong one -- or requiring both -- would drop a node out of the topology for the
    exact window in which its joint-consensus state is what you are trying to observe."""
    joint = member("node-4", voters=("node-1", "node-2", "node-3", "node-4"),
                   old_voters=("node-1", "node-2", "node-3"))
    got = run(["127.0.0.1:8004"], {"127.0.0.1:8004": joint})
    assert got["members"] == ["127.0.0.1:8004"]
    assert got["staged"] == []


def test_every_address_is_classified_exactly_once():
    """The two rows are a partition of NODES, not two independent filters. Anything that
    falls out of both is a node the operator can see in `docker ps` and nowhere on this
    page -- the failure mode that started this whole feature."""
    nodes = CLUSTER + STAGING
    states = {CLUSTER[0]: member("node-1", role="leader"),
              CLUSTER[1]: None,
              CLUSTER[2]: member("node-3"),
              STAGING[0]: unattached("node-4"),
              STAGING[1]: None}
    got = run(nodes, states)
    assert sorted(got["members"] + got["staged"]) == sorted(nodes)
    assert not set(got["members"]) & set(got["staged"])


def test_classification_preserves_probe_order():
    """Both rows are rebuilt from scratch twice a second. Filtering NODES in place keeps
    each card where it was; anything set-based reorders them, and cards that swap places
    under the pointer make the buttons a coin flip."""
    nodes = list(reversed(CLUSTER)) + STAGING
    states = {**{a: member(f"node-{a[-1]}") for a in CLUSTER},
              **{a: unattached(f"node-{a[-1]}") for a in STAGING}}
    assert run(nodes, states)["members"] == list(reversed(CLUSTER))
