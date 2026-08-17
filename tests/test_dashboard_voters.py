"""The per-card "N voting" summary, executed rather than grepped.

Same slice-and-run technique as test_dashboard_keys.py, and here for a sharper reason
than readability. The card summary is the operator's answer to "how many votes are there",
and it used to be derived from the transport peer map:

    `infrastructure · ${Object.keys(peers).length + 1} voting`

A voter dials every OTHER voter, so peers + self is right for a voter by arithmetic
accident. A learner dials every voter too — and is not one — so the same expression counts
the learner itself and a 4-voter cluster rendered "5 voting" on the learner's own card,
one row below a system panel correctly reading "4 voters, tolerates 1". Seen live on
2026-08-16 against the running cluster.

That is the exact confusion this codebase's standing rule exists to prevent — read
`config.voters`, never `cfg.peers`, when you mean who votes — displayed to a human as fact.
`config.voters` is the log-derived voter set that quorum is actually computed over, so
sourcing the summary from it makes the card and the strip incapable of disagreeing.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- voter count"
END = "/* --- end voter count"


def voter_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def count(state: dict | None) -> int:
    """What the card would print, from a /state payload."""
    script = f"""
{voter_source()}
console.log(JSON.stringify(voterCount({json.dumps(state)})));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def state(voters: list[str], learners: list[str] = (), peers: list[str] = ()) -> dict:
    """A /state payload shaped like the real one — peers deliberately included, so a
    regression that reads them instead of the configuration is visible here."""
    addr = "127.0.0.1:8000"
    return {
        "peers": {p: addr for p in peers},
        "learners": {learner: addr for learner in learners},
        "config": {
            "op": "config",
            "voters": {v: addr for v in voters},
            "old_voters": {},
            "learners": {learner: addr for learner in learners},
        },
    }


# ---- the reported bug -------------------------------------------------------


def test_a_learner_does_not_count_itself_as_a_voter():
    """The live symptom: node-5 is a learner in a 4-voter cluster. Its own card said 5.

    A learner's transport peer map holds all four voters, so `peers + 1` is 5. The
    configuration says 4, and the configuration is what quorum is computed over.
    """
    learner_view = state(
        voters=["node-1", "node-2", "node-3", "node-4"],
        learners=["node-5"],
        peers=["node-1", "node-2", "node-3", "node-4"],
    )
    assert count(learner_view) == 4


def test_a_voter_card_agrees_with_the_voter_set():
    """Guards the arithmetic accident that hid the bug: for a voter, peers + 1 is also
    right, so a card built from peers passes this test and fails the one above."""
    voter_view = state(
        voters=["node-1", "node-2", "node-3", "node-4"],
        learners=["node-5"],
        peers=["node-2", "node-3", "node-4"],
    )
    assert count(voter_view) == 4


def test_a_node_in_nobody_s_configuration_reports_zero():
    """A staged node has an empty voter set. "0 voting" is the honest answer — and the
    old expression's answer was 1, which reads as a healthy one-node cluster."""
    assert count(state(voters=[], peers=[])) == 0


# ---- the shapes /state can actually take ------------------------------------


def test_a_joint_configuration_counts_the_new_voter_set():
    """Mid-promotion the leader holds C-old,new. `voters` is C-new, which is the set the
    cluster is moving to and the one the card should name; `old_voters` is displayed by
    the joint-phase indicator instead, not added in here."""
    joint = state(voters=["node-1", "node-2", "node-3", "node-4"], peers=["node-2"])
    joint["config"]["old_voters"] = {
        n: "127.0.0.1:8000" for n in ["node-1", "node-2", "node-3"]
    }
    assert count(joint) == 4


def test_a_single_node_cluster_counts_itself():
    assert count(state(voters=["node-1"], peers=[])) == 1


def test_a_card_with_no_state_yet_does_not_throw():
    """Cards render before the first /state lands, and a card that throws takes the whole
    render with it — the page has one tick() for every node."""
    assert count(None) == 0
    assert count({}) == 0
    assert count({"config": None}) == 0
    assert count({"config": {}}) == 0
