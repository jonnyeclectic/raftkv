"""The election threshold at each cluster size, walked 3 → 4 → 5.

Named sizes, because the arithmetic only misbehaves at particular ones and a suite that
never states the size cannot say which. The even row is the interesting one; the odd rows
either side are what give it meaning.

Quorum is `floor(N/2) + 1`. For every ODD N that is also `ceil(N/2)`, so nothing built at
three nodes can tell the two apart. At N = 4 they part company — floor(4/2) + 1 = 3,
ceil(4/2) = 2 — and the difference is split brain, because two disjoint pairs would each
hold a "majority" and each elect a leader in the same term.

The arithmetic is right, and swapping it for `ceil` was ALREADY caught before this file
existed — by `test_commit_advances_only_as_far_as_a_majority_holds`,
`test_promote_endpoint_round_trip` and
`test_an_undialable_voter_does_not_kill_the_election_loop`, none of which are about
elections. They reach four voters incidentally, on the commit and membership paths, so the
coverage is real but accidental: it would disappear the day any of them were rewritten at
three nodes, and it names nothing an operator would recognise. What was missing is a test
that fails and SAYS "two of four is not a majority".

So this file is the even-N election threshold stated on purpose rather than reached by
accident. The repo grows to four voters by design (`provision` → `attach` → `promote`), so
the size is not hypothetical — it is what the dashboard's own demo produces. The
arithmetic test sweeps N = 1..6 so the boundary is asserted from both sides;
`scripts/mutate.py` carries the matching mutations.

The whole ladder below was also walked against a live cluster through the dashboard on
2026-08-19 — 3 voters, grown to 4 and then 5 through provision → attach → promote — and
every threshold here matched what the running nodes reported. See docs/FAILURE_MODES.md.
"""

import asyncio

import pytest

from conftest import SimCluster, eventually, make_cfg
from invariants import RaftInvariantMonitor
from raftkv.models import ClusterConfig, Command, Role
from raftkv.raft import RaftNode
from raftkv.storage import Storage
from raftkv.transport import MemoryTransport


def _voters(n: int) -> dict[str, str]:
    return {f"node-{i}": f"addr-{i}" for i in range(1, n + 1)}


# ---- the arithmetic itself ----------------------------------------------------


@pytest.mark.parametrize(
    ("voters", "expected"),
    # floor(N/2)+1 on the left, and for the even rows ceil(N/2) is strictly smaller:
    # N=2 -> 1, N=4 -> 2, N=6 -> 3. Those three rows are the whole point of the table.
    [(1, 1), (2, 2), (3, 2), (4, 3), (5, 3), (6, 4)],
)
def test_quorum_is_floor_of_half_plus_one(tmp_path, voters, expected):
    node = RaftNode(
        make_cfg("node-1", peers={p: a for p, a in _voters(voters).items() if p != "node-1"}),
        Storage(str(tmp_path / f"q{voters}.db")),
        MemoryTransport(),
    )
    try:
        assert node.quorum == expected
    finally:
        node.storage.close()


@pytest.mark.parametrize("acked", [0, 1, 2])
def test_half_of_four_voters_is_not_a_majority(acked):
    """The mutation this file exists to kill. Two of four is exactly half, and half is
    not a majority — if it were, the other two would be one too, and both halves could
    elect a leader in the same term."""
    voters = _voters(4)
    assert not RaftNode._is_majority(voters, set(list(voters)[:acked]))


@pytest.mark.parametrize("acked", [3, 4])
def test_more_than_half_of_four_voters_is_a_majority(acked):
    voters = _voters(4)
    assert RaftNode._is_majority(voters, set(list(voters)[:acked]))


def test_quorum_counts_voters_only_however_many_learners_are_attached(tmp_path):
    """A learner is a replica, not a vote. Four voters plus three learners is still a
    quorum of three — counting the learners would give five and stall the cluster on a
    single voter failure."""
    node = RaftNode(make_cfg("node-1"), Storage(str(tmp_path / "learners.db")), MemoryTransport())
    try:
        node.config = ClusterConfig(voters=_voters(4),
                                    learners={f"L{i}": f"a{i}" for i in range(3)})
        assert node.quorum == 3
    finally:
        node.storage.close()


# ---- and the same arithmetic, elected for real ---------------------------------


async def _elect(sim: SimCluster) -> RaftNode:
    await eventually(lambda: sim.leader() is not None, timeout=5.0)
    return sim.leader()


async def _assert_no_leader_for(sim: SimCluster, seconds: float, monitor=None) -> None:
    """Assert a NON-event, so it has to be given time to happen. One election timeout
    would be the minimum honest window; several gives a stuck candidate room to retry
    and be refused again."""
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        if monitor is not None:
            monitor.observe(sim)
        leader = sim.leader()
        assert leader is None, (
            f"{leader.cfg.node_id} elected itself leader of term {leader.current_term} "
            f"without a majority — quorum is {leader.quorum} of 4"
        )
        await asyncio.sleep(0.02)


@pytest.fixture
async def four(tmp_path):
    sim = SimCluster(tmp_path, n=4)
    sim.start()
    yield sim
    await sim.stop()


async def test_three_of_four_voters_elect_a_leader(four):
    leader = await _elect(four)
    assert leader.quorum == 3

    await four.crash(leader.cfg.node_id)

    replacement = None

    def elected():
        nonlocal replacement
        replacement = four.leader()
        return replacement is not None and replacement.cfg.node_id != leader.cfg.node_id

    await eventually(elected, timeout=5.0)
    assert replacement.current_term > leader.current_term


async def test_two_of_four_voters_cannot_elect_a_leader(four):
    """The floor/ceiling test. Two survivors of four are exactly half — one short of a
    majority — so they must campaign and lose, forever. Under `ceil(N/2)` they would each
    reach a "quorum" of two and this assertion would fail immediately."""
    leader = await _elect(four)
    monitor = RaftInvariantMonitor()
    monitor.observe(four)

    survivors = [i for i in four.ids if i != leader.cfg.node_id][:2]
    for node_id in four.ids:
        if node_id not in survivors:
            await four.crash(node_id)

    await _assert_no_leader_for(four, seconds=1.5, monitor=monitor)

    # ...and they were genuinely trying, rather than sitting idle: a test that asserts
    # "no leader" against a cluster that never campaigned asserts nothing at all.
    assert any(four.nodes[n].metrics.pre_votes_started > 0 for n in survivors)


async def test_a_two_two_partition_elects_nobody_on_either_side(four):
    """Both halves are the same size, and neither is a majority. This is the shape that
    `ceil(N/2)` turns into split brain: two leaders, one term, two divergent logs.

    The incumbent is given its CheckQuorum window first, deliberately. A partition does
    not depose a leader on contact — it keeps the role, and the entry it cannot commit,
    until its own silence tells it otherwise (thesis §6.2, and tests/test_partition.py
    for the window itself). Asserting "no leader" before that fires would fail on the
    incumbent and never reach the property this test is about, which is that NOBODY on
    either side of a 2-2 split can win the election that follows."""
    incumbent = await _elect(four)
    monitor = RaftInvariantMonitor()

    left, right = set(four.ids[:2]), set(four.ids[2:])
    four.transport.partition(left, right)

    await eventually(lambda: incumbent.role is not Role.LEADER, timeout=5.0)
    await _assert_no_leader_for(four, seconds=1.5, monitor=monitor)

    four.transport.heal()
    leader = await _elect(four)
    monitor.observe(four)
    assert leader.quorum == 3


async def test_a_four_voter_cluster_replaces_a_dead_leader_promptly(four):
    """The liveness property, at the size where it was observed to fail.

    Found live on 2026-08-19: killing the leader of a four-voter cluster left it with no
    leader for ~45 s while three of four voters were up with identical logs, because each
    survivor's own campaign renewed the PreVote lease it then used to refuse the others
    (see tests/test_pre_vote.py, "the lease clock"). Four voters is where it became
    visible rather than merely slow — a candidate needs TWO grants instead of one, so the
    chance of clearing every peer's self-renewed lease in the same round is squared.

    The deadline is deliberately tight in election timeouts rather than in seconds: at
    TEST_TIMING that is 0.1-0.2 s per election, so ten of them is a cluster that has had
    every chance and is not taking it."""
    leader = await _elect(four)
    monitor = RaftInvariantMonitor()
    await four.crash(leader.cfg.node_id)

    deadline = 10 * make_cfg().election_timeout_max
    await eventually(
        lambda: (four.leader() is not None
                 and four.leader().cfg.node_id != leader.cfg.node_id),
        timeout=deadline,
    )
    monitor.observe(four)
    assert four.leader().role is Role.LEADER


# ---- five voters: the odd neighbour, and what the growth path is FOR ------------


@pytest.fixture
async def five(tmp_path):
    sim = SimCluster(tmp_path, n=5)
    sim.start()
    yield sim
    await sim.stop()


async def test_five_voters_elect_and_commit_after_two_failures(five):
    """The reason to grow past four at all: quorum is 3 at BOTH sizes, so the fifth
    voter buys a second tolerated failure for free.

    `test_membership_growth.py::test_five_voters_tolerate_two_failures_where_four_
    tolerated_one` asserts the arithmetic — `len(voters) - quorum == 2`. This asserts the
    consequence, which is the part an operator cares about and the part that can break
    independently: after two real crashes the survivors still ELECT, and a write still
    COMMITS on a majority that is now exactly the quorum with nothing to spare."""
    leader = await _elect(five)
    assert leader.quorum == 3  # floor(5/2)+1, the same as at four voters
    monitor = RaftInvariantMonitor()

    victim = next(i for i in five.ids if i != leader.cfg.node_id)
    await five.crash(leader.cfg.node_id)
    await five.crash(victim)

    survivor = None

    def elected():
        nonlocal survivor
        survivor = five.leader()
        return survivor is not None and survivor.cfg.node_id not in (leader.cfg.node_id, victim)

    await eventually(elected, timeout=5.0)
    monitor.observe(five)

    # exactly three voters remain, and three IS the quorum -- nothing to spare
    await survivor.submit(Command(op="set", key="five", value="voters", request_id="r5"))
    assert survivor.storage.kv_all()["five"] == "voters"
    monitor.observe(five)


async def test_two_of_five_voters_cannot_elect_a_leader(five):
    """One failure past what five voters tolerate. Two of five is below the quorum of
    three, so the survivors campaign and lose — the same boundary as two of four, at the
    size where losing it would be least obvious."""
    leader = await _elect(five)
    monitor = RaftInvariantMonitor()

    survivors = [i for i in five.ids if i != leader.cfg.node_id][:2]
    for node_id in five.ids:
        if node_id not in survivors:
            await five.crash(node_id)

    await _assert_no_leader_for(five, seconds=1.5, monitor=monitor)
    assert any(five.nodes[n].metrics.pre_votes_started > 0 for n in survivors)
