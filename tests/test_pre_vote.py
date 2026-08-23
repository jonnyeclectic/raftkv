"""PreVote (thesis §9.6): ask before running, and stay quiet if the answer is no.

This ships as one change with CheckQuorum, and neither is safe alone. CheckQuorum makes
a partitioned leader resign — which converts a quiet stale leader into a candidate, and
a candidate that can never reach a quorum burns a term per election timeout for as long
as the partition lasts. It rejoins tens of terms ahead, and that term alone deposes a
leader that was serving perfectly well. Fixing an invisible failure by creating a noisy
one is not a fix.

The straw poll closes it. A node asks "would you vote for me at term+1?" without
incrementing anything, and only becomes a candidate if the answer is yes. So the pair
reads: step down when you cannot lead, and stay silent while you cannot win.

Everything a pre-vote must NOT do is a test below, because each omission would quietly
turn the mechanism back into the problem it exists to solve.
"""

import asyncio
import time

from mockito import ANY

from conftest import StubTransport, eventually
from raftkv.models import (
    AppendEntriesRequest,
    LogEntry,
    NoOp,
    RequestVoteRequest,
    RequestVoteResponse,
    Role,
)
from raftkv.transport import TransportError


def poll(term: int, *, last_log_index: int = 0, last_log_term: int = 0,
         candidate: str = "node-2") -> RequestVoteRequest:
    return RequestVoteRequest(
        term=term, candidate_id=candidate, last_log_index=last_log_index,
        last_log_term=last_log_term, pre_vote=True,
    )


# ---- the receiving side: what a straw poll must not cost the receiver ----------


def test_a_pre_vote_does_not_move_the_receivers_term(make_node):
    """The whole mechanism in one assertion. `req.term` is a term nobody is running in
    yet — it is the term the sender WOULD use. Observing it would inflate our term on the
    strength of a question, which is precisely the disruption PreVote exists to prevent:
    the mechanism would become a cheaper way to cause the problem."""
    node = make_node()
    node.current_term = 3

    resp = node.handle_request_vote(poll(9))

    assert node.current_term == 3
    assert resp.term == 3  # our own term, so a candidate that is behind learns it is
    assert resp.vote_granted


def test_a_pre_vote_is_not_recorded_as_a_vote(make_node):
    """A poll binds nothing, so §5.2's one-vote-per-term rule has nothing to record. A
    node may answer any number of straw polls in a term and still cast exactly one real
    vote — and persisting one here would let a single poll spend the vote a genuine
    candidate is about to ask for."""
    node = make_node()
    node.current_term = 3
    node.storage.save_term_and_vote(3, None)

    assert node.handle_request_vote(poll(4)).vote_granted
    assert node.handle_request_vote(poll(4, candidate="node-3")).vote_granted

    assert node.voted_for is None
    assert node.storage.load()[:2] == (3, None)


def test_a_pre_vote_does_not_reset_the_election_timer(make_node):
    """Granting a poll is not "I heard from a leader". If it reset the timer, a
    partitioned candidate could suppress the elections of the very nodes it keeps
    asking — holding the cluster leaderless by repeatedly failing to be elected."""
    node = make_node()
    node._last_reset = time.monotonic() - 5.0
    before = node._last_reset

    node.handle_request_vote(poll(4))

    assert node._last_reset == before


def test_a_node_that_just_heard_from_its_leader_refuses(make_node):
    """The lease, and the reason CheckQuorum and PreVote compose instead of fighting. A
    node inside one `election_timeout_min` of its leader has no business helping depose
    it: the leader is audibly fine, and the asker is the one having connectivity
    trouble.

    An AppendEntries from the leader is what "heard from" MEANS, so that is what this
    drives. The lease used to read `_last_reset` -- the election timer -- and setting that
    field directly was how this test said "recently". It is not the same statement: the
    election timer is also pushed forward by granting a vote and by starting an election
    of our own, so a node that had merely campaigned looked, to this assertion, exactly
    like a node whose leader had just spoken. See "the lease clock" below."""
    node = make_node()
    node.current_term = 3
    node.handle_append_entries(
        AppendEntriesRequest(term=3, leader_id="node-3", prev_log_index=0,
                             prev_log_term=0, entries=[], leader_commit=0)
    )

    assert node.leader_id == "node-3"
    assert not node.handle_request_vote(poll(4)).vote_granted


def test_a_node_that_knows_no_leader_grants_immediately(make_node):
    """`leader_id is not None` is what makes the lease safe to apply, and dropping that
    clause is a plausible-looking simplification that costs every cold start an extra
    election timeout: at boot every node is inside its own startup window with no leader
    to protect, so all of them would refuse the first poll of the cluster's life."""
    node = make_node()
    node.leader_id = None
    node._last_heard_from_leader = time.monotonic()  # fresh, but protecting nobody

    assert node.handle_request_vote(poll(4)).vote_granted


def test_a_leader_refuses_every_poll_for_as_long_as_it_leads(make_node):
    """Found live, invisible in a stub: a leader's `_last_reset` is stale for its whole
    tenure by design, so the elapsed-time test alone reads a perfectly healthy leader as
    one nobody has heard from. It would then grant the poll of the first follower whose
    timer drifted, and that follower would depose it using the incumbent's own vote —
    a worse election storm than the one PreVote exists to stop.

    `_check_quorum` is the only thing allowed to end a leadership. Until it fires, this
    node IS the lease."""
    node = make_node()
    node.current_term = 4
    node._become_leader()  # appends its opening no-op at index 1, term 4
    node._last_reset = time.monotonic() - 99.0  # stale, as every leader's timer is

    # An UNIMPEACHABLE poll: newer term, and a log at least as up to date as ours. The
    # only rule left that can refuse it is the lease.
    #
    # The first draft of this test used the default (0, 0) log position and was refused by
    # the §5.4.1 election restriction instead — so it passed just as happily against a
    # build with no leader clause at all, asserting on a regime where the code under test
    # could not fail. scripts/mutate.py caught it (`prevote: a leader no longer holds its
    # own lease` survived); the pair of assertions below is what makes it fail.
    unimpeachable = poll(9, last_log_index=1, last_log_term=4)

    assert not node.handle_request_vote(unimpeachable).vote_granted

    # ...and it is the LEADERSHIP doing the refusing, not something incidental: the very
    # same poll is granted the moment this node stops leading and has no leader to protect.
    node._become_follower()
    node.leader_id = None
    assert node.handle_request_vote(unimpeachable).vote_granted


def test_a_lapsed_lease_grants_again(make_node):
    node = make_node()
    node.leader_id = "node-3"
    node._last_heard_from_leader = time.monotonic() - 5.0  # the leader has gone quiet

    assert node.handle_request_vote(poll(4)).vote_granted


def test_a_poll_for_a_term_we_have_already_passed_is_refused(make_node):
    """A poll asks about a FUTURE term. One at or below ours would lose the real election
    it is rehearsing for, so the honest answer is no plus our term — which is how a node
    returning from a partition discovers it is behind without running at all."""
    node = make_node()
    node.current_term = 7

    assert not node.handle_request_vote(poll(7)).vote_granted
    assert not node.handle_request_vote(poll(6)).vote_granted
    assert node.handle_request_vote(poll(8)).vote_granted


def test_the_election_restriction_applies_to_the_poll_too(make_node):
    """§5.4.1 is not relaxed for a rehearsal. A poll that ignored the log would encourage
    exactly the candidates the restriction exists to keep out, and the pre-election would
    stop predicting the election."""
    node = make_node()
    node.current_term = 2
    node.storage.append([LogEntry(term=2, command=NoOp())])

    assert not node.handle_request_vote(poll(3, last_log_index=0, last_log_term=0)).vote_granted
    assert node.handle_request_vote(poll(3, last_log_index=1, last_log_term=2)).vote_granted


def test_a_learner_answers_no(make_node):
    """A node outside every voter set is counted by nobody, so its encouragement is
    worthless — and a poll that counted it would predict a majority that does not
    exist (§6)."""
    node = make_node(peer_ids=(), learner=True)

    assert not node.handle_request_vote(poll(4)).vote_granted


# ---- the asking side ----------------------------------------------------------


class Answering(StubTransport):
    """A peer that answers pre-votes and real votes differently, which is the only way to
    tell the two rounds apart from outside."""

    def __init__(self, *, pre: bool, real: bool):
        self.pre, self.real = pre, real
        self.seen: list[RequestVoteRequest] = []

    async def request_vote(self, peer_id, req):
        self.seen.append(req)
        granted = self.pre if req.pre_vote else self.real
        # `req.term - 1` is the poller's own term by construction; replying with a higher
        # one would depose the node we are pretending to encourage.
        return RequestVoteResponse(term=req.term - 1 if req.pre_vote else req.term,
                                   vote_granted=granted)


async def test_winning_the_poll_runs_the_real_election(make_node):
    node = make_node(transport=Answering(pre=True, real=True))

    await node._start_election()

    assert node.role is Role.LEADER
    assert node.current_term == 1
    assert node.metrics.pre_votes_started == 1
    assert node.metrics.elections_started == 1


async def test_losing_the_poll_leaves_the_term_and_the_disk_alone(make_node):
    """The payoff. A node that cannot win does not run, so it costs the cluster nothing —
    no term burnt, no vote persisted, no RequestVote for anyone else to observe."""
    node = make_node(transport=Answering(pre=False, real=True))

    await node._start_election()

    assert node.role is Role.FOLLOWER
    assert node.current_term == 0
    assert node.storage.load()[:2] == (0, None)
    assert node.metrics.elections_started == 0


async def test_a_failed_poll_costs_a_full_election_timeout_before_the_next(make_node):
    """The timer is reset BEFORE the poll, not after a win. Without that the timer loop
    re-fires on its next tick — one tenth of a timeout — and an isolated node polls its
    unreachable peers ten times as often as it would ever have campaigned, turning a
    quiet failure into a busy one."""
    transport = Answering(pre=False, real=False)
    node = make_node(transport=transport)
    node._last_reset = 0.0

    await node._start_election()

    assert node._last_reset > time.monotonic() - 1.0


async def test_the_poll_asks_for_the_term_it_would_run_in(make_node):
    transport = Answering(pre=False, real=False)
    node = make_node(transport=transport)
    node.current_term = 6

    await node._start_election()

    polled = [r for r in transport.seen if r.pre_vote]
    assert polled and all(r.term == 7 for r in polled)
    assert node.current_term == 6  # asking about 7 is not being in 7


async def test_a_higher_term_in_a_reply_is_still_adopted(make_node, when):
    """A poll is how a node returning from a partition finds out it is behind, and
    adopting that term costs nobody an election — so `_observe_term` still runs on the
    REPLIES even though it must never run on the request."""
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenReturn(
        RequestVoteResponse(term=12, vote_granted=False)
    )

    await node._start_election()

    assert node.current_term == 12
    assert node.role is Role.FOLLOWER
    assert node.metrics.elections_started == 0  # caught up without running


async def test_campaign_skips_the_poll(make_node):
    """`/admin/campaign` is this repo's stand-in for leadership transfer, and TimeoutNow
    bypasses PreVote for exactly this reason (thesis §3.10): a transfer is the operator
    deposing a HEALTHY leader on purpose, which is the one thing the lease is built to
    refuse. Routed through the poll, the control would silently do nothing: the request
    would be accepted and no transfer would ever happen."""
    transport = Answering(pre=False, real=True)  # every peer refuses to encourage it
    node = make_node(transport=transport)

    await node.campaign()

    assert node.role is Role.LEADER
    assert not any(r.pre_vote for r in transport.seen)
    assert node.metrics.pre_votes_started == 0


async def test_an_isolated_node_stays_quiet_for_as_long_as_it_is_isolated(make_node, when):
    """The behaviour the pair exists to produce, over the real timer loop. Before PreVote
    this node would have reached a term in the dozens and deposed a healthy leader with it
    the moment the partition healed."""
    transport = StubTransport()
    node = make_node(transport=transport)
    when(transport).request_vote(ANY(str), ANY(RequestVoteRequest)).thenRaise(
        TransportError("partitioned")
    )
    node._last_reset = 0.0
    task = asyncio.create_task(node._election_timer_loop())
    try:
        await eventually(lambda: node.metrics.pre_votes_started >= 4, timeout=3.0)
    finally:
        task.cancel()

    assert node.current_term == 0
    assert node.role is Role.FOLLOWER


# ---- the lease clock: "heard from the leader", not "did something" -------------
#
# Found live, on a four-voter cluster, by killing the leader and watching: nodes 2 and 4
# each ran ELEVEN consecutive straw polls over ~35 s, every one refused by the other,
# while three of four voters were up and every log was identical. The cluster sat with no
# leader for ~45 s on a fault it tolerates by construction.
#
# The lease is documented -- in _handle_pre_vote and in the tests above -- as "this node
# has heard from its leader inside one election_timeout_min". `_last_reset` is not that
# quantity. It is also advanced by STARTING AN ELECTION (_start_election resets before it
# polls, deliberately, so a failed poll costs a full timeout) and by GRANTING A VOTE. So a
# node's own campaign renews the lease it then uses to refuse everyone else's campaign,
# and two followers whose timers fire within one election_timeout_min of each other
# suppress one another for as long as they both keep trying.
#
# The even-sized cluster is what made it visible rather than merely slow: at four voters a
# candidate needs TWO grants rather than one, so the chance of clearing every peer's
# self-renewed lease in the same round is squared. See tests/test_election_quorum_by_size.py.


def test_our_own_campaign_does_not_renew_the_lease_we_refuse_others_with(make_node):
    """The root cause, isolated. `_start_election(pre_vote=True)` resets the election
    timer BEFORE polling. If the lease reads that same timestamp, a node that has just
    campaigned looks -- to itself -- like a node that has just heard from its leader."""
    node = make_node()
    node.leader_id = "node-3"
    node._last_reset = time.monotonic() - 99.0  # node-3 has been silent for ages

    node._reset_election_timer()  # exactly what _start_election does before it polls

    # A peer whose timer fired a moment later asks the same question this node just asked.
    # Nothing about our own campaign is evidence that node-3 is alive.
    assert node.handle_request_vote(poll(4)).vote_granted


def test_granting_a_vote_does_not_renew_the_lease(make_node):
    """The other half of the same conflation. Granting node-2 a vote is evidence that
    node-2 is campaigning, not evidence that the leader is alive -- so it must not buy a
    dead leader another election_timeout_min of protection.

    The vote is granted AT OUR OWN TERM on purpose. A vote that arrives with a higher
    term clears `leader_id` on the way past (rule 4), which drops the lease's first
    clause and hides the bug behind an unrelated mechanism -- the first draft of this
    test did exactly that and passed against the broken build."""
    node = make_node()
    node.current_term = 4
    node.leader_id = "node-3"
    node._last_reset = time.monotonic() - 99.0

    granted = node.handle_request_vote(  # same term: no deposition, timer still reset
        RequestVoteRequest(term=4, candidate_id="node-2", last_log_index=0, last_log_term=0)
    )
    assert granted.vote_granted and node.leader_id == "node-3"  # the regime under test

    assert node.handle_request_vote(poll(5, candidate="node-4")).vote_granted
