from raftkv.models import Command, LogEntry, RequestVoteRequest, Role


def vote_req(term=1, candidate="node-2", last_idx=0, last_term=0):
    return RequestVoteRequest(
        term=term, candidate_id=candidate, last_log_index=last_idx, last_log_term=last_term
    )


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def test_grants_vote_to_fresh_candidate(make_node):
    node = make_node()
    resp = node.handle_request_vote(vote_req(term=1))
    assert resp.vote_granted
    assert node.voted_for == "node-2"
    # rule 1: the grant is PERSISTED before the reply — without this line on disk,
    # a rebooted voter double-votes and two leaders can share a term
    assert node.storage.load()[:2] == (1, "node-2")


def test_one_vote_per_term_but_regrant_same_candidate(make_node):
    node = make_node()
    assert node.handle_request_vote(vote_req(term=1, candidate="node-2")).vote_granted
    assert not node.handle_request_vote(vote_req(term=1, candidate="node-3")).vote_granted
    # a retried RPC from the SAME candidate is re-granted (rule 6 wording)
    assert node.handle_request_vote(vote_req(term=1, candidate="node-2")).vote_granted


def test_rejects_stale_term(make_node):
    node = make_node()
    node.current_term = 5
    resp = node.handle_request_vote(vote_req(term=3))
    assert not resp.vote_granted
    assert resp.term == 5  # candidate learns the newer term


def test_rejects_candidate_with_older_last_term(make_node):
    node = make_node()
    node.storage.append([entry(term=3)])
    # candidate's log is LONGER but its last term is older -> not up-to-date (§5.4.1)
    assert not node.handle_request_vote(vote_req(term=4, last_idx=5, last_term=2)).vote_granted


def test_rejects_candidate_with_shorter_log_same_term(make_node):
    node = make_node()
    node.storage.append([entry(term=1, rid="a"), entry(term=1, rid="b")])
    assert not node.handle_request_vote(vote_req(term=2, last_idx=1, last_term=1)).vote_granted


def test_empty_log_candidate_never_wins_however_high_its_term(make_node):
    """Seen in the wild on 2026-08-15: a node whose database had been wiped rejoined a
    cluster holding one committed entry and campaigned 16 consecutive times, terms 5
    through 20. It never won a single vote. §5.4.1 compares LOGS, not terms, so no
    amount of term inflation lets a node without the data lead the nodes that have it —
    which is precisely what stops it from erasing that data.

    It did depose the leader every time, though (see the term assertion): that is the
    disruptive-server problem PreVote solves, listed as a deliberate omission in
    docs/FAILURE_MODES.md."""
    voter = make_node()
    voter.storage.append([entry(term=6)])  # the one committed entry the peers held
    for term in range(5, 21):
        resp = voter.handle_request_vote(vote_req(term=term, last_idx=0, last_term=0))
        assert not resp.vote_granted, f"empty-log candidate won a vote at term {term}"
        assert voter.voted_for is None  # and nothing was persisted on its behalf
        assert resp.term == term  # ...yet the voter DOES adopt the term: the disruption


def test_higher_term_deposes_leader_and_persists(make_node):
    node = make_node()
    node.role = Role.LEADER
    node.current_term = 2
    node.voted_for = "node-1"
    node.handle_request_vote(vote_req(term=7))
    assert node.role is Role.FOLLOWER
    assert node.current_term == 7
    assert node.storage.load()[0] == 7  # rule 1: a reopened node must not double-vote


def test_vote_grant_resets_election_timer(make_node):
    node = make_node()
    node._last_reset = 0.0
    node.handle_request_vote(vote_req(term=1))
    assert node._last_reset > 0.0


def test_rejected_vote_does_not_reset_timer(make_node):
    """Students' Guide bug #1: resetting on vote REQUESTS (vs grants) causes livelock."""
    node = make_node()
    node.current_term = 5
    node._last_reset = 0.0
    node.handle_request_vote(vote_req(term=3))
    assert node._last_reset == 0.0


def test_deposed_leader_timer_is_reset(make_node):
    """A leader's timer is stale from its whole tenure. Without a reset on
    step-down, an ex-leader re-elects itself within one tick after being deposed."""
    node = make_node()
    node.role = Role.LEADER
    node.current_term = 2
    node._last_reset = 0.0
    node._observe_term(9)  # e.g. a higher term seen in an RPC response
    assert node.role is Role.FOLLOWER
    assert node._last_reset > 0.0


def test_grants_vote_when_the_logs_are_exactly_equal(make_node):
    """§5.4.1 compares with >=, not >. Two in-sync nodes are the common case, so a
    strict > here means a healthy cluster can never elect anyone."""
    node = make_node()
    node.storage.append([entry(term=2, rid="a"), entry(term=2, rid="b")])
    assert node.handle_request_vote(vote_req(term=3, last_idx=2, last_term=2)).vote_granted


def test_grants_vote_when_our_own_log_is_empty(make_node):
    node = make_node()
    assert node.storage.last_log_index() == 0
    assert node.handle_request_vote(vote_req(term=2, last_idx=7, last_term=3)).vote_granted
    assert node.voted_for == "node-2"


def test_observe_term_never_moves_the_term_backwards(make_node):
    node = make_node()
    node.current_term = 5
    node.role = Role.LEADER
    node._observe_term(2)
    assert node.current_term == 5
    assert node.role is Role.LEADER  # and a stale term does not depose us


def test_each_reset_draws_a_timeout_inside_the_configured_range(make_node):
    """§5.2 requires randomization per reset. A fixed timeout makes every follower
    campaign on the same tick, split the vote, and repeat."""
    node = make_node()
    low, high = node.cfg.election_timeout_range
    drawn = set()
    for _ in range(50):
        node._reset_election_timer()
        assert low <= node._election_timeout <= high
        drawn.add(node._election_timeout)
    assert len(drawn) > 1
