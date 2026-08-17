from raftkv.models import AppendEntriesRequest, Command, LogEntry, Role


def entry(term: int, rid: str = "r") -> LogEntry:
    return LogEntry(term=term, command=Command(op="set", key="k", value="v", request_id=rid))


def ae(term=1, prev_idx=0, prev_term=0, entries=(), commit=0, leader="node-9"):
    return AppendEntriesRequest(
        term=term, leader_id=leader, prev_log_index=prev_idx,
        prev_log_term=prev_term, entries=list(entries), leader_commit=commit,
    )


def test_rejects_stale_term(make_node):
    node = make_node()
    node.current_term = 5
    resp = node.handle_append_entries(ae(term=3))
    assert not resp.success
    assert resp.term == 5


def test_appends_and_recognizes_leader(make_node):
    node = make_node()
    resp = node.handle_append_entries(ae(term=1, entries=[entry(1, "a")]))
    assert resp.success
    assert node.leader_id == "node-9"
    assert node.storage.last_log_index() == 1


def test_consistency_check_rejects_gap(make_node):
    node = make_node()  # empty log, leader claims prev at index 5
    assert not node.handle_append_entries(ae(term=1, prev_idx=5, prev_term=1)).success


def test_consistency_check_rejects_term_mismatch(make_node):
    node = make_node()
    node.storage.append([entry(1, "a")])
    assert not node.handle_append_entries(ae(term=2, prev_idx=1, prev_term=2)).success


def test_heartbeat_runs_full_checks(make_node):
    """Students' Guide: an empty AppendEntries must NOT shortcut to success —
    success implicitly reports a log match through prev_log_index."""
    node = make_node()
    assert not node.handle_append_entries(ae(term=1, prev_idx=3, prev_term=1)).success


def test_conflict_truncates_suffix(make_node):
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    resp = node.handle_append_entries(
        ae(term=2, prev_idx=1, prev_term=1, entries=[entry(2, "x")])
    )
    assert resp.success
    assert node.storage.last_log_index() == 2
    assert node.storage.entry(2).command.request_id == "x"
    assert node.storage.term_at(2) == 2


def test_stale_append_does_not_truncate_matching_entries(make_node):
    """The conditional the Students' Guide calls crucial: a duplicated/reordered
    older AppendEntries carrying a PREFIX of our log must leave newer entries alone."""
    node = make_node()
    node.handle_append_entries(ae(term=1, entries=[entry(1, "a"), entry(1, "b")]))
    resp = node.handle_append_entries(ae(term=1, entries=[entry(1, "a")]))  # replayed prefix
    assert resp.success
    assert node.storage.last_log_index() == 2  # "b" survived


def test_follower_commit_capped_by_verified_entries(make_node):
    """Fig. 2 receiver step 5: commit = min(leaderCommit, last new entry)."""
    node = make_node()
    resp = node.handle_append_entries(ae(term=1, entries=[entry(1, "a")], commit=99))
    assert resp.success
    assert node.commit_index == 1


def test_candidate_steps_down_on_current_leader(make_node):
    node = make_node()
    node.role = Role.CANDIDATE
    node.current_term = 2
    node.handle_append_entries(ae(term=2))
    assert node.role is Role.FOLLOWER


def test_timer_resets_only_for_current_leader(make_node):
    node = make_node()
    node.current_term = 5
    node._last_reset = 0.0
    node.handle_append_entries(ae(term=3))  # stale leader: no reset
    assert node._last_reset == 0.0
    node.handle_append_entries(ae(term=5))  # current leader: reset
    assert node._last_reset > 0.0


def test_empty_append_never_shortens_the_log(make_node):
    """The truncation loop is driven by req.entries, so a heartbeat cannot fire it. A
    leader still walking nextIndex backwards sends entry-less appends by design, and
    those must not delete the entries it is about to confirm."""
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    resp = node.handle_append_entries(ae(term=1, prev_idx=1, prev_term=1, commit=3))
    assert resp.success
    assert node.storage.last_log_index() == 3
    # Fig. 2 step 5 caps commit at prev_log_index + len(entries); with no entries that is
    # prev_log_index itself, so a lagging heartbeat commits only as far as it verified
    assert node.commit_index == 1


def test_commit_index_never_moves_backwards(make_node):
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node.commit_index = 3
    resp = node.handle_append_entries(ae(term=1, prev_idx=3, prev_term=1, commit=1))
    assert resp.success
    assert node.commit_index == 3


def test_a_rejected_append_mutates_nothing(make_node):
    """A failed consistency check is a question answered, not a change made: the leader
    retries lower, and the log must be exactly as it was."""
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b"), entry(1, "c")])
    node.current_term = 3
    node.commit_index = 2
    resp = node.handle_append_entries(
        ae(term=3, prev_idx=3, prev_term=2, entries=[entry(3, "x")], commit=9)
    )
    assert not resp.success
    assert [node.storage.entry(i).command.request_id for i in (1, 2, 3)] == ["a", "b", "c"]
    assert node.storage.last_log_index() == 3
    assert node.commit_index == 2


def test_a_term_gap_between_entries_is_not_a_conflict(make_node):
    """What every log looks like after an election: the next entry carries a higher term
    than its predecessor. Only a term MISMATCH at an occupied index is a conflict."""
    node = make_node()
    node.storage.append([entry(1, "a"), entry(1, "b")])
    resp = node.handle_append_entries(
        ae(term=4, prev_idx=2, prev_term=1, entries=[entry(4, "x")])
    )
    assert resp.success
    assert node.storage.last_log_index() == 3
    assert node.storage.term_at(3) == 4


def test_a_failed_consistency_check_still_resets_the_timer(make_node):
    """"From the current leader" gates on the TERM being current, not on the append
    succeeding — thesis §3.5 has the leader send entry-less appends while walking
    nextIndex backwards, failing the check by design, and defines follower liveness as
    receipt of *valid* RPCs rather than successful ones. etcd/raft orders it the same way
    in stepFollower: electionElapsed = 0 and lead = m.From both precede the check.

    Gate on success instead and a follower whose log has diverged deposes a healthy
    leader for the whole duration of its own repair. That is a liveness failure, not a
    safety one, which is exactly why it survives casual testing."""
    node = make_node()
    node.storage.append([entry(1, "a")])
    node.current_term = 1
    node._last_reset = 0.0
    resp = node.handle_append_entries(ae(term=1, prev_idx=1, prev_term=9))
    assert not resp.success
    assert node._last_reset > 0.0


def test_a_failed_consistency_check_still_records_the_leader(make_node):
    """The same ordering, for leader_id, and it is user-visible: leader_id is what a
    follower puts in its 503 so the client knows where to retry. A follower that is
    behind — precisely the one whose appends fail — would otherwise answer "not leader,
    and I do not know who is", stranding the write until its log is repaired."""
    node = make_node()
    node.storage.append([entry(1, "a")])
    node.current_term = 1
    node.leader_id = None
    resp = node.handle_append_entries(ae(term=1, prev_idx=1, prev_term=9, leader="node-7"))
    assert not resp.success
    assert node.leader_id == "node-7", "a lagging follower cannot redirect its client"


def test_a_stale_leader_names_nobody(make_node):
    """The other half of the rule, and why it is not simply "always record": a deposed
    leader still sending at an old term must not name itself to a node that moved on."""
    node = make_node()
    node.current_term = 5
    node.leader_id = None
    resp = node.handle_append_entries(ae(term=3, leader="node-9"))
    assert not resp.success
    assert node.leader_id is None


def test_a_candidate_ignores_a_stale_leader(make_node):
    """The complement of stepping down for a current one: an old-term append must not
    end a candidacy that has already moved past it."""
    node = make_node()
    node.role = Role.CANDIDATE
    node.current_term = 5
    resp = node.handle_append_entries(ae(term=3))
    assert not resp.success
    assert node.role is Role.CANDIDATE
    assert node.leader_id is None
