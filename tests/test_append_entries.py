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
