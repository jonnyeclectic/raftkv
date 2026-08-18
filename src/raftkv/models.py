"""Every shared data shape in the system. If a dict shape is reused, it lives here."""

from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Most entries in one AppendEntries — the leader's send cap AND the wire cap, deliberately
# one number (see AppendEntriesRequest.entries). The batch used to be unbounded — every
# outstanding entry, to every follower, every round — so a burst of N in flight shipped an
# O(N) payload per peer per round and paid to decode it out of SQLite each time. Measured
# at 2000 pending: ~195 KiB per peer and ~33 ms of event-loop time per round across four
# followers, work that repeats until the burst drains.
#
# The number is a payload budget, not a tuning dial: at roughly 100 bytes an entry this
# caps one message near 50 KiB. Raising it walks back toward the unbounded behaviour;
# lowering it costs more round trips, which is cheap ONLY because a successful append that
# leaves a follower behind re-fires replication immediately instead of waiting for the next
# heartbeat (see raft._append_to_peer). Change one without the other and catch-up after a
# flood goes back to taking minutes.
#
# It lives here rather than in raft.py because the receiving side needs it too, and a
# second constant that merely happens to match is the drift this avoids.
MAX_ENTRIES_PER_APPEND = 512


class Role(StrEnum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class Command(BaseModel):
    """A state-machine command carried inside a log entry."""

    model_config = ConfigDict(frozen=True)

    op: Literal["set", "delete"]
    key: str = Field(min_length=1, max_length=256)
    value: str | None = Field(default=None, max_length=4096)
    # Minted SERVER-side (app.py, uuid4) — the API exposes no way for a client to supply
    # one. So it cannot deduplicate a client retry even in principle: two retries of the
    # same logical write carry different ids by construction. Its one job is detecting a
    # log index reused by a different leader (Students' Guide, "re-appearing index").
    # Exactly-once would need client sessions; see FAILURE_MODES.md table 2.
    request_id: str

    @model_validator(mode="after")
    def _set_requires_value(self) -> Self:
        if self.op == "set" and self.value is None:
            raise ValueError("op=set requires a value")
        return self


class NoOp(BaseModel):
    """An entry with no state-machine effect, appended by a leader on winning.

    Two jobs, both load-bearing. It commits an entry from the NEW leader's term, which
    (thesis §6.4) flushes through anything a previous leader committed but never
    applied; and it is the precondition for any membership change, because a leader
    holding an uncommitted configuration entry from a prior term cannot otherwise tell
    whether that entry is committed."""

    model_config = ConfigDict(frozen=True)

    op: Literal["noop"] = "noop"


class ClusterConfig(BaseModel):
    """A configuration entry in the log (§6).

    Membership lives IN THE LOG so it replicates like anything else: it survives a
    leader change, and it reverts if the entry is truncated. That last property is why
    a configuration takes effect when APPENDED rather than when committed -- the
    configuration may be needed for the very election that commits it.

    During a joint transition `old_voters` is non-empty and this entry is C-old,new:
    every decision then needs a majority of BOTH sets, which is what makes it
    impossible for the two configurations to elect separate leaders."""

    model_config = ConfigDict(frozen=True)

    op: Literal["config"] = "config"
    voters: dict[str, str] = Field(default_factory=dict)      # id -> addr
    old_voters: dict[str, str] = Field(default_factory=dict)  # set only while joint
    learners: dict[str, str] = Field(default_factory=dict)

    @property
    def joint(self) -> bool:
        return bool(self.old_voters)


class LogEntry(BaseModel):
    model_config = ConfigDict(frozen=True)

    term: int
    # Discriminated on `op`, so a log row round-trips back to the right type. Config
    # entries share the log with client commands precisely so ordering is total.
    command: Command | ClusterConfig | NoOp = Field(discriminator="op")


class RequestVoteRequest(BaseModel):
    term: int
    candidate_id: str
    last_log_index: int
    last_log_term: int
    # PreVote (thesis §9.6): a straw poll, not a candidacy. `term` is then the term the
    # sender WOULD run in — one above its own — and the receiver must not treat it as an
    # observed term, because the whole purpose is to find out whether running is worth the
    # disruption BEFORE paying for it. Optional and defaulting to False so a peer running
    # older code is simply asked for a real vote; see RaftNode._handle_pre_vote.
    pre_vote: bool = False


class RequestVoteResponse(BaseModel):
    term: int
    vote_granted: bool


class AppendEntriesRequest(BaseModel):
    term: int
    leader_id: str
    prev_log_index: int
    prev_log_term: int
    # Bounded because `/raft/append-entries` is UNAUTHENTICATED. Every entry was already
    # bounded (`Command.key` 256, `Command.value` 4096) and the list was not, so the size
    # of one request was capped only by how many entries a caller cared to put in it —
    # 50,000 of them decode to ~205 MB, from anyone who can open a socket to the port.
    # The asymmetry was the tell: every *admin* model here is bounded, and the two models
    # reachable without credentials were the ones that were not.
    #
    # Sharing the leader's send cap rather than declaring a second number is the point: two
    # constants that must agree are a drift waiting to happen, and raising the send cap
    # while leaving a smaller wire cap in place would make every follower reject a
    # well-formed append.
    entries: list[LogEntry] = Field(
        default_factory=list, max_length=MAX_ENTRIES_PER_APPEND
    )
    leader_commit: int


class AppendEntriesResponse(BaseModel):
    term: int
    success: bool
    # §5.3 accelerated backtracking. Set only on a consistency-check rejection, and
    # OPTIONAL on purpose: a stale-term rejection carries no useful hint, and a peer
    # running older code sends none at all. Both cases fall back to the naive one-index
    # walk rather than to a wrong jump — see RaftNode._next_index_after_rejection.
    conflict_index: int | None = None
    conflict_term: int | None = None


class Metrics(BaseModel):
    elections_started: int = 0
    # Straw polls attempted (thesis §9.6). It exists because PreVote deliberately removes
    # the only evidence there used to be: an isolated node used to announce that it was
    # still trying by inflating its term, visibly, once per election timeout. Now it stays
    # quiet — which is the point, and which would otherwise make "wedged" and "patiently
    # failing to win" look identical from outside. The gap between this and
    # `elections_started` IS the count of elections PreVote suppressed.
    pre_votes_started: int = 0
    votes_granted: int = 0
    append_entries_sent: int = 0  # counts sends (incl. heartbeats), not acks
    append_entries_received: int = 0
    rpc_failures: int = 0


class NodeState(BaseModel):
    """Payload of GET /state — the dashboard's entire world view of one node."""

    node_id: str
    role: Role
    term: int
    voted_for: str | None
    leader_id: str | None
    log_length: int
    last_log_term: int
    commit_index: int
    last_applied: int
    kv: dict[str, str]
    peers: dict[str, str]  # peer_id -> host:port, so the dashboard can show the topology
    learners: dict[str, str] = Field(default_factory=dict)  # replicated to, never counted
    learner: bool = False  # this node is a non-voting member
    advertise_addr: str = ""  # how peers should dial this node (may differ from :port)
    blocked: list[str] = Field(default_factory=list)  # simulated partition, both ways
    # The configuration this node is currently operating under, taken from the latest
    # config entry in its log. `config.joint` is the transition the dashboard draws.
    config: ClusterConfig = Field(default_factory=ClusterConfig)
    quorum: int = 0  # majority of `config.voters`; during a joint phase, of C-new
    metrics: Metrics
