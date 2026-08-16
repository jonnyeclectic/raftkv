"""Node configuration. Raft's liveness depends on heartbeat << election timeout,
so the model enforces that invariant at the trust boundary instead of hoping."""

import os

from pydantic import BaseModel, Field, model_validator


class NodeConfig(BaseModel):
    node_id: str = Field(min_length=1)
    peers: dict[str, str] = Field(default_factory=dict)
    db_path: str = "data/raft.db"
    log_dir: str = "logs"
    heartbeat_interval: float = Field(default=0.5, gt=0)
    election_timeout_min: float = Field(default=1.5, gt=0)
    election_timeout_max: float = Field(default=3.0, gt=0)
    rpc_timeout: float = Field(default=0.5, gt=0)
    commit_timeout: float = Field(default=5.0, gt=0)
    # POST /admin/crash lets the dashboard fail a node with no shell access — every
    # failure control in one browser tab. It is UNAUTHENTICATED remote "stop serving",
    # so it is a demo affordance, not a production one: real deployments set
    # RAFT_ADMIN_ENABLED=0 and inject failure from the orchestrator
    # (kubectl delete pod / compose stop).
    admin_enabled: bool = True
    # A learner receives the log but never votes and never campaigns, so adding one
    # does NOT move quorum — which is why it needs no joint consensus (§6). This is how
    # etcd and LogCabin stage a new member before promoting it to a voter.
    learner: bool = False
    # How OTHER nodes should dial this one. Differs from the address a browser uses
    # whenever there is a port mapping in the way (compose publishes 8004 to the host
    # but peers must use raft-node-4:8000). Empty means "not advertising".
    advertise_addr: str = ""

    @model_validator(mode="after")
    def _sane_timing(self) -> NodeConfig:
        if self.election_timeout_min >= self.election_timeout_max:
            raise ValueError("election_timeout_min must be < election_timeout_max")
        if self.heartbeat_interval >= self.election_timeout_min:
            raise ValueError("heartbeat_interval must be < election_timeout_min (Raft §5.2)")
        if self.rpc_timeout + self.heartbeat_interval >= self.election_timeout_min:
            raise ValueError(
                "rpc_timeout + heartbeat_interval must be < election_timeout_min "
                "(worst-case heartbeat gap must not trigger elections)"
            )
        return self

    @property
    def election_timeout_range(self) -> tuple[float, float]:
        return (self.election_timeout_min, self.election_timeout_max)

    @property
    def tick_interval(self) -> float:
        return self.election_timeout_min / 10

    @classmethod
    def from_env(cls) -> NodeConfig:
        peers: dict[str, str] = {}
        for pair in filter(None, os.getenv("RAFT_PEERS", "").split(",")):
            peer_id, _, addr = pair.partition("=")
            peers[peer_id.strip()] = addr.strip()
        raw: dict[str, object] = {
            "node_id": os.getenv("RAFT_NODE_ID", "node-1"),
            "peers": peers,
        }
        for field, env in [
            ("db_path", "RAFT_DB_PATH"),
            ("log_dir", "RAFT_LOG_DIR"),
            ("heartbeat_interval", "RAFT_HEARTBEAT"),
            ("election_timeout_min", "RAFT_ELECTION_MIN"),
            ("election_timeout_max", "RAFT_ELECTION_MAX"),
            ("commit_timeout", "RAFT_COMMIT_TIMEOUT"),
            ("admin_enabled", "RAFT_ADMIN_ENABLED"),
            ("learner", "RAFT_LEARNER"),
            ("advertise_addr", "RAFT_ADVERTISE"),
        ]:
            if (value := os.getenv(env)) is not None:
                raw[field] = value
        return cls.model_validate(raw)
