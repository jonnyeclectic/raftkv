import asyncio
import os
import time

import pytest

from raftkv.config import NodeConfig
from raftkv.models import Role
from raftkv.storage import Storage

# How much slower than a developer laptop this machine is assumed to be.
#
# The timers below are ratios, not absolutes: what every simulation test actually
# depends on is election_timeout >> heartbeat >> rpc_timeout, and scaling all of them
# together leaves those ratios exactly as they are. A shared CI runner is roughly an
# order of magnitude noisier than a laptop, and at a 100 ms election timeout that noise
# is a spurious election in the middle of an assertion about something else.
#
# The alternative was widening the assertions themselves, which is worse: it makes the
# suite tolerate the very behaviour a failure would be trying to report. A test that
# says "growing the cluster keeps committing" should keep meaning that on every machine,
# and only the clock it measures against should move.
TIMING_SCALE = float(os.environ.get("RAFT_TEST_TIMING_SCALE", "1"))


def scaled(**timing: float) -> dict[str, float]:
    """Stretch every duration by TIMING_SCALE, preserving the ratios between them."""
    return {k: v * TIMING_SCALE for k, v in timing.items()}


# Fast timers for simulation tests; unit tests never start timers at all.
TEST_TIMING = scaled(
    heartbeat_interval=0.03,
    election_timeout_min=0.1,
    election_timeout_max=0.2,
    rpc_timeout=0.05,
    commit_timeout=2.0,
)


def make_cfg(node_id: str = "node-1", peers: dict[str, str] | None = None, **overrides):
    return NodeConfig.model_validate(
        {"node_id": node_id, "peers": peers or {}, **TEST_TIMING, **overrides}
    )


async def eventually(predicate, timeout: float = 3.0, interval: float = 0.02) -> None:
    """Await a condition with a deadline — async assertions without flaky sleeps.

    The deadline scales with TIMING_SCALE for the same reason the timers do: on a slower
    machine the cluster needs proportionally longer to reach the state being waited for,
    and a fixed deadline would turn that into a failure about nothing."""
    timeout *= TIMING_SCALE
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s")


class StubTransport:
    """Real object with the Transport shape; mockito stubs its methods per-test."""

    def add_peer(self, peer_id, addr):
        """Config entries register dial addresses at runtime; nothing to do in-process."""

    async def request_vote(self, peer_id, req):
        raise NotImplementedError

    async def append_entries(self, peer_id, req):
        raise NotImplementedError


class SimCluster:
    """N in-process RaftNodes over one MemoryTransport with real (fast) timers.
    crash() cancels tasks and blocks the node's links (volatile state survives in
    the object but is never consulted again); restart() builds a FRESH RaftNode
    on the SAME db file — exactly what a process restart loses and keeps."""

    def __init__(self, tmp_path, n: int = 3, transport=None, **overrides):
        from raftkv.transport import MemoryTransport

        self.tmp_path = tmp_path
        # Injectable so a randomized run can swap in tests/chaos.py's ChaosTransport, which
        # subclasses MemoryTransport and keeps the crash/partition controls this class
        # drives. Defaulting to a fresh MemoryTransport keeps every existing caller
        # unchanged.
        self.transport = transport if transport is not None else MemoryTransport()
        self.ids = [f"node-{i}" for i in range(1, n + 1)]
        # Applied to every node, and re-applied by restart(). Lets a test widen a single
        # timing knob (a flood needs a commit_timeout that survives a loaded CI box)
        # without turning TEST_TIMING into a per-test negotiation.
        self.overrides = overrides
        self.nodes = {}
        for node_id in self.ids:
            self._build(node_id)

    def _build(self, node_id: str):
        from raftkv.raft import RaftNode

        cfg = make_cfg(node_id, peers={p: p for p in self.ids if p != node_id},
                       **self.overrides)
        storage = Storage(str(self.tmp_path / f"{node_id}.db"))
        node = RaftNode(cfg, storage, self.transport)
        self.transport.register(node_id, node)
        self.nodes[node_id] = node
        return node

    def start(self):
        for node in self.nodes.values():
            node.start()

    async def stop(self):
        # Stop EVERY node before closing ANY database. Interleaving the two (stop node-1,
        # close node-1, stop node-2) leaves node-2's replication loop calling
        # handle_append_entries on a node whose sqlite connection is already closed —
        # harmless at teardown, but it prints a spurious traceback that looks exactly
        # like a real failure. A flood in flight makes it fire almost every run.
        for node in self.nodes.values():
            await node.stop()
        for node in self.nodes.values():
            node.storage.close()

    def leader(self):
        live = [
            n for n in self.nodes.values()
            if n.role is Role.LEADER and n.cfg.node_id not in self.transport.down
        ]
        return max(live, key=lambda n: n.current_term) if live else None

    async def crash(self, node_id: str):
        await self.nodes[node_id].stop()
        self.nodes[node_id].storage.close()
        self.transport.crash(node_id)

    def restart(self, node_id: str):
        node = self._build(node_id)  # fresh volatile state, same persisted file
        self.transport.restore(node_id)
        node.start()
        return node


@pytest.fixture
async def cluster(tmp_path):
    sim = SimCluster(tmp_path)
    sim.start()
    yield sim
    await sim.stop()


@pytest.fixture
def make_node(tmp_path):
    """RaftNode factory: own Storage file, MemoryTransport by default, NOT started —
    unit tests drive rule methods directly, so no timers can interfere."""
    from raftkv.raft import RaftNode
    from raftkv.transport import MemoryTransport

    created = []

    def factory(node_id="node-1", peer_ids=("node-2", "node-3"), transport=None, **overrides):
        cfg = make_cfg(node_id, peers={p: p for p in peer_ids}, **overrides)
        storage = Storage(str(tmp_path / f"{node_id}.db"))
        node = RaftNode(cfg, storage, transport or MemoryTransport())
        created.append(node)
        return node

    yield factory
    for node in created:
        node.storage.close()
