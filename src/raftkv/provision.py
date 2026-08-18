"""Provision another raftkv PROCESS from the dashboard, on demand, as many times as asked.

This is the missing half of the growth story. Membership is a configuration entry in the
replicated log, so `/admin/add-learner` can only ever attach something that is *already
running* -- which until now meant the runtime had to pre-start idle nodes and hope you
wanted exactly that many. Two idle nodes shipped in `run_local.sh` and `docker-compose.yml`
for precisely that reason, and they capped the cluster at five.

Spawning here removes the cap and, more usefully, makes the two steps visible as two steps:
the staged row starts EMPTY, `provision` puts a process in it, and `attach` is still a
separate press. Nothing about that is new behaviour in Raft -- it is the same
`add_learner` append as before -- but a cluster can now go from three nodes to N without a
terminal, and it stays observable that provisioning bought nothing until a leader appended
a configuration entry.

It is a DEMO AFFORDANCE, on the same footing as `/admin/crash` and `flood.py`, and a
noticeably sharper one: the other admin endpoints make this process misbehave, while this
one starts *new* processes. That is why it has its own flag (`RAFT_PROVISION_ENABLED`) on
top of `RAFT_ADMIN_ENABLED` rather than riding along on it -- a deployment that wants the
failure controls for a demo should not have to accept process spawning to get them.

Three properties keep it from being a remote-code-execution endpoint in practice, and all
three are load-bearing:

  - **The argv is a constant.** `_argv()` builds a fixed list from `sys.executable` and an
    integer. No shell (`create_subprocess_exec`, never `create_subprocess_shell`), no
    string interpolation into a command, and the only caller-supplied value is an ordinal
    that pydantic has already bounded to 4..99. There is no input that can add an argument.
  - **It is bounded.** `MAX_NODES` caps how many processes one cluster will start, for the
    same reason `flood.MAX_TOTAL` exists: an unauthenticated endpoint that spawns
    unbounded processes on request is a fork bomb with a REST API. Both allocation paths
    honor it: the automatic allocator searches only inside the window, and an explicit
    ordinal beyond the window is refused rather than trusted just because pydantic
    bounded it to two digits.
  - **It refuses inside a container.** See `_containerised()` below -- the refusal is a
    correctness requirement, not a hardening nicety.

`sys.executable -m uvicorn` rather than `uv run uvicorn` (which `scripts/node_up.sh` uses):
inside a running server the current interpreter is already the project venv, so this needs
neither `uv` on PATH nor a resolve step, and it cannot pick a different environment than
the one the parent is running.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import pathlib
import sys

import httpx

from raftkv.logging_setup import log_event

logger = logging.getLogger("raftkv.provision")

# Nodes 1-3 are the bootstrap cluster: they boot as voters with a peer list, from
# run_local.sh or the IDE debugger. Provisioning starts above them, so the first press
# yields node-4 -- the node the cluster used to have to ship idle.
FIRST_ORDINAL = 4
# Ordinal N answers on 8000 + N, which is the convention every script and the probe list
# already use. Keeping it means a provisioned node-4 lands on 8004, an address
# the dashboard is already probing, so it appears in the staged row with no further wiring.
PORT_BASE = 8000
# The outer edge of what one host should be asked to run. Each node is a Python process
# with its own SQLite file and its own election timer; the point lands at five and the
# arithmetic stops being interesting well before twelve. Raised via RAFT_PROVISION_MAX for
# quorum arithmetic on a bigger cluster.
MAX_NODES = 12
# A fresh uvicorn worker imports FastAPI, opens SQLite and binds a port. Measured at
# ~1.2 s cold on this project's dev machine; 15 s is generous enough that a slow first
# import is not reported as a failure, and short enough that a genuinely broken spawn
# does not hang the button.
HEALTH_TIMEOUT = 15.0
HEALTH_INTERVAL = 0.25


class ProvisionError(RuntimeError):
    """Refused, with a reason meant to be shown to whoever pressed the button."""


# ordinal -> Process, for every child THIS node started. Deliberately not persisted: a
# node that restarts has no claim on processes it no longer supervises, and the port
# probe below is the authority on what is actually running either way.
_children: dict[int, asyncio.subprocess.Process] = {}


def _containerised() -> bool:
    """True when this process is itself inside a container.

    Refusing there is a correctness requirement rather than caution. A child spawned
    inside node-1's container gets node-1's network namespace, so it would bind
    127.0.0.1:8004 *of that container*. The address it then advertises resolves, for every
    peer that dials it, to that peer's own loopback -- so the attach succeeds, replication
    never connects, and the failure looks like a Raft bug instead of a topology mistake.

    Growing a containerised cluster is the orchestrator's job, and both runtimes already
    have the verb: `docker compose up -d raft-node-N` against a declared service, or
    `kubectl scale statefulset/raftkv`. Neither belongs behind an unauthenticated button.
    """
    if pathlib.Path("/.dockerenv").exists():
        return True
    # Podman, systemd-nspawn and some rootless runtimes leave no /.dockerenv but do set
    # this. Lowercase is not a typo — it is the name those runtimes actually set, so the
    # SIM112 "use a capitalized env var" suggestion would silently disable the check.
    return os.getenv("container") is not None  # noqa: SIM112


async def _port_is_bound(port: int) -> bool:
    """Something is already listening -- a previous spawn, a compose publish, or the
    node the operator started by hand. All three mean 'do not take this ordinal'."""
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", port), timeout=0.35
        )
    except (TimeoutError, OSError):
        return False
    writer.close()
    with contextlib.suppress(Exception):
        await writer.wait_closed()
    del reader
    return True


async def _next_free_ordinal(max_nodes: int) -> int:
    """Lowest unused ordinal at or above FIRST_ORDINAL.

    Lowest rather than highest-plus-one so that provisioning after a staged node was
    stopped reuses its slot instead of drifting the port range upward for the rest of the
    session -- the dashboard's probe list is short and fixed, and a node that lands on
    8011 because 8004 was briefly busy is a node nothing is probing.
    """
    for ordinal in range(FIRST_ORDINAL, FIRST_ORDINAL + max_nodes):
        if ordinal in _children and _children[ordinal].returncode is None:
            continue
        if not await _port_is_bound(PORT_BASE + ordinal):
            return ordinal
    raise ProvisionError(
        f"all {max_nodes} provisionable slots are in use "
        f"(node-{FIRST_ORDINAL} through node-{FIRST_ORDINAL + max_nodes - 1}). "
        "Raise RAFT_PROVISION_MAX, or stop a staged node first."
    )


def _argv(port: int) -> list[str]:
    """The whole command, as a constant shaped by one integer.

    Kept separate so `tests/test_provision.py` can assert on it directly: the security
    argument for this module is that no caller-supplied string reaches this list, and an
    assertion is worth more than a comment saying so.
    """
    return [
        sys.executable,
        "-m",
        "uvicorn",
        "--factory",
        "raftkv.app:create_app",
        "--host",
        "127.0.0.1",
        "--port",
        str(port),
    ]


def _child_env(node_id: str, addr: str, log_dir: str) -> dict[str, str]:
    """A staged node: a learner with NO peers, which is an empty voter set, which is what
    'in nobody's configuration' means. It never campaigns and nothing replicates to it
    until a leader appends a configuration entry naming it.

    RAFT_ADVERTISE is the address PEERS dial, and it is the field that makes the attach
    work; see docker-compose.yml for the same setting solving the same problem the other
    way round.
    """
    env = dict(os.environ)
    env.update(
        {
            "RAFT_NODE_ID": node_id,
            "RAFT_DB_PATH": f"data/{node_id}.db",
            "RAFT_LOG_DIR": log_dir,
            "RAFT_LEARNER": "1",
            "RAFT_ADVERTISE": addr,
            # Inherited peers would make the child a voter in a configuration nobody
            # else holds -- a second cluster wearing this one's node ids.
            "RAFT_PEERS": "",
        }
    )
    return env


async def _wait_healthy(port: int, timeout: float = HEALTH_TIMEOUT) -> None:
    """Return once the child answers, so the endpoint never hands back an address that is
    still importing. The next thing that happens to this address is an attach, and a
    learner that 503s its first AppendEntries is a confusing way to start."""
    if (
        port < PORT_BASE + FIRST_ORDINAL
        or port >= PORT_BASE + FIRST_ORDINAL + MAX_NODES
    ):
        raise ProvisionError(f"refusing health probe outside provision range: {port}")

    addr = f"127.0.0.1:{port}"
    health_url = httpx.URL(scheme="http", host="127.0.0.1", port=port, path="/healthz")
    deadline = asyncio.get_running_loop().time() + timeout
    async with httpx.AsyncClient(timeout=1.0) as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                if (await client.get(health_url)).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(HEALTH_INTERVAL)
    raise ProvisionError(f"{addr} did not answer within {timeout:.0f}s; see logs/")


async def spawn(
    ordinal: int | None = None,
    *,
    max_nodes: int = MAX_NODES,
    log_dir: str = "logs",
) -> dict:
    """Start one staged node and return once it is answering.

    Awaits throughout rather than blocking: this runs on the same event loop that owes
    every follower a heartbeat, and a synchronous spawn-and-poll would hold it for the
    ~1.2 s the child takes to boot. That is longer than `heartbeat_interval`, so the
    leader would be voted out for being busy -- the same failure the unindexed
    `last_config()` scan caused, from a different direction.
    """
    if _containerised():
        raise ProvisionError(
            "this node is running inside a container, where a spawned process would "
            "share its network namespace and be unreachable from its peers. Grow a "
            "containerised cluster with `docker compose up -d raft-node-N` or "
            "`kubectl scale statefulset/raftkv` instead."
        )
    max_nodes = max(1, min(max_nodes, 90))  # ordinals stay two digits, matching the model
    if ordinal is None:
        ordinal = await _next_free_ordinal(max_nodes)
    elif ordinal < FIRST_ORDINAL:
        raise ProvisionError(
            f"node-{ordinal} is part of the bootstrap cluster; provisioning starts at "
            f"node-{FIRST_ORDINAL}."
        )
    elif ordinal >= FIRST_ORDINAL + max_nodes:
        raise ProvisionError(
            f"node-{ordinal} is outside the provisionable window: the cap allows "
            f"node-{FIRST_ORDINAL} through node-{FIRST_ORDINAL + max_nodes - 1} "
            "(RAFT_PROVISION_MAX). An explicit ordinal does not skip the bound the "
            "automatic path enforces."
        )
    elif await _port_is_bound(PORT_BASE + ordinal):
        raise ProvisionError(f"port {PORT_BASE + ordinal} is already in use.")

    port = PORT_BASE + ordinal
    node_id = f"node-{ordinal}"
    addr = f"127.0.0.1:{port}"

    pathlib.Path("data").mkdir(exist_ok=True)
    pathlib.Path(log_dir).mkdir(parents=True, exist_ok=True)
    stdout = pathlib.Path(log_dir) / f"{node_id}.stdout"

    with stdout.open("ab") as sink:
        child = await asyncio.create_subprocess_exec(
            *_argv(port),
            env=_child_env(node_id, addr, log_dir),
            stdout=sink,
            stderr=asyncio.subprocess.STDOUT,
            # Its own session, so it survives whatever kills this node's shell. A staged
            # node dying with its parent would make `provision` look flaky in exactly the
            # situation it exists for -- a session where the operator restarts node-1.
            start_new_session=True,
        )
    _children[ordinal] = child

    try:
        await _wait_healthy(port)
    except ProvisionError:
        with contextlib.suppress(ProcessLookupError):
            child.terminate()
        _children.pop(ordinal, None)
        raise

    log_event(logger, "node_provisioned", node=node_id, addr=addr, pid=child.pid)
    return {
        "ok": True,
        "node_id": node_id,
        "ordinal": ordinal,
        # What peers dial AND what this browser should probe -- identical here, because a
        # host process has no port mapping in the way. They diverge under compose, which
        # is exactly why provisioning refuses to run there.
        "addr": addr,
        "pid": child.pid,
        "staged": True,
    }


def running() -> list[int]:
    """Ordinals this node has provisioned that are still alive. Used by the cap and by
    tests; the port probe remains the authority on what is actually listening."""
    return sorted(o for o, c in _children.items() if c.returncode is None)


def _reset_for_tests() -> None:
    _children.clear()
