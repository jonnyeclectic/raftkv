#!/usr/bin/env python3
"""IDE debug entrypoint for ONE raftkv node. Point the debugger at this file.

Why this exists instead of debugging `-m uvicorn` directly: PyCharm/IntelliJ 2024.3
resolves a `module` run target through `pkgutil.get_loader()`, which Python 3.14
REMOVED. pydevd swallows the resulting error in a bare `except:` and reports
"No module named uvicorn" -- which is a lie: it fails for every module target on
3.14, not just this one. A *script* target never enters that code path.
(The same config still runs fine un-debugged, which is what makes it confusing.)

Run/Debug configuration:
    script:  scripts/debug_node.py
    env:     RAFT_NODE_ID=node-1 RAFT_DB_PATH=data/node-1.db RAFT_LOG_DIR=logs
             RAFT_PEERS=node-2=127.0.0.1:8002,node-3=127.0.0.1:8003
             RAFT_ADVERTISE=127.0.0.1:8001
             RAFT_PORT=8001
    working directory: the repo root

Two things in there are load-bearing, and both were wrong in this docstring at some
point -- which matters more than usual because this block is what people paste into
the IDE dialog. `127.0.0.1` and not `localhost`: localhost resolves to 127.0.0.1 OR
::1, and a compose cluster publishes on both while a local node binds only the
former, so the same string can reach two different clusters (run_local.sh and the
dashboard use the literal address for exactly this reason). And RAFT_ADVERTISE is
required even on a bootstrap node: a node writes its OWN address into the
configuration entry, that entry replicates, and an empty one leaves a later-joined
member holding a voter it can never dial.

Start the other two nodes first with `make run-local`, then debug this. Slow the
cluster down so a breakpoint does not instantly cost you the leadership you were
about to inspect: RAFT_HEARTBEAT=5 RAFT_ELECTION_MIN=60 RAFT_ELECTION_MAX=120.

"""

import os

import uvicorn

from raftkv.app import create_app

# uvicorn's listen port is not part of NodeConfig (it is a process concern, not a
# raft one), so it is read here rather than in config.py.
DEFAULT_PORT = 8001


def main() -> None:
    port = int(os.environ.get("RAFT_PORT", DEFAULT_PORT))
    # The app object, not the "raftkv.app:create_app" factory string: passing a string
    # makes uvicorn import it in ITS machinery, which is exactly the indirection this
    # file exists to avoid. reload stays off -- a debugger cannot follow a reloader
    # subprocess, and the point is to sit inside THIS process's event loop.
    uvicorn.run(create_app(), host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()
