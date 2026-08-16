"""FastAPI wiring. One process = one raft node = one app instance.
Run: uvicorn --factory raftkv.app:create_app (config comes from RAFT_* env vars)."""

import logging
from contextlib import asynccontextmanager
from importlib import resources
from typing import Literal
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, ValidationError

from raftkv.build import SERVER_BUILD, ui_build
from raftkv.config import NodeConfig
from raftkv.flood import MAX_CONCURRENCY, MAX_TOTAL, FloodBusyError, FloodRunner
from raftkv.logging_setup import RingBufferHandler, log_event, setup_logging
from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    Command,
    NodeState,
    RequestVoteRequest,
    RequestVoteResponse,
)
from raftkv.raft import NotLeaderError, RaftNode
from raftkv.storage import Storage
from raftkv.transport import HttpTransport

logger = logging.getLogger("raftkv.app")


class KVWrite(BaseModel):
    value: str = Field(min_length=1, max_length=4096)


class AddLearner(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)
    addr: str = Field(min_length=3, max_length=255, pattern=r"^[A-Za-z0-9.\-]+:\d{1,5}$")


class Promote(BaseModel):
    node_id: str = Field(min_length=1, max_length=64)


class Partition(BaseModel):
    """Replace semantics: the full set of peers this node cannot talk to. []=healed."""

    peers: list[str] = Field(default_factory=list, max_length=16)


class FloodRequest(BaseModel):
    """A load-generator run. `concurrency` is separate from `total` on purpose: the same
    total sails through at low concurrency and collapses at high, and being able to show
    both is the whole point of exposing the knob (see flood.py)."""

    workload: Literal["distinct", "overwrite", "mixed"] = "mixed"
    total: int = Field(default=200, ge=1, le=MAX_TOTAL)
    concurrency: int = Field(default=50, ge=1, le=MAX_CONCURRENCY)


def create_app(cfg: NodeConfig | None = None) -> FastAPI:
    cfg = cfg or NodeConfig.from_env()
    setup_logging(cfg)
    storage = Storage(cfg.db_path)
    transport = HttpTransport(cfg.peers, cfg.rpc_timeout)
    node = RaftNode(cfg, storage, transport)
    # One generator per node. Constructed unconditionally so /admin/flood's handlers can
    # close over it, but only ever reachable when cfg.admin_enabled mounts those routes.
    flood = FloodRunner(node)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        node.start()
        yield
        # Cancel the generator BEFORE stopping the node: a flood still in flight holds
        # submit() futures that node.stop() would otherwise leave awaiting a commit that
        # can never arrive, and the task would outlive the app it belongs to.
        await flood.cancel()
        await node.stop()
        await transport.aclose()
        storage.close()

    app = FastAPI(title=f"raftkv {cfg.node_id}", lifespan=lifespan)
    # The dashboard is served by ONE node but polls ALL nodes cross-origin.
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )
    app.state.node = node  # exposed for debugging sessions

    # ---- centralized exception handling ------------------------------------
    @app.exception_handler(NotLeaderError)
    async def not_leader(_: Request, exc: NotLeaderError) -> JSONResponse:
        return JSONResponse(
            status_code=503, content={"error": "not_leader", "leader_id": exc.leader_id}
        )

    @app.exception_handler(TimeoutError)
    async def commit_timeout(_: Request, exc: TimeoutError) -> JSONResponse:
        return JSONResponse(status_code=504, content={"error": "commit_timeout"})

    @app.exception_handler(ValidationError)
    async def invalid_command(_: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422, content={"error": "invalid_command", "detail": exc.errors()}
        )

    @app.exception_handler(Exception)
    async def unhandled(request: Request, exc: Exception) -> JSONResponse:
        # anything unexpected still flows through the JSON logging pipeline
        log_event(logger, "unhandled_exception", path=request.url.path, error=repr(exc))
        return JSONResponse(status_code=500, content={"error": "internal"})

    def refuse_if_crashed() -> None:
        """A crashed node answers nothing except /admin/recover and the dashboard —
        peers see connection-level failure semantics (503), not a polite reply."""
        if node.crashed:
            raise HTTPException(status_code=503, detail="node is down (simulated crash)")

    # ---- raft RPCs (async def => they run ON the event loop, keeping the
    # single-threaded atomicity the whole design rests on; a plain `def` would
    # be shipped to a threadpool by FastAPI) --------------------------------
    def refuse_if_partitioned(peer_id: str) -> None:
        """The inbound half of set_blocked(). A partition has to cut both directions
        or it is only a one-way delay, and one-way links make Raft look broken in
        ways real networks rarely are."""
        if peer_id in node.blocked:
            raise HTTPException(status_code=503, detail=f"link to {peer_id} is partitioned")

    @app.post("/raft/request-vote")
    async def request_vote(req: RequestVoteRequest) -> RequestVoteResponse:
        refuse_if_crashed()
        refuse_if_partitioned(req.candidate_id)
        return app.state.node.handle_request_vote(req)

    @app.post("/raft/append-entries")
    async def append_entries(req: AppendEntriesRequest) -> AppendEntriesResponse:
        refuse_if_crashed()
        refuse_if_partitioned(req.leader_id)
        return app.state.node.handle_append_entries(req)

    # ---- client KV API -----------------------------------------------------
    @app.put("/kv/{key}")
    async def put_key(key: str, body: KVWrite) -> dict:
        refuse_if_crashed()
        await node.submit(
            Command(op="set", key=key, value=body.value, request_id=uuid4().hex)
        )
        return {"ok": True, "key": key}

    @app.delete("/kv/{key}")
    async def delete_key(key: str) -> dict:
        refuse_if_crashed()
        await node.submit(Command(op="delete", key=key, request_id=uuid4().hex))
        return {"ok": True, "key": key}

    @app.get("/kv/{key}")
    async def get_key(key: str) -> dict:
        refuse_if_crashed()
        value = storage.kv_get(key)
        if value is None:
            raise HTTPException(status_code=404, detail="key not found")
        # Local read — honest about consistency: may lag the leader (FAILURE_MODES.md)
        return {"key": key, "value": value, "read_from": cfg.node_id, "role": node.role}

    # ---- simulated failure -------------------------------------------------
    if cfg.admin_enabled:
        # Unauthenticated on purpose: this is a demo control, and the config flag
        # (RAFT_ADMIN_ENABLED=0) is how a real deployment removes it. See NodeConfig.
        @app.post("/admin/crash")
        async def admin_crash() -> dict:
            await node.crash()
            log_event(logger, "admin_crash", node=cfg.node_id)
            return {"ok": True, "node": cfg.node_id, "crashed": True}

        @app.post("/admin/recover")
        async def admin_recover() -> dict:
            node.recover()
            log_event(logger, "admin_recover", node=cfg.node_id)
            return {"ok": True, "node": cfg.node_id, "crashed": False}

        @app.post("/admin/reset")
        async def admin_reset(membership: Literal["keep", "bootstrap"] = "keep") -> dict:
            """Destroy this node's durable state and restart it. Deliberately removes
            the term and vote that Fig. 2 requires to survive a reboot -- see
            RaftNode.reset(). Demo control; RAFT_ADMIN_ENABLED=0 removes it.

            `membership=keep` (the default) preserves the configuration, because
            reverting it hands this node the voter set it BOOTED with -- a strict
            subset of the real one after any growth, and therefore a second quorum.
            `membership=bootstrap` opts into that revert and is sound only when sent
            to every node at once, which is why the dashboard only ever does that."""
            await node.reset(keep_membership=membership == "keep")
            log_event(logger, "admin_reset", node=cfg.node_id, membership=membership)
            return {"ok": True, "node": cfg.node_id, "reset": True,
                    "membership": membership}

        @app.post("/admin/partition")
        async def admin_partition(body: Partition) -> dict:
            """Cut or heal this node's links. Unlike /admin/crash the node stays up:
            a partitioned leader keeps serving reads and accepting writes it can
            never commit, which is the failure crash-testing cannot show you."""
            refuse_if_crashed()
            try:
                node.set_blocked(body.peers)
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            log_event(logger, "admin_partition", node=cfg.node_id, blocked=sorted(node.blocked))
            return {"ok": True, "node": cfg.node_id, "blocked": sorted(node.blocked)}

        @app.post("/admin/promote")
        async def admin_promote(body: Promote) -> dict:
            """Promote a learner to a voting member via joint consensus (§6).

            Leader-only. 409 means a precondition is unmet -- a change already in
            flight, the previous one uncommitted, or this leader has not yet committed
            an entry from its own term -- and every one of those is a reason to wait
            and retry, not an error in the request."""
            refuse_if_crashed()
            try:
                node.promote_learner(body.node_id)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            return {
                "ok": True,
                "promoting": body.node_id,
                "joint": True,
                "old_voters": sorted(node.config.old_voters),
                "new_voters": sorted(node.config.voters),
            }

        @app.post("/admin/add-learner")
        async def admin_add_learner(body: AddLearner) -> dict:
            """Attach a non-voting member. Leader-only, and NotLeaderError already
            maps to 503 + leader_id so the caller knows where to retry."""
            refuse_if_crashed()
            try:
                node.add_learner(body.node_id, body.addr)
            except ValueError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            return {"ok": True, "learner": body.node_id, "addr": body.addr}

        @app.post("/admin/flood")
        async def admin_flood(body: FloodRequest) -> dict:
            """Start a mixed-workload flood on this node. Returns IMMEDIATELY.

            The generator runs in the background so the dashboard can poll
            GET /admin/flood and draw progress; a synchronous endpoint would hold the
            connection for the whole burst and show nothing until it was already over."""
            refuse_if_crashed()
            try:
                return flood.start(body.workload, body.total, body.concurrency)
            except FloodBusyError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except ValueError as exc:
                raise HTTPException(status_code=422, detail=str(exc)) from exc

        @app.delete("/admin/flood")
        async def admin_flood_cancel() -> dict:
            """Stop a running flood. Writes already in flight settle on their own."""
            refuse_if_crashed()
            return await flood.cancel()

        @app.get("/admin/flood")
        async def admin_flood_status() -> dict:
            """Live counters while running, final counters afterwards. Polled by the
            dashboard, so it must stay cheap: it reads counters, never the log."""
            return flood.status()

    # ---- observability -----------------------------------------------------
    @app.get("/state")
    async def state() -> NodeState:
        refuse_if_crashed()  # the dashboard card must go red, same as a real crash
        return node.state()

    @app.get("/logs")
    async def logs() -> list[dict]:
        return RingBufferHandler.recent()

    @app.get("/healthz")
    async def healthz() -> dict:
        refuse_if_crashed()  # compose/k8s probes see the failure too
        return {"ok": True, "node": cfg.node_id}

    @app.get("/build")
    async def build() -> dict:
        """Which build is this node actually running? Deliberately answers while
        crashed, same as `/`: during an incident the version is precisely what you
        want, and a stamp that goes dark with the node cannot report a stale one."""
        return {"node": cfg.node_id, "server": SERVER_BUILD, "ui": ui_build()}

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        # Stamped at serve time rather than fetched by the page. The page must report
        # the build it IS, not the build on disk when it got round to asking -- those
        # differ exactly when a tab has gone stale, which is the case worth catching.
        page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
        return page.replace("__UI_BUILD__", ui_build())

    return app
