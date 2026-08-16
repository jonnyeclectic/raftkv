"""FastAPI wiring. One process = one raft node = one app instance.
Run: uvicorn --factory raftkv.app:create_app (config comes from RAFT_* env vars)."""

import logging
from contextlib import asynccontextmanager
from importlib import resources
from uuid import uuid4

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field, ValidationError

from raftkv.config import NodeConfig
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


class Partition(BaseModel):
    """Replace semantics: the full set of peers this node cannot talk to. []=healed."""

    peers: list[str] = Field(default_factory=list, max_length=16)


def create_app(cfg: NodeConfig | None = None) -> FastAPI:
    cfg = cfg or NodeConfig.from_env()
    setup_logging(cfg)
    storage = Storage(cfg.db_path)
    transport = HttpTransport(cfg.peers, cfg.rpc_timeout)
    node = RaftNode(cfg, storage, transport)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        node.start()
        yield
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
        """A crashed node answers nothing except /admin/recover — peers see
        connection-level failure semantics (503), not a polite reply."""
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
        # Local read — honest about consistency: may lag the leader
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

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        # Re-read per request rather than cached at import: the page has no build step,
        # so a UI edit should show up on reload instead of on a restart.
        return (resources.files("raftkv") / "static" / "dashboard.html").read_text()

    return app
