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

    # ---- raft RPCs (async def => they run ON the event loop, keeping the
    # single-threaded atomicity the whole design rests on; a plain `def` would
    # be shipped to a threadpool by FastAPI) --------------------------------
    @app.post("/raft/request-vote")
    async def request_vote(req: RequestVoteRequest) -> RequestVoteResponse:
        return app.state.node.handle_request_vote(req)

    @app.post("/raft/append-entries")
    async def append_entries(req: AppendEntriesRequest) -> AppendEntriesResponse:
        return app.state.node.handle_append_entries(req)

    # ---- client KV API -----------------------------------------------------
    @app.put("/kv/{key}")
    async def put_key(key: str, body: KVWrite) -> dict:
        await node.submit(
            Command(op="set", key=key, value=body.value, request_id=uuid4().hex)
        )
        return {"ok": True, "key": key}

    @app.delete("/kv/{key}")
    async def delete_key(key: str) -> dict:
        await node.submit(Command(op="delete", key=key, request_id=uuid4().hex))
        return {"ok": True, "key": key}

    @app.get("/kv/{key}")
    async def get_key(key: str) -> dict:
        value = storage.kv_get(key)
        if value is None:
            raise HTTPException(status_code=404, detail="key not found")
        # Local read — honest about consistency: may lag the leader
        return {"key": key, "value": value, "read_from": cfg.node_id, "role": node.role}

    # ---- observability -----------------------------------------------------
    @app.get("/state")
    async def state() -> NodeState:
        return node.state()

    @app.get("/logs")
    async def logs() -> list[dict]:
        return RingBufferHandler.recent()

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True, "node": cfg.node_id}

    @app.get("/", response_class=HTMLResponse)
    async def dashboard() -> str:
        # Re-read per request rather than cached at import: the page has no build step,
        # so a UI edit should show up on reload instead of on a restart.
        return (resources.files("raftkv") / "static" / "dashboard.html").read_text()

    return app
