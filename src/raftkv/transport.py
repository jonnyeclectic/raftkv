"""RPC transport behind a Protocol so RaftNode never knows HTTP exists.
- HttpTransport: httpx POSTs between real nodes (docker/k8s).
- MemoryTransport: in-process delivery with crash/partition switches — the
  MIT-6.824-style simulated network that turns failure scenarios into
  deterministic pytest cases."""

import asyncio
from typing import Protocol

import httpx

from raftkv.models import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    RequestVoteRequest,
    RequestVoteResponse,
)


class TransportError(Exception):
    """Peer unreachable, timed out, or spoke garbage. In Raft this is normal
    weather, not an exception path: callers count it in metrics and move on."""


class Transport(Protocol):
    async def request_vote(
        self, peer_id: str, req: RequestVoteRequest
    ) -> RequestVoteResponse: ...

    async def append_entries(
        self, peer_id: str, req: AppendEntriesRequest
    ) -> AppendEntriesResponse: ...


class HttpTransport:
    def __init__(self, peers: dict[str, str], rpc_timeout: float) -> None:
        self._peers = dict(peers)
        self._client = httpx.AsyncClient(timeout=rpc_timeout)

    async def request_vote(
        self, peer_id: str, req: RequestVoteRequest
    ) -> RequestVoteResponse:
        return await self._post(peer_id, "/raft/request-vote", req, RequestVoteResponse)

    async def append_entries(
        self, peer_id: str, req: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        return await self._post(peer_id, "/raft/append-entries", req, AppendEntriesResponse)

    async def _post(self, peer_id, path, req, response_model):
        url = f"http://{self._peers[peer_id]}{path}"
        try:
            resp = await self._client.post(url, json=req.model_dump())
            resp.raise_for_status()
            return response_model.model_validate(resp.json())
        except (httpx.HTTPError, ValueError) as exc:
            raise TransportError(f"{path} -> {peer_id}: {exc}") from exc

    async def aclose(self) -> None:
        await self._client.aclose()


class MemoryTransport:
    """Test double with real semantics: delivers RPCs by calling the peer's
    handler, unless a crash or partition blocks the link."""

    def __init__(self) -> None:
        self.nodes: dict[str, object] = {}
        self.down: set[str] = set()
        self.blocked: set[frozenset[str]] = set()

    def register(self, node_id: str, node: object) -> None:
        self.nodes[node_id] = node

    def crash(self, node_id: str) -> None:
        self.down.add(node_id)

    def restore(self, node_id: str) -> None:
        self.down.discard(node_id)

    def partition(self, *groups: set[str]) -> None:
        self.blocked = {
            frozenset((a, b))
            for g1 in groups
            for g2 in groups
            if g1 is not g2
            for a in g1
            for b in g2
        }

    def heal(self) -> None:
        self.blocked.clear()

    def _check_link(self, src: str, dst: str) -> None:
        if src in self.down or dst in self.down or frozenset((src, dst)) in self.blocked:
            raise TransportError(f"link {src} -> {dst} is down")

    async def request_vote(
        self, peer_id: str, req: RequestVoteRequest
    ) -> RequestVoteResponse:
        self._check_link(req.candidate_id, peer_id)
        await asyncio.sleep(0)  # yield control: mimic a network hop
        return self.nodes[peer_id].handle_request_vote(req)

    async def append_entries(
        self, peer_id: str, req: AppendEntriesRequest
    ) -> AppendEntriesResponse:
        self._check_link(req.leader_id, peer_id)
        await asyncio.sleep(0)
        return self.nodes[peer_id].handle_append_entries(req)
