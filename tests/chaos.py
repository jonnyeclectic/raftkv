"""A network that misbehaves on purpose, reproducibly.

`MemoryTransport` delivers every RPC immediately, exactly once, in the order it was
sent (`transport.py:133`: one `await asyncio.sleep(0)`, then a direct call into the
peer's handler). That is what makes the failure suite deterministic, and it is the right
default — but it means the suite explores exactly ONE interleaving of one message
schedule, and a real network offers none of those guarantees.

The gap this closes is not hypothetical. This repo's own rules name the guards in
`_start_election`, `_append_to_peer` and `submit` as load-bearing — "the Students' Guide
discipline, not paranoia" — and the guards only fire when a reply arrives after the world
has moved on. Against a transport that replies within the same event-loop turn, the window
those guards protect is almost never open. So the most safety-critical lines in `raft.py`
were being executed constantly and *tested* barely.

Four kinds of misbehaviour, each corresponding to a real thing networks do:

  - **delay** — the reply lands late, so the sender may have changed role or term. This is
    the stale-reply window, held open on demand.
  - **drop** — a `TransportError`, which `raft.py` already treats as ordinary weather.
  - **duplicate** — the handler runs twice for one send. Raft receiver rules must be
    idempotent; a duplicated AppendEntries that truncated on a second look, or a
    duplicated vote that got counted twice, is a two-leaders bug.
  - **reorder** — a consequence of per-message delay rather than a fifth knob: two RPCs
    sent in order can arrive out of order, which is how a stale AppendEntries gets a
    chance to truncate entries that already match.

Every choice comes from a seeded `random.Random`, and nothing else in this module consults
global randomness. A failing seed is therefore a complete bug report: re-running with the
same seed replays the same schedule. Print the seed on failure and never regenerate it.

Crash and partition semantics are inherited unchanged — chaos is layered *on top of* the
existing link checks, so `_check_link` still runs first and a partitioned link stays cut
rather than becoming merely unreliable.
"""

import asyncio
import random

from raftkv.transport import MemoryTransport, TransportError


class ChaosTransport(MemoryTransport):
    """MemoryTransport plus seeded delay, loss, duplication and reordering.

    Defaults are all-zero, so a ChaosTransport with no knobs set behaves exactly like the
    MemoryTransport it extends. Tests opt in to each hazard, which keeps a failure
    attributable to one cause instead of four.
    """

    def __init__(
        self,
        seed: int,
        *,
        drop_rate: float = 0.0,
        duplicate_rate: float = 0.0,
        max_delay: float = 0.0,
    ) -> None:
        super().__init__()
        self.seed = seed
        self.rng = random.Random(seed)
        self.drop_rate = drop_rate
        self.duplicate_rate = duplicate_rate
        self.max_delay = max_delay
        # Counters, so a test can assert the chaos it asked for actually happened. A run
        # that silently injected nothing would pass while proving nothing — the same
        # vacuous-test failure mode the docs-citation checks guard against.
        self.delivered = 0
        self.dropped = 0
        self.duplicated = 0

    def _delay(self) -> float:
        return self.rng.uniform(0, self.max_delay) if self.max_delay else 0.0

    async def _hop(self, deliver):
        """One RPC: outbound flight, delivery (possibly twice), return flight.

        The delay is split deliberately. Delaying only the request would reorder arrivals
        but always answer a caller that is still in the state it sent from; delaying the
        RESPONSE is what leaves the sender to change role or term mid-flight, which is the
        case the stale-reply guards exist for.
        """
        if self.rng.random() < self.drop_rate:
            self.dropped += 1
            raise TransportError("chaos: message dropped in flight")

        outbound = self._delay()
        if outbound:
            await asyncio.sleep(outbound)

        response = deliver()
        self.delivered += 1

        inbound = self._delay()
        if inbound:
            await asyncio.sleep(inbound)

        # A duplicate is the SAME message arriving twice; the peer handles it again and the
        # second answer is discarded, so the sender never learns it happened. Handling it
        # must be a no-op — that is the property under test.
        #
        # It is delivered AFTER the delay above, not back-to-back with the original,
        # because an immediate second copy arrives at a node whose log has not moved and
        # asks a question it just answered. The copy worth testing is the late one: by then
        # the leader may have advanced, so a receiver rule that re-truncates on a second
        # look, or re-grants a vote it already granted, has a real window to do damage.
        if self.rng.random() < self.duplicate_rate:
            self.duplicated += 1
            deliver()

        return response

    async def request_vote(self, peer_id, req):
        self._check_link(req.candidate_id, peer_id)
        return await self._hop(lambda: self.nodes[peer_id].handle_request_vote(req))

    async def append_entries(self, peer_id, req):
        self._check_link(req.leader_id, peer_id)
        return await self._hop(lambda: self.nodes[peer_id].handle_append_entries(req))
