"""Grow the Kubernetes deployment from the dashboard: one press, one more replica.

This is provisioning's second backend. `provision.py` starts a sibling PROCESS, which is
the right verb on a bare host and a category error inside a pod (the child would share
the pod's network namespace). Here the right verb is the orchestrator's own:
`kubectl scale statefulset/node --replicas=N+1` — so this module speaks the same scale
subresource that kubectl does, over HTTPS to the API server, authenticated as the pod's
ServiceAccount.

That authentication is what makes this admissible behind the dashboard button when
process spawning was not. `/admin/spawn-node` is unauthenticated by design (a demo has
no auth), so whatever it does must be bounded by construction. The subprocess backend
bounds itself with a constant argv and a cap; this backend is bounded by the API server:
every request is authenticated (the ServiceAccount token), authorized (an RBAC Role that
allows exactly `get`/`patch` on `statefulsets/node`'s scale subresource and nothing
else), audited, and validated. The blast radius of this module, with the manifest's
Role, is "the `node` StatefulSet has a different replica count" — and the code below
narrows even that to "one higher, at most RAFT_PROVISION_MAX".

Two backends must never be reachable from the same process, which is enforced from both
sides rather than trusted to configuration:

  - `provision.py` refuses inside ANY container (`_containerised()`).
  - This module refuses unless it is demonstrably inside a Kubernetes pod: the kubelet's
    env var AND the mounted ServiceAccount credentials must both be present, and a
    `/.dockerenv` (a docker container is not a pod, even with k8s env vars leaked into
    it) is an immediate refusal.

On top of that, the backend is chosen by explicit opt-in (`RAFT_PROVISION_K8S=1`, set
only in k8s/raftkv.yaml), never by runtime sniffing: a deployment states what it is, and
the checks above make lying about it fail closed.

A scaled replica is a PROCESS, not a member — the same two-step story as every other
growth path in this repo. The new pod boots as a staged learner (the manifest's launcher
sets RAFT_LEARNER=1 above the bootstrap ordinal), appears on the staged row, and joins
nothing until a leader appends a configuration entry via attach + promote.
"""

from __future__ import annotations

import asyncio
import logging
import os
import pathlib

import httpx

from raftkv.logging_setup import log_event
from raftkv.provision import (
    HEALTH_INTERVAL,
    HEALTH_TIMEOUT,
    PORT_BASE,
    ProvisionError,
)

logger = logging.getLogger("raftkv.k8s_provision")

# Constants, not configuration: the StatefulSet and headless Service names are pinned in
# k8s/raftkv.yaml, and the RBAC Role pins `resourceNames: ["node"]` — an env var here
# would suggest a flexibility the Role deliberately does not grant.
STATEFULSET = "node"
HEADLESS_SERVICE = "raftkv-hl"
CONTAINER_PORT = 8000

# Where the kubelet mounts the ServiceAccount credentials in every pod that has one.
# Module-level so tests can point it at a tmp_path; nothing else should.
_SA_DIR = pathlib.Path("/var/run/secrets/kubernetes.io/serviceaccount")


def _in_kubernetes_pod() -> str | None:
    """The reason this process is NOT a pod, or None if it demonstrably is one.

    Demonstrably means both halves: the env var the kubelet injects unconditionally,
    and the mounted ServiceAccount files this module needs anyway. Either alone can be
    counterfeited by an environment that merely resembles a pod (a docker container
    with copied env vars, a developer shell with direnv); requiring the credentials
    keeps the failure closed — without them no API call could succeed anyway.
    """
    if pathlib.Path("/.dockerenv").exists():
        return "a /.dockerenv exists, so this is a docker container, not a pod"
    if os.getenv("KUBERNETES_SERVICE_HOST") is None:
        return "KUBERNETES_SERVICE_HOST is not set"
    for name in ("token", "ca.crt", "namespace"):
        if not (_SA_DIR / name).is_file():
            return f"no ServiceAccount {name} is mounted"
    return None


def _api_client(transport: httpx.AsyncBaseTransport | None) -> httpx.AsyncClient:
    """A client for the API server, authenticated as this pod's ServiceAccount.

    The token is read per-call, not cached: kubelets rotate projected tokens, and a
    demo cluster left running overnight should not wake up with a stale one.
    """
    host = os.environ["KUBERNETES_SERVICE_HOST"]
    port = os.getenv("KUBERNETES_SERVICE_PORT", "443")
    token = (_SA_DIR / "token").read_text().strip()
    return httpx.AsyncClient(
        base_url=f"https://{host}:{port}",
        headers={"authorization": f"Bearer {token}"},
        # Unconditional, even under an injected test transport: httpx only builds the
        # TLS context for the default transport, and verification must never have an
        # off switch on this path.
        verify=str(_SA_DIR / "ca.crt"),
        transport=transport,
        timeout=5.0,
    )


async def _wait_member_healthy(
    ordinal: int, transport: httpx.AsyncBaseTransport | None
) -> None:
    """Poll the new pod over the headless Service until it answers.

    Same contract as provision._wait_healthy: the endpoint never hands back an address
    that is still booting, because the next thing that happens to it is an attach.
    Dialled by pod DNS, not the browser's port-forward address — this code runs inside
    the cluster, where `node-N.raftkv-hl` is the name that resolves.
    """
    url = f"http://node-{ordinal}.{HEADLESS_SERVICE}:{CONTAINER_PORT}/healthz"
    deadline = asyncio.get_running_loop().time() + HEALTH_TIMEOUT
    async with httpx.AsyncClient(timeout=1.0, transport=transport) as client:
        while asyncio.get_running_loop().time() < deadline:
            try:
                if (await client.get(url)).status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(HEALTH_INTERVAL)
    raise ProvisionError(
        f"node-{ordinal} did not answer within {HEALTH_TIMEOUT:.0f}s; "
        "the scale-up was rolled back — `kubectl get pods` will say why it was slow"
    )


async def scale_up(
    ordinal: int | None = None,
    *,
    max_nodes: int,
    transport: httpx.AsyncBaseTransport | None = None,
) -> dict:
    """Add exactly one replica to the StatefulSet and return once the pod answers.

    Growth only, +1 only: the desired count is always `current + 1`, so no input can
    scale down (removal must go through the configuration change first — see the
    Makefile's k8s-scale comment) and no input can jump the count. The PATCH carries
    the resourceVersion from the read, so two simultaneous presses conflict at the API
    server (409) instead of racing to the same answer and miscounting.

    `transport` exists for tests only: it stands in for both the API server and the
    new pod, so the unit lane can exercise this path with no cluster anywhere.
    """
    reason = _in_kubernetes_pod()
    if reason is not None:
        raise ProvisionError(
            f"RAFT_PROVISION_K8S is set but {reason}. This backend only runs inside a "
            "Kubernetes pod with a mounted ServiceAccount; unset the flag everywhere "
            "else."
        )

    namespace = (_SA_DIR / "namespace").read_text().strip()
    scale_path = (
        f"/apis/apps/v1/namespaces/{namespace}/statefulsets/{STATEFULSET}/scale"
    )
    async with _api_client(transport) as api:
        current = await api.get(scale_path)
        if current.status_code == 403:
            raise ProvisionError(
                "the API server refused to show the StatefulSet's scale (403); the "
                "raftkv-provisioner Role in k8s/raftkv.yaml is missing or unbound"
            )
        current.raise_for_status()
        body = current.json()
        replicas = int(body["spec"]["replicas"])
        next_ordinal = replicas + 1  # ordinals.start: 1, so pod N exists iff N <= replicas

        if next_ordinal > max_nodes:
            raise ProvisionError(
                f"the StatefulSet already runs {replicas} replicas and the cap is "
                f"{max_nodes} (RAFT_PROVISION_MAX)."
            )
        if ordinal is not None and ordinal != next_ordinal:
            raise ProvisionError(
                f"StatefulSet ordinals are sequential: the next replica is "
                f"node-{next_ordinal}, so an explicit node-{ordinal} cannot be granted."
            )

        patch = await api.patch(
            scale_path,
            json={
                # Optimistic lock: if anything rescaled between the read and this
                # write, the API server answers 409 and nobody double-counts.
                "metadata": {"resourceVersion": body["metadata"]["resourceVersion"]},
                "spec": {"replicas": next_ordinal},
            },
            headers={"content-type": "application/merge-patch+json"},
        )
        if patch.status_code == 409:
            raise ProvisionError(
                "another scale change landed first; press again to grow from the new "
                "count."
            )
        if patch.status_code == 403:
            raise ProvisionError(
                "the API server refused the scale patch (403); the raftkv-provisioner "
                "Role in k8s/raftkv.yaml is missing or unbound"
            )
        patch.raise_for_status()

        try:
            await _wait_member_healthy(next_ordinal, transport)
        except ProvisionError:
            # Mirror of the subprocess backend terminating an unhealthy child: never
            # leave a spawn this endpoint cannot vouch for. The pod never joined
            # anything (it boots as a peerless learner), so undoing it loses nothing.
            # Best-effort without the lock — rolling back a count someone else already
            # changed would be worse than leaving the slow pod to finish booting.
            rollback = await api.patch(
                scale_path,
                json={"spec": {"replicas": replicas}},
                headers={"content-type": "application/merge-patch+json"},
            )
            if rollback.is_error:
                logger.warning(
                    "rollback to %d replicas failed: %s", replicas, rollback.text
                )
            raise

    node_id = f"node-{next_ordinal}"
    log_event(
        logger, "node_scaled", node=node_id, statefulset=STATEFULSET, replicas=next_ordinal
    )
    return {
        "ok": True,
        "node_id": node_id,
        "ordinal": next_ordinal,
        # What THIS BROWSER should probe: `make k8s-forward` maps node-N to 800N on
        # localhost for every provisionable ordinal, so this port is live a moment
        # after the pod is. Peers never see this address — the pod advertises its own
        # node-N.raftkv-hl:8000, and attach uses advertise_addr.
        "addr": f"127.0.0.1:{PORT_BASE + next_ordinal}",
        "staged": True,
        "replicas": next_ordinal,
    }
