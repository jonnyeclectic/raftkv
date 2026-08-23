"""The deploy files' shape, pinned the way test_ci_gate.py pins the workflows.

docker-compose.yml and k8s/raftkv.yaml are configuration this suite never executes (no
docker in the unit lane), yet shipped behaviour leans on their shape from three sides:
`make demo` must boot exactly the bootstrap three, provision.py's refusal advertises
`docker compose up -d raft-node-4` and `kubectl scale statefulset/node` as the growth
verbs, and the k8s launcher computes peers named node-1..node-$RAFT_BOOTSTRAP. The text
of the refusal is already pinned (test_provision.py); these two tests pin the files that
make the advertised verbs true.
"""

import pathlib
import re

import yaml

from raftkv.config import NodeConfig

REPO = pathlib.Path(__file__).resolve().parent.parent


def test_exactly_the_two_staged_learners_carry_a_profile():
    """`make demo` (a plain `up -d`) starts every unprofiled service, and naming a
    profiled one is the compose growth verb. A voter gaining a profile silently shrinks
    the bootstrap below quorum; a staged node losing its profile pre-populates the
    staged row on every demo, which is the regression the profile was added to fix."""
    services = yaml.safe_load(
        (REPO / "docker-compose.yml").read_text(encoding="utf-8")
    )["services"]
    profiled = {name: spec["profiles"] for name, spec in services.items() if "profiles" in spec}
    assert profiled == {"raft-node-4": ["staged"], "raft-node-5": ["staged"]}, (
        f"profiles moved: {profiled or 'none'} — the dashboard probes 8004/8005 for staged "
        "learners and provision.py's refusal names `raft-node-4`; keep the profile on "
        "exactly those two services (docker-compose.yml explains why they are declared)."
    )


def test_the_statefulset_is_node_with_ordinals_from_one():
    """The Makefile's rollout/scale targets and provision.py's refusal both say
    `statefulset/node`, and the pod launcher builds peer lists node-1..node-N. Default
    0-based ordinals would name the pods node-0..node-2 while every peer list still says
    node-1..node-3: node-0 dials a pod that does not exist, nobody dials node-0, and no
    quorum ever forms — a breakage only visible on a real cluster unless pinned here."""
    docs = yaml.safe_load_all((REPO / "k8s" / "raftkv.yaml").read_text(encoding="utf-8"))
    stateful_sets = [d for d in docs if d and d.get("kind") == "StatefulSet"]
    assert len(stateful_sets) == 1, "k8s/raftkv.yaml no longer ships exactly one StatefulSet"
    spec = stateful_sets[0]
    assert spec["metadata"]["name"] == "node", (
        "the StatefulSet rename is load-bearing: Makefile k8s-demo/k8s-scale and the "
        "provision refusal all address `statefulset/node`"
    )
    assert spec["spec"].get("ordinals") == {"start": 1}, (
        "`ordinals.start: 1` is what makes pod names match the node-1..node-N convention "
        "the launcher's peer arithmetic and `make k8s-forward` both assume"
    )


def test_kubernetes_provisioning_uses_the_scale_backend_not_the_spawn_one():
    """The provision button works on k8s, but only ever as a StatefulSet scale-up:
    RAFT_PROVISION_K8S=1 swaps the backend BEFORE any subprocess code runs, and
    posture stays explicit in the manifest rather than leaning on runtime detection.
    History matters here: the manifest once inherited the default-on subprocess flag,
    a kind pod slipped past the old container check, and the spawn died on the
    read-only root filesystem as a 500 the dashboard could not even read
    (docs/FAILURE_MODES.md). Pinned so an env cleanup cannot silently hand pods a
    process-spawning endpoint: both flags together are what make the route mean
    `kubectl scale` and nothing else."""
    docs = yaml.safe_load_all((REPO / "k8s" / "raftkv.yaml").read_text(encoding="utf-8"))
    stateful_sets = [d for d in docs if d and d.get("kind") == "StatefulSet"]
    container = stateful_sets[0]["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e.get("value") for e in container["env"]}
    assert env.get("RAFT_PROVISION_ENABLED") == "1", (
        "the k8s provision button rides the same route as everywhere else; without "
        "this flag the endpoint is not mounted and the button dead-ends in a 404"
    )
    assert env.get("RAFT_PROVISION_K8S") == "1", (
        "without the backend swap, an enabled endpoint would fall through to the "
        "subprocess path — which refuses inside a pod, so the button would 409 on a "
        "deployment that CAN grow (through the API server; see k8s_provision.py)"
    )


def test_the_provisioner_can_scale_exactly_one_statefulset_and_nothing_else():
    """The RBAC triple is the entire authority behind the k8s provision button, so its
    shape IS the security argument: a dedicated ServiceAccount (worn by the pods via
    serviceAccountName), one Role allowing get/patch on the scale subresource of the
    one named StatefulSet, and the binding between them. Any loosening — a wildcard
    verb, the whole statefulsets resource, a missing resourceNames pin — widens what an
    unauthenticated dashboard press can do to the cluster, which is exactly the drift
    this test exists to catch."""
    docs = [
        d
        for d in yaml.safe_load_all((REPO / "k8s" / "raftkv.yaml").read_text(encoding="utf-8"))
        if d
    ]
    by_kind = {d["kind"]: d for d in docs}

    role = by_kind["Role"]
    assert role["rules"] == [
        {
            "apiGroups": ["apps"],
            "resources": ["statefulsets/scale"],
            "resourceNames": ["node"],
            "verbs": ["get", "patch"],
        }
    ], (
        "the Role must stay minimal: get/patch on statefulsets/scale for resourceName "
        "`node` only — k8s_provision.py needs nothing more, so nothing more may be "
        "granted"
    )

    binding = by_kind["RoleBinding"]
    assert binding["roleRef"]["name"] == role["metadata"]["name"]
    assert {(s["kind"], s["name"]) for s in binding["subjects"]} == {
        ("ServiceAccount", by_kind["ServiceAccount"]["metadata"]["name"])
    }

    pod_spec = by_kind["StatefulSet"]["spec"]["template"]["spec"]
    assert pod_spec.get("serviceAccountName") == by_kind["ServiceAccount"]["metadata"]["name"], (
        "the pods must wear the provisioner ServiceAccount, or the button authenticates "
        "as `default` and every scale press 403s"
    )


def test_every_provisionable_ordinal_is_forwardable_and_probeable():
    """The provision cap lives in three places that never execute together: config.py
    (how many replicas the button will create), the Makefile's forward range (which of
    them get a 127.0.0.1:800N), and the dashboard's probe-ladder ceiling (which of
    those a reloaded page will look for). Drift between them recreates the observed
    bug this pins: the button truthfully reported node-6..8 staged while `make
    k8s-forward` stopped at 8005 and no fresh page load ever knocked past it — eight
    healthy pods, five visible. Raise RAFT_PROVISION_MAX's default and this test names
    the other two numbers to move with it."""
    cap = NodeConfig.model_fields["provision_max_nodes"].default

    makefile = (REPO / "Makefile").read_text(encoding="utf-8")
    fwd = re.search(r"^K8S_FORWARD_MAX \?= (\d+)$", makefile, re.MULTILINE)
    assert fwd is not None, "Makefile no longer declares K8S_FORWARD_MAX"
    assert int(fwd.group(1)) == cap, (
        f"`make k8s-forward` covers {fwd.group(1)} ordinals but the button can create "
        f"{cap}: every provisionable pod must get its 800N forward"
    )

    page = (REPO / "src" / "raftkv" / "static" / "dashboard.html").read_text(encoding="utf-8")
    top = re.search(r"^const LADDER_TOP = (\d+);$", page, re.MULTILINE)
    assert top is not None, "dashboard.html no longer declares LADDER_TOP"
    assert int(top.group(1)) == 8000 + cap, (
        f"the probe ladder stops at {top.group(1)} but provisioning reaches port "
        f"{8000 + cap}: a reloaded page would never find the topmost nodes"
    )


def test_liveness_and_readiness_read_different_endpoints():
    """Liveness pointed at /healthz turned the dashboard's kill button into a ~30s
    self-resurrection: /healthz 503s while an admin has simulated a crash, so after
    3 x 10s of failed probes the kubelet restarted the container and the fresh process
    booted with crashed=False — recovered with no admin action, observed live on kind.
    Readiness answers "treat as serving?" and SHOULD fail while killed (the node goes
    NotReady and looks down to the demo); liveness answers "beyond self-repair?", which
    a killed node is not — only /admin/recover or /admin/reset may bring it back. The
    endpoints must therefore differ, and each probe must read its own."""
    docs = yaml.safe_load_all((REPO / "k8s" / "raftkv.yaml").read_text(encoding="utf-8"))
    stateful_sets = [d for d in docs if d and d.get("kind") == "StatefulSet"]
    container = stateful_sets[0]["spec"]["template"]["spec"]["containers"][0]
    assert container["readinessProbe"]["httpGet"]["path"] == "/healthz", (
        "readiness must read /healthz: a killed node has to drop out of the Service, "
        "and /livez deliberately answers 200 while crashed"
    )
    assert container["livenessProbe"]["httpGet"]["path"] == "/livez", (
        "liveness must read /livez: pointed at /healthz the kubelet un-kills a node "
        "~30s after the dashboard's kill button (app.py::livez is the exemption)"
    )
