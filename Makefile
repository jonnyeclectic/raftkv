.PHONY: install test lint gate locks relock demo down smoke clean-start-check run-local node-up demo-reset e2e
install:
	uv sync --all-extras
test:
	uv run pytest
# The browser lane: real uvicorn nodes, real chromium, the shipped dashboard —
# the scope where the provision-failure fixes are actually visible (a refusal is only
# fixed once the STATUS LINE shows it; see tests/test_e2e_provision_dashboard.py).
# `--with` keeps playwright out of the lockfiles: it is a lane dependency, not a
# project one, and `make test` skips this file cleanly when it is absent.
e2e:
	uv run --with playwright==1.62.0 playwright install chromium
	uv run --with playwright==1.62.0 pytest tests/test_e2e_provision_dashboard.py -v
lint:
	uv run ruff check .
# Every gate ci.yml runs that does not need Docker, in one command. The two that do are
# `smoke` (per pull request, against compose) and `clean-start-check` (nightly, from a
# cold build) — CI runs those same scripts, not a re-implementation of them.
gate: lint locks test
	@echo "gate PASSED — the docker gates are 'make smoke' and 'make clean-start-check'"
# Two lockfiles answering to different consumers: uv.lock is what the test suite resolves
# against, requirements.lock is what the Dockerfile installs from, hash-checked. The
# script says why that matters and does both directions; CI runs this same target.
locks:
	./scripts/check_locks.sh
# After changing a dependency in pyproject.toml. Both files, always in the same commit.
relock:
	uv lock
	uv export --frozen --no-dev --no-emit-project -o requirements.lock
demo:
	docker compose up -d --wait --build
	@echo "dashboard: http://localhost:8001/"
# Staged learners live behind a profile, and `down` only covers active profiles — "*"
# tears down every profile, so a raft-node-4/5 started by hand does not outlive the rest.
down:
	docker compose --profile "*" down --volumes
smoke:
	./scripts/smoke.sh
clean-start-check:
	./scripts/clean_start_check.sh
run-local:
	./scripts/run_local.sh
# Provision one more PROCESS while the cluster runs — the run-local answer to
# `kubectl scale`. It comes up staged (learner, no peers, empty voter set); attaching
# and promoting it from the dashboard is what makes it a member. N defaults to 4: nothing
# above node-3 starts by itself any more, so 4 is the first free slot.
#
# The dashboard's `provision node` button does exactly this over HTTP
# (POST /admin/spawn-node); this target is the same operation from a terminal, for when
# you want the mechanism visible rather than behind a button.
node-up:
	@./scripts/node_up.sh $(or $(N),4)
# DESTRUCTIVE, and the fastest way back to a known-good starting state: stops every local
# node and deletes every database and log. `data/` and `logs/` are gitignored build
# products, so nothing here is recoverable and nothing here needs to be. Use it between
# runs — a half-grown 5-voter cluster left behind by the last one is state the next one
# silently inherits.
demo-reset:
	-@pkill -f '[r]aftkv.app' 2>/dev/null || true
	-@pkill -f '[c]reate_app --port' 2>/dev/null || true
	-@pkill -f '[d]ebug_node.py' 2>/dev/null || true
	rm -rf data logs
	@echo "clean. now: make run-local, then start node-1 in the IDE debugger"

# kindest/node:v1.33.1 pinned deliberately: v1.36.1 (kind's current default) fails
# to boot on this project's dev machine ("could not find a log line that matches
# Reached target .*Multi-User System") — 1.33.1 is confirmed working here.
KIND_NODE_IMAGE := kindest/node:v1.33.1

.PHONY: k8s-demo k8s-forward k8s-scale k8s-down k8s-require-cluster
# With no kubeconfig context, kubectl silently falls back to localhost:8080 and reports
# `connection refused` under four lines of memcache.go stack noise — which reads as "the
# scale command is broken" rather than "there is no cluster". Every target below needs
# the cluster k8s-demo creates, so ask once and say so in one line.
k8s-require-cluster:
	@kubectl cluster-info --request-timeout=3s >/dev/null 2>&1 || { \
	  echo "No reachable Kubernetes cluster — kubectl has no context."; \
	  echo "Run 'make k8s-demo' first: it creates the kind cluster and deploys raftkv."; \
	  exit 1; }
# The StatefulSet is named `node` (pods node-1..node-N, see k8s/raftkv.yaml). A
# StatefulSet cannot be renamed in place: applying this over a cluster that still runs
# the old `raftkv` one creates a SECOND StatefulSet beside it — tear down first
# (`make k8s-down && make k8s-demo`). The rename orphans the old `data-raftkv-N` PVCs,
# which is acceptable for a demo cluster kind rebuilds from scratch anyway.
k8s-demo:
	kind get clusters | grep -q '^raftkv$$' || kind create cluster --name raftkv --image $(KIND_NODE_IMAGE)
	docker build -t raftkv:local .
	kind load docker-image raftkv:local --name raftkv
	kubectl apply -f k8s/raftkv.yaml
	kubectl rollout status statefulset/node --timeout=180s
	@echo "now: make k8s-forward, then open http://localhost:8001/"
# Each forward retries in a loop: `kubectl port-forward` is bound to one pod instance
# and dies with it — and killing pods is the failure this setup exists to exercise.
# Without the retry, a deleted node's dashboard card stays UNREACHABLE even after the
# StatefulSet has replaced the pod. The mapping is node-N ↔ 800N, same as compose/local.
# The range is every PROVISIONABLE ordinal (RAFT_PROVISION_MAX defaults to 12, pinned to
# this number by tests/test_deploy_topology.py), not the pods that exist right now: a
# forward for a pod that is not there yet fails quietly and retries, and comes alive a
# couple of seconds after the dashboard's provision button creates the pod. Bootstrap
# pods retry every 1s (their deaths are the demo), provisionable slots every 2s.
K8S_FORWARD_MAX ?= 12
k8s-forward: k8s-require-cluster
	@trap 'kill 0' EXIT; \
	for i in $$(seq 1 $(K8S_FORWARD_MAX)); do \
	  s=1; [ $$i -gt 3 ] && s=2; \
	  ( while true; do kubectl port-forward pod/node-$$i $$((8000+i)):8000 2>/dev/null; sleep $$s; done ) & \
	done; \
	wait
# `kubectl scale` provisions PROCESSES, not members: pods above the bootstrap size come
# up as staged learners with no peers (see k8s/raftkv.yaml). They then appear on the
# dashboard's "staged" row, and attach + promote is what actually grows the cluster.
# Scaling DOWN is not symmetric — a voter must be removed from the configuration first,
# and this build implements growth only. Reset and re-apply instead.
k8s-scale: k8s-require-cluster
	kubectl scale statefulset/node --replicas=$(or $(N),5)
	kubectl rollout status statefulset/node --timeout=180s
	@echo "staged pods are up; attach + promote them from the dashboard"
k8s-down:
	kind delete cluster --name raftkv
