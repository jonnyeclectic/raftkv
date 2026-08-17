# CI/CD — what is gated, and why that is the gate

Three workflows, one required check, and a deliberate split between what every pull
request pays for and what a cron pays for once a night.

| Workflow | Trigger | Question it answers |
|---|---|---|
| [`ci.yml`](../.github/workflows/ci.yml) | pull request, push to `main`, merge queue | Is this change safe to merge? |
| [`nightly.yml`](../.github/workflows/nightly.yml) | 06:00 UTC, manual | Does the cluster still come up starting from nothing? |
| [`release.yml`](../.github/workflows/release.yml) | tag `v*`, manual | Is this tag safe to publish, and can anyone prove what built it? |

## The gates in `ci.yml`

| Job | What it proves | The failure it exists to catch |
|---|---|---|
| `static` | `ruff check` is clean, and the workflows themselves survive a security linter | Style drift; and the Actions footguns this file is written against — expression injection, unpinned actions, over-broad tokens |
| `test` | The whole suite passes **and not one test skipped** | The suite reporting success one layer lighter — see below |
| `provenance` | `uv.lock` matches `pyproject.toml`, and `requirements.lock` matches `uv.lock` | The image installing a different dependency set than the tests ran against |
| `image` | The container builds, runs as uid 1000, and carries no fixable HIGH/CRITICAL CVE | A `USER` line lost in a refactor; a base image that has quietly aged into a known vulnerability |
| `smoke` | Real containers elect, replicate, fail over, converge — then a canary value written through the HTTP API appears in **no** node's `/logs` | Everything that only works in-process; and the PII guarantee decaying from a property into a claim |
| `codeql` | Python **and** the dashboard's JavaScript pass `security-extended` | A DOM-XSS path returning to the page that renders remote node state (two were found there once) |
| `dependency-review` | No new dependency arrives with a known HIGH vulnerability or a copyleft licence | A transitive addition nobody reviewed |
| `gate` | Every job above did what it was supposed to | All of the above being required in name only |

### Why `test` asserts zero skips

Eight files in `tests/` carry `skipif(shutil.which("node") is None)`. They slice blocks
out of the shipped `dashboard.html` and execute them under node, which is the only way to
test that page's logic by running it rather than grepping it — the page has no build step,
so a test that re-implements the function it covers passes forever regardless of what the
browser loads.

Measured on this repository: **377 tests with node present, 289 passed and 88 skipped
without it.** On a runner without node the suite still exits 0, still reports success, and
has silently stopped checking a quarter of itself. Installing node is half the fix. The
other half is the assertion, because "we installed node" is a belief and "zero tests
skipped" is a measurement. There is exactly one conditional skip in the repository, `test`
removes its condition, and then proves the removal worked.

The job also fails if fewer than 300 tests are collected. A `conftest.py` import error or
a bad `testpaths` can collect nothing and exit 0, which is the same failure mode one level
further up: a green build that ran almost nothing.

### Why there is exactly one required status check

GitHub reports a **skipped** job as a passing required check. A required check that can be
skipped is therefore not required — it is a check that fails open, which is the worst
available default because it looks identical to passing.

`gate` is the one job that can never be skipped (`if: always()`), and it decides for
itself what each dependency's result is allowed to be:

- `static`, `test`, `provenance`, `image`, `smoke` — **must be `success`.** Nothing in
  the workflow can skip them, so a skip means something changed that `gate` has not been
  taught about, and that is a failure rather than a pass.
- `codeql`, `dependency-review` — **`success` or `skipped`.** Code scanning is free on
  public repositories and a GitHub Advanced Security feature on private ones. Rather than
  fail every run on a private repo, both jobs declare the condition and `gate` accepts
  their absence. They become required automatically the moment the repository is public.
- Anything else — a job added to `needs` and not to either list — fails the gate. New
  gates default to being enforced, not to being ignored.

So branch protection requires the single context `gate`, and the composition of the
pipeline is a code change reviewed like any other rather than a checkbox in a settings
page that no diff ever shows.

## The pipeline's own threat model

A CI runner holds a write-capable token and executes third-party code on every push. It
is the most privileged untested surface in most repositories, so it is treated here as
production.

| Control | Where | What it stops |
|---|---|---|
| Every action pinned to a full commit SHA | all three workflows | A moved tag. In March 2025 `tj-actions/changed-files` had its published tags repointed at a malicious commit; every workflow tracking `@v35` executed it on the next run. A SHA cannot be moved |
| `permissions: {}` at workflow level, granted back per job | all three workflows | A workflow with no `permissions:` block inherits the repository default — historically read/write on everything. The `gate` job holds no token scope at all |
| `persist-credentials: false` on every checkout | all three workflows | `actions/checkout` leaves a usable token in `.git/config` for the rest of the job by default, readable by any later step or anything it shells out to |
| No `${{ }}` inside any `run:` block | all three workflows | Template injection: GitHub interpolates the expression into the shell script *before* the shell sees it, so an attacker-controlled value becomes an attacker-controlled command. Values reach scripts through `env:` instead |
| `pull_request`, never `pull_request_target` | `ci.yml` | The "pwn request" pattern — running fork code in a context that holds the base repository's secrets |
| zizmor in the gate | `static` | Everything above, checked rather than asserted. The pipeline lints itself |
| Dependabot with a 7-day cooldown | [`dependabot.yml`](../.github/dependabot.yml) | Pinning means nothing updates on its own; this pays that bill weekly, and waits a week before proposing anything newly published |
| OIDC (`id-token: write`), no registry password | `release.yml` | A long-lived registry credential in repository secrets. The signing identity is minted per run and expires with it |

## Turning it on

The workflows do nothing on their own — a check nobody requires is a suggestion. Three
one-time settings, in order of how much they matter:

```bash
# 1. Require the gate. Blocks direct pushes to main, requires the branch be current,
#    and makes `gate` the one check that must be green.
gh api -X POST "repos/{owner}/{repo}/rulesets" --input .github/rulesets/main.json

# 2. Merge queue (Settings -> General -> Pull Requests). ci.yml already listens for
#    merge_group, so the gate re-runs against the merged result rather than against a
#    branch that was green before the last three merges landed.

# 3. The human gate on publishing (Settings -> Environments -> New environment: `ghcr`).
#    Add a required reviewer. release.yml already targets that environment, so approval
#    switches on with no code change and leaves a record of who approved which release.
```

Optional, once there is a second pair of hands: raise
`required_approving_review_count` in the ruleset from `0` to `1`. It is `0` deliberately
— on a one-person repository, requiring an approval is a lock rather than a gate, because
GitHub does not accept self-approval.

The same gates run locally. `make gate` is `lint` + `test` + `locks`, which is every
check in `ci.yml` that does not need Docker; `make smoke` and `make clean-start-check`
are the two that do, and CI runs the identical scripts.

## Cadence, and why the expensive things are not on every pull request

`make clean-start-check` removes every container, volume and local image, refuses if
anything else still holds ports 8001–8003 (a `run-local` node's specific `127.0.0.1`
bind would beat compose's wildcard forward, and smoke would test the wrong cluster —
the symptom is a false `no failover leader` thirty seconds in), rebuilds with
`--no-cache`, starts the cluster, runs the full smoke, and tears down. It is the gate
that was previously run by hand before anything that depended on it — which makes it
exactly as reliable as whoever remembers to run it.

Putting it on every pull request would charge each one several minutes to re-answer a
question the cached build already answered. Putting it on a nightly cron costs nothing a
human waits for and means the answer is known by morning. The portability matrix
(ubuntu + macOS) sits there for the same reason and one more: the real-timer election
tests are the most timing-sensitive thing in the suite, and a slower runner is where a
flaky gate would come from. A flaky gate is worse than no gate, because it teaches people
to re-run rather than to read.

## Deliberately not gated

Same discipline as FAILURE_MODES.md table 2: name the omission, its cost, and the fix.

| Not gated | Why not | What it would take |
|---|---|---|
| A coverage threshold | The baseline has never been measured, and a threshold picked to be safely below an unmeasured number gates nothing. Inventing one would be the exact mistake FAILURE_MODES.md spends a section repudiating | `uv run --with pytest-cov pytest --cov=raftkv --cov-report=term` once, read the number, then add `--cov-fail-under=<number>` to the `test` job. `--with` keeps it out of `uv.lock`, so no lockfile churn |
| `ruff format --check` | 35 of 57 files would be reformatted today. A gate the repository fails on the day it lands is a gate people learn to bypass | One formatting commit, reviewed on its own, then add the flag to `static` |
| The base image by digest | `FROM python:3.14-slim` floats, so two builds a month apart install different bytes. Pinning needs a digest fetched from the registry, and an unpinned float is at least honestly visible to Trivy | Pin `FROM python:3.14-slim@sha256:…` and add a `docker` ecosystem to `dependabot.yml`, which then bumps the digest on a schedule |
| `pip-audit` on `requirements.lock` | Dependency review and Dependabot already cover the same ground through GitHub's own advisory database, without a second tool to keep working | `uvx pip-audit --requirement requirements.lock` as a step in `provenance` |
| StepSecurity `harden-runner` | It would add a third-party agent to every job in order to audit the egress of jobs whose actions are already SHA-pinned and whose tokens are already scoped to nothing. The marginal gain did not look worth the marginal dependency | One step at the top of each job, in `audit` mode first |
| CodeQL findings failing the build | `analyze` publishes alerts; it does not fail on them. The alerts are the signal, and treating a first-run backlog as a merge blocker is how a security tool gets disabled | Settings → Code security → add a code scanning merge protection rule, once the backlog is triaged |
