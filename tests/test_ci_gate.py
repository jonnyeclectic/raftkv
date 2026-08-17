"""The gating policy, tested like the rest of the system.

`ci.yml` protects `main` through exactly one required status check, `gate`, which inspects
every other job's result and decides. That indirection exists because GitHub reports a
**skipped** job as a *passing* required check — so a required check that can be skipped
fails open, and looks identical to passing while it does it (docs/CI.md, OVERVIEW.md D17).

Which makes `gate` load-bearing, and gives it a failure mode of its own: add a job to
`ci.yml`, forget to add it to `gate`'s `needs`, and the new gate runs on every pull request
while blocking nothing. Nothing goes red. The check list in branch protection is unchanged.
The only symptom is that a failing job no longer stops a merge, which is discovered by a
merge that should not have happened.

So the four properties the pipeline's security argument actually rests on are asserted
here rather than believed:

  1. `gate` depends on every other job, and classifies every job it depends on
  2. every third-party action is pinned to a full commit SHA
  3. no `${{ }}` expression is interpolated into a shell script
  4. no checkout leaves a usable token behind in `.git/config`

`yaml` is imported directly rather than through `importorskip`. It arrives with
`uvicorn[standard]`, a pinned direct dependency whose resolution `make locks` holds
fixed — and a skip here would be the exact hole this file exists to close.
"""

import pathlib
import re

import yaml

REPO = pathlib.Path(__file__).resolve().parent.parent
WORKFLOWS = sorted((REPO / ".github" / "workflows").glob("*.yml"))
CI = next(p for p in WORKFLOWS if p.name == "ci.yml")

SHA = re.compile(r"^[0-9a-f]{40}$")
EXPRESSION = re.compile(r"\$\{\{")


def load(path: pathlib.Path) -> dict:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def steps_of(workflow: dict):
    for job_name, job in workflow["jobs"].items():
        for step in job.get("steps", []):
            yield job_name, step


def test_there_are_workflows_to_check():
    """Guards every assertion below: an empty glob makes all of them vacuously true."""
    assert {p.name for p in WORKFLOWS} >= {"ci.yml", "nightly.yml", "release.yml"}


def test_gate_depends_on_every_other_job():
    jobs = load(CI)["jobs"]
    assert "gate" in jobs, "ci.yml has no `gate` job; branch protection requires that context"
    others = set(jobs) - {"gate"}
    needs = set(jobs["gate"]["needs"])
    missing = sorted(others - needs)
    assert not missing, (
        f"ci.yml job(s) {', '.join(missing)} are not in gate's `needs`, so they gate "
        "nothing. Add them to `needs` and to REQUIRED or MAY_SKIP in the gate script."
    )


def test_gate_classifies_every_job_it_depends_on():
    """`needs` decides what gate waits for; the two sets decide what it accepts."""
    jobs = load(CI)["jobs"]
    script = "".join(s.get("run", "") for _, s in steps_of({"jobs": {"gate": jobs["gate"]}}))
    classified: set[str] = set()
    for name in ("REQUIRED", "MAY_SKIP"):
        match = re.search(rf"{name}\s*=\s*\{{(.*?)\}}", script, re.S)
        assert match, f"the gate script no longer defines {name}"
        classified |= set(re.findall(r'"([a-z-]+)"', match.group(1)))
    unclassified = sorted(set(jobs["gate"]["needs"]) - classified)
    assert not unclassified, (
        f"gate waits for {', '.join(unclassified)} but never says what result is "
        "acceptable. An unclassified job fails the gate at runtime by design — classify "
        "it here instead of discovering it on a pull request."
    )


def test_every_action_is_pinned_to_a_commit_sha():
    unpinned = []
    for path in WORKFLOWS:
        for job, step in steps_of(load(path)):
            uses = step.get("uses")
            if uses and not SHA.match(uses.partition("@")[2]):
                unpinned.append(f"{path.name}:{job} -> {uses}")
    assert not unpinned, (
        "a tag is a mutable pointer, and moving one is how tj-actions/changed-files "
        "compromised every workflow tracking it in March 2025. Pin to a 40-character "
        "commit SHA: " + "; ".join(unpinned)
    )


def test_no_expression_is_interpolated_into_a_shell_script():
    """GitHub substitutes `${{ }}` before the shell sees the script, so an
    attacker-controlled value becomes an attacker-controlled command. Values reach
    scripts through `env:`, which the shell reads as data."""
    offenders = [
        f"{path.name}:{job}"
        for path in WORKFLOWS
        for job, step in steps_of(load(path))
        if EXPRESSION.search(step.get("run", ""))
    ]
    assert not offenders, "template injection risk in run block(s): " + "; ".join(offenders)


def test_no_checkout_persists_credentials():
    """actions/checkout writes a usable token into .git/config by default and leaves it
    there for the rest of the job, readable by any later step or anything it shells out to."""
    leaky = [
        f"{path.name}:{job}"
        for path in WORKFLOWS
        for job, step in steps_of(load(path))
        if str(step.get("uses", "")).startswith("actions/checkout@")
        and step.get("with", {}).get("persist-credentials") is not False
    ]
    assert not leaky, "checkout without `persist-credentials: false`: " + "; ".join(leaky)


def test_every_job_has_a_timeout():
    """The default is six hours. A hung job holds a runner and reports nothing."""
    untimed = [
        f"{path.name}:{job}"
        for path in WORKFLOWS
        for job, spec in load(path)["jobs"].items()
        if "timeout-minutes" not in spec
    ]
    assert not untimed, "job(s) with no timeout-minutes: " + "; ".join(untimed)


def test_workflow_tokens_start_at_nothing():
    """Without an explicit block a workflow inherits the repository default, which is
    the largest blast radius a single compromised step can have."""
    wrong = [p.name for p in WORKFLOWS if load(p).get("permissions") != {}]
    assert not wrong, "workflow(s) without top-level `permissions: {}`: " + ", ".join(wrong)
