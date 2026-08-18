# Security policy

## Supported versions

This is a single-branch demonstration project. Only the current `main` receives fixes;
there are no maintenance branches and no backports.

| Version | Supported |
| ------- | --------- |
| `main`  | yes       |
| tagged releases | no, they are point-in-time snapshots |

## Reporting a vulnerability

Report privately through GitHub: **Security → Advisories → Report a vulnerability** on
this repository. That opens a draft advisory only you and the maintainer can see. Please
do not open a public issue for anything exploitable.

Expect an acknowledgement within seven days. If a report is accepted the fix and the
advisory are published together; if it is declined you get the reasoning, not silence.

## What is deliberately insecure

Read this before reporting, because the two largest findings in this repository are
intentional and documented, and a report that rediscovers them is not a finding.

- **The `/admin/*` endpoints are unauthenticated by design.** `crash`, `recover`, `reset`,
  `partition`, `add-learner`, `promote`, `flood`, `campaign`, `timing` and `spawn-node`
  exist so failures can be injected into a running cluster without a terminal. They are
  gated behind `RAFT_ADMIN_ENABLED` (default on), and turning that off is what removes
  them from a deployment. `flood` in particular makes the cluster do unbounded work on
  request; its bounds live on the pydantic model and the flag is what removes it entirely.
- **`POST /admin/spawn-node` starts operating-system processes**, which is why it carries
  a second flag of its own, `RAFT_PROVISION_ENABLED`, and a cap, `RAFT_PROVISION_MAX`.
  Three properties keep it from being remote code execution — the argv is a constant built
  from `sys.executable` and one bounded integer through `create_subprocess_exec`, it
  refuses to run inside a container, and it never blocks the event loop. All three are
  asserted in `tests/test_provision.py` rather than trusted. A way *around* any of those
  three is a real vulnerability and worth reporting.

There is no authentication, authorisation, TLS or rate limiting on the Raft RPCs or the
key-value API either. The threat model is a trusted network; nothing here is intended to
face one that is not.

## Scope

In scope: anything that violates a documented Raft safety property (see `docs/RAFT.md`),
lets an unauthenticated caller escape the three `spawn-node` properties above, or leaks a
key's value into a log — `log_event()` raises on a `value=` kwarg precisely to prevent
that, so a path around it is a finding.

Out of scope: the absence of authentication, the `/admin/*` surface with
`RAFT_ADMIN_ENABLED=1`, and anything requiring an attacker who already has the host.
