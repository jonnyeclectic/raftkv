"""The load generator's on-screen summary, executed rather than grepped.

Same technique as test_dashboard_leader.py, for the same reason: the page has no build
step, so the only way to tell a correct formatter from a plausible-looking one is to slice
the shipped block out of the HTML and run it. Grepping for a control proves it is present;
it cannot prove the panel says something true.

What is worth pinning here is not arithmetic but WORDING AND TINT, because those are what
an operator acts on. A flood that finishes having timed out half its writes is
the single most interesting outcome the panel can show, and rendering it in the same
"done" green as a clean sweep would bury exactly the finding the generator exists to
produce.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- flood summary"
END = "/* --- end flood summary"


def flood_source() -> str:
    """The shipped block, not a copy of it. A test that re-implements the function it is
    testing passes forever regardless of what the browser loads."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(state: dict | None) -> dict:
    """Evaluate the block against one /admin/flood snapshot."""
    script = f"""
{flood_source()}
const s = {json.dumps(state)};
console.log(JSON.stringify({{summary: floodSummary(s), widths: floodWidths(s)}}));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def snapshot(**overrides) -> dict:
    """A finished, wholly successful 200-write flood — the shape /admin/flood returns."""
    return {
        "running": False, "workload": "mixed", "total": 200, "concurrency": 50,
        "done": 200, "ok": 200, "not_leader": 0, "timeout": 0,
        "elapsed_ms": 2000, "cancelled": False,
    } | overrides


# ---- the panel says nothing before anything has run -------------------------


@pytest.mark.parametrize("state", [None, {}, {"total": 0}])
def test_no_flood_yet_renders_an_empty_bar_and_no_claim(state):
    """An idle panel must not assert an outcome. Zero-width segments, no summary line —
    a "0/0 · 0.0s done" would read as a flood that ran and found nothing."""
    result = run(state)
    assert result["summary"] is None
    assert result["widths"] == {"ok": "0%", "timeout": "0%", "notleader": "0%",
                                "failed": "0%"}


# ---- the bar is proportional to TOTAL, not to progress ----------------------


def test_bar_fills_progressively_against_the_total():
    """Half done is half filled. Measuring against `done` instead would peg the bar at
    100% from the first write and show no progress at all."""
    widths = run(snapshot(running=True, done=100, ok=100))["widths"]
    assert widths["ok"] == "50.00%"
    assert widths["timeout"] == "0%" or widths["timeout"] == "0.00%"


def test_bar_segments_are_proportional_and_sum_to_the_completed_share():
    """A three-way split renders as three segments, and their widths sum to the fraction
    of the total that has finished — the untouched remainder stays as visible track."""
    widths = run(snapshot(done=200, ok=150, timeout=30, not_leader=20))["widths"]
    assert widths == {"ok": "75.00%", "timeout": "15.00%", "notleader": "10.00%",
                      "failed": "0.00%"}
    total = sum(float(w.rstrip("%")) for w in widths.values())
    assert total == pytest.approx(100.0)


# ---- wording and tint -------------------------------------------------------


def test_a_clean_sweep_reads_as_done_and_is_tinted_good():
    summary = run(snapshot())["summary"]
    assert summary["cls"] == "good"
    assert "mixed done" in summary["text"]
    assert "200/200" in summary["text"]
    assert "200 committed" in summary["text"]
    assert "100/s committed" in summary["text"], "throughput is the number this panel is about"


def test_the_rate_counts_committed_writes_not_merely_settled_ones():
    """Caught by rendering the panel rather than reading it: the measured 500-write
    overload commits 1 and times out 499 in 2.3s. Dividing `done` by elapsed time
    advertises that as "217/s" — a healthy-looking throughput figure sitting directly
    beside "1 committed", and the precise opposite of what the run demonstrated. The rate
    must count what actually made it into the log."""
    summary = run(snapshot(workload="distinct", total=500, done=500, ok=1,
                           timeout=499, elapsed_ms=2300))["summary"]
    assert "217/s" not in summary["text"], "a failed flood advertised high throughput"
    assert "0/s committed" in summary["text"]
    assert summary["cls"] == "warn"


def test_failures_are_named_and_tinted_amber_even_though_the_flood_finished():
    """THE case this file exists for. 120 of 200 writes timed out; the run completed, so
    a naive formatter calls it done and tints it green. Amber, and the failures spelled
    out, is what makes the throughput ceiling visible instead of hidden."""
    summary = run(snapshot(done=200, ok=80, timeout=120))["summary"]
    assert summary["cls"] == "warn", "a flood that lost 60% of its writes read as success"
    assert "120 timed out" in summary["text"]
    assert "80 committed" in summary["text"]


def test_a_mid_flood_election_is_reported_as_rejected_writes():
    """not_leader means leadership moved while the burst was in flight — the most
    interesting thing the generator can surface, so it must be named, not folded into a
    generic failure count."""
    summary = run(snapshot(done=200, ok=50, not_leader=150))["summary"]
    assert summary["cls"] == "warn"
    assert "150 rejected" in summary["text"]


def test_a_running_flood_is_untinted_and_omits_the_rate():
    """Neutral while in flight: a partial run has not earned a verdict yet, and a rate
    computed from an unfinished burst would be wrong and then silently corrected."""
    summary = run(snapshot(running=True, done=40, ok=40, elapsed_ms=500))["summary"]
    assert summary["cls"] == ""
    assert "40/200" in summary["text"]
    assert "/s" not in summary["text"]


def test_a_cancelled_flood_says_cancelled_rather_than_done():
    """Distinguishing the two matters: "done · 30/200" invites the reader to conclude the
    cluster dropped 170 writes, when in fact they were never sent."""
    summary = run(snapshot(done=30, ok=30, cancelled=True, elapsed_ms=400))["summary"]
    assert summary["text"].startswith("cancelled")
    assert summary["cls"] == "warn"


def test_zero_elapsed_time_does_not_divide_by_zero():
    """A flood fast enough to round to 0ms must not print Infinity/s or NaN/s."""
    summary = run(snapshot(elapsed_ms=0))["summary"]
    assert "Infinity" not in summary["text"]
    assert "NaN" not in summary["text"]
    assert "/s" not in summary["text"]


def test_a_missing_elapsed_time_degrades_to_a_placeholder():
    """elapsed_ms is null between start() and the first completion. Rendering that as
    `null s` or `NaN` is how a reader loses confidence in the whole panel."""
    summary = run(snapshot(running=True, elapsed_ms=None, done=0, ok=0))["summary"]
    assert "?s" in summary["text"]
    assert "NaN" not in summary["text"]


def test_zero_counts_are_omitted_rather_than_printed_as_zero():
    """"200/200 · 200 committed · 2.0s" beats "… · 0 timed out · 0 rejected": a zero that
    is always on screen stops being read, and then a real one is missed too."""
    text = run(snapshot())["summary"]["text"]
    assert "0 timed out" not in text
    assert "0 rejected" not in text


@pytest.mark.parametrize("workload", ["distinct", "overwrite", "mixed"])
def test_the_workload_is_always_named(workload):
    """Which mix ran is not recoverable from the counters, so the panel has to say it —
    otherwise a screenshot of a finished flood is unattributable."""
    assert workload in run(snapshot(workload=workload))["summary"]["text"]


# ---- an unexpected error is not a fourth normal outcome ---------------------
#
# `timeout` and `not_leader` are Raft working as designed under load; a heavy burst is
# SUPPOSED to produce them. `failed` means submit() raised something this build did not
# anticipate, which is a bug rather than a finding about the cluster. The panel has to
# separate those two categories or a bug reads as a feature.


def test_an_unexpected_error_is_tinted_red_not_amber():
    """Amber is the "Raft is under strain" colour and it is already spoken for. A red
    tint is the only thing on this panel that means "this build is broken"."""
    got = run(snapshot(ok=195, failed=5, done=200))
    assert got["summary"]["cls"] == "bad", "an unexpected error read as mere strain"
    assert "5 FAILED" in got["summary"]["text"]


def test_failed_outranks_timeouts_in_the_tint():
    """A burst that both timed out and hit a real error is red, not amber: the amber
    story is the expected one and would bury the unexpected one."""
    got = run(snapshot(ok=100, timeout=99, failed=1, done=200))
    assert got["summary"]["cls"] == "bad"
    assert "1 FAILED" in got["summary"]["text"]
    assert "99 timed out" in got["summary"]["text"]  # ...and still names both


def test_a_burst_that_stopped_short_is_called_incomplete():
    """The bug this whole change exists for: `gather` abandoned the remaining writes, so
    `done` froze below `total` with every counter clean. Reading that as "done" in green
    is the worst available outcome -- it claims success for a burst that never ran."""
    got = run(snapshot(ok=3, done=3, total=200))
    assert got["summary"]["cls"] == "bad"
    assert "INCOMPLETE" in got["summary"]["text"]
    assert "3/200" in got["summary"]["text"]


def test_a_cancelled_burst_is_not_called_incomplete():
    """Cancelling is a deliberate act and already has its own wording. Flagging it as
    INCOMPLETE would cry bug every time an operator pressed stop."""
    got = run(snapshot(ok=40, done=40, total=200, cancelled=True))
    assert "INCOMPLETE" not in got["summary"]["text"]
    assert got["summary"]["text"].startswith("cancelled")
    assert got["summary"]["cls"] == "warn"


def test_a_clean_full_sweep_is_still_green():
    """The regression guard on all of the above: none of these new branches may fire on
    the ordinary happy path."""
    got = run(snapshot())
    assert got["summary"]["cls"] == "good"
    assert "INCOMPLETE" not in got["summary"]["text"]
    assert "FAILED" not in got["summary"]["text"]


def test_the_failed_segment_is_proportional_and_zero_when_clean():
    assert run(snapshot(ok=150, failed=50, done=200))["widths"]["failed"] == "25.00%"
    assert run(snapshot())["widths"]["failed"] == "0.00%"
    assert run(None)["widths"]["failed"] == "0%"
