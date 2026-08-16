"""The state machine's key ordering, executed rather than grepped.

Third slice-and-run block, same technique and same reason as test_dashboard_leader.py:
the page has no build step, so a comparator can only be shown correct by running it.

The bug this pins: `storage.kv_all()` is `ORDER BY key`, which is byte order, so a flood
that writes k0..k199 arrives as k0, k1, k10, k100, k101 … k11, k110 … k2, k20. Scanning
that for "did k80 land" means reading the whole list, because k80 sits between k8 and k81
and nowhere near k79. The dashboard therefore re-sorts numerically before rendering.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

from raftkv.flood import MAX_TOTAL, FloodRunner

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- key ordering"
END = "/* --- end key ordering"


def ordering_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def order(kv: dict | None) -> list[str]:
    """The keys, in the order the panel would render them."""
    script = f"""
{ordering_source()}
console.log(JSON.stringify(sortedKv({json.dumps(kv)}).map(e => e[0])));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


# ---- the reported bug -------------------------------------------------------


def test_numeric_runs_sort_as_numbers_not_as_text():
    """The reported symptom exactly: k79, k8, k80 must read k8, k79, k80."""
    assert order({"k79": "v", "k8": "v", "k80": "v"}) == ["k8", "k79", "k80"]


def test_a_two_hundred_key_flood_reads_in_order():
    """The whole point, at flood scale. Byte order interleaves the decades — k1, k10,
    k100, k11, k2 — so the check is against the natural sequence, not a spot value."""
    kv = {f"k{i}": f"v{i}" for i in range(200)}
    assert order(kv) == [f"k{i}" for i in range(200)]


def test_ordering_survives_keys_of_different_widths_together():
    """A cluster does not only hold one flood's keys: an earlier padded run, a later
    unpadded one, and hand-typed keys all share the table."""
    assert order({"k007": "v", "k7": "v", "k70": "v", "k8": "v"}) == [
        "k007", "k7", "k8", "k70",
    ]


# ---- it must not break the ordinary case ------------------------------------


def test_plain_text_keys_stay_alphabetical():
    """Keys typed into the write control are arbitrary. Numeric awareness must not cost
    the obvious behaviour for keys with no digits in them at all."""
    assert order({"temp": "v", "alpha": "v", "zulu": "v", "mode": "v"}) == [
        "alpha", "mode", "temp", "zulu",
    ]


def test_mixed_text_and_numeric_keys_coexist():
    assert order({"k10": "v", "temp": "v", "k2": "v", "alpha": "v"}) == [
        "alpha", "k2", "k10", "temp",
    ]


def test_digits_inside_a_longer_key_still_compare_numerically():
    """Numbers are not always a suffix: `sensor-2-reading` is as real as `k2`."""
    assert order({"bed-10-temp": "v", "bed-2-temp": "v", "bed-1-temp": "v"}) == [
        "bed-1-temp", "bed-2-temp", "bed-10-temp",
    ]


@pytest.mark.parametrize("empty", [None, {}])
def test_an_empty_state_machine_is_not_an_error(empty):
    """Every node holds an empty map before the first write, and a crashed one answers
    with no kv at all — neither may throw in the middle of a render."""
    assert order(empty) == []


def test_the_ordering_is_total_so_the_list_does_not_jitter():
    """The card is rebuilt twice a second. A comparator that returns 0 for two distinct
    keys leaves their order to sort stability, and rows that swap places on their own
    are indistinguishable from data changing."""
    kv = {"K1": "v", "k1": "v", "k01": "v", "k1a": "v"}
    first = order(kv)
    assert len(first) == 4, "a key was dropped"
    for _ in range(3):
        assert order(kv) == first, "the same input ordered two different ways"


# ---- the other half: keys that arrive already ordered -----------------------


def generated(workload: str, count: int) -> list[str]:
    """Keys straight out of the SHIPPED generator.

    Building the expected format inline instead — `f"k{i:0{width}d}"` — is how the first
    version of these tests passed while the generator emitted unpadded keys: they asserted
    a re-implementation agreed with itself. `_command` is the code that actually runs."""
    runner = FloodRunner.__new__(FloodRunner)  # no node needed: _command touches none
    return [runner._command(workload, i).key for i in range(count)]


def test_the_generator_really_emits_padded_keys():
    """The claim under test, made against `_command` itself."""
    keys = generated("distinct", 200)
    assert keys[0] == "k0000"
    assert keys[8] == "k0008" and keys[79] == "k0079" and keys[80] == "k0080"


def test_generated_keys_need_no_re_sorting_at_all():
    """They sort correctly under plain byte order — which is what SQLite's
    `ORDER BY key` and `curl /state | jq` both use. The dashboard's comparator exists
    for everyone else's keys; this is so the raw data is readable without it."""
    keys = generated("distinct", 500)
    assert sorted(keys) == keys, "byte order and numeric order disagree"


def test_generated_keys_and_the_dashboard_comparator_agree():
    """Two orderings now exist — SQLite's and the dashboard's. If they disagreed, the
    panel would reorder rows relative to the API for no visible reason."""
    kv = dict.fromkeys(generated("distinct", 200), "v")
    assert order(kv) == sorted(kv)


def test_one_key_width_for_every_run_so_re_running_overwrites():
    """Sizing the padding to the run instead would make a second flood at a different size
    write a DISJOINT family: a 50-write run gives k00..k49 and a 200-write run k000..k199, so
    the node ends up holding 250 keys that byte order interleaves — worse than no padding
    at all. Changing the number and pressing the button again is the ordinary gesture."""
    small, large = set(generated("distinct", 50)), set(generated("distinct", 200))
    assert small < large, "a smaller run left orphans a larger one cannot overwrite"
    assert len({len(k) for k in small | large}) == 1, "two key widths in one namespace"


def test_the_padding_covers_the_largest_flood_the_endpoint_allows():
    """The width has to be right at the ceiling, not just at the default: MAX_TOTAL is
    what the pydantic model lets through, so k4999 must still be the same length as
    k0000 or the very largest run is the one that sorts wrong."""
    keys = generated("distinct", MAX_TOTAL)
    assert len({len(k) for k in keys}) == 1
    assert sorted(keys) == keys


def test_the_mixed_workload_pads_to_the_same_width():
    """Both workloads write into the same table, so a mismatch would interleave them."""
    assert len({len(k) for k in generated("mixed", 40)}) == 1
    assert len(generated("mixed", 1)[0]) == len(generated("distinct", 1)[0])


# ---- scrolling survives the 500ms rebuild -----------------------------------
#
# A capped, scrollable list is useless if the position resets twice a second, and that
# is the default here: renderNodes() calls replaceChildren, so every card is a brand new
# subtree on every tick. These run the SHIPPED restoreScroll against a DOM stub small
# enough to be obviously faithful — querySelectorAll, dataset, scrollTop, addEventListener
# and nothing else. Not a browser, so it proves the bookkeeping, not the rendering.

SCROLL_START = "/* --- scroll persistence"
SCROLL_END = "/* --- end scroll persistence"

DOM_STUB = """
class Box {
  constructor(key) { this.dataset = {scrollkey: key}; this.scrollTop = 0; this._on = []; }
  addEventListener(kind, fn) { this._on.push([kind, fn]); }
  scrollTo(y) { this.scrollTop = y; for (const [k, fn] of this._on) if (k === "scroll") fn(); }
}
class Host {
  constructor(boxes) { this.boxes = boxes; }
  querySelectorAll() { return this.boxes; }
}
const SCROLL_POS = new Map();
// The block registers window-level pointerup/pointercancel handlers to release the
// rebuild hold; node has no window, so stand one in and keep the handlers reachable.
const WINDOW_ON = [];
function addEventListener(kind, fn) { WINDOW_ON.push([kind, fn]); }
function fireWindow(kind) { for (const [k, fn] of WINDOW_ON) if (k === kind) fn(); }
"""


def scroll_source() -> str:
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(SCROLL_START), page.index(SCROLL_END)
    return page[start:end]


def run_scroll(body: str) -> dict:
    script = f"{DOM_STUB}\n{scroll_source()}\n{body}"
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_scroll_position_survives_a_rebuild():
    """Scroll down, let the tick rebuild the card, and stay where you were."""
    result = run_scroll("""
      const first = new Box("127.0.0.1:8001:kv");
      restoreScroll(new Host([first]));
      first.scrollTo(420);                       // the viewer scrolls
      const rebuilt = new Box("127.0.0.1:8001:kv");   // 500ms later: a fresh subtree
      restoreScroll(new Host([rebuilt]));
      console.log(JSON.stringify({at: rebuilt.scrollTop}));
    """)
    assert result["at"] == 420, "the list snapped back to the top on the next tick"


def test_each_node_keeps_its_own_scroll_position():
    """Three cards, three independent lists — scrolling one to compare against another
    must not drag the others with it."""
    result = run_scroll("""
      const boxes = ["a:kv", "b:kv", "c:kv"].map(k => new Box(k));
      restoreScroll(new Host(boxes));
      boxes[0].scrollTo(100); boxes[1].scrollTo(250);
      const rebuilt = ["a:kv", "b:kv", "c:kv"].map(k => new Box(k));
      restoreScroll(new Host(rebuilt));
      console.log(JSON.stringify({at: rebuilt.map(b => b.scrollTop)}));
    """)
    assert result["at"] == [100, 250, 0]


def test_a_node_seen_for_the_first_time_starts_at_the_top():
    """A card with no remembered position must not inherit another card's."""
    result = run_scroll("""
      const known = new Box("a:kv");
      restoreScroll(new Host([known]));
      known.scrollTo(300);
      const fresh = new Box("brand-new:kv");
      restoreScroll(new Host([fresh]));
      console.log(JSON.stringify({at: fresh.scrollTop}));
    """)
    assert result["at"] == 0


def test_scrolling_back_to_the_top_is_remembered_as_the_top():
    """0 is a remembered position, not "nothing remembered".

    The rebuilt box is handed a non-zero scrollTop so the assertion cannot pass merely
    because a fresh element starts at 0 — which is exactly how this test passed while
    the code still read `if (at)` and skipped restoring a remembered top."""
    result = run_scroll("""
      const a = new Box("a:kv");
      restoreScroll(new Host([a]));
      a.scrollTo(300);
      a.scrollTo(0);                       // the viewer scrolls back up
      const rebuilt = new Box("a:kv");
      rebuilt.scrollTop = 300;             // whatever the box would otherwise sit at
      restoreScroll(new Host([rebuilt]));
      console.log(JSON.stringify({at: rebuilt.scrollTop}));
    """)
    assert result["at"] == 0, "scrolling to the top was forgotten and the list jumped"
