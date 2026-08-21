"""The dashboard's probe list, executed rather than grepped.

Same technique as the other three dashboard tests: slice the shipped block out of the
HTML, run it under node, assert on return values.

What this block decides is which addresses the page knocks on twice a second forever,
looking for processes that are running but in nobody's configuration. Two ways to get it
wrong, and both are quiet: too few and a node provisioned while the page is open (`make
node-up N=6`) never appears, which is the bug that started this feature; too many, or an
unfiltered one, and every unused slot is a console line of ERR_CONNECTION_REFUSED twice a
second for as long as the page stays open.
"""

import json
import shutil
import subprocess
from importlib import resources

import pytest

pytestmark = pytest.mark.skipif(
    shutil.which("node") is None, reason="node not installed; JS behaviour untestable"
)

START = "/* --- probe list"
END = "/* --- end probe list"

IDLE = ["127.0.0.1:8004", "127.0.0.1:8005"]


def probe_source() -> str:
    """The shipped block, not a copy of it."""
    page = (resources.files("raftkv") / "static" / "dashboard.html").read_text()
    start, end = page.index(START), page.index(END)
    return page[start:end]


def run(query: str) -> list[str]:
    """What the page would probe when opened with this query string."""
    script = f"""
{probe_source()}
console.log(JSON.stringify(probeList(new URLSearchParams({json.dumps(query)}))));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_default_is_the_first_two_provisionable_slots():
    """8004 and 8005 are where the first two `provision node` presses land, and they are
    also the two staged learners docker-compose declares dormant behind a profile (a
    container cannot spawn its own siblings, so `docker compose up -d raft-node-4` is
    that runtime's provision verb). Probing them by default is what makes a provisioned
    node-4 appear in the staged row with no query string and no PROBE mutation at all."""
    assert run("") == IDLE


def test_an_explicit_watch_list_turns_probing_off():
    """`?nodes=` means the operator has named the cluster; knocking on addresses they
    did not name would put cards on the page they explicitly did not ask for."""
    assert run("?nodes=127.0.0.1:8001,127.0.0.1:8002") == []
    assert run("?nodes=127.0.0.1:8001&probe=8006") == [], "?nodes= must win over ?probe="


def test_probe_extends_the_list_for_a_node_provisioned_while_open():
    """The `make node-up N=6` path: the script prints this URL, and the new process
    shows up in the staged row without the operator typing an address anywhere."""
    assert run("?probe=8004,8005,8006") == [*IDLE, "127.0.0.1:8006"]


def test_a_bare_number_means_loopback():
    """Ports are what the operator has in hand — `node-up` prints one, the Makefile
    takes one. Making them type the host back is one more chance to typo it."""
    assert run("?probe=8006") == ["127.0.0.1:8006"]
    assert run("?probe=8006,127.0.0.1:8007") == ["127.0.0.1:8006", "127.0.0.1:8007"]


def test_a_non_address_is_dropped_rather_than_fetched():
    """Anything that is not host:port cannot be a node, and every surviving entry
    becomes a fetch twice a second for the rest of the session."""
    assert run("?probe=8006,,not-an-address,http://x:1,8007") == [
        "127.0.0.1:8006", "127.0.0.1:8007",
    ]
    assert run("?probe=") == IDLE, "an empty probe= falls back to the default"


def test_whitespace_around_an_entry_does_not_disqualify_it():
    """A pasted list carries spaces. Dropping `8006` because it arrived as ` 8006`
    would look exactly like the node failing to start."""
    assert run("?probe=8004, 8005 ,8006") == [*IDLE, "127.0.0.1:8006"]


def successor(addr: str) -> str | None:
    """What the ladder would knock on after `addr` answered."""
    script = f"""
{probe_source()}
console.log(JSON.stringify(probeSuccessor({json.dumps(addr)})));
"""
    out = subprocess.run(
        ["node", "--input-type=module", "-e", script],
        capture_output=True, text=True, timeout=30,
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_an_answering_port_earns_a_knock_on_the_next_one():
    """The reload path for a cluster grown past the default list: ordinals are
    sequential in every runtime here, so a node at 800N answering is evidence 800N+1
    is worth knocking on. This is what climbs a fresh page back up to node-8 when the
    default probe list stops at 8005."""
    assert successor("127.0.0.1:8004") == "127.0.0.1:8005"
    assert successor("127.0.0.1:8011") == "127.0.0.1:8012"


def test_the_ladder_stops_at_the_provision_ceiling():
    """8012 is the last provisionable port (RAFT_PROVISION_MAX defaults to 12, and
    test_deploy_topology.py pins the two numbers together). Without the cap the block
    comment's 'deliberately NOT open-ended' promise stops being true: a full cluster
    would knock on 8013 twice a second forever."""
    assert successor("127.0.0.1:8012") is None


def test_the_ladder_never_leaves_loopback():
    """800N is the browser's side of a port-forward or a published port — a convention
    about THIS machine. An operator's custom ?probe= host proves nothing about its
    neighbours' ports."""
    assert successor("10.0.0.5:8004") is None
    assert successor("raft-node-4:8000") is None
