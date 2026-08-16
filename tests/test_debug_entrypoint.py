"""The IDE debug entrypoint is the one file nobody runs in CI and everybody runs the
moment they need a breakpoint inside a live node. These tests keep it from rotting
silently.

Context: it is a *script* rather than a `-m uvicorn` module target because
PyCharm 2024.3's pydevd resolves module targets via `pkgutil.get_loader()`, removed
in Python 3.14 -- see the module docstring in scripts/debug_node.py.
"""

import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "debug_node.py"


@pytest.fixture
def debug_node():
    spec = importlib.util.spec_from_file_location("debug_node", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # safe: everything real is behind main()
    return module


def test_script_exists_and_imports(debug_node):
    assert callable(debug_node.main)


def test_main_serves_a_real_app_object_on_the_configured_port(
    debug_node, monkeypatch, tmp_path
):
    """Guards the two things that break this entrypoint: passing uvicorn the factory *string*
    (re-introducing the import indirection this script avoids) and ignoring RAFT_PORT."""
    monkeypatch.setenv("RAFT_NODE_ID", "node-1")
    monkeypatch.setenv("RAFT_DB_PATH", str(tmp_path / "node-1.db"))
    monkeypatch.setenv("RAFT_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("RAFT_PEERS", "node-2=localhost:8002,node-3=localhost:8003")
    monkeypatch.setenv("RAFT_PORT", "8009")

    captured = {}
    monkeypatch.setattr(
        debug_node.uvicorn, "run", lambda app, **kw: captured.update(app=app, **kw)
    )
    debug_node.main()

    assert captured["port"] == 8009
    assert not isinstance(captured["app"], str)  # an app, not a factory path
    assert captured["app"].state.node.cfg.node_id == "node-1"


def test_the_pasteable_config_block_names_no_ambiguous_address():
    """This docstring is not documentation, it is INPUT: it is the block that gets
    pasted into the IDE's env-variables field, so a wrong string in it becomes a wrong
    string in someone's run configuration. It shipped saying `localhost`, and that is
    the one address on this path that can reach two different clusters -- localhost
    resolves to 127.0.0.1 or ::1, a compose cluster publishes on both, and a local node
    binds only the first. The symptom is a dashboard showing a mix of the two, which
    doc = SCRIPT.read_text()
    block = doc[doc.index("Run/Debug configuration:"):doc.index("Breakpoints:")]
    assert "localhost:800" not in block, "the pasted peer list can reach two clusters"
    assert "RAFT_PEERS=node-2=127.0.0.1:8002" in block


def test_the_pasteable_config_block_sets_an_advertise_address():
    """Omitting RAFT_ADVERTISE is silent until it isn't: a node writes its own address
    into the configuration entry, that entry replicates, and an empty one leaves a
    later-joined member holding a voter it can never dial. The failure surfaces during
    a later membership change, several steps after the mistake."""
    doc = SCRIPT.read_text()
    block = doc[doc.index("Run/Debug configuration:"):doc.index("Breakpoints:")]
    assert "RAFT_ADVERTISE=127.0.0.1:8001" in block


def test_port_defaults_when_unset(debug_node, monkeypatch, tmp_path):
    monkeypatch.delenv("RAFT_PORT", raising=False)
    monkeypatch.setenv("RAFT_NODE_ID", "node-1")
    monkeypatch.setenv("RAFT_DB_PATH", str(tmp_path / "node-1.db"))
    monkeypatch.setenv("RAFT_LOG_DIR", str(tmp_path / "logs"))
    monkeypatch.setenv("RAFT_PEERS", "node-2=localhost:8002")

    captured = {}
    monkeypatch.setattr(
        debug_node.uvicorn, "run", lambda app, **kw: captured.update(**kw)
    )
    debug_node.main()

    assert captured["port"] == debug_node.DEFAULT_PORT == 8001
