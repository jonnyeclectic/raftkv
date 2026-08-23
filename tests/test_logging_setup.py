import json
import logging

import pytest

from raftkv.config import NodeConfig
from raftkv.logging_setup import JsonFormatter, RingBufferHandler, log_event, setup_logging


@pytest.fixture(autouse=True)
def fresh_logging():
    """setup_logging is idempotent per process, so another module (e.g. test_api)
    may have installed handlers first. Isolate this module: clear the raftkv
    logger's handlers, restore whatever was there afterward.

    propagate must be saved/restored too: setup_logging sets it False, and
    pytest's log capture attaches its own handlers to every non-propagating
    logger at phase start — which would trip setup_logging's idempotence check
    in later tests if the flag leaked."""
    root = logging.getLogger("raftkv")
    saved = root.handlers[:]
    saved_propagate = root.propagate
    root.handlers.clear()
    root.propagate = True
    yield
    root.handlers[:] = saved
    root.propagate = saved_propagate


def make_cfg(tmp_path):
    return NodeConfig(node_id="node-t", log_dir=str(tmp_path / "logs"))


def test_json_formatter_merges_ctx():
    record = logging.LogRecord("raftkv.x", logging.INFO, __file__, 1, "became_leader", (), None)
    record.ctx = {"node": "node-1", "term": 3, "event": "became_leader"}
    line = json.loads(JsonFormatter().format(record))
    assert line["term"] == 3
    assert line["msg"] == "became_leader"
    assert "ts" in line and "level" in line


def test_setup_creates_rolling_file_and_ring_buffer(tmp_path):
    cfg = make_cfg(tmp_path)
    setup_logging(cfg)
    logger = logging.getLogger("raftkv.test")
    log_event(logger, "vote_granted", node="node-t", term=2, candidate="node-2")
    logfile = tmp_path / "logs" / "node-t.log"
    assert logfile.exists()
    assert json.loads(logfile.read_text().splitlines()[-1])["event"] == "vote_granted"
    events = RingBufferHandler.recent()
    assert events[-1]["candidate"] == "node-2"


def test_setup_is_idempotent(tmp_path):
    cfg = make_cfg(tmp_path)
    setup_logging(cfg)
    setup_logging(cfg)
    root = logging.getLogger("raftkv")
    assert len(root.handlers) == 3  # stdout + rotating file + ring buffer, not doubled


def test_log_event_refuses_pii_value():
    logger = logging.getLogger("raftkv.test")
    with pytest.raises(ValueError, match="value"):
        log_event(logger, "submit", key="heart_rate", value="61bpm")
