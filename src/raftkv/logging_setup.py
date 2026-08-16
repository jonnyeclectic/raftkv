"""Structured JSON logging: stdout (docker/k8s collect it), rolling files, and an
in-memory ring buffer served at GET /logs so the dashboard can centralize all nodes.

PII policy: log KEYS and metadata, never VALUES. Enforced in log_event."""

import json
import logging
import time
from collections import deque
from logging.handlers import RotatingFileHandler
from pathlib import Path

from raftkv.config import NodeConfig


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "ts": round(time.time(), 3),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "ctx", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class RingBufferHandler(logging.Handler):
    _buffer: deque[dict] = deque(maxlen=200)

    def emit(self, record: logging.LogRecord) -> None:
        try:
            RingBufferHandler._buffer.append(json.loads(self.format(record)))
        except Exception:  # a broken log line must never break the node
            self.handleError(record)

    @classmethod
    def recent(cls) -> list[dict]:
        return list(cls._buffer)


def log_event(logger: logging.Logger, event: str, **ctx: object) -> None:
    if "value" in ctx:
        raise ValueError("PII policy: never log a KV 'value' — log the key and metadata only")
    logger.info(event, extra={"ctx": {"event": event, **ctx}})


def setup_logging(cfg: NodeConfig) -> None:
    root = logging.getLogger("raftkv")
    if root.handlers:  # idempotent: uvicorn reload / repeated create_app must not stack handlers
        return
    root.setLevel(logging.INFO)
    root.propagate = False
    formatter = JsonFormatter()

    stream = logging.StreamHandler()
    Path(cfg.log_dir).mkdir(parents=True, exist_ok=True)
    rolling = RotatingFileHandler(
        Path(cfg.log_dir) / f"{cfg.node_id}.log", maxBytes=1_000_000, backupCount=5
    )
    ring = RingBufferHandler()
    for handler in (stream, rolling, ring):
        handler.setFormatter(formatter)
        root.addHandler(handler)
