"""SQLite is load-bearing here: Raft REQUIRES currentTerm/votedFor/log[] on stable
storage before responding to RPCs (paper Fig. 2, §5.2) — that is the database
integration, not an accessory. The same file holds the applied KV state machine;
apply() writes the KV row and last_applied in ONE transaction, so a crash between
"applied" and "recorded" is impossible (exactly-once apply across restarts)."""

import json
import sqlite3
from pathlib import Path

from pydantic import TypeAdapter

from raftkv.models import ClusterConfig, Command, LogEntry, NoOp

# The log holds client commands AND configuration entries (§6). One adapter decodes
# either, discriminated on `op`, so a row always returns the type it was written as.
_ENTRY_COMMAND = TypeAdapter(Command | ClusterConfig | NoOp)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    current_term INTEGER NOT NULL,
    voted_for TEXT,
    last_applied INTEGER NOT NULL
);
INSERT OR IGNORE INTO meta (id, current_term, voted_for, last_applied) VALUES (1, 0, NULL, 0);
CREATE TABLE IF NOT EXISTS log (
    idx INTEGER PRIMARY KEY,
    term INTEGER NOT NULL,
    command TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


class Storage:
    """Log is 1-indexed with an implicit sentinel at index 0 (term 0) so
    prev_log_index=0 needs no special-casing (MIT 6.824 hint).

    check_same_thread=False: all runtime access happens on the single asyncio
    event-loop thread; Starlette's TestClient drives the app from a portal
    thread, which sqlite's default check would (wrongly, for us) reject.
    Synchronous sqlite on the event loop: writes are sub-ms at this scale;
    move to a worker thread if profiling ever says otherwise."""

    def __init__(self, path: str) -> None:
        if path != ":memory:":  # sqlite will not create parent directories itself
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        with self._conn:
            self._conn.executescript(_SCHEMA)

    # ---- persistent raft state (Fig. 2) ----
    def load(self) -> tuple[int, str | None, int]:
        row = self._conn.execute(
            "SELECT current_term, voted_for, last_applied FROM meta WHERE id = 1"
        ).fetchone()
        return (row[0], row[1], row[2])

    def save_term_and_vote(self, term: int, voted_for: str | None) -> None:
        with self._conn:
            self._conn.execute(
                "UPDATE meta SET current_term = ?, voted_for = ? WHERE id = 1",
                (term, voted_for),
            )

    # ---- log ----
    def last_log_index(self) -> int:
        return self._conn.execute("SELECT COALESCE(MAX(idx), 0) FROM log").fetchone()[0]

    def term_at(self, idx: int) -> int | None:
        if idx == 0:
            return 0  # sentinel
        row = self._conn.execute("SELECT term FROM log WHERE idx = ?", (idx,)).fetchone()
        return row[0] if row else None

    def entry(self, idx: int) -> LogEntry | None:
        row = self._conn.execute(
            "SELECT term, command FROM log WHERE idx = ?", (idx,)
        ).fetchone()
        if row is None:
            return None
        return LogEntry(term=row[0], command=_ENTRY_COMMAND.validate_python(json.loads(row[1])))

    def entries_from(self, start: int) -> list[LogEntry]:
        rows = self._conn.execute(
            "SELECT term, command FROM log WHERE idx >= ? ORDER BY idx", (start,)
        ).fetchall()
        return [
            LogEntry(term=t, command=_ENTRY_COMMAND.validate_python(json.loads(c)))
            for t, c in rows
        ]

    def last_config(self) -> tuple[int, ClusterConfig] | None:
        """The highest-index configuration entry in the log, committed or not (§6).

        "Committed or not" is the whole point: a server uses the latest configuration
        in its log because that configuration may be needed for the very election that
        commits it. Reading it back from the log — rather than caching it — is also
        what makes truncation revert the configuration for free."""
        row = self._conn.execute(
            "SELECT idx, command FROM log WHERE json_extract(command, '$.op') = 'config' "
            "ORDER BY idx DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        return (row[0], ClusterConfig.model_validate(json.loads(row[1])))

    def append(self, entries: list[LogEntry]) -> None:
        start = self.last_log_index() + 1
        with self._conn:
            self._conn.executemany(
                "INSERT INTO log (idx, term, command) VALUES (?, ?, ?)",
                [(start + i, e.term, e.command.model_dump_json()) for i, e in enumerate(entries)],
            )

    def truncate_from(self, idx: int) -> None:
        with self._conn:
            self._conn.execute("DELETE FROM log WHERE idx >= ?", (idx,))

    # ---- applied state machine ----
    def advance_applied(self, idx: int) -> None:
        """A configuration entry changes membership, not the key-value state machine,
        but last_applied must still move or the single applier stalls on it forever."""
        with self._conn:
            self._conn.execute("UPDATE meta SET last_applied = ? WHERE id = 1", (idx,))

    def apply(self, idx: int, cmd: Command) -> None:
        with self._conn:  # KV mutation + last_applied advance: one transaction
            if cmd.op == "set":
                self._conn.execute(
                    "INSERT INTO kv (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (cmd.key, cmd.value),
                )
            else:  # delete
                self._conn.execute("DELETE FROM kv WHERE key = ?", (cmd.key,))
            self._conn.execute("UPDATE meta SET last_applied = ? WHERE id = 1", (idx,))

    def kv_get(self, key: str) -> str | None:
        row = self._conn.execute("SELECT value FROM kv WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def kv_all(self) -> dict[str, str]:
        return dict(self._conn.execute("SELECT key, value FROM kv ORDER BY key").fetchall())

    def wipe(self) -> None:
        """Destroy every durable trace and return the file to first-boot state.

        This deliberately discards the two things Raft calls stable storage — the
        current term and the vote — which is the guarantee that stops a rebooted node
        from voting twice in one term (Fig. 2, §5.2). That makes it a demo control and
        nothing else: it exists so a cluster can be reset without a shell, and it is
        the fastest way to expose why that persistence requirement exists."""
        with self._conn:  # one transaction: never half-wiped
            self._conn.execute("DELETE FROM log")
            self._conn.execute("DELETE FROM kv")
            self._conn.execute(
                "UPDATE meta SET current_term = 0, voted_for = NULL, last_applied = 0 "
                "WHERE id = 1"
            )

    def close(self) -> None:
        self._conn.close()
