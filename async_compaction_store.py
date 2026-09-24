"""Optional, non-canonical staging storage for background leaf compaction.

This module does not start a worker or call a model. In particular, importing
it or constructing it with ``enabled=False`` does not open/create a database.
Prepared leaves live outside the canonical DAG until a separate, validated
publication transaction is implemented.
"""

from __future__ import annotations

import json
from pathlib import Path
import sqlite3
import time

from .db_bootstrap import configure_connection, refuse_schema_version_too_new
from .sqlite_util import _prepare_private_sqlite_file, _restrict_existing_sqlite_artifacts


_BATCH_STATES = (
    "pending", "preparing", "ready", "promoting", "promoted", "rejected",
    "failed", "superseded",
)

_CREATE_BATCHES = """
CREATE TABLE IF NOT EXISTS lcm_compaction_batches (
    batch_id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN (
        'pending', 'preparing', 'ready', 'promoting', 'promoted',
        'rejected', 'failed', 'superseded'
    )),
    frontier_start_store_id INTEGER NOT NULL CHECK (frontier_start_store_id >= 0),
    frontier_end_store_id INTEGER NOT NULL CHECK (frontier_end_store_id > frontier_start_store_id),
    fresh_tail_count INTEGER NOT NULL CHECK (fresh_tail_count >= 0),
    leaf_chunk_tokens INTEGER NOT NULL CHECK (leaf_chunk_tokens > 0),
    policy_fingerprint TEXT NOT NULL,
    summary_route_fingerprint TEXT NOT NULL,
    source_coverage_hash TEXT NOT NULL,
    expected_leaf_count INTEGER NOT NULL CHECK (expected_leaf_count > 0),
    prepared_leaf_count INTEGER NOT NULL DEFAULT 0 CHECK (prepared_leaf_count >= 0),
    failure_count INTEGER NOT NULL DEFAULT 0 CHECK (failure_count >= 0),
    next_retry_at REAL,
    last_error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    promoted_at REAL,
    rejected_reason TEXT
)
"""

_CREATE_PENDING = """
CREATE TABLE IF NOT EXISTS lcm_pending_summary_nodes (
    pending_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL REFERENCES lcm_compaction_batches(batch_id) ON DELETE CASCADE,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    depth INTEGER NOT NULL DEFAULT 0 CHECK (depth = 0),
    summary TEXT NOT NULL,
    token_count INTEGER NOT NULL CHECK (token_count > 0),
    source_token_count INTEGER NOT NULL CHECK (source_token_count >= 0),
    source_ids TEXT NOT NULL,
    source_identity_hashes TEXT NOT NULL,
    source_range_start_store_id INTEGER NOT NULL,
    source_range_end_store_id INTEGER NOT NULL,
    previous_pending_ids TEXT NOT NULL DEFAULT '[]',
    created_at REAL NOT NULL,
    earliest_at REAL,
    latest_at REAL,
    expand_hint TEXT NOT NULL DEFAULT '',
    CHECK (source_range_end_store_id >= source_range_start_store_id),
    UNIQUE (batch_id, source_range_start_store_id, source_range_end_store_id)
)
"""


class AsyncCompactionStore:
    """Stage uncommitted summary work without exposing it to active readers."""

    def __init__(self, db_path: str | Path, *, enabled: bool = False) -> None:
        self.enabled = bool(enabled)
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        if not self.enabled:
            return
        # The normal MessageStore owns core-schema creation. This optional
        # sidecar must never silently create an incomplete, standalone lcm.db.
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        _prepare_private_sqlite_file(self.db_path)
        conn = sqlite3.connect(
            str(self.db_path), timeout=5.0, check_same_thread=False,
            isolation_level=None,
        )
        try:
            refuse_schema_version_too_new(conn)
            configure_connection(conn)
            conn.execute("PRAGMA foreign_keys=ON")
            conn.row_factory = sqlite3.Row
            self._conn = conn
            self._ensure_schema()
            _restrict_existing_sqlite_artifacts(self.db_path)
        except BaseException:
            conn.close()
            self._conn = None
            raise

    @property
    def connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise RuntimeError("async compaction storage is disabled or closed")
        return self._conn

    def _ensure_schema(self) -> None:
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(_CREATE_BATCHES)
            conn.execute(_CREATE_PENDING)
            self._verify_schema(conn)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lcm_compaction_batches_conversation "
                "ON lcm_compaction_batches(conversation_id, state, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lcm_compaction_batches_session "
                "ON lcm_compaction_batches(session_id, state, created_at)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lcm_compaction_batches_retry "
                "ON lcm_compaction_batches(next_retry_at, state)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_lcm_pending_nodes_batch_source "
                "ON lcm_pending_summary_nodes(batch_id, source_range_start_store_id)"
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    @staticmethod
    def _verify_schema(conn: sqlite3.Connection) -> None:
        required = {
            "lcm_compaction_batches": {
                "batch_id", "conversation_id", "session_id", "state",
                "frontier_start_store_id", "frontier_end_store_id",
                "policy_fingerprint", "summary_route_fingerprint",
                "source_coverage_hash", "expected_leaf_count", "prepared_leaf_count",
            },
            "lcm_pending_summary_nodes": {
                "pending_id", "batch_id", "conversation_id", "session_id",
                "summary", "source_ids", "source_identity_hashes",
                "source_range_start_store_id", "source_range_end_store_id",
            },
        }
        for table, fields in required.items():
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if not fields <= columns:
                raise sqlite3.OperationalError(f"incompatible async compaction table: {table}")

    def create_batch(
        self, *, batch_id: str, conversation_id: str, session_id: str,
        frontier_start_store_id: int, frontier_end_store_id: int,
        fresh_tail_count: int, leaf_chunk_tokens: int,
        policy_fingerprint: str, summary_route_fingerprint: str,
        source_coverage_hash: str, expected_leaf_count: int,
    ) -> None:
        """Record a planning fence; this never changes canonical LCM state."""
        now = time.time()
        self.connection.execute(
            """INSERT INTO lcm_compaction_batches (
                batch_id, conversation_id, session_id, state,
                frontier_start_store_id, frontier_end_store_id,
                fresh_tail_count, leaf_chunk_tokens, policy_fingerprint,
                summary_route_fingerprint, source_coverage_hash,
                expected_leaf_count, created_at, updated_at
            ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                batch_id, conversation_id, session_id,
                frontier_start_store_id, frontier_end_store_id,
                fresh_tail_count, leaf_chunk_tokens, policy_fingerprint,
                summary_route_fingerprint, source_coverage_hash,
                expected_leaf_count, now, now,
            ),
        )

    def get_batch(self, batch_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?", (batch_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def stage_leaf(
        self, *, pending_id: str, batch_id: str, summary: str,
        token_count: int, source_token_count: int,
        source_ids: list[int], source_identity_hashes: list[str],
        previous_pending_ids: list[str] | None = None,
        earliest_at: float | None = None, latest_at: float | None = None,
        expand_hint: str = "",
    ) -> None:
        """Stage one complete leaf; never publish it to the canonical DAG."""
        if (
            not summary.strip() or token_count <= 0 or source_token_count < 0
            or not source_ids or len(source_ids) != len(source_identity_hashes)
            or any(not isinstance(item, int) or item <= 0 for item in source_ids)
            or source_ids != sorted(set(source_ids))
            or any(not digest for digest in source_identity_hashes)
        ):
            raise ValueError("invalid pending leaf")
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            batch = conn.execute(
                "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?", (batch_id,),
            ).fetchone()
            if batch is None or batch["state"] not in {"pending", "preparing"}:
                raise ValueError("batch is not accepting pending leaves")
            if not (batch["frontier_start_store_id"] < source_ids[0]
                    <= source_ids[-1] <= batch["frontier_end_store_id"]):
                raise ValueError("pending leaf exceeds batch frontier")
            for row in conn.execute(
                "SELECT source_ids FROM lcm_pending_summary_nodes WHERE batch_id = ?",
                (batch_id,),
            ):
                if set(json.loads(row[0])) & set(source_ids):
                    raise ValueError("pending leaf overlaps an existing leaf")
            if batch["prepared_leaf_count"] >= batch["expected_leaf_count"]:
                raise ValueError("batch already has its expected leaf count")
            now = time.time()
            conn.execute(
                """INSERT INTO lcm_pending_summary_nodes (
                    pending_id, batch_id, conversation_id, session_id, summary,
                    token_count, source_token_count, source_ids,
                    source_identity_hashes, source_range_start_store_id,
                    source_range_end_store_id, previous_pending_ids, created_at,
                    earliest_at, latest_at, expand_hint
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    pending_id, batch_id, batch["conversation_id"], batch["session_id"],
                    summary, token_count, source_token_count,
                    json.dumps(source_ids), json.dumps(source_identity_hashes),
                    source_ids[0], source_ids[-1],
                    json.dumps(previous_pending_ids or []), now,
                    earliest_at, latest_at, expand_hint,
                ),
            )
            conn.execute(
                "UPDATE lcm_compaction_batches SET prepared_leaf_count = "
                "prepared_leaf_count + 1, state = 'preparing', updated_at = ? "
                "WHERE batch_id = ?",
                (now, batch_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    def counts(self, *, conversation_id: str | None = None) -> dict[str, int]:
        """Count staged generations; active DAG counters remain independent."""
        result = {state: 0 for state in _BATCH_STATES}
        if conversation_id is None:
            rows = self.connection.execute(
                "SELECT state, COUNT(*) FROM lcm_compaction_batches GROUP BY state"
            )
        else:
            rows = self.connection.execute(
                "SELECT state, COUNT(*) FROM lcm_compaction_batches "
                "WHERE conversation_id = ? GROUP BY state", (conversation_id,),
            )
        result.update({state: count for state, count in rows})
        return result

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> AsyncCompactionStore:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
