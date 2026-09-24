"""Optional, non-canonical staging storage for background leaf compaction.

This module does not start a worker or call a model. In particular, importing
it or constructing it with ``enabled=False`` does not open/create a database.
Prepared leaves live outside the canonical DAG until a separate, validated
publication transaction is implemented.
"""

from __future__ import annotations

from dataclasses import dataclass
import functools
import json
import hashlib
from pathlib import Path
import re
import sqlite3
import threading
import time

from .db_bootstrap import configure_connection, refuse_schema_version_too_new
from .sqlite_util import (
    _prepare_private_sqlite_file,
    _restrict_existing_sqlite_artifacts,
)


_BATCH_STATES = (
    "pending",
    "preparing",
    "ready",
    "promoting",
    "promoted",
    "rejected",
    "failed",
    "superseded",
)


def _synchronized(method):
    @functools.wraps(method)
    def wrapper(self, *args, **kwargs):
        with self._lock:
            return method(self, *args, **kwargs)

    return wrapper


@dataclass(frozen=True)
class PromotionResult:
    promoted: bool
    reason: str
    node_ids: tuple[int, ...] = ()


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
    source_ids_json TEXT NOT NULL,
    source_identity_hashes_json TEXT NOT NULL,
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
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        if not self.enabled:
            return
        # The normal MessageStore owns core-schema creation. This optional
        # sidecar must never silently create an incomplete, standalone lcm.db.
        if not self.db_path.is_file():
            raise FileNotFoundError(self.db_path)
        _prepare_private_sqlite_file(self.db_path)
        conn = sqlite3.connect(
            str(self.db_path),
            timeout=5.0,
            check_same_thread=False,
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

    @_synchronized
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
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_lcm_compaction_one_active_frontier "
                "ON lcm_compaction_batches(conversation_id, session_id, "
                "frontier_start_store_id) "
                "WHERE state IN ('pending', 'preparing', 'ready')"
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
                "batch_id",
                "conversation_id",
                "session_id",
                "state",
                "frontier_start_store_id",
                "frontier_end_store_id",
                "policy_fingerprint",
                "summary_route_fingerprint",
                "source_coverage_hash",
                "source_ids_json",
                "source_identity_hashes_json",
                "expected_leaf_count",
                "prepared_leaf_count",
            },
            "lcm_pending_summary_nodes": {
                "pending_id",
                "batch_id",
                "conversation_id",
                "session_id",
                "summary",
                "source_ids",
                "source_identity_hashes",
                "source_range_start_store_id",
                "source_range_end_store_id",
            },
        }
        for table, fields in required.items():
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if not fields <= columns:
                raise sqlite3.OperationalError(
                    f"incompatible async compaction table: {table}"
                )

    @_synchronized
    def create_batch(
        self,
        *,
        batch_id: str,
        conversation_id: str,
        session_id: str,
        frontier_start_store_id: int,
        frontier_end_store_id: int,
        fresh_tail_count: int,
        leaf_chunk_tokens: int,
        policy_fingerprint: str,
        summary_route_fingerprint: str,
        expected_leaf_count: int,
    ) -> dict:
        """Snapshot exact raw source rows and record a planning fence atomically.

        The caller performs provider work only after this transaction commits.
        A later publisher must re-read and compare these same identities.
        """
        if (
            not batch_id
            or not conversation_id
            or not session_id
            or not policy_fingerprint
            or not summary_route_fingerprint
            or expected_leaf_count <= 0
        ):
            raise ValueError("invalid compaction batch identity or policy")
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            source_ids, identity_hashes = self._source_snapshot(
                conn,
                conversation_id=conversation_id,
                session_id=session_id,
                frontier_start_store_id=frontier_start_store_id,
                frontier_end_store_id=frontier_end_store_id,
            )
            if (
                not source_ids
                or source_ids[-1] != frontier_end_store_id
                or expected_leaf_count > len(source_ids)
            ):
                raise ValueError("batch has no complete source coverage")
            coverage_hash = self._coverage_hash(source_ids, identity_hashes)
            now = time.time()
            conn.execute(
                """INSERT INTO lcm_compaction_batches (
                    batch_id, conversation_id, session_id, state,
                    frontier_start_store_id, frontier_end_store_id,
                    fresh_tail_count, leaf_chunk_tokens, policy_fingerprint,
                    summary_route_fingerprint, source_coverage_hash,
                    source_ids_json, source_identity_hashes_json,
                    expected_leaf_count, created_at, updated_at
                ) VALUES (?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    batch_id,
                    conversation_id,
                    session_id,
                    frontier_start_store_id,
                    frontier_end_store_id,
                    fresh_tail_count,
                    leaf_chunk_tokens,
                    policy_fingerprint,
                    summary_route_fingerprint,
                    coverage_hash,
                    json.dumps(source_ids),
                    json.dumps(identity_hashes),
                    expected_leaf_count,
                    now,
                    now,
                ),
            )
            conn.execute("COMMIT")
            return {
                "batch_id": batch_id,
                "source_ids": source_ids,
                "source_identity_hashes": identity_hashes,
                "source_coverage_hash": coverage_hash,
            }
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    @staticmethod
    def _source_snapshot(
        conn: sqlite3.Connection,
        *,
        conversation_id: str,
        session_id: str,
        frontier_start_store_id: int,
        frontier_end_store_id: int,
    ) -> tuple[list[int], list[str]]:
        rows = conn.execute(
            """SELECT store_id, conversation_id, session_id, role, content,
                      tool_call_id, tool_calls, tool_name, timestamp
               FROM messages
               WHERE conversation_id = ? AND session_id = ?
                 AND store_id > ? AND store_id <= ?
               ORDER BY store_id""",
            (
                conversation_id,
                session_id,
                frontier_start_store_id,
                frontier_end_store_id,
            ),
        ).fetchall()
        source_ids: list[int] = []
        identity_hashes: list[str] = []
        for row in rows:
            (
                source_id,
                conversation,
                session,
                role,
                content,
                tool_call_id,
                tool_calls,
                tool_name,
                timestamp,
            ) = row
            identity = (
                int(source_id),
                conversation,
                session,
                role,
                hashlib.sha256((content or "").encode("utf-8")).hexdigest(),
                tool_call_id,
                hashlib.sha256((tool_calls or "").encode("utf-8")).hexdigest(),
                tool_name,
                timestamp,
            )
            source_ids.append(int(source_id))
            identity_hashes.append(
                hashlib.sha256(
                    json.dumps(
                        identity, ensure_ascii=False, separators=(",", ":")
                    ).encode("utf-8")
                ).hexdigest()
            )
        return source_ids, identity_hashes

    @staticmethod
    def _coverage_hash(source_ids: list[int], identity_hashes: list[str]) -> str:
        pairs = list(zip(source_ids, identity_hashes))
        return hashlib.sha256(
            json.dumps(pairs, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

    @_synchronized
    def get_batch(self, batch_id: str) -> dict | None:
        row = self.connection.execute(
            "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    @_synchronized
    def active_batch_for_frontier(
        self,
        *,
        conversation_id: str,
        session_id: str,
        frontier_store_id: int,
    ) -> dict | None:
        row = self.connection.execute(
            """SELECT * FROM lcm_compaction_batches
               WHERE conversation_id = ? AND session_id = ?
                 AND frontier_start_store_id = ?
                 AND state IN ('pending', 'preparing', 'ready')
               ORDER BY created_at DESC LIMIT 1""",
            (conversation_id, session_id, frontier_store_id),
        ).fetchone()
        return dict(row) if row is not None else None

    @_synchronized
    def retry_blocked_until(
        self,
        *,
        conversation_id: str,
        session_id: str,
        frontier_store_id: int,
    ) -> float | None:
        row = self.connection.execute(
            """SELECT MAX(next_retry_at) FROM lcm_compaction_batches
               WHERE conversation_id = ? AND session_id = ?
                 AND frontier_start_store_id = ? AND state = 'failed'""",
            (conversation_id, session_id, frontier_store_id),
        ).fetchone()
        return float(row[0]) if row and row[0] is not None else None

    @_synchronized
    def source_snapshot_matches(self, batch_id: str) -> bool:
        """Read-only probe used to retire stale jobs that no longer map live."""
        conn = self.connection
        conn.execute("BEGIN")
        try:
            batch = conn.execute(
                "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if batch is None:
                return False
            ids, hashes = self._source_snapshot(
                conn,
                conversation_id=batch["conversation_id"],
                session_id=batch["session_id"],
                frontier_start_store_id=batch["frontier_start_store_id"],
                frontier_end_store_id=batch["frontier_end_store_id"],
            )
            return (
                ids == json.loads(batch["source_ids_json"])
                and hashes == json.loads(batch["source_identity_hashes_json"])
                and self._coverage_hash(ids, hashes) == batch["source_coverage_hash"]
            )
        finally:
            conn.execute("ROLLBACK")

    @_synchronized
    def reject_batch(self, batch_id: str, *, reason: str) -> bool:
        """Retire stale prepared work without touching canonical summaries."""
        safe_reason = re.match(r"[A-Za-z_][A-Za-z0-9_]{0,79}", reason)
        updated = self.connection.execute(
            """UPDATE lcm_compaction_batches
               SET state = 'rejected', rejected_reason = ?, updated_at = ?
               WHERE batch_id = ? AND state IN ('pending', 'preparing', 'ready')""",
            (
                safe_reason.group(0) if safe_reason else "invalid_batch",
                time.time(),
                batch_id,
            ),
        )
        return updated.rowcount == 1

    @_synchronized
    def fail_batch(
        self, batch_id: str, *, error_type: str, backoff_seconds: float
    ) -> None:
        """Record a compact, secret-free failure without touching canonical state."""
        match = re.match(r"[A-Za-z_][A-Za-z0-9_.]{0,79}", str(error_type))
        safe_type = match.group(0) if match else "SummaryError"
        now = time.time()
        updated = self.connection.execute(
            """UPDATE lcm_compaction_batches
               SET state = 'failed', failure_count = failure_count + 1,
                   next_retry_at = ?, last_error = ?, updated_at = ?
               WHERE batch_id = ? AND state IN ('pending', 'preparing')""",
            (now + max(0.0, float(backoff_seconds)), safe_type, now, batch_id),
        )
        if updated.rowcount != 1:
            raise ValueError("batch is not eligible for failure recording")

    @_synchronized
    def stage_leaf(
        self,
        *,
        pending_id: str,
        batch_id: str,
        summary: str,
        token_count: int,
        source_token_count: int,
        source_ids: list[int],
        source_identity_hashes: list[str],
        previous_pending_ids: list[str] | None = None,
        earliest_at: float | None = None,
        latest_at: float | None = None,
        expand_hint: str = "",
    ) -> None:
        """Stage one complete leaf; never publish it to the canonical DAG."""
        if (
            not summary.strip()
            or token_count <= 0
            or source_token_count < 0
            or not source_ids
            or len(source_ids) != len(source_identity_hashes)
            or any(not isinstance(item, int) or item <= 0 for item in source_ids)
            or source_ids != sorted(set(source_ids))
            or any(not digest for digest in source_identity_hashes)
        ):
            raise ValueError("invalid pending leaf")
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            batch = conn.execute(
                "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if batch is None or batch["state"] not in {"pending", "preparing"}:
                raise ValueError("batch is not accepting pending leaves")
            if not (
                batch["frontier_start_store_id"]
                < source_ids[0]
                <= source_ids[-1]
                <= batch["frontier_end_store_id"]
            ):
                raise ValueError("pending leaf exceeds batch frontier")
            planned = dict(
                zip(
                    json.loads(batch["source_ids_json"]),
                    json.loads(batch["source_identity_hashes_json"]),
                )
            )
            if any(
                planned.get(source_id) != digest
                for source_id, digest in zip(
                    source_ids,
                    source_identity_hashes,
                )
            ):
                raise ValueError(
                    "pending leaf differs from the planned source snapshot"
                )
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
                    pending_id,
                    batch_id,
                    batch["conversation_id"],
                    batch["session_id"],
                    summary,
                    token_count,
                    source_token_count,
                    json.dumps(source_ids),
                    json.dumps(source_identity_hashes),
                    source_ids[0],
                    source_ids[-1],
                    json.dumps(previous_pending_ids or []),
                    now,
                    earliest_at,
                    latest_at,
                    expand_hint,
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

    @_synchronized
    def mark_ready(self, batch_id: str) -> None:
        """Make a fully covered batch eligible for later *validated* promotion.

        Readiness does not publish canonical summaries or advance a frontier.
        The publisher must repeat source and live-policy validation in its own
        ``BEGIN IMMEDIATE`` transaction after provider work has finished.
        """
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            batch = conn.execute(
                "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if batch is None or batch["state"] != "preparing":
                raise ValueError("batch is not preparing")
            leaves = conn.execute(
                "SELECT source_ids, source_identity_hashes "
                "FROM lcm_pending_summary_nodes WHERE batch_id = ? "
                "ORDER BY source_range_start_store_id",
                (batch_id,),
            ).fetchall()
            if (
                len(leaves) != batch["expected_leaf_count"]
                or len(leaves) != batch["prepared_leaf_count"]
            ):
                raise ValueError("batch leaf count is incomplete")
            staged = [
                (source_id, digest)
                for leaf in leaves
                for source_id, digest in zip(json.loads(leaf[0]), json.loads(leaf[1]))
            ]
            planned = list(
                zip(
                    json.loads(batch["source_ids_json"]),
                    json.loads(batch["source_identity_hashes_json"]),
                )
            )
            if staged != planned:
                raise ValueError("batch source coverage is incomplete or out of order")
            current_ids, current_hashes = self._source_snapshot(
                conn,
                conversation_id=batch["conversation_id"],
                session_id=batch["session_id"],
                frontier_start_store_id=batch["frontier_start_store_id"],
                frontier_end_store_id=batch["frontier_end_store_id"],
            )
            if list(zip(current_ids, current_hashes)) != planned:
                raise ValueError(
                    "source identity changed during background preparation"
                )
            if (
                self._coverage_hash(current_ids, current_hashes)
                != batch["source_coverage_hash"]
            ):
                raise ValueError("source coverage hash changed during preparation")
            conn.execute(
                "UPDATE lcm_compaction_batches SET state = 'ready', updated_at = ? "
                "WHERE batch_id = ?",
                (time.time(), batch_id),
            )
            conn.execute("COMMIT")
        except BaseException:
            conn.execute("ROLLBACK")
            raise

    @_synchronized
    def promote_batch(
        self,
        batch_id: str,
        *,
        live_policy_fingerprint: str,
        live_summary_route_fingerprint: str,
        max_publishable_store_id: int,
    ) -> PromotionResult:
        """Publish all prepared leaves and the lifecycle frontier in one txn.

        The caller must derive ``max_publishable_store_id`` from the *current*
        fresh-tail boundary. No model/provider call or active-context assembly
        happens here. Rejected batches leave canonical tables untouched.
        """
        conn = self.connection
        conn.execute("BEGIN IMMEDIATE")
        try:
            batch = conn.execute(
                "SELECT * FROM lcm_compaction_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if batch is None:
                raise ValueError("unknown compaction batch")

            def reject(reason: str) -> PromotionResult:
                conn.execute(
                    "UPDATE lcm_compaction_batches "
                    "SET state = 'rejected', rejected_reason = ?, updated_at = ? "
                    "WHERE batch_id = ?",
                    (reason, time.time(), batch_id),
                )
                conn.execute("COMMIT")
                return PromotionResult(False, reason)

            if batch["state"] != "ready":
                # A second promoter must never demote an already-published batch.
                if batch["state"] == "promoted":
                    conn.execute("COMMIT")
                    return PromotionResult(False, "already_promoted")
                return reject("batch_not_ready")
            if live_policy_fingerprint != batch["policy_fingerprint"]:
                return reject("policy_fingerprint_mismatch")
            if live_summary_route_fingerprint != batch["summary_route_fingerprint"]:
                return reject("summary_route_fingerprint_mismatch")
            if max_publishable_store_id < batch["frontier_end_store_id"]:
                return reject("fresh_tail_boundary_changed")

            lifecycle = conn.execute(
                "SELECT current_session_id, current_frontier_store_id "
                "FROM lcm_lifecycle_state WHERE conversation_id = ?",
                (batch["conversation_id"],),
            ).fetchone()
            if (
                lifecycle is None
                or lifecycle["current_session_id"] != batch["session_id"]
            ):
                return reject("session_binding_changed")
            if (
                lifecycle["current_frontier_store_id"]
                != batch["frontier_start_store_id"]
            ):
                return reject("frontier_changed")

            planned = list(
                zip(
                    json.loads(batch["source_ids_json"]),
                    json.loads(batch["source_identity_hashes_json"]),
                )
            )
            current_ids, current_hashes = self._source_snapshot(
                conn,
                conversation_id=batch["conversation_id"],
                session_id=batch["session_id"],
                frontier_start_store_id=batch["frontier_start_store_id"],
                frontier_end_store_id=batch["frontier_end_store_id"],
            )
            if (
                list(zip(current_ids, current_hashes)) != planned
                or self._coverage_hash(current_ids, current_hashes)
                != batch["source_coverage_hash"]
            ):
                return reject("source_identity_mismatch")

            leaves = conn.execute(
                "SELECT * FROM lcm_pending_summary_nodes WHERE batch_id = ? "
                "ORDER BY source_range_start_store_id",
                (batch_id,),
            ).fetchall()
            if (
                len(leaves) != batch["expected_leaf_count"]
                or len(leaves) != batch["prepared_leaf_count"]
                or [
                    (source_id, digest)
                    for leaf in leaves
                    for source_id, digest in zip(
                        json.loads(leaf["source_ids"]),
                        json.loads(leaf["source_identity_hashes"]),
                    )
                ]
                != planned
            ):
                return reject("pending_coverage_mismatch")

            planned_ids = set(current_ids)
            canonical = conn.execute(
                "SELECT source_ids FROM summary_nodes "
                "WHERE session_id = ? AND depth = 0 AND source_type = 'messages'",
                (batch["session_id"],),
            )
            if any(planned_ids.intersection(json.loads(row[0])) for row in canonical):
                return reject("canonical_source_overlap")

            now = time.time()
            node_ids: list[int] = []
            for leaf in leaves:
                cur = conn.execute(
                    """INSERT INTO summary_nodes
                       (session_id, depth, summary, token_count, source_token_count,
                        source_ids, source_type, created_at, earliest_at, latest_at,
                        expand_hint)
                       VALUES (?, 0, ?, ?, ?, ?, 'messages', ?, ?, ?, ?)""",
                    (
                        batch["session_id"],
                        leaf["summary"],
                        leaf["token_count"],
                        leaf["source_token_count"],
                        leaf["source_ids"],
                        now,
                        leaf["earliest_at"],
                        leaf["latest_at"],
                        leaf["expand_hint"],
                    ),
                )
                node_ids.append(int(cur.lastrowid))
            updated = conn.execute(
                """UPDATE lcm_lifecycle_state
                   SET current_frontier_store_id = ?, updated_at = ?
                   WHERE conversation_id = ? AND current_session_id = ?
                     AND current_frontier_store_id = ?""",
                (
                    batch["frontier_end_store_id"],
                    now,
                    batch["conversation_id"],
                    batch["session_id"],
                    batch["frontier_start_store_id"],
                ),
            )
            if updated.rowcount != 1:
                raise RuntimeError(
                    "frontier compare-and-swap failed during publication"
                )
            conn.execute(
                "UPDATE lcm_compaction_batches SET state = 'promoted', "
                "promoted_at = ?, updated_at = ? WHERE batch_id = ?",
                (now, now, batch_id),
            )
            conn.execute(
                """UPDATE lcm_compaction_batches
                   SET state = 'superseded', updated_at = ?
                   WHERE batch_id != ? AND conversation_id = ? AND session_id = ?
                     AND state IN ('pending', 'preparing', 'ready')
                     AND frontier_start_store_id < ?""",
                (
                    now,
                    batch_id,
                    batch["conversation_id"],
                    batch["session_id"],
                    batch["frontier_end_store_id"],
                ),
            )
            conn.execute("COMMIT")
            return PromotionResult(True, "promoted", tuple(node_ids))
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise

    @_synchronized
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
                "WHERE conversation_id = ? GROUP BY state",
                (conversation_id,),
            )
        result.update({state: count for state, count in rows})
        return result

    @_synchronized
    def diagnostics(self, *, conversation_id: str | None = None) -> dict:
        """Read bounded operator counters without exposing pending summary text."""
        conn = self.connection
        where = "WHERE conversation_id = ?" if conversation_id else ""
        params = (conversation_id,) if conversation_id else ()
        counts = self.counts(conversation_id=conversation_id)
        pending_nodes = conn.execute(
            """SELECT COUNT(*) FROM lcm_pending_summary_nodes AS n
               JOIN lcm_compaction_batches AS b ON b.batch_id = n.batch_id """
            + ("WHERE b.conversation_id = ?" if conversation_id else ""),
            params,
        ).fetchone()[0]
        oldest = conn.execute(
            "SELECT MIN(created_at) FROM lcm_compaction_batches "
            + where
            + (" AND " if where else " WHERE ")
            + "state IN ('pending', 'preparing', 'ready')",
            params,
        ).fetchone()[0]
        rejected = conn.execute(
            "SELECT rejected_reason FROM lcm_compaction_batches "
            + where
            + (" AND " if where else " WHERE ")
            + "state = 'rejected' ORDER BY updated_at DESC LIMIT 1",
            params,
        ).fetchone()
        failed = conn.execute(
            "SELECT last_error FROM lcm_compaction_batches "
            + where
            + (" AND " if where else " WHERE ")
            + "state = 'failed' ORDER BY updated_at DESC LIMIT 1",
            params,
        ).fetchone()
        return {
            **{f"{state}_batches": count for state, count in counts.items()},
            "pending_summaries": int(pending_nodes),
            "oldest_pending_age_seconds": (
                max(0.0, time.time() - float(oldest)) if oldest is not None else None
            ),
            "last_rejected_reason": rejected[0] if rejected else None,
            "last_error": failed[0] if failed else None,
        }

    @_synchronized
    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> AsyncCompactionStore:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()
