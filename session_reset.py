"""Explicit Hermes /new forgets automatic summary carry for one conversation.

The outgoing host session proves ownership. Raw messages remain available for
explicit recall; summary nodes for the current/finalized segment are removed at
all depths, so a later compression boundary cannot accidentally inherit them.
"""

from __future__ import annotations

from contextlib import closing
import logging
from pathlib import Path
import sqlite3
import threading
import time

from .dag import SummaryDAG
from .lifecycle_state import LifecycleStateStore
from .vector_store import VectorStore

logger = logging.getLogger(__name__)


class CliNewSessionPairing:
    """Pair CLI finalize/reset hooks only when the host proves a /new rotation.

    Hermes CLI finalizes the outgoing session on the same thread immediately
    before its reset hook, but the latter currently names only the new session.
    A thread-local hint alone is insufficient: an unrelated finalize could be
    followed by a reset. Verify both rows in the host's state.db before using
    the outgoing ID to forget any LCM summary carry.
    """

    _MAX_AGE_SECONDS = 120.0

    def __init__(self) -> None:
        self._local = threading.local()

    def note_finalization(self, **payload) -> None:
        self._local.pending = None
        if (
            str(payload.get("platform") or "").lower() != "cli"
            or str(payload.get("reason") or "") != "session_boundary"
        ):
            return
        old_session_id = str(payload.get("session_id") or "")
        if old_session_id:
            self._local.pending = (old_session_id, time.monotonic())

    def consume_verified_old_session(
        self, *, new_session_id: str, hermes_home: str
    ) -> str:
        pending = getattr(self._local, "pending", None)
        self._local.pending = None
        if not pending or not new_session_id or not hermes_home:
            return ""
        old_session_id, noted_at = pending
        if (
            old_session_id == new_session_id
            or time.monotonic() - noted_at > self._MAX_AGE_SECONDS
        ):
            return ""
        path = Path(hermes_home).expanduser().resolve() / "state.db"
        if not path.is_file():
            return ""
        try:
            with closing(
                sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=1.0)
            ) as conn:
                rows = conn.execute(
                    "SELECT id, source, profile_name, started_at, ended_at, end_reason "
                    "FROM sessions WHERE id IN (?, ?)",
                    (old_session_id, new_session_id),
                ).fetchall()
        except (OSError, sqlite3.Error):
            return ""
        if len(rows) != 2:
            return ""
        sessions = {row[0]: row for row in rows}
        old, new = sessions.get(old_session_id), sessions.get(new_session_id)
        if old is None or new is None:
            return ""
        if (
            old[1] != new[1]
            or old[2] != new[2]
            or old[5] != "new_session"
            or old[4] is None
            or new[4] is not None
        ):
            return ""
        try:
            elapsed = float(new[3]) - float(old[4])
        except (TypeError, ValueError):
            return ""
        if not -1.0 <= elapsed <= self._MAX_AGE_SECONDS:
            return ""
        return old_session_id


def reset_explicit_new_carry(
    db_path: str | Path,
    old_session_id: str,
    *,
    conversation_id: str = "",
) -> dict:
    """Forget only the conversation proven to own the outgoing session."""
    path = Path(db_path)
    if not old_session_id or not path.is_file():
        return {"found": False, "reason": "missing_session_or_database"}

    lifecycle = LifecycleStateStore(path)
    try:
        state = lifecycle.get_by_session(old_session_id)
        if state is None or (
            conversation_id and state.conversation_id != conversation_id
        ):
            return {"found": False, "reason": "session_not_in_conversation"}
        owned_sessions = sorted(
            {
                session_id
                for session_id in (
                    state.current_session_id,
                    state.last_finalized_session_id,
                )
                if session_id
            }
        )
        dag = SummaryDAG(path)
        try:

            def purge_embeddings(node_ids: list[int]) -> None:
                # Summary nodes and vector rows share this SQLite database.
                # The delete callback is bounded to 256 ids by SummaryDAG.
                try:
                    VectorStore.purge_embedding_batch_on_connection(
                        dag.connection, node_ids
                    )
                    dag.connection.commit()
                except Exception:
                    dag.connection.rollback()
                    logger.warning(
                        "LCM explicit /new embedding cleanup failed", exc_info=True
                    )

            deleted = sum(
                dag.delete_session_nodes(session_id, on_deleted_batch=purge_embeddings)
                for session_id in owned_sessions
            )
        finally:
            dag.close()
        # Node deletion is idempotent, but clearing the carry pointer is not
        # reversible: the outgoing session would no longer be discoverable on
        # a retry. Keep ownership intact until every summary batch is removed.
        updated = lifecycle.clear_carry_for_explicit_new(old_session_id)
        if updated is None:
            return {"found": False, "reason": "stale_session"}
        logger.info(
            "LCM explicit /new forgot summary carry for conversation=%s sessions=%d nodes=%d",
            updated.conversation_id,
            len(owned_sessions),
            deleted,
        )
        return {
            "found": True,
            "conversation_id": updated.conversation_id,
            "deleted_nodes": deleted,
        }
    finally:
        lifecycle.close()
