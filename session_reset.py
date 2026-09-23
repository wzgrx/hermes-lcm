"""Explicit Hermes /new forgets automatic summary carry for one conversation.

The outgoing host session proves ownership. Raw messages remain available for
explicit recall; summary nodes for the current/finalized segment are removed at
all depths, so a later compression boundary cannot accidentally inherit them.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .dag import SummaryDAG
from .lifecycle_state import LifecycleStateStore
from .vector_store import VectorStore

logger = logging.getLogger(__name__)


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
        if state is None or (conversation_id and state.conversation_id != conversation_id):
            return {"found": False, "reason": "session_not_in_conversation"}
        owned_sessions = sorted({
            session_id for session_id in (
                state.current_session_id, state.last_finalized_session_id
            ) if session_id
        })
        updated = lifecycle.clear_carry_for_explicit_new(old_session_id)
        if updated is None:
            return {"found": False, "reason": "stale_session"}

        dag = SummaryDAG(path)
        try:
            def purge_embeddings(node_ids: list[int]) -> None:
                # Summary nodes and vector rows share this SQLite database.
                # The delete callback is bounded to 256 ids by SummaryDAG.
                try:
                    VectorStore.purge_embedding_batch_on_connection(dag.connection, node_ids)
                    dag.connection.commit()
                except Exception:
                    dag.connection.rollback()
                    logger.warning("LCM explicit /new embedding cleanup failed", exc_info=True)

            deleted = sum(
                dag.delete_session_nodes(session_id, on_deleted_batch=purge_embeddings)
                for session_id in owned_sessions
            )
        finally:
            dag.close()
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
