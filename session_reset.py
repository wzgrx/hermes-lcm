"""Explicit host /new boundary for durable LCM summary carry.

The old DAG and raw transcript stay searchable. Clearing the lifecycle carry
pointer prevents an old summary frontier from reappearing in the new active
session, without destructive graph surgery on a live WAL database.
"""

from __future__ import annotations

import logging
from pathlib import Path

from .lifecycle_state import LifecycleStateStore

logger = logging.getLogger(__name__)


def reset_explicit_new_carry(
    db_path: str | Path,
    old_session_id: str,
    *,
    conversation_id: str = "",
) -> dict:
    """Clear only the conversation proven to own the outgoing session."""
    path = Path(db_path)
    if not old_session_id or not path.is_file():
        return {"found": False, "reason": "missing_session_or_database"}

    lifecycle = LifecycleStateStore(path)
    try:
        state = lifecycle.get_by_session(old_session_id)
        if state is None or (conversation_id and state.conversation_id != conversation_id):
            return {"found": False, "reason": "session_not_in_conversation"}
        updated = lifecycle.clear_carry_for_explicit_new(old_session_id)
        if updated is None:
            return {"found": False, "reason": "stale_session"}
        logger.info(
            "LCM explicit /new cleared carry for conversation=%s old_session=%s",
            updated.conversation_id,
            old_session_id,
        )
        return {"found": True, "conversation_id": updated.conversation_id}
    finally:
        lifecycle.close()
