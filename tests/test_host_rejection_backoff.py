"""Host rejection feedback stops repeated automatic LCM attempts (upstream #582)."""

import time

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine, _HOST_REJECTION_BACKOFF_BY_SESSION


def _engine(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(database_path=str(tmp_path / "lcm.db")),
        hermes_home=str(tmp_path),
    )
    engine.on_session_start("session-a", platform="telegram", conversation_id="chat-a")
    return engine


def test_would_grow_feedback_defers_automatic_retry_but_expires(tmp_path):
    engine = _engine(tmp_path)
    try:
        assert engine._automatic_compression_blocked() is False
        engine.record_rejected_compaction()
        assert engine._automatic_compression_blocked() is True
        # Hermes ignore_cooldown is for provider cooldown, not growth refusal.
        assert engine._automatic_compression_blocked(ignore_cooldown=True) is True
        assert engine._compression_block_reason().startswith("structural_backoff:")
        status = engine.get_status()
        assert status["host_rejection_backoff_seconds"] > 0
        assert status["host_rejection_reason"] == "would_grow"

        _HOST_REJECTION_BACKOFF_BY_SESSION[engine._host_rejection_key()] = (
            time.monotonic() - 1, "would_grow"
        )
        assert engine._automatic_compression_blocked() is False
        assert engine._compression_block_reason() is None
        assert engine.get_status()["host_rejection_reason"] == ""
    finally:
        engine.shutdown()


def test_structural_noop_feedback_is_bounded_and_session_scoped(tmp_path):
    engine = _engine(tmp_path)
    try:
        engine._record_structural_no_op("unchanged transcript")
        assert engine._automatic_compression_blocked() is True
        assert "no_progress" in engine.get_status()["host_rejection_reason"]

        engine.on_session_start("session-b", platform="telegram", conversation_id="chat-b")
        assert engine._automatic_compression_blocked() is False
        assert engine.get_status()["host_rejection_backoff_seconds"] == 0
    finally:
        engine.shutdown()


def test_compaction_reset_clears_host_rejection_feedback(tmp_path):
    engine = _engine(tmp_path)
    try:
        engine.record_rejected_compaction()
        engine._reset_session_scoped_runtime_state()
        assert engine._automatic_compression_blocked() is False
        assert engine.get_status()["host_rejection_reason"] == ""
    finally:
        engine.shutdown()


def test_committed_boundary_lifts_backoff(tmp_path):
    engine = _engine(tmp_path)
    try:
        engine.record_rejected_compaction()
        assert engine._automatic_compression_blocked() is True
        engine.record_completed_compaction(used_fallback=False, feasibility_skip=False)
        assert engine._verify_compaction_cleared_threshold is True
        assert engine._automatic_compression_blocked() is False
        assert engine.get_status()["host_rejection_reason"] == ""
    finally:
        engine.shutdown()


def test_recreated_agent_clone_keeps_same_session_backoff(tmp_path):
    engine = _engine(tmp_path)
    clone = engine.clone_for_agent()
    try:
        engine.record_rejected_compaction()
        clone.on_session_start("session-a", platform="telegram", conversation_id="chat-a")
        assert clone._automatic_compression_blocked() is True
        assert clone.get_status()["host_rejection_reason"] == "would_grow"

        clone.record_completed_compaction()
        assert engine._automatic_compression_blocked() is False
    finally:
        clone.shutdown()
        engine.shutdown()


def test_shared_backoff_does_not_cross_database_profiles(tmp_path):
    engine = _engine(tmp_path / "profile-a")
    other = _engine(tmp_path / "profile-b")
    try:
        engine.record_rejected_compaction()
        assert other._automatic_compression_blocked() is False
    finally:
        engine.shutdown()
        other.shutdown()
