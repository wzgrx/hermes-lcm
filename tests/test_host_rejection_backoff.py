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


def test_preflight_honors_host_rejection_but_preserves_emergency_and_cleanup(tmp_path):
    engine = _engine(tmp_path)
    try:
        engine.record_rejected_compaction()
        assert engine._mark_preflight_compression_requested(
            operation="compact", reason="eligible_leaf", trigger="threshold",
        ) is False
        assert engine.last_compression_was_noop is True
        assert "backoff" in engine.last_compression_noop_reason

        assert engine._mark_preflight_compression_requested(
            operation="compact", reason="overflow_recovery",
        ) is True
        assert engine._mark_preflight_compression_requested(
            operation="compact", reason="eligible_leaf", trigger="critical_pressure",
        ) is True
        assert engine._mark_preflight_compression_requested(
            operation="sanitize", reason="replay_cleanup",
        ) is True
    finally:
        engine.shutdown()


def test_rejected_eligible_preflight_does_not_reenter_until_backoff_expires(tmp_path):
    engine = _engine(tmp_path)
    engine._config.fresh_tail_count = 4
    engine._config.leaf_chunk_tokens = 100
    engine.threshold_tokens = 100
    messages = [
        {"role": "system", "content": "system"},
        {"role": "user", "content": "old backlog " + "compressible segment " * 100},
        {"role": "assistant", "content": "old answer " + "detail " * 100},
        {"role": "user", "content": "fresh request"},
        {"role": "assistant", "content": "fresh answer"},
        {"role": "user", "content": "new request"},
    ]
    try:
        assert engine.should_compress_preflight(messages) is True
        engine.record_rejected_compaction()
        assert engine.should_compress_preflight(messages) is False
        assert engine.last_compression_was_noop is True
        assert "backoff" in engine.last_compression_noop_reason

        _HOST_REJECTION_BACKOFF_BY_SESSION[engine._host_rejection_key()] = (
            time.monotonic() - 1, "would_grow",
        )
        assert engine.should_compress_preflight(messages) is True
    finally:
        engine.shutdown()
