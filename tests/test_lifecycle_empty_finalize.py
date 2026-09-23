from hermes_lcm.dag import SummaryDAG
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.lifecycle_state import LifecycleStateStore
from hermes_lcm.store import MessageStore


def test_finalizing_empty_session_releases_current_without_creating_carry(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("empty", conversation_id="conversation")
        state = lifecycle.finalize_session("conversation", "empty", frontier_store_id=0)
        assert state.current_session_id is None
        assert state.current_frontier_store_id == 0
        assert state.last_finalized_session_id is None
        assert state.last_finalized_at is None
    finally:
        lifecycle.close()
        messages.close()


def test_empty_session_does_not_replace_prior_carry(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("source", conversation_id="conversation")
        frontier = messages.append(
            "source", {"role": "user", "content": "durable context"},
            conversation_id="conversation",
        )
        original = lifecycle.finalize_session("conversation", "source", frontier)
        assert original.last_finalized_session_id == "source"
        lifecycle.bind_session("empty", conversation_id="conversation")

        state = lifecycle.finalize_session("conversation", "empty", 0)
        assert state.current_session_id is None
        assert state.last_finalized_session_id == "source"
        assert state.last_finalized_frontier_store_id == frontier
        assert state.last_finalized_at == original.last_finalized_at
    finally:
        lifecycle.close()
        messages.close()


def test_zero_frontier_with_durable_message_still_finalizes(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("with-data", conversation_id="conversation")
        messages.append("with-data", {"role": "user", "content": "durable"})
        state = lifecycle.finalize_session("conversation", "with-data", 0)
        assert state.current_session_id is None
        assert state.last_finalized_session_id == "with-data"
        assert state.last_finalized_at is not None
    finally:
        lifecycle.close()
        messages.close()


def test_summary_only_session_still_finalizes(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    dag = SummaryDAG(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("summary-only", conversation_id="conversation")
        dag.connection.execute(
            "INSERT INTO summary_nodes(session_id, summary, created_at) VALUES (?, ?, 1)",
            ("summary-only", "durable summary"),
        )
        dag.connection.commit()
        state = lifecycle.finalize_session("conversation", "summary-only", 0)
        assert state.current_session_id is None
        assert state.last_finalized_session_id == "summary-only"
    finally:
        lifecycle.close()
        dag.close()
        messages.close()


def test_finalized_session_resume_restores_its_frontier_after_reopen(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("continuing", conversation_id="conversation")
        frontier = messages.append(
            "continuing", {"role": "user", "content": "compacted source"},
            conversation_id="conversation",
        )
        lifecycle.advance_frontier("conversation", "continuing", frontier)
        lifecycle.record_debt("conversation", kind="pending_leaf", size_estimate=400)
        finalized = lifecycle.finalize_session("conversation", "continuing", frontier)
        assert finalized.current_session_id is None
        assert finalized.last_finalized_frontier_store_id == frontier
    finally:
        lifecycle.close()
        messages.close()

    resumed = LifecycleStateStore(db_path)
    try:
        state = resumed.bind_session("continuing", conversation_id="conversation")
        assert state.current_frontier_store_id == frontier
        assert state.last_finalized_session_id is None
        assert state.last_finalized_frontier_store_id == 0
        assert state.last_finalized_at is None
        assert state.debt_kind is None
        assert state.debt_size_estimate == 0
        assert state.last_rollover_at is None
        assert resumed.bind_session("continuing", conversation_id="conversation").current_frontier_store_id == frontier
        assert resumed.bind_session("genuinely-new", conversation_id="conversation").current_frontier_store_id == 0
    finally:
        resumed.close()


def test_explicit_new_fence_prevents_same_id_frontier_restoration(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("outgoing", conversation_id="conversation")
        frontier = messages.append(
            "outgoing", {"role": "user", "content": "old compacted source"},
            conversation_id="conversation",
        )
        lifecycle.advance_frontier("conversation", "outgoing", frontier)
        lifecycle.finalize_session("conversation", "outgoing", frontier)
        lifecycle.clear_carry_for_explicit_new("outgoing")
        state = lifecycle.bind_session("outgoing", conversation_id="conversation")
        assert state.current_frontier_store_id == 0
    finally:
        lifecycle.close()
        messages.close()


def test_engine_resumes_finalized_frontier_after_gateway_restart(tmp_path):
    config = LCMConfig(database_path=str(tmp_path / "lcm.db"))
    hermes_home = str(tmp_path / "hermes")
    engine = LCMEngine(config=config, hermes_home=hermes_home)
    engine.on_session_start("continuing", platform="cli", conversation_id="conversation")
    try:
        frontier = engine._store.append(
            "continuing", {"role": "user", "content": "previously summarized"},
            conversation_id="conversation",
        )
        engine._last_compacted_store_id = frontier
        engine._persist_frontier_marker()
        engine.on_session_end("continuing", [])
        finalized = engine._lifecycle.get_by_conversation("conversation")
        assert finalized.current_session_id is None
        assert finalized.last_finalized_frontier_store_id == frontier
    finally:
        engine.shutdown()

    resumed = LCMEngine(config=config, hermes_home=hermes_home)
    try:
        resumed.on_session_start("continuing", platform="cli", conversation_id="conversation")
        state = resumed._lifecycle.get_by_conversation("conversation")
        assert state.current_frontier_store_id == frontier
        assert resumed._last_compacted_store_id == frontier
    finally:
        resumed.shutdown()


def test_empty_lifecycle_gc_probes_referenced_sessions_only(tmp_path):
    db_path = tmp_path / "lcm.db"
    messages = MessageStore(db_path)
    dag = SummaryDAG(db_path)
    lifecycle = LifecycleStateStore(db_path)
    try:
        messages.append("historical", {"role": "user", "content": "retain"})
        lifecycle.bind_session("empty", conversation_id="empty-conversation")
        statements = []
        lifecycle.connection.set_trace_callback(statements.append)
        assert lifecycle.prune_empty_sessions() == 1
        lifecycle.connection.set_trace_callback(None)
        normalized = [statement.upper() for statement in statements]
        assert any("SELECT 1 FROM MESSAGES WHERE SESSION_ID" in sql for sql in normalized)
        assert any("SELECT 1 FROM SUMMARY_NODES WHERE SESSION_ID" in sql for sql in normalized)
        assert not any("SELECT DISTINCT SESSION_ID" in sql for sql in normalized)
        retained = messages.connection.execute(
            "SELECT COUNT(*) FROM messages WHERE session_id = ?", ("historical",)
        ).fetchone()[0]
        assert retained == 1
    finally:
        lifecycle.close()
        dag.close()
        messages.close()
