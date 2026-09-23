from hermes_lcm.dag import SummaryDAG
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
