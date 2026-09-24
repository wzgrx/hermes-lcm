"""Conservative, explicit off-turn preparation of one stable raw leaf."""

from __future__ import annotations

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


def _engine_with_stable_backlog(tmp_path):
    config = LCMConfig(
        database_path=str(tmp_path / "prepared.db"),
        async_background_compaction_enabled=True,
        summary_model="explicit-summary-model",
        fresh_tail_count=2,
        leaf_chunk_tokens=20,
    )
    engine = LCMEngine(config=config)
    engine.on_session_start(
        "session-1",
        conversation_id="conversation-1",
        platform="test",
        context_length=1000,
    )
    anchor_id = engine._store.append(
        "session-1",
        {"role": "system", "content": "system anchor"},
        conversation_id="conversation-1",
    )
    engine._lifecycle.advance_frontier("conversation-1", "session-1", anchor_id)
    for index in range(10):
        engine._store.append(
            "session-1",
            {"role": "user", "content": f"stable source {index} " + ("x " * 20)},
            conversation_id="conversation-1",
        )
    return engine


def test_manual_preparation_calls_provider_outside_sqlite_transaction(
    tmp_path, monkeypatch
):
    engine = _engine_with_stable_backlog(tmp_path)
    calls = []
    try:

        def summarize(**kwargs):
            calls.append(kwargs)
            assert engine._async_compaction_store.connection.in_transaction is False
            assert engine._dag.get_session_node_count("session-1") == 0
            return "Prepared summary preserving the old source.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch is not None
        assert batch["state"] == "ready"
        assert len(calls) == 1
        assert engine._dag.get_session_node_count("session-1") == 0
        assert (
            engine._lifecycle.get_by_conversation(
                "conversation-1"
            ).current_frontier_store_id
            == 1
        )
        assert (
            engine._async_compaction_store.connection.execute(
                "SELECT COUNT(*) FROM lcm_pending_summary_nodes"
            ).fetchone()[0]
            == 1
        )
        assert (
            engine.prepare_background_compaction_once(host_config={})["batch_id"]
            == batch["batch_id"]
        )
        assert len(calls) == 1
    finally:
        engine.shutdown()


def test_source_rewrite_during_summary_preparation_fails_closed(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path)
    try:

        def summarize(**_kwargs):
            engine._store._conn.execute(
                "UPDATE messages SET content = 'rewritten' WHERE store_id = 2"
            )
            engine._store._conn.commit()
            return "Prepared but stale summary.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch is not None
        assert batch["state"] == "failed"
        assert batch["last_error"] == "ValueError"
        assert batch["failure_count"] == 1
        assert engine._dag.get_session_node_count("session-1") == 0
        assert (
            engine._lifecycle.get_by_conversation(
                "conversation-1"
            ).current_frontier_store_id
            == 1
        )
        assert engine.prepare_background_compaction_once(host_config={}) is None
        assert engine._async_compaction_store.counts()["failed"] == 1
    finally:
        engine.shutdown()


def test_disabled_preparation_is_inert(tmp_path):
    engine = LCMEngine(config=LCMConfig(database_path=str(tmp_path / "disabled.db")))
    try:
        assert engine.prepare_background_compaction_once(host_config={}) is None
        assert engine._async_compaction_store is None
    finally:
        engine.shutdown()


def test_summary_failure_records_type_only_and_enforces_backoff(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path)
    calls = []
    try:

        def summarize(**_kwargs):
            calls.append(True)
            raise RuntimeError("provider detail TOKEN=private")

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "failed"
        assert batch["last_error"] == "RuntimeError"
        assert "TOKEN" not in str(batch)
        assert engine.prepare_background_compaction_once(host_config={}) is None
        assert len(calls) == 1
    finally:
        engine.shutdown()
