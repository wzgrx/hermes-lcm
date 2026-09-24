"""Conservative, explicit off-turn preparation of one stable raw leaf."""

from __future__ import annotations

import json
import threading

from hermes_lcm.config import LCMConfig
from hermes_lcm.async_compaction_store import AsyncCompactionStore
from hermes_lcm.engine import LCMEngine


def _engine_with_stable_backlog(tmp_path, *, advance_anchor=True):
    config = LCMConfig(
        database_path=str(tmp_path / "prepared.db"),
        async_background_compaction_enabled=True,
        summary_model="explicit-summary-model",
        fresh_tail_count=2,
        leaf_chunk_tokens=20,
        threshold_full_sweep_enabled=False,
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
    if advance_anchor:
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
        async_status = engine.get_async_compaction_status()
        assert async_status["ready_batches"] == 1
        assert async_status["pending_summaries"] == 1
        assert async_status["worker_enabled"] is False
        tool_status = json.loads(engine.handle_tool_call("lcm_status", {}))
        assert tool_status["async_compaction"]["ready_batches"] == 1
        doctor = json.loads(engine.handle_tool_call("lcm_doctor", {}))
        async_check = next(
            check for check in doctor["checks"] if check["check"] == "async_compaction"
        )
        assert async_check["status"] == "pass"
        assert async_check["detail"]["ready_batches"] == 1
        assert (
            engine.prepare_background_compaction_once(host_config={})["batch_id"]
            == batch["batch_id"]
        )
        assert len(calls) == 1
    finally:
        engine.shutdown()


def test_first_leaf_skips_system_anchor_and_promotes_atomically(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("First leaf summary without the system anchor.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch is not None and batch["state"] == "ready"
        assert batch["frontier_start_store_id"] == 0
        assert batch["source_frontier_start_store_id"] == 1
        assert json.loads(batch["source_ids_json"])[0] == 2
        assert engine._dag.get_session_node_count("session-1") == 0
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})

        def unexpected_provider(**_kwargs):
            raise AssertionError("first foreground leaf should use prepared work")

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", unexpected_provider,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        output = engine.compress(messages, current_tokens=900)
        assert engine._async_compaction_store.get_batch(batch["batch_id"])["state"] == "promoted"
        assert engine._lifecycle.get_by_conversation("conversation-1").current_frontier_store_id == batch["frontier_end_store_id"]
        assert engine._dag.get_session_node_count("session-1") == 1
        assert engine._store.get_session_messages("session-1")[0]["role"] == "system"
        assert any("First leaf summary" in str(msg.get("content")) for msg in output)
    finally:
        engine.shutdown()


def test_first_leaf_rejects_anchor_reclassified_as_raw_source(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared before anchor rewrite.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        engine._store._conn.execute(
            "UPDATE messages SET role = 'user' WHERE store_id = 1"
        )
        engine._store._conn.commit()
        store = engine._async_compaction_store
        assert store.source_snapshot_matches(batch["batch_id"]) is False
        result = store.promote_batch(
            batch["batch_id"],
            live_policy_fingerprint=batch["policy_fingerprint"],
            live_summary_route_fingerprint=batch["summary_route_fingerprint"],
            max_publishable_store_id=batch["frontier_end_store_id"],
        )
        assert result.promoted is False
        assert result.reason == "source_identity_mismatch"
        assert engine._dag.get_session_node_count("session-1") == 0
        assert engine._lifecycle.get_by_conversation("conversation-1").current_frontier_store_id == 0
    finally:
        engine.shutdown()


def test_ingest_schedules_private_background_preparation(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    engine._config.async_background_compaction_worker_enabled = True
    provider_threads = []
    try:
        monkeypatch.setattr(
            "hermes_cli.config.load_config_readonly", lambda: {},
        )

        def summarize(**_kwargs):
            provider_threads.append(threading.current_thread().name)
            return "Prepared on the dedicated background worker.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        engine.ingest([{"role": "user", "content": "a new turn after the stable backlog"}])
        from hermes_lcm.engine import _ASYNC_COMPACTION_SCHEDULER

        assert _ASYNC_COMPACTION_SCHEDULER.drain_owner(
            engine._rollup_maintenance_owner, timeout=10,
        )
        assert provider_threads
        assert all(name != threading.main_thread().name for name in provider_threads)
        assert engine.get_async_compaction_status()["worker_enabled"] is True
        assert engine._async_compaction_store.counts()["ready"] == 1
        assert engine._dag.get_session_node_count("session-1") == 0
    finally:
        engine.shutdown(wait_for_background_work=True)


def test_background_provider_survives_foreground_engine_retirement(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path)
    engine._config.async_background_compaction_worker_enabled = True
    entered = threading.Event()
    release = threading.Event()
    from hermes_lcm.engine import _ASYNC_COMPACTION_SCHEDULER

    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})

    def summarize(**_kwargs):
        entered.set()
        assert release.wait(10)
        return "Prepared after foreground engine retirement.", 1

    monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
    owner = engine._rollup_maintenance_owner
    db_path = engine._storage_db_path
    try:
        assert engine._schedule_background_compaction() is True
        assert entered.wait(10)
        assert engine._async_compaction_store.counts()["preparing"] == 1
        assert engine._dag.get_session_node_count("session-1") == 0
        engine.shutdown(wait_for_background_work=False)
        release.set()
        assert _ASYNC_COMPACTION_SCHEDULER.drain_owner(owner, timeout=10)
        with AsyncCompactionStore(db_path, enabled=True) as reopened:
            assert reopened.counts()["ready"] == 1
    finally:
        release.set()
        _ASYNC_COMPACTION_SCHEDULER.drain_owner(owner, timeout=10)
        engine.shutdown(wait_for_background_work=True)


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
        assert engine.get_async_compaction_status()["enabled"] is False
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


def test_foreground_compress_consumes_ready_leaf_without_provider_call(
    tmp_path, monkeypatch
):
    engine = _engine_with_stable_backlog(tmp_path)
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared summary of old turns.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        monkeypatch.setattr(
            "hermes_cli.config.load_config_readonly",
            lambda: {},
        )

        def unexpected_provider(**_kwargs):
            raise AssertionError("foreground provider call after ready leaf")

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            unexpected_provider,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        output = engine.compress(messages, current_tokens=900)

        assert (
            engine._async_compaction_store.get_batch(batch["batch_id"])["state"]
            == "promoted"
        )
        assert engine._dag.get_session_node_count("session-1") == 1
        assert (
            engine._lifecycle.get_by_conversation(
                "conversation-1"
            ).current_frontier_store_id
            == batch["frontier_end_store_id"]
        )
        assert any(
            "Prepared summary of old turns." in str(msg.get("content"))
            for msg in output
        )
    finally:
        engine.shutdown()


def test_foreground_falls_back_when_summary_route_changes(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path)
    calls = []
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared summary.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        monkeypatch.setattr(
            "hermes_cli.config.load_config_readonly",
            lambda: {"auxiliary": {"compression": {"model": "new-route"}}},
        )

        def foreground_summary(**kwargs):
            calls.append(kwargs)
            return "Foreground summary after route change.", 1

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            foreground_summary,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        output = engine.compress(messages, current_tokens=900)

        assert len(calls) == 1
        assert (
            engine._async_compaction_store.get_batch(batch["batch_id"])["state"]
            == "rejected"
        )
        assert engine._dag.get_session_node_count("session-1") == 1
        assert any(
            "Foreground summary after route change." in str(msg.get("content"))
            for msg in output
        )
    finally:
        engine.shutdown()


def test_foreground_rejects_rewritten_source_then_summarizes_current_rows(
    tmp_path, monkeypatch
):
    engine = _engine_with_stable_backlog(tmp_path)
    calls = []
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared before rewrite.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        engine._store._conn.execute(
            "UPDATE messages SET content = 'reconciled source' WHERE store_id = 2"
        )
        engine._store._conn.commit()
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})

        def foreground_summary(**kwargs):
            calls.append(kwargs)
            return "Current-row foreground summary.", 1

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            foreground_summary,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        output = engine.compress(messages, current_tokens=900)

        assert len(calls) == 1
        assert (
            engine._async_compaction_store.get_batch(batch["batch_id"])["state"]
            == "rejected"
        )
        assert engine._dag.get_session_node_count("session-1") == 1
        assert any(
            "Current-row foreground summary." in str(msg.get("content"))
            for msg in output
        )
    finally:
        engine.shutdown()
