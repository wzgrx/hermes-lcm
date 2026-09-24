"""Conservative, explicit off-turn preparation of one stable raw leaf."""

from __future__ import annotations

import copy
import json
import multiprocessing
import queue
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

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


def _prepare_in_process(config, ready, release, provider_release, events, results):
    import hermes_lcm.engine as engine_module

    engine = None
    try:
        def summarize(**_kwargs):
            events.put("provider")
            if not provider_release.wait(15):
                raise TimeoutError("provider release gate timed out")
            return "Cross-process prepared summary.", 1

        engine_module.summarize_with_escalation = summarize
        engine = LCMEngine(config=config)
        engine.on_session_start(
            "session-1", conversation_id="conversation-1",
            platform="test", context_length=1000,
        )
        ready.set()
        if not release.wait(15):
            raise TimeoutError("preparation start gate timed out")
        batch = engine.prepare_background_compaction_once(host_config={})
        results.put(("ok", batch["state"] if batch else None))
    except BaseException as exc:
        results.put(("error", type(exc).__name__))
    finally:
        if engine is not None:
            engine.shutdown()


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
        next_batch = engine.prepare_background_compaction_once(host_config={})
        assert next_batch["state"] == "ready"
        assert next_batch["batch_id"] != batch["batch_id"]
        assert next_batch["frontier_start_store_id"] == batch["frontier_end_store_id"]
        third_batch = engine.prepare_background_compaction_once(host_config={})
        fourth_batch = engine.prepare_background_compaction_once(host_config={})
        assert third_batch["frontier_start_store_id"] == next_batch["frontier_end_store_id"]
        assert fourth_batch["frontier_start_store_id"] == third_batch["frontier_end_store_id"]
        assert engine.prepare_background_compaction_once(host_config={})["batch_id"] == fourth_batch["batch_id"]
        assert len(calls) == 4
        assert engine._dag.get_session_node_count("session-1") == 0
    finally:
        engine.shutdown()


def test_pending_summary_text_is_absent_from_active_search(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path)
    marker = "PendingOnlySummaryMarker9471"
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: (marker, 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        grep = json.loads(engine.handle_tool_call("lcm_grep", {"query": marker}))
        assert grep["total_results"] == 0
        assert grep["results"] == []
        assert engine._dag.get_session_node_count("session-1") == 0
        assert engine.get_async_compaction_status()["ready_batches"] == 1
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
        assert engine._async_compaction_store.counts()["ready"] == 4
        assert engine._dag.get_session_node_count("session-1") == 0
    finally:
        engine.shutdown(wait_for_background_work=True)


def test_background_preparation_honors_sqlite_integrity_gate(
    tmp_path, monkeypatch,
):
    import hermes_lcm.engine as engine_module

    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    engine._config.async_background_compaction_worker_enabled = True
    checks = []
    from hermes_lcm.engine import _ASYNC_COMPACTION_SCHEDULER

    monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})
    monkeypatch.setattr(
        engine_module, "inspect_orphaned_sqlite_handles",
        lambda path: checks.append(path) or {"status": "fail"},
    )

    def unexpected_summary(**_kwargs):
        raise AssertionError("provider ran after a failed SQLite preflight")

    monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", unexpected_summary)
    try:
        engine.ingest([{"role": "user", "content": "schedule after old backlog"}])
        assert _ASYNC_COMPACTION_SCHEDULER.drain_owner(
            engine._rollup_maintenance_owner, timeout=10,
        )
        assert checks == [engine._storage_db_path.resolve()]
        assert engine._async_compaction_store.counts()["ready"] == 0
        assert str(engine._storage_db_path.resolve()) in engine_module._ROLLUP_INTEGRITY_RETRY_UNTIL

        monkeypatch.setattr(
            engine_module, "inspect_orphaned_sqlite_handles",
            lambda _path: {"status": "pass"},
        )
        engine_module._ROLLUP_INTEGRITY_RETRY_UNTIL.pop(str(engine._storage_db_path.resolve()))
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared after integrity gate recovered.", 1),
        )
        assert engine._schedule_background_compaction()
        assert _ASYNC_COMPACTION_SCHEDULER.drain_owner(
            engine._rollup_maintenance_owner, timeout=10,
        )
        assert engine._async_compaction_store.counts()["ready"] == 4
    finally:
        engine_module._ROLLUP_INTEGRITY_RETRY_UNTIL.pop(
            str(engine._storage_db_path.resolve()), None,
        )
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
            assert reopened.counts()["ready"] == 4
    finally:
        release.set()
        _ASYNC_COMPACTION_SCHEDULER.drain_owner(owner, timeout=10)
        engine.shutdown(wait_for_background_work=True)


@pytest.mark.parametrize("run", range(5))
def test_competing_preparers_claim_one_frontier_and_call_provider_once(
    tmp_path, monkeypatch, run,
):
    first = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    second = LCMEngine(config=copy.deepcopy(first._config))
    second.on_session_start(
        "session-1", conversation_id="conversation-1",
        platform="test", context_length=1000,
    )
    barrier = threading.Barrier(2)
    calls = []
    original_create = AsyncCompactionStore.create_batch
    try:
        def competing_create(store, **kwargs):
            barrier.wait(timeout=10)
            return original_create(store, **kwargs)

        monkeypatch.setattr(AsyncCompactionStore, "create_batch", competing_create)

        def summarize(**_kwargs):
            calls.append(True)
            return f"One summary for race {run}.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [
                pool.submit(engine.prepare_background_compaction_once, host_config={})
                for engine in (first, second)
            ]
            results = [future.result(timeout=15) for future in futures]
        assert all(result is not None for result in results)
        assert len(calls) == 1
        assert first._async_compaction_store.counts()["ready"] == 1
        assert first._dag.get_session_node_count("session-1") == 0
    finally:
        second.shutdown()
        first.shutdown()


@pytest.mark.skipif("fork" not in multiprocessing.get_all_start_methods(), reason="fork unavailable")
def test_two_processes_prepare_one_frontier_with_one_provider_call(tmp_path):
    context = multiprocessing.get_context("fork")
    for attempt in range(3):
        case_dir = tmp_path / str(attempt)
        case_dir.mkdir()
        seed = _engine_with_stable_backlog(case_dir, advance_anchor=False)
        config = copy.deepcopy(seed._config)
        seed.shutdown()
        ready = [context.Event(), context.Event()]
        release = context.Event()
        provider_release = context.Event()
        events = context.Queue()
        results = context.Queue()
        workers = [
            context.Process(
                target=_prepare_in_process,
                args=(config, gate, release, provider_release, events, results),
            )
            for gate in ready
        ]
        for worker in workers:
            worker.start()
        try:
            assert all(gate.wait(15) for gate in ready)
            release.set()
            assert events.get(timeout=15) == "provider"
            assert results.get(timeout=15) in {("ok", "pending"), ("ok", "preparing")}
            provider_release.set()
            assert results.get(timeout=15) == ("ok", "ready")
            for worker in workers:
                worker.join(15)
                assert worker.exitcode == 0
            with pytest.raises(queue.Empty):
                events.get(timeout=0.1)
            with AsyncCompactionStore(config.database_path, enabled=True) as observer:
                assert observer.counts()["ready"] == 1
                assert observer.counts()["preparing"] == 0
                assert observer.connection.execute(
                    "SELECT COUNT(*) FROM summary_nodes"
                ).fetchone()[0] == 0
        finally:
            provider_release.set()
            for worker in workers:
                if worker.is_alive():
                    worker.terminate()
                    worker.join(5)
            events.close()
            results.close()


@pytest.mark.parametrize("force_overflow", [False, True])
def test_foreground_winner_fences_inflight_background_provider(
    tmp_path, monkeypatch, force_overflow,
):
    foreground = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    background = LCMEngine(config=copy.deepcopy(foreground._config))
    background.on_session_start(
        "session-1", conversation_id="conversation-1",
        platform="test", context_length=1000,
    )
    entered = threading.Event()
    release = threading.Event()
    try:
        if force_overflow:
            monkeypatch.setattr(
                foreground, "_should_force_overflow_recovery", lambda **_kwargs: True,
            )
        def summarize(**_kwargs):
            if threading.current_thread() is threading.main_thread():
                return "Foreground winner summary.", 1
            entered.set()
            assert release.wait(10)
            return "Late background summary.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                background.prepare_background_compaction_once, host_config={},
            )
            assert entered.wait(10)
            messages = [
                foreground._store.to_openai_msg(row)
                for row in foreground._store.get_session_messages("session-1")
            ]
            foreground.compress(messages, current_tokens=900)
            release.set()
            batch = future.result(timeout=10)
        assert batch["state"] == "failed"
        assert background._async_compaction_store.counts()["ready"] == 0
        assert foreground._dag.get_session_node_count("session-1") == 1
        assert foreground._lifecycle.get_by_conversation("conversation-1").current_frontier_store_id > 0
    finally:
        release.set()
        background.shutdown()
        foreground.shutdown()


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


def test_session_reset_during_provider_cannot_ready_old_batch(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path)
    try:
        def summarize(**_kwargs):
            engine._lifecycle.bind_session(
                "session-2", conversation_id="conversation-1",
            )
            return "Summary produced after a session reset.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "failed"
        assert batch["last_error"] == "ValueError"
        assert engine._dag.get_session_node_count("session-1") == 0
        assert engine._async_compaction_store.counts()["ready"] == 0
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


def test_background_failure_backoff_does_not_block_foreground_compaction(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path)
    try:
        def failed_summary(**_kwargs):
            raise RuntimeError("background provider unavailable")

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", failed_summary,
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "failed"
        assert engine.prepare_background_compaction_once(host_config={}) is None
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Foreground recovery summary.", 1),
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        result = engine.compress(messages, current_tokens=900)
        assert engine._dag.get_session_node_count("session-1") == 1
        assert any("Foreground recovery summary." in str(msg.get("content")) for msg in result)
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


def test_two_prepared_leaves_publish_in_frontier_order(tmp_path, monkeypatch):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    calls = []
    try:
        def summarize(**_kwargs):
            calls.append(True)
            return f"Prepared leaf {len(calls)}.", 1

        monkeypatch.setattr("hermes_lcm.engine.summarize_with_escalation", summarize)
        first = engine.prepare_background_compaction_once(host_config={})
        second = engine.prepare_background_compaction_once(host_config={})
        assert first["state"] == second["state"] == "ready"
        assert second["frontier_start_store_id"] == first["frontier_end_store_id"]
        assert len(calls) == 2
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})

        def unexpected_provider(**_kwargs):
            raise AssertionError("both foreground leaves must reuse staged summaries")

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", unexpected_provider,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        after_first = engine.compress(messages, current_tokens=900)
        store = engine._async_compaction_store
        assert store.get_batch(first["batch_id"])["state"] == "promoted"
        assert store.get_batch(second["batch_id"])["state"] == "ready"
        assert engine._lifecycle.get_by_conversation("conversation-1").current_frontier_store_id == first["frontier_end_store_id"]
        after_second = engine.compress(after_first, current_tokens=900)
        assert store.get_batch(second["batch_id"])["state"] == "promoted"
        assert engine._lifecycle.get_by_conversation("conversation-1").current_frontier_store_id == second["frontier_end_store_id"]
        assert any("Prepared leaf 2." in str(msg.get("content")) for msg in after_second)
    finally:
        engine.shutdown()


def test_overflow_reuses_ready_leaf_only_when_it_covers_all_old_raw(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    engine._config.fresh_tail_count = 9
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Complete old-prefix summary.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        assert len(json.loads(batch["source_ids_json"])) == 1
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})
        monkeypatch.setattr(
            engine, "_should_force_overflow_recovery", lambda **_kwargs: True,
        )

        def unexpected_provider(**_kwargs):
            raise AssertionError("complete prepared prefix should avoid emergency provider")

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", unexpected_provider,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        engine.compress(messages, current_tokens=900)
        assert engine._async_compaction_store.get_batch(batch["batch_id"])["state"] == "promoted"
    finally:
        engine.shutdown()


def test_overflow_keeps_full_foreground_summary_for_partial_ready_leaf(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    calls = []
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Partial prepared prefix.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        assert len(json.loads(batch["source_ids_json"])) < 8
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})
        monkeypatch.setattr(
            engine, "_should_force_overflow_recovery", lambda **_kwargs: True,
        )

        def foreground_summary(**_kwargs):
            calls.append(True)
            return "Full emergency foreground summary.", 1

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", foreground_summary,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        engine.compress(messages, current_tokens=900)
        assert calls
        assert engine._async_compaction_store.get_batch(batch["batch_id"])["state"] != "promoted"
        assert len(engine._store.get_session_messages("session-1")) == 11
        engine.prepare_background_compaction_once(host_config={})
        assert engine._async_compaction_store.get_batch(batch["batch_id"])["state"] == "superseded"
    finally:
        engine.shutdown()


def test_rejected_predecessor_retires_unreachable_ready_descendant(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("A prepared leaf.", 1),
        )
        first = engine.prepare_background_compaction_once(host_config={})
        second = engine.prepare_background_compaction_once(host_config={})
        store = engine._async_compaction_store
        assert store.reject_batch(first["batch_id"], reason="test_source_changed")
        replacement = engine.prepare_background_compaction_once(host_config={})
        assert replacement["state"] == "ready"
        assert replacement["batch_id"] != first["batch_id"]
        assert store.get_batch(second["batch_id"])["state"] == "superseded"
        assert store.counts()["ready"] == 1
    finally:
        engine.shutdown()


def test_future_batch_claim_requires_a_ready_frontier_chain(tmp_path):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    try:
        with pytest.raises(ValueError, match="no ready predecessor"):
            engine._async_compaction_store.create_batch(
                batch_id="unreachable",
                conversation_id="conversation-1",
                session_id="session-1",
                frontier_start_store_id=3,
                frontier_end_store_id=4,
                fresh_tail_count=2,
                leaf_chunk_tokens=20,
                policy_fingerprint="policy",
                summary_route_fingerprint="route",
                expected_leaf_count=1,
                require_frontier_chain=True,
            )
        assert engine._async_compaction_store.get_batch("unreachable") is None
    finally:
        engine.shutdown()


def test_predecessor_rejected_during_second_provider_blocks_readiness(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path, advance_anchor=False)
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("First prepared leaf.", 1),
        )
        first = engine.prepare_background_compaction_once(host_config={})
        assert first["state"] == "ready"

        def summarize_after_rejection(**_kwargs):
            engine._async_compaction_store.reject_batch(
                first["batch_id"], reason="test_parent_rejected",
            )
            return "Second leaf after parent rejection.", 1

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            summarize_after_rejection,
        )
        second = engine.prepare_background_compaction_once(host_config={})
        assert second["state"] == "failed"
        assert second["last_error"] == "ValueError"
        assert engine._dag.get_session_node_count("session-1") == 0
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


def test_foreground_uses_live_threshold_policy_over_prepared_batch(
    tmp_path, monkeypatch,
):
    engine = _engine_with_stable_backlog(tmp_path)
    calls = []
    try:
        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation",
            lambda **_kwargs: ("Prepared under old threshold.", 1),
        )
        batch = engine.prepare_background_compaction_once(host_config={})
        assert batch["state"] == "ready"
        engine._config.context_threshold = 0.75
        monkeypatch.setattr("hermes_cli.config.load_config_readonly", lambda: {})

        def foreground_summary(**_kwargs):
            calls.append(True)
            return "Foreground under live threshold.", 1

        monkeypatch.setattr(
            "hermes_lcm.engine.summarize_with_escalation", foreground_summary,
        )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("session-1")
        ]
        result = engine.compress(messages, current_tokens=900)
        assert calls
        assert engine._async_compaction_store.get_batch(batch["batch_id"])["state"] == "rejected"
        assert any("Foreground under live threshold." in str(msg.get("content")) for msg in result)
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
