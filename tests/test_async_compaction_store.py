"""Tests for the default-off, non-canonical async compaction staging store."""

from __future__ import annotations

import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from hermes_lcm.async_compaction_store import AsyncCompactionStore, _CREATE_BATCHES
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.store import MessageStore


def _seed_sources(core: MessageStore) -> None:
    for idx in range(10):
        core.append(
            "session-1",
            {"role": "user", "content": f"source {idx}"},
            conversation_id="conversation-1",
        )


def _create_batch(store: AsyncCompactionStore, *, batch_id: str = "batch-1") -> dict:
    return store.create_batch(
        batch_id=batch_id,
        conversation_id="conversation-1",
        session_id="session-1",
        frontier_start_store_id=0,
        frontier_end_store_id=10,
        fresh_tail_count=2,
        leaf_chunk_tokens=128,
        policy_fingerprint="policy-hash",
        summary_route_fingerprint="route-hash",
        expected_leaf_count=2,
    )


def test_disabled_store_does_not_create_database_or_optional_tables(tmp_path):
    db_path = tmp_path / "disabled.db"
    with AsyncCompactionStore(db_path) as store:
        assert store.enabled is False
        with pytest.raises(RuntimeError, match="disabled or closed"):
            store.counts()
    assert not db_path.exists()

    core = MessageStore(db_path)
    try:
        with AsyncCompactionStore(db_path, enabled=False):
            pass
        names = {
            row[0]
            for row in core._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "lcm_compaction_batches" not in names
        assert "lcm_pending_summary_nodes" not in names
    finally:
        core.close()


def test_enabled_store_requires_normal_core_bootstrap(tmp_path):
    db_path = tmp_path / "missing.db"
    with pytest.raises(FileNotFoundError):
        AsyncCompactionStore(db_path, enabled=True)
    assert not db_path.exists()


def test_engine_binds_optional_store_only_when_enabled(tmp_path):
    disabled_path = tmp_path / "engine-disabled.db"
    disabled = LCMEngine(config=LCMConfig(database_path=str(disabled_path)))
    try:
        assert disabled._async_compaction_store is None
        assert (
            disabled._store._conn.execute(
                "SELECT name FROM sqlite_master WHERE name = 'lcm_compaction_batches'"
            ).fetchone()
            is None
        )
    finally:
        disabled.shutdown()

    enabled_path = tmp_path / "engine-enabled.db"
    enabled = LCMEngine(
        config=LCMConfig(
            database_path=str(enabled_path),
            async_background_compaction_enabled=True,
        )
    )
    try:
        assert enabled._async_compaction_store is not None
        assert enabled._async_compaction_store.enabled is True
        assert enabled._async_compaction_store.counts()["pending"] == 0
        assert (
            enabled._store._conn.execute(
                "SELECT name FROM sqlite_master WHERE name = 'lcm_compaction_batches'"
            ).fetchone()
            is not None
        )
    finally:
        enabled.shutdown()


def test_staged_leaf_is_durable_but_invisible_to_canonical_dag(tmp_path):
    db_path = tmp_path / "staging.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            plan = _create_batch(store)
            store.stage_leaf(
                pending_id="leaf-1",
                batch_id="batch-1",
                summary="prepared summary",
                token_count=12,
                source_token_count=40,
                source_ids=plan["source_ids"][:2],
                source_identity_hashes=plan["source_identity_hashes"][:2],
            )
            assert store.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert store.counts(conversation_id="conversation-1")["preparing"] == 1
            assert store.counts(conversation_id="other")["preparing"] == 0
            canonical = {
                row[0]
                for row in core._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('summary_nodes', 'nodes_fts')"
                )
            }
            assert canonical == set()
            assert (
                core._conn.execute(
                    "SELECT COUNT(*) FROM lcm_pending_summary_nodes"
                ).fetchone()[0]
                == 1
            )
        with AsyncCompactionStore(db_path, enabled=True) as reopened:
            assert reopened.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert reopened.counts()["preparing"] == 1
        assert os.stat(db_path).st_mode & 0o777 == 0o600
    finally:
        core.close()


def test_restart_recovery_releases_only_abandoned_incomplete_claims(tmp_path):
    db_path = tmp_path / "recovery.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            _create_batch(store)
            assert store.mark_preparing("batch-1") is True
            assert store.mark_preparing("batch-1") is False
            assert store.recover_abandoned_batches(
                conversation_id="conversation-1", session_id="session-1",
                stale_after_seconds=300,
            ) == 0
            store.connection.execute(
                "UPDATE lcm_compaction_batches SET updated_at = ? WHERE batch_id = ?",
                (time.time() - 601, "batch-1"),
            )
        with AsyncCompactionStore(db_path, enabled=True) as reopened:
            assert reopened.recover_abandoned_batches(
                conversation_id="conversation-1", session_id="session-1",
                stale_after_seconds=300,
            ) == 1
            abandoned = reopened.get_batch("batch-1")
            assert abandoned["state"] == "failed"
            assert abandoned["last_error"] == "PreparationAbandoned"
            assert abandoned["next_retry_at"] <= time.time()
            assert reopened.recover_abandoned_batches(
                conversation_id="conversation-1", session_id="session-1",
                stale_after_seconds=300,
            ) == 0
            _create_batch(reopened, batch_id="batch-2")
            reopened.connection.execute(
                "UPDATE lcm_compaction_batches SET state = 'ready', updated_at = ? "
                "WHERE batch_id = 'batch-2'",
                (time.time() - 601,),
            )
            assert reopened.recover_abandoned_batches(
                conversation_id="conversation-1", session_id="session-1",
                stale_after_seconds=300,
            ) == 0
            assert reopened.get_batch("batch-2")["state"] == "ready"
    finally:
        core.close()


def test_invalid_or_overlapping_leaf_rolls_back_without_count_drift(tmp_path):
    db_path = tmp_path / "rollback.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            plan = _create_batch(store)
            store.stage_leaf(
                pending_id="leaf-1",
                batch_id="batch-1",
                summary="first",
                token_count=2,
                source_token_count=10,
                source_ids=[1, 3],
                source_identity_hashes=[
                    plan["source_identity_hashes"][0],
                    plan["source_identity_hashes"][2],
                ],
            )
            with pytest.raises(ValueError, match="overlaps"):
                store.stage_leaf(
                    pending_id="leaf-2",
                    batch_id="batch-1",
                    summary="overlap",
                    token_count=2,
                    source_token_count=10,
                    source_ids=[3, 4],
                    source_identity_hashes=plan["source_identity_hashes"][2:4],
                )
            with pytest.raises(ValueError, match="invalid"):
                store.stage_leaf(
                    pending_id="leaf-3",
                    batch_id="batch-1",
                    summary="blank ids",
                    token_count=2,
                    source_token_count=10,
                    source_ids=[],
                    source_identity_hashes=[],
                )
            assert store.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert (
                store.connection.execute(
                    "SELECT COUNT(*) FROM lcm_pending_summary_nodes"
                ).fetchone()[0]
                == 1
            )
    finally:
        core.close()


def test_ready_requires_exact_coverage_and_current_source_identity(tmp_path):
    db_path = tmp_path / "ready.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            plan = _create_batch(store)
            store.stage_leaf(
                pending_id="leaf-1",
                batch_id="batch-1",
                summary="first",
                token_count=2,
                source_token_count=10,
                source_ids=plan["source_ids"][:5],
                source_identity_hashes=plan["source_identity_hashes"][:5],
            )
            with pytest.raises(ValueError, match="incomplete"):
                store.mark_ready("batch-1")
            store.stage_leaf(
                pending_id="leaf-2",
                batch_id="batch-1",
                summary="second",
                token_count=2,
                source_token_count=10,
                source_ids=plan["source_ids"][5:],
                source_identity_hashes=plan["source_identity_hashes"][5:],
            )
            store.mark_ready("batch-1")
            assert store.get_batch("batch-1")["state"] == "ready"
            assert (
                core._conn.execute(
                    "SELECT name FROM sqlite_master WHERE name = 'summary_nodes'"
                ).fetchone()
                is None
            )
    finally:
        core.close()


def test_ready_rejects_source_rewrite_after_provider_work(tmp_path):
    db_path = tmp_path / "stale.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            plan = _create_batch(store)
            for index, source_slice in enumerate((slice(0, 5), slice(5, 10)), 1):
                store.stage_leaf(
                    pending_id=f"leaf-{index}",
                    batch_id="batch-1",
                    summary=f"summary {index}",
                    token_count=2,
                    source_token_count=10,
                    source_ids=plan["source_ids"][source_slice],
                    source_identity_hashes=plan["source_identity_hashes"][source_slice],
                )
            core._conn.execute(
                "UPDATE messages SET content = 'changed' WHERE store_id = 3"
            )
            core._conn.commit()
            with pytest.raises(ValueError, match="source identity changed"):
                store.mark_ready("batch-1")
            assert store.get_batch("batch-1")["state"] == "preparing"
    finally:
        core.close()


def test_only_one_active_batch_claims_a_frontier_and_failure_backs_off(tmp_path):
    db_path = tmp_path / "claim.db"
    core = MessageStore(db_path)
    try:
        _seed_sources(core)
        with AsyncCompactionStore(db_path, enabled=True) as store:
            _create_batch(store)
            with pytest.raises(sqlite3.IntegrityError):
                _create_batch(store, batch_id="batch-2")
            store.fail_batch(
                "batch-1",
                error_type="RuntimeError: secret=TOKEN",
                backoff_seconds=30,
            )
            failed = store.get_batch("batch-1")
            assert failed["state"] == "failed"
            assert failed["failure_count"] == 1
            assert "TOKEN" not in failed["last_error"]
            assert failed["next_retry_at"] > failed["updated_at"]
            _create_batch(store, batch_id="batch-2")
            assert (
                store.active_batch_for_frontier(
                    conversation_id="conversation-1",
                    session_id="session-1",
                    frontier_store_id=0,
                )["batch_id"]
                == "batch-2"
            )
    finally:
        core.close()


def _ready_engine(tmp_path):
    db_path = tmp_path / "publish.db"
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(db_path),
            async_background_compaction_enabled=True,
        )
    )
    engine.on_session_start(
        "session-1",
        conversation_id="conversation-1",
        platform="test",
        context_length=1000,
    )
    _seed_sources(engine._store)
    store = engine._async_compaction_store
    plan = _create_batch(store)
    for index, source_slice in enumerate((slice(0, 5), slice(5, 10)), 1):
        store.stage_leaf(
            pending_id=f"leaf-{index}",
            batch_id="batch-1",
            summary=f"summary {index}",
            token_count=2,
            source_token_count=10,
            source_ids=plan["source_ids"][source_slice],
            source_identity_hashes=plan["source_identity_hashes"][source_slice],
        )
    store.mark_ready("batch-1")
    return engine


def _canonical_state(engine):
    conn = engine._async_compaction_store.connection
    rows = conn.execute(
        "SELECT source_ids FROM summary_nodes ORDER BY node_id"
    ).fetchall()
    frontier = conn.execute(
        "SELECT current_frontier_store_id FROM lcm_lifecycle_state "
        "WHERE conversation_id = 'conversation-1'"
    ).fetchone()[0]
    state = engine._async_compaction_store.get_batch("batch-1")["state"]
    return [row[0] for row in rows], frontier, state


def test_promotion_publishes_nodes_frontier_and_batch_in_one_transaction(tmp_path):
    engine = _ready_engine(tmp_path)
    try:
        store = engine._async_compaction_store
        assert _canonical_state(engine) == ([], 0, "ready")
        result = store.promote_batch(
            "batch-1",
            live_policy_fingerprint="policy-hash",
            live_summary_route_fingerprint="route-hash",
            max_publishable_store_id=10,
        )
        assert result.promoted is True
        assert len(result.node_ids) == 2
        assert _canonical_state(engine) == (
            ["[1, 2, 3, 4, 5]", "[6, 7, 8, 9, 10]"],
            10,
            "promoted",
        )
        second = store.promote_batch(
            "batch-1",
            live_policy_fingerprint="policy-hash",
            live_summary_route_fingerprint="route-hash",
            max_publishable_store_id=10,
        )
        assert second.promoted is False
        assert second.reason == "already_promoted"
        assert len(_canonical_state(engine)[0]) == 2
    finally:
        engine.shutdown()


@pytest.mark.parametrize(
    ("override", "reason"),
    [
        ({"live_policy_fingerprint": "changed"}, "policy_fingerprint_mismatch"),
        (
            {"live_summary_route_fingerprint": "changed"},
            "summary_route_fingerprint_mismatch",
        ),
        ({"max_publishable_store_id": 9}, "fresh_tail_boundary_changed"),
    ],
)
def test_promotion_rejects_stale_policy_route_or_fresh_tail(tmp_path, override, reason):
    engine = _ready_engine(tmp_path)
    try:
        args = {
            "live_policy_fingerprint": "policy-hash",
            "live_summary_route_fingerprint": "route-hash",
            "max_publishable_store_id": 10,
            **override,
        }
        result = engine._async_compaction_store.promote_batch("batch-1", **args)
        assert result.promoted is False
        assert result.reason == reason
        assert _canonical_state(engine) == ([], 0, "rejected")
    finally:
        engine.shutdown()


def test_promotion_rejects_source_rewrite_and_foreground_frontier_race(tmp_path):
    for change, reason in (
        (
            "UPDATE messages SET content = 'changed' WHERE store_id = 3",
            "source_identity_mismatch",
        ),
        (
            "UPDATE lcm_lifecycle_state SET current_frontier_store_id = 2 "
            "WHERE conversation_id = 'conversation-1'",
            "frontier_changed",
        ),
    ):
        engine = _ready_engine(tmp_path / reason)
        try:
            conn = engine._async_compaction_store.connection
            conn.execute(change)
            result = engine._async_compaction_store.promote_batch(
                "batch-1",
                live_policy_fingerprint="policy-hash",
                live_summary_route_fingerprint="route-hash",
                max_publishable_store_id=10,
            )
            assert result.promoted is False
            assert result.reason == reason
            rows, frontier, state = _canonical_state(engine)
            assert rows == []
            assert frontier == (2 if reason == "frontier_changed" else 0)
            assert state == "rejected"
        finally:
            engine.shutdown()


def test_mid_publication_failure_rolls_back_all_canonical_changes(tmp_path):
    engine = _ready_engine(tmp_path)
    try:
        conn = engine._async_compaction_store.connection
        conn.execute(
            """CREATE TRIGGER fail_second_node BEFORE INSERT ON summary_nodes
               WHEN NEW.summary = 'summary 2'
               BEGIN SELECT RAISE(ABORT, 'fixture insertion failure'); END"""
        )
        with pytest.raises(sqlite3.IntegrityError, match="fixture insertion failure"):
            engine._async_compaction_store.promote_batch(
                "batch-1",
                live_policy_fingerprint="policy-hash",
                live_summary_route_fingerprint="route-hash",
                max_publishable_store_id=10,
            )
        assert _canonical_state(engine) == ([], 0, "ready")
    finally:
        engine.shutdown()


def test_canonical_overlap_rejects_prepared_batch_without_double_coverage(tmp_path):
    engine = _ready_engine(tmp_path)
    try:
        conn = engine._async_compaction_store.connection
        conn.execute(
            """INSERT INTO summary_nodes
               (session_id, depth, summary, token_count, source_token_count,
                source_ids, source_type, created_at)
               VALUES ('session-1', 0, 'foreground won', 2, 10,
                       '[1]', 'messages', 1)"""
        )
        result = engine._async_compaction_store.promote_batch(
            "batch-1",
            live_policy_fingerprint="policy-hash",
            live_summary_route_fingerprint="route-hash",
            max_publishable_store_id=10,
        )
        assert result.reason == "canonical_source_overlap"
        assert _canonical_state(engine) == (["[1]"], 0, "rejected")
    finally:
        engine.shutdown()


def test_two_publishers_serialize_and_publish_once(tmp_path):
    engine = _ready_engine(tmp_path)
    db_path = tmp_path / "publish.db"
    second_store = AsyncCompactionStore(db_path, enabled=True)
    try:
        barrier = Barrier(2)

        def publish(store):
            barrier.wait(timeout=5)
            return store.promote_batch(
                "batch-1",
                live_policy_fingerprint="policy-hash",
                live_summary_route_fingerprint="route-hash",
                max_publishable_store_id=10,
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(publish, engine._async_compaction_store)
            second = pool.submit(publish, second_store)
            results = [first.result(timeout=10), second.result(timeout=10)]
        assert sorted(result.reason for result in results) == [
            "already_promoted",
            "promoted",
        ]
        assert _canonical_state(engine)[1:] == (10, "promoted")
        assert len(_canonical_state(engine)[0]) == 2
    finally:
        second_store.close()
        engine.shutdown()


def test_schema_incompatibility_does_not_leave_partial_optional_tables(tmp_path):
    db_path = tmp_path / "incompatible.db"
    core = MessageStore(db_path)
    try:
        core._conn.execute("CREATE TABLE lcm_compaction_batches (batch_id TEXT)")
        core._conn.commit()
        with pytest.raises(sqlite3.OperationalError, match="incompatible"):
            AsyncCompactionStore(db_path, enabled=True)
        names = {
            row[0]
            for row in core._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "lcm_pending_summary_nodes" not in names
    finally:
        core.close()


def test_existing_optional_batch_schema_gains_source_frontier(tmp_path):
    db_path = tmp_path / "legacy-optional.db"
    core = MessageStore(db_path)
    try:
        legacy_schema = _CREATE_BATCHES.replace(
            "    source_frontier_start_store_id INTEGER NOT NULL CHECK (\n"
            "        source_frontier_start_store_id >= frontier_start_store_id\n"
            "    ),\n",
            "",
        )
        core._conn.execute(legacy_schema)
        core._conn.execute(
            """INSERT INTO lcm_compaction_batches (
                batch_id, conversation_id, session_id, state,
                frontier_start_store_id, frontier_end_store_id,
                fresh_tail_count, leaf_chunk_tokens, policy_fingerprint,
                summary_route_fingerprint, source_coverage_hash,
                source_ids_json, source_identity_hashes_json,
                expected_leaf_count, created_at, updated_at
            ) VALUES ('old', 'conversation-1', 'session-1', 'pending',
                      1, 2, 1, 64, 'policy', 'route', 'coverage',
                      '[2]', '["digest"]', 1, 1, 1)"""
        )
        core._conn.commit()
        with AsyncCompactionStore(db_path, enabled=True) as upgraded:
            assert upgraded.get_batch("old")["source_frontier_start_store_id"] == 1
            assert upgraded.counts()["pending"] == 1
    finally:
        core.close()


def test_config_flags_default_off_and_parse_environment(monkeypatch):
    config = LCMConfig()
    assert config.async_background_compaction_enabled is False
    assert config.async_background_compaction_worker_enabled is False
    monkeypatch.setenv("LCM_BACKGROUND_COMPACTION_ENABLED", "true")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_WORKER_ENABLED", "1")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_MAX_BATCHES", "3")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_RETRY_BACKOFF_SECONDS", "42")
    parsed = LCMConfig.from_env()
    assert parsed.async_background_compaction_enabled is True
    assert parsed.async_background_compaction_worker_enabled is True
    assert parsed.async_background_compaction_max_batches == 3
    assert parsed.async_background_compaction_retry_backoff_seconds == 42
