"""Tests for the default-off, non-canonical async compaction staging store."""

from __future__ import annotations

import os
import sqlite3

import pytest

from hermes_lcm.async_compaction_store import AsyncCompactionStore
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.store import MessageStore


def _create_batch(store: AsyncCompactionStore, *, batch_id: str = "batch-1") -> None:
    store.create_batch(
        batch_id=batch_id,
        conversation_id="conversation-1",
        session_id="session-1",
        frontier_start_store_id=0,
        frontier_end_store_id=10,
        fresh_tail_count=2,
        leaf_chunk_tokens=128,
        policy_fingerprint="policy-hash",
        summary_route_fingerprint="route-hash",
        source_coverage_hash="source-hash",
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
            row[0] for row in core._conn.execute(
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
        assert disabled._store._conn.execute(
            "SELECT name FROM sqlite_master WHERE name = 'lcm_compaction_batches'"
        ).fetchone() is None
    finally:
        disabled.shutdown()

    enabled_path = tmp_path / "engine-enabled.db"
    enabled = LCMEngine(config=LCMConfig(
        database_path=str(enabled_path),
        async_background_compaction_enabled=True,
    ))
    try:
        assert enabled._async_compaction_store is not None
        assert enabled._async_compaction_store.enabled is True
        assert enabled._async_compaction_store.counts()["pending"] == 0
        assert enabled._store._conn.execute(
            "SELECT name FROM sqlite_master WHERE name = 'lcm_compaction_batches'"
        ).fetchone() is not None
    finally:
        enabled.shutdown()


def test_staged_leaf_is_durable_but_invisible_to_canonical_dag(tmp_path):
    db_path = tmp_path / "staging.db"
    core = MessageStore(db_path)
    try:
        with AsyncCompactionStore(db_path, enabled=True) as store:
            _create_batch(store)
            store.stage_leaf(
                pending_id="leaf-1", batch_id="batch-1", summary="prepared summary",
                token_count=12, source_token_count=40,
                source_ids=[1, 2], source_identity_hashes=["h1", "h2"],
            )
            assert store.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert store.counts(conversation_id="conversation-1")["preparing"] == 1
            assert store.counts(conversation_id="other")["preparing"] == 0
            canonical = {
                row[0] for row in core._conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table' "
                    "AND name IN ('summary_nodes', 'nodes_fts')"
                )
            }
            assert canonical == set()
            assert core._conn.execute(
                "SELECT COUNT(*) FROM lcm_pending_summary_nodes"
            ).fetchone()[0] == 1
        with AsyncCompactionStore(db_path, enabled=True) as reopened:
            assert reopened.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert reopened.counts()["preparing"] == 1
        assert os.stat(db_path).st_mode & 0o777 == 0o600
    finally:
        core.close()


def test_invalid_or_overlapping_leaf_rolls_back_without_count_drift(tmp_path):
    db_path = tmp_path / "rollback.db"
    core = MessageStore(db_path)
    try:
        with AsyncCompactionStore(db_path, enabled=True) as store:
            _create_batch(store)
            store.stage_leaf(
                pending_id="leaf-1", batch_id="batch-1", summary="first",
                token_count=2, source_token_count=10,
                source_ids=[1, 3], source_identity_hashes=["h1", "h3"],
            )
            with pytest.raises(ValueError, match="overlaps"):
                store.stage_leaf(
                    pending_id="leaf-2", batch_id="batch-1", summary="overlap",
                    token_count=2, source_token_count=10,
                    source_ids=[3, 4], source_identity_hashes=["h3", "h4"],
                )
            with pytest.raises(ValueError, match="invalid"):
                store.stage_leaf(
                    pending_id="leaf-3", batch_id="batch-1", summary="blank ids",
                    token_count=2, source_token_count=10,
                    source_ids=[], source_identity_hashes=[],
                )
            assert store.get_batch("batch-1")["prepared_leaf_count"] == 1
            assert store.connection.execute(
                "SELECT COUNT(*) FROM lcm_pending_summary_nodes"
            ).fetchone()[0] == 1
    finally:
        core.close()


def test_schema_incompatibility_does_not_leave_partial_optional_tables(tmp_path):
    db_path = tmp_path / "incompatible.db"
    core = MessageStore(db_path)
    try:
        core._conn.execute("CREATE TABLE lcm_compaction_batches (batch_id TEXT)")
        core._conn.commit()
        with pytest.raises(sqlite3.OperationalError, match="incompatible"):
            AsyncCompactionStore(db_path, enabled=True)
        names = {
            row[0] for row in core._conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "lcm_pending_summary_nodes" not in names
    finally:
        core.close()


def test_config_flags_default_off_and_parse_environment(monkeypatch):
    config = LCMConfig()
    assert config.async_background_compaction_enabled is False
    assert config.async_background_compaction_worker_enabled is False
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_ENABLED", "true")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_WORKER_ENABLED", "1")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_MAX_BATCHES", "3")
    monkeypatch.setenv("LCM_ASYNC_BACKGROUND_COMPACTION_RETRY_BACKOFF_SECONDS", "42")
    parsed = LCMConfig.from_env()
    assert parsed.async_background_compaction_enabled is True
    assert parsed.async_background_compaction_worker_enabled is True
    assert parsed.async_background_compaction_max_batches == 3
    assert parsed.async_background_compaction_retry_backoff_seconds == 42
