"""Explicit /new removes summary carry while preserving raw source messages."""

import importlib.util
import sqlite3
import sys
import threading
import time
from pathlib import Path

import pytest

from hermes_lcm.dag import SummaryDAG, SummaryNode
from hermes_lcm.lifecycle_state import LifecycleStateStore
from hermes_lcm.store import MessageStore
from hermes_lcm.vector_store import VectorStore
from hermes_lcm.session_reset import CliNewSessionPairing, reset_explicit_new_carry


def _load_plugin_module():
    repo_root = Path(__file__).resolve().parent.parent
    name = "hermes_lcm_explicit_new_hook_test"
    spec = importlib.util.spec_from_file_location(
        name,
        str(repo_root / "__init__.py"),
        submodule_search_locations=[str(repo_root)],
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_explicit_new_forgets_owned_summary_nodes_and_keeps_other_conversations(
    tmp_path,
):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        lifecycle.bind_session("other", conversation_id="chat-b")
        lifecycle.finalize_session("chat-b", "other", frontier_store_id=9)
        dag.add_node(
            SummaryNode(session_id="old", depth=2, summary="historical evidence")
        )
        dag.add_node(SummaryNode(session_id="other", depth=1, summary="other chat"))

        result = reset_explicit_new_carry(db_path, "old")
        assert result["conversation_id"] == "chat-a"
        state = lifecycle.get_by_conversation("chat-a")
        assert state.last_finalized_session_id is None
        assert state.last_finalized_frontier_store_id == 0
        assert state.current_frontier_store_id == 0
        assert state.last_reset_at is not None
        assert dag.get_session_nodes("old") == []
        assert len(dag.get_session_nodes("other")) == 1
        assert result["deleted_nodes"] == 1
        assert (
            lifecycle.get_by_conversation("chat-b").last_finalized_session_id == "other"
        )

        lifecycle.bind_session("new", conversation_id="chat-a")
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
        assert dag.get_session_nodes("new") == []
    finally:
        dag.close()
        lifecycle.close()


def test_explicit_new_forgets_current_and_finalized_summaries_not_raw_rows(tmp_path):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    store = MessageStore(db_path)
    vectors = VectorStore(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        lifecycle.bind_session("fresh", conversation_id="chat-a")
        lifecycle.bind_session("unrelated", conversation_id="chat-b")
        old_node_id = dag.add_node(
            SummaryNode(session_id="old", depth=0, summary="old leaf")
        )
        dag.add_node(SummaryNode(session_id="old", depth=2, summary="old high-level"))
        dag.add_node(SummaryNode(session_id="fresh", depth=0, summary="new segment"))
        dag.add_node(
            SummaryNode(session_id="unrelated", depth=1, summary="other topic")
        )
        store.append("old", {"role": "user", "content": "raw source remains"})
        identity = vectors.register_profile("cleanup-test", "local", 2)
        vectors.connection.execute(
            "INSERT INTO lcm_embedding_vectors(embedded_id, identity_hash, vec) VALUES (?, ?, ?)",
            (str(old_node_id), identity, bytes(8)),
        )
        vectors.connection.execute(
            "INSERT INTO lcm_embedding_meta(embedded_id, embedded_kind, identity_hash, embedded_at, source_token_count, archived) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (str(old_node_id), "summary", identity, "2026-01-01", 1, 0),
        )
        vectors.connection.commit()

        result = reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")
        assert result["deleted_nodes"] == 3
        assert dag.get_session_nodes("old") == []
        assert dag.get_session_nodes("fresh") == []
        assert len(dag.get_session_nodes("unrelated")) == 1
        assert len(store.get_session_messages("old")) == 1
        assert (
            vectors.connection.execute(
                "SELECT COUNT(*) FROM lcm_embedding_vectors WHERE embedded_id = ?",
                (str(old_node_id),),
            ).fetchone()[0]
            == 0
        )
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
    finally:
        vectors.close()
        store.close()
        dag.close()
        lifecycle.close()


@pytest.mark.parametrize("fail_after_delete", [False, True])
def test_explicit_new_keeps_carry_retryable_when_node_cleanup_fails(
    tmp_path, monkeypatch, fail_after_delete,
):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        dag.add_node(SummaryNode(session_id="old", depth=2, summary="old carry"))

        original_delete = SummaryDAG.delete_session_nodes

        def fail_once(self, session_id, *, on_deleted_batch=None, on_deleted_batch_in_transaction=None):
            if fail_after_delete:
                original_delete(
                    self, session_id,
                    on_deleted_batch=on_deleted_batch,
                    on_deleted_batch_in_transaction=on_deleted_batch_in_transaction,
                )
            raise OSError("injected node cleanup failure")

        monkeypatch.setattr(SummaryDAG, "delete_session_nodes", fail_once)
        with pytest.raises(OSError, match="injected node cleanup failure"):
            reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "old"

        monkeypatch.setattr(SummaryDAG, "delete_session_nodes", original_delete)
        assert reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")["found"] is True
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
        assert dag.get_session_nodes("old") == []
    finally:
        dag.close()
        lifecycle.close()


def test_explicit_new_rolls_back_node_when_embedding_cleanup_fails(
    tmp_path, monkeypatch,
):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    vectors = VectorStore(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        node_id = dag.add_node(SummaryNode(session_id="old", depth=2, summary="old carry"))
        identity = vectors.register_profile("cleanup-test", "local", 2)
        vectors.connection.execute(
            "INSERT INTO lcm_embedding_vectors(embedded_id, identity_hash, vec) VALUES (?, ?, ?)",
            (str(node_id), identity, bytes(8)),
        )
        vectors.connection.execute(
            "INSERT INTO lcm_embedding_meta(embedded_id, embedded_kind, identity_hash, embedded_at, source_token_count, archived) "
            "VALUES (?, 'summary', ?, '2026-01-01', 1, 0)",
            (str(node_id), identity),
        )
        vectors.connection.commit()

        original_purge = VectorStore.purge_embedding_batch_on_connection

        def fail_purge(_conn, _node_ids):
            raise sqlite3.OperationalError("injected embedding cleanup failure")

        monkeypatch.setattr(VectorStore, "purge_embedding_batch_on_connection", staticmethod(fail_purge))
        with pytest.raises(sqlite3.OperationalError, match="injected embedding cleanup failure"):
            reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "old"
        assert dag.get_node(node_id) is not None
        assert vectors.connection.execute(
            "SELECT COUNT(*) FROM lcm_embedding_vectors WHERE embedded_id = ?", (str(node_id),)
        ).fetchone()[0] == 1

        monkeypatch.setattr(VectorStore, "purge_embedding_batch_on_connection", staticmethod(original_purge))
        assert reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")["found"] is True
        assert dag.get_node(node_id) is None
        assert vectors.connection.execute(
            "SELECT COUNT(*) FROM lcm_embedding_vectors WHERE embedded_id = ?", (str(node_id),)
        ).fetchone()[0] == 0
    finally:
        vectors.close()
        dag.close()
        lifecycle.close()


def test_explicit_new_retries_remaining_batches_after_late_embedding_failure(
    tmp_path, monkeypatch,
):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    vectors = VectorStore(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        ids = list(range(1, 301))
        dag.connection.executemany(
            "INSERT INTO summary_nodes(node_id, session_id, depth, summary, source_token_count, "
            "source_ids, source_type, created_at) VALUES (?, 'old', 0, 'summary', 1, '[]', 'messages', ?)",
            ((node_id, float(node_id)) for node_id in ids),
        )
        dag.connection.commit()
        identity = vectors.register_profile("cleanup-test", "local", 2)
        vectors.connection.executemany(
            "INSERT INTO lcm_embedding_vectors(embedded_id, identity_hash, vec) VALUES (?, ?, ?)",
            ((str(node_id), identity, bytes(8)) for node_id in ids),
        )
        vectors.connection.executemany(
            "INSERT INTO lcm_embedding_meta(embedded_id, embedded_kind, identity_hash, embedded_at, "
            "source_token_count, archived) VALUES (?, 'summary', ?, '2026-01-01', 1, 0)",
            ((str(node_id), identity) for node_id in ids),
        )
        vectors.connection.commit()

        original_purge = VectorStore.purge_embedding_batch_on_connection
        calls = 0

        def fail_second(conn, node_ids):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise sqlite3.OperationalError("second embedding batch failed")
            return original_purge(conn, node_ids)

        monkeypatch.setattr(VectorStore, "purge_embedding_batch_on_connection", staticmethod(fail_second))
        with pytest.raises(sqlite3.OperationalError, match="second embedding batch failed"):
            reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")
        assert calls == 2
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "old"
        assert dag.get_session_node_count("old") == 44
        assert vectors.connection.execute("SELECT COUNT(*) FROM lcm_embedding_vectors").fetchone()[0] == 44

        monkeypatch.setattr(VectorStore, "purge_embedding_batch_on_connection", staticmethod(original_purge))
        result = reset_explicit_new_carry(db_path, "old", conversation_id="chat-a")
        assert result["deleted_nodes"] == 44
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
        assert dag.get_session_node_count("old") == 0
        assert vectors.connection.execute("SELECT COUNT(*) FROM lcm_embedding_vectors").fetchone()[0] == 0
    finally:
        vectors.close()
        dag.close()
        lifecycle.close()


def test_late_old_finalize_does_not_rearm_explicit_new_carry(tmp_path):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        assert reset_explicit_new_carry(db_path, "old")["found"] is True
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=99)
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None

        lifecycle.bind_session("new", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=99)
        state = lifecycle.get_by_conversation("chat-a")
        assert state.current_session_id == "new"
        assert state.last_finalized_session_id is None
        assert state.current_frontier_store_id == 0

        lifecycle.finalize_session("chat-a", "new", frontier_store_id=7)
        assert (
            lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "new"
        )
    finally:
        lifecycle.close()


def test_explicit_new_requires_matching_old_session_and_existing_database(tmp_path):
    db_path = tmp_path / "lcm.db"
    assert reset_explicit_new_carry(db_path, "old")["found"] is False
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("other", conversation_id="chat-b")
        assert reset_explicit_new_carry(db_path, "old")["found"] is False
        assert (
            reset_explicit_new_carry(db_path, "other", conversation_id="chat-a")[
                "found"
            ]
            is False
        )
        assert lifecycle.get_by_conversation("chat-b").current_session_id == "other"
    finally:
        lifecycle.close()


def test_gateway_reset_hook_requires_explicit_reason_and_outgoing_session(tmp_path):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    hook = _load_plugin_module()._on_explicit_session_reset
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=12)

        class _Engine:
            _hermes_home = str(tmp_path)

            def _resolve_db_path(self, _home):
                return db_path

        engine = _Engine()

        assert hook(engine, reason="compression", old_session_id="old") is None
        assert hook(engine, reason="new_session", session_id="new") is None
        assert (
            lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "old"
        )

        result = hook(
            engine,
            reason="new_session",
            old_session_id="old",
            new_session_id="new",
        )
        assert result["found"] is True
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
    finally:
        lifecycle.close()


def _write_host_sessions(path, *, old_end_reason="new_session", new_profile="default"):
    with sqlite3.connect(path) as conn:
        conn.execute(
            "CREATE TABLE sessions (id TEXT PRIMARY KEY, source TEXT, profile_name TEXT, "
            "started_at REAL, ended_at REAL, end_reason TEXT)"
        )
        conn.execute(
            "INSERT INTO sessions VALUES ('old', 'cli', 'default', 10, 20, ?)",
            (old_end_reason,),
        )
        conn.execute(
            "INSERT INTO sessions VALUES ('new', 'cli', ?, 21, NULL, NULL)",
            (new_profile,),
        )


def test_cli_new_pairs_host_verified_old_session_and_forgets_only_its_carry(tmp_path):
    db_path = tmp_path / "lcm.db"
    _write_host_sessions(tmp_path / "state.db")
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    pairing = CliNewSessionPairing()
    hook = _load_plugin_module()._on_explicit_session_reset
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        lifecycle.bind_session("unrelated", conversation_id="chat-b")
        dag.add_node(SummaryNode(session_id="old", depth=2, summary="old carry"))
        dag.add_node(SummaryNode(session_id="unrelated", depth=2, summary="other chat"))

        class _Engine:
            _hermes_home = str(tmp_path)

            def _resolve_db_path(self, _home):
                return db_path

        pairing.note_finalization(
            platform="cli", reason="session_boundary", session_id="old"
        )
        result = hook(
            _Engine(),
            pairing,
            platform="cli",
            reason="new_session",
            session_id="new",
        )
        assert result["found"] is True
        assert result["deleted_nodes"] == 1
        assert dag.get_session_nodes("old") == []
        assert len(dag.get_session_nodes("unrelated")) == 1
        assert (
            pairing.consume_verified_old_session(
                new_session_id="new",
                hermes_home=str(tmp_path),
            )
            == ""
        )
    finally:
        dag.close()
        lifecycle.close()


def test_cli_new_rejects_unproven_rotation_and_expired_or_cross_thread_hint(tmp_path):
    pairing = CliNewSessionPairing()
    host_db = tmp_path / "state.db"
    _write_host_sessions(host_db, old_end_reason="quit")
    pairing.note_finalization(
        platform="cli", reason="session_boundary", session_id="old"
    )
    assert (
        pairing.consume_verified_old_session(
            new_session_id="new", hermes_home=str(tmp_path)
        )
        == ""
    )


    with sqlite3.connect(host_db) as conn:
        conn.execute("UPDATE sessions SET end_reason = 'new_session' WHERE id = 'old'")
        conn.execute("UPDATE sessions SET profile_name = 'other' WHERE id = 'new'")
    pairing.note_finalization(
        platform="cli", reason="session_boundary", session_id="old"
    )
    assert (
        pairing.consume_verified_old_session(
            new_session_id="new", hermes_home=str(tmp_path)
        )
        == ""
    )

    with sqlite3.connect(host_db) as conn:
        conn.execute("UPDATE sessions SET profile_name = 'default' WHERE id = 'new'")
    pairing.note_finalization(
        platform="cli", reason="session_boundary", session_id="old"
    )
    observed = []
    other_thread = threading.Thread(
        target=lambda: observed.append(
            pairing.consume_verified_old_session(
                new_session_id="new",
                hermes_home=str(tmp_path),
            )
        )
    )
    other_thread.start()
    other_thread.join(5)
    assert observed == [""]

    pairing._local.pending = ("old", time.monotonic() - 121)
    assert (
        pairing.consume_verified_old_session(
            new_session_id="new", hermes_home=str(tmp_path)
        )
        == ""
    )


def test_installed_host_sessiondb_cli_rotation_is_verifiable(tmp_path):
    """Check the installed host's real session writer, not only a schema stub."""
    plugin_root = Path(__file__).resolve().parents[1]
    shadowing_paths = [
        entry for entry in sys.path if Path(entry or ".").resolve() == plugin_root
    ]
    for entry in shadowing_paths:
        sys.path.remove(entry)
    shadowed_tools = sys.modules.pop("tools", None)
    try:
        host = pytest.importorskip("hermes_state")
    finally:
        if shadowed_tools is not None:
            sys.modules["tools"] = shadowed_tools
        sys.path[:0] = shadowing_paths

    db = host.SessionDB(tmp_path / "state.db")
    try:
        db.create_session("old", source="cli")
        db.end_session("old", "new_session")
        db.create_session("new", source="cli")
        pairing = CliNewSessionPairing()
        pairing.note_finalization(
            platform="cli", reason="session_boundary", session_id="old"
        )
        assert pairing.consume_verified_old_session(
            new_session_id="new", hermes_home=str(tmp_path)
        ) == "old"
    finally:
        db.close()


def test_interleaved_old_finalize_cannot_restore_cleared_carry(tmp_path, monkeypatch):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    lifecycle.bind_session("old", conversation_id="chat-a")
    lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
    first_read = threading.Event()
    reset_done = threading.Event()
    original_get = lifecycle.get_by_conversation
    observed = []

    def paused_get(conversation_id):
        state = original_get(conversation_id)
        if threading.current_thread() is worker and not first_read.is_set():
            first_read.set()
            assert reset_done.wait(5)
        return state

    monkeypatch.setattr(lifecycle, "get_by_conversation", paused_get)
    worker = threading.Thread(
        target=lambda: observed.append(
            lifecycle.finalize_session("chat-a", "old", frontier_store_id=99)
        )
    )
    try:
        worker.start()
        assert first_read.wait(5)
        assert reset_explicit_new_carry(db_path, "old")["found"] is True
    finally:
        reset_done.set()
        worker.join(5)
        lifecycle.close()
    assert not worker.is_alive()
    assert observed[0].last_finalized_session_id is None
