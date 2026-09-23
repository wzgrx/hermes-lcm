"""Explicit /new fences summary carry without deleting historical evidence."""

import importlib.util
import sys
import threading
from pathlib import Path

from hermes_lcm.dag import SummaryDAG, SummaryNode
from hermes_lcm.lifecycle_state import LifecycleStateStore
from hermes_lcm.session_reset import reset_explicit_new_carry


def _load_plugin_module():
    repo_root = Path(__file__).resolve().parent.parent
    name = "hermes_lcm_explicit_new_hook_test"
    spec = importlib.util.spec_from_file_location(
        name, str(repo_root / "__init__.py"), submodule_search_locations=[str(repo_root)]
    )
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def test_explicit_new_clears_carry_but_keeps_history_and_other_conversations(tmp_path):
    db_path = tmp_path / "lcm.db"
    lifecycle = LifecycleStateStore(db_path)
    dag = SummaryDAG(db_path)
    try:
        lifecycle.bind_session("old", conversation_id="chat-a")
        lifecycle.finalize_session("chat-a", "old", frontier_store_id=42)
        lifecycle.bind_session("other", conversation_id="chat-b")
        lifecycle.finalize_session("chat-b", "other", frontier_store_id=9)
        dag.add_node(SummaryNode(session_id="old", depth=2, summary="historical evidence"))
        dag.add_node(SummaryNode(session_id="other", depth=1, summary="other chat"))

        result = reset_explicit_new_carry(db_path, "old")
        assert result["conversation_id"] == "chat-a"
        state = lifecycle.get_by_conversation("chat-a")
        assert state.last_finalized_session_id is None
        assert state.last_finalized_frontier_store_id == 0
        assert state.current_frontier_store_id == 0
        assert state.last_reset_at is not None
        assert len(dag.get_session_nodes("old")) == 1
        assert lifecycle.get_by_conversation("chat-b").last_finalized_session_id == "other"

        lifecycle.bind_session("new", conversation_id="chat-a")
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id is None
        assert dag.get_session_nodes("new") == []
    finally:
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
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "new"
    finally:
        lifecycle.close()


def test_explicit_new_requires_matching_old_session_and_existing_database(tmp_path):
    db_path = tmp_path / "lcm.db"
    assert reset_explicit_new_carry(db_path, "old")["found"] is False
    lifecycle = LifecycleStateStore(db_path)
    try:
        lifecycle.bind_session("other", conversation_id="chat-b")
        assert reset_explicit_new_carry(db_path, "old")["found"] is False
        assert reset_explicit_new_carry(db_path, "other", conversation_id="chat-a")["found"] is False
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
        assert lifecycle.get_by_conversation("chat-a").last_finalized_session_id == "old"

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
