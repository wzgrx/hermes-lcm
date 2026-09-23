"""Regression for concurrent reads on MessageStore's shared SQLite connection."""

import threading
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from hermes_lcm.retrieval_core import _resolve_semantic_conversation_scope
from hermes_lcm.store import MessageStore


def test_simultaneous_reads_share_connection_safely(tmp_path):
    store = MessageStore(tmp_path / "concurrent.db")
    try:
        store.append("session-a", {"role": "user", "content": "hello"})
        for _ in range(20):
            barrier = threading.Barrier(16)

            def read(index):
                barrier.wait(timeout=5)
                if index % 2:
                    return store.get_session_count("session-a")
                return len(store.get_session_messages("session-a"))

            with ThreadPoolExecutor(max_workers=16) as pool:
                assert list(pool.map(read, range(16))) == [1] * 16
    finally:
        store.close()


def test_semantic_conversation_scope_uses_locked_message_reads(tmp_path):
    store = MessageStore(tmp_path / "scope-concurrent.db")
    try:
        store.append(
            "session-a",
            {"role": "user", "content": "hello"},
            conversation_id="conversation-a",
        )
        engine = SimpleNamespace(_store=store)
        for _ in range(20):
            barrier = threading.Barrier(16)

            def read(index):
                barrier.wait(timeout=5)
                if index % 2:
                    return _resolve_semantic_conversation_scope(
                        engine,
                        search_session_id=None,
                        conversation_id="conversation-a",
                    )
                return ["session-a"] if store.get_session_count("session-a") == 1 else []

            with ThreadPoolExecutor(max_workers=16) as pool:
                assert list(pool.map(read, range(16))) == [["session-a"]] * 16
    finally:
        store.close()
