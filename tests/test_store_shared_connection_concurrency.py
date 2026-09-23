"""Regression for concurrent reads on MessageStore's shared SQLite connection."""

import threading
from concurrent.futures import ThreadPoolExecutor

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
