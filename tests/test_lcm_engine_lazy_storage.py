"""Lazy storage binding tests for cloned LCM engines."""

import copy
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


def test_deepcopy_does_not_open_sqlite_until_first_storage_use(tmp_path, monkeypatch):
    config = LCMConfig(
        database_path=str(tmp_path / "lazy-copy.db"),
        temporal_rollups_enabled=True,
    )
    engine = LCMEngine(config=config, hermes_home=str(tmp_path / "hermes"))
    try:
        engine._store.append("session-a", {"role": "user", "content": "hello"})

        real_connect = sqlite3.connect
        connect_calls = 0
        rollup_initializations = 0

        def counting_connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return real_connect(*args, **kwargs)

        def counting_initialize_rollups(dag):
            nonlocal rollup_initializations
            rollup_initializations += 1

        monkeypatch.setattr(sqlite3, "connect", counting_connect)
        monkeypatch.setattr(
            "hermes_lcm.engine.initialize_rollup_invalidation_outbox",
            counting_initialize_rollups,
        )

        clone = copy.deepcopy(engine)

        assert connect_calls == 0
        assert rollup_initializations == 0
        assert clone._store.get_session_count("session-a") == 1
        assert connect_calls > 0
        assert rollup_initializations == 1

        connect_calls_after_bind = connect_calls
        assert clone._store.get_session_count("session-a") == 1
        assert connect_calls == connect_calls_after_bind
        assert rollup_initializations == 1
    finally:
        engine.shutdown()
        if "clone" in locals():
            clone.shutdown()


def test_lazy_clone_shutdown_is_noop_without_sqlite_connect(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(database_path=str(tmp_path / "lazy-shutdown.db")),
        hermes_home=str(tmp_path / "hermes"),
    )
    try:
        clone = copy.deepcopy(engine)

        def fail_connect(*args, **kwargs):
            raise AssertionError("lazy clone shutdown opened sqlite")

        monkeypatch.setattr(sqlite3, "connect", fail_connect)

        clone.shutdown()
    finally:
        engine.shutdown()


def test_deepcopy_loop_does_not_open_sqlite(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(database_path=str(tmp_path / "lazy-loop.db")),
        hermes_home=str(tmp_path / "hermes"),
    )
    try:
        real_connect = sqlite3.connect
        connect_calls = 0

        def counting_connect(*args, **kwargs):
            nonlocal connect_calls
            connect_calls += 1
            return real_connect(*args, **kwargs)

        monkeypatch.setattr(sqlite3, "connect", counting_connect)

        clones = [copy.deepcopy(engine) for _ in range(20)]

        assert connect_calls == 0
    finally:
        engine.shutdown()
        for clone in locals().get("clones", []):
            clone.shutdown()


def test_concurrent_first_access_binds_storage_once(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(database_path=str(tmp_path / "lazy-concurrent.db")),
        hermes_home=str(tmp_path / "hermes"),
    )
    clone = copy.deepcopy(engine)
    real_bind = clone._bind_storage
    bind_calls = 0
    count_lock = threading.Lock()

    def counting_bind(*args, **kwargs):
        nonlocal bind_calls
        with count_lock:
            bind_calls += 1
        time.sleep(0.01)
        return real_bind(*args, **kwargs)

    monkeypatch.setattr(clone, "_bind_storage", counting_bind)
    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            counts = list(pool.map(lambda _: clone._store.get_session_count("session-a"), range(8)))
        assert counts == [0] * 8
        assert bind_calls == 1
    finally:
        clone.shutdown()
        engine.shutdown()


def test_unbound_clone_rebinds_profile_before_first_storage_access(tmp_path):
    first_home = tmp_path / "first"
    second_home = tmp_path / "second"
    first_home.mkdir()
    second_home.mkdir()
    engine = LCMEngine(config=LCMConfig(), hermes_home=str(first_home))
    clone = copy.deepcopy(engine)
    try:
        assert clone._storage_bound is False
        assert clone._rebind_storage_for_home(str(second_home)) is True
        assert clone._storage_bound is False
        assert clone._store.db_path == second_home / "lcm.db"
        assert clone._storage_bound is True
    finally:
        clone.shutdown()
        engine.shutdown()
