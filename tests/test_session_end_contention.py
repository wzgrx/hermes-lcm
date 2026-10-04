"""Final flush tolerates brief SQLite contention without unbounded retries."""

import sqlite3
import threading
import time

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.tokens import count_message_tokens


@pytest.fixture
def engine(tmp_path):
    instance = LCMEngine(LCMConfig(database_path=str(tmp_path / "end.db")),
                         hermes_home=str(tmp_path / "profile"))
    instance.on_session_start("session", platform="telegram", conversation_id="lane")
    count_message_tokens({"role":"user", "content":"warm tokenizer"})
    instance._store._conn.execute("PRAGMA busy_timeout=750")
    instance._lifecycle._conn.execute("PRAGMA busy_timeout=900")
    try:
        yield instance
    finally:
        instance.shutdown()


@pytest.mark.parametrize("stage", ["ingest", "finalize"])
def test_session_end_survives_transient_writer_lock(engine, monkeypatch, stage):
    ready, release = threading.Event(), threading.Event()
    errors = []

    def hold_writer():
        try:
            with sqlite3.connect(engine._store.db_path, isolation_level=None) as writer:
                writer.execute("BEGIN IMMEDIATE")
                ready.set()
                release.wait(0.15)
                writer.execute("ROLLBACK")
        except Exception as error:
            errors.append(error)
            ready.set()

    thread = threading.Thread(target=hold_writer)
    original = engine._ingest_messages

    def start_contention():
        thread.start()
        assert ready.wait(3) and not errors

    def ingest(messages):
        if stage == "ingest":
            start_contention()
        result = original(messages)
        if stage == "finalize":
            start_contention()
        return result

    monkeypatch.setattr(engine, "_ingest_messages", ingest)
    try:
        engine.on_session_end("session", [{"role":"user", "content":"final durable fact"}])
    finally:
        release.set()
        thread.join(3)
    assert not thread.is_alive() and not errors
    assert [row["content"] for row in engine._store.get_session_messages("session")] == ["final durable fact"]
    assert engine._lifecycle.get_by_conversation("lane").current_session_id is None
    assert engine._store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 750
    assert engine._lifecycle._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 900


def test_persistent_lock_is_bounded_and_later_finalization_is_idempotent(engine, caplog):
    messages = [{"role":"user", "content":"end after contention"}]
    writer = sqlite3.connect(engine._store.db_path, isolation_level=None)
    writer.execute("BEGIN IMMEDIATE")
    started = time.monotonic()
    try:
        engine.on_session_end("session", messages)
    finally:
        elapsed = time.monotonic() - started
        writer.rollback()
        writer.close()
    assert elapsed < 1.5
    assert "ingest skipped due to SQLite lock" in caplog.text
    assert engine._lifecycle.get_by_conversation("lane").current_session_id == "session"
    assert engine._store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 750
    assert engine._lifecycle._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 900
    engine.on_session_end("session", messages)
    engine.on_session_end("session", messages)
    assert [row["content"] for row in engine._store.get_session_messages("session")] == [messages[0]["content"]]
    assert engine._lifecycle.get_by_conversation("lane").current_session_id is None


def test_finalization_uses_bounded_500ms_and_restores_timeouts_on_error(engine, monkeypatch):
    def fail_after_inspecting(messages):
        assert engine._store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 500
        assert engine._lifecycle._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 500
        raise ValueError("synthetic unrelated failure")
    monkeypatch.setattr(engine, "_ingest_messages", fail_after_inspecting)
    with pytest.raises(ValueError, match="synthetic unrelated failure"):
        engine.on_session_end("session", [{"role":"user", "content":"end"}])
    assert engine._store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 750
    assert engine._lifecycle._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 900
