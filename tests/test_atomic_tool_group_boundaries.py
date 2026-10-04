"""Maintained-fork restart and overflow boundaries for upstream PR #657."""
import copy
import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.tokens import count_messages_tokens


def exchange():
    return [
        {"role": "user", "content": "Compare both files."},
        {"role": "assistant", "content": "", "tool_calls": [
            {"id": "a", "function": {"name": "read", "arguments": "{}"}},
            {"id": "b", "function": {"name": "read", "arguments": "{}"}},
        ]},
        {"role": "tool", "tool_call_id": "a", "content": "A output " * 80},
        {"role": "tool", "tool_call_id": "b", "content": "B output " * 80},
    ]


@pytest.fixture
def engine(tmp_path):
    obj = LCMEngine(LCMConfig(database_path=str(tmp_path / "lcm.db")))
    obj._session_id = "atomic-boundary"
    obj.compression_count = 1
    try:
        yield obj
    finally:
        obj.shutdown()


def test_whole_exchange_at_every_cap_boundary(engine):
    tail = exchange()
    original = copy.deepcopy(tail)
    for cap in range(0, count_messages_tokens(tail) + 2):
        result = engine._assemble_context(None, tail, assembly_cap_override=cap)
        results = [m for m in result if m["role"] == "tool"]
        assert len(results) in (0, 2), cap
        if results:
            assert results == tail[2:]
    assert tail == original


def test_later_complete_assembly_clears_dropped_group_flag(engine):
    tail = exchange()
    engine._assemble_context(None, tail, assembly_cap_override=100)
    assert engine._assembly_protected_group_dropped
    full_cap = count_messages_tokens(tail) + 10
    result = engine._assemble_context(None, tail, assembly_cap_override=full_cap)
    assert result == tail and not engine._assembly_protected_group_dropped
    engine._finalize_forced_overflow_result(tail, result, assembly_cap_override=full_cap)
    assert not engine.get_status()["overflow_recovery_failed"]


def test_uncapped_assembly_clears_previous_dropped_group_flag(engine):
    tail = exchange()
    engine._assemble_context(None, tail, assembly_cap_override=100)
    assert engine._assembly_protected_group_dropped
    engine._config.max_assembly_tokens = 0
    engine._config.reserve_tokens_floor = 0
    result = engine._assemble_context(None, tail)
    assert result == tail and not engine._assembly_protected_group_dropped


def test_overflow_log_names_lost_exchange_even_when_token_count_fits(engine, caplog):
    tail = exchange()
    cap = 100
    result = engine._assemble_context(None, tail, assembly_cap_override=cap)
    assert count_messages_tokens(result) <= cap
    engine._finalize_forced_overflow_result(tail, result, assembly_cap_override=cap)
    assert engine.get_status()["overflow_recovery_failed"]
    assert "protected_tool_group_dropped=True" in caplog.text


def test_restart_after_omitted_exchange_does_not_duplicate_raw_user(engine):
    tail = exchange()
    engine.ingest(tail)
    before = engine._store.get_session_messages(engine._session_id)
    assembled = engine._assemble_context(None, tail, assembly_cap_override=100)
    assert not any(m.get("role") == "tool" for m in assembled)
    config, session = engine._config, engine._session_id
    engine.shutdown()
    reopened = LCMEngine(config)
    try:
        reopened._session_id = session
        reopened._ingest_cursor_needs_reconcile = True
        reopened.ingest(assembled + [{"role":"user", "content":"Continue after restart."}])
        after = reopened._store.get_session_messages(session)
        assert len(after) == len(before) + 1
        assert [m["content"] for m in after].count(tail[0]["content"]) == 1
        assert after[-1]["content"] == "Continue after restart."
    finally:
        reopened.shutdown()
