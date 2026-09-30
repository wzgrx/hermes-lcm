"""Regression coverage for upstream #645: compression must not replay durable tail."""

import pytest

import hermes_lcm.engine as lcm_engine
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


@pytest.mark.parametrize("repeat_noop", [False, True])
def test_in_place_compression_keeps_tail_ingestion_exactly_once(tmp_path, monkeypatch, repeat_noop):
    monkeypatch.setattr(
        lcm_engine, "summarize_with_escalation",
        lambda **kwargs: ("Stored discussion summary. Expand for the original discussion.", 1),
    )
    engine = LCMEngine(config=LCMConfig(
        database_path=str(tmp_path / "same-session.db"),
        fresh_tail_count=4,
        leaf_chunk_tokens=100,
    ), hermes_home=str(tmp_path / "home"))
    try:
        engine.on_session_start("same-session", conversation_id="same-conversation", context_length=200_000)
        messages = [{"role": "system", "content": "System prompt."}]
        for turn in range(12):
            messages.extend([
                {"role": "user", "content": f"Question {turn}: " + "original payload " * 20},
                {"role": "assistant", "content": f"Answer {turn}: " + "original response " * 20},
            ])
        compressed = engine.compress(messages)
        assert len(compressed) < len(messages)
        original_rows = engine._store.get_session_messages("same-session")
        for _ in range(3 if repeat_noop else 1):
            engine.on_session_start(
                "same-session", boundary_reason="compression", old_session_id="same-session",
                conversation_id="same-conversation", context_length=200_000,
            )
            engine.ingest(compressed)
            assert engine._store.get_session_messages("same-session") == original_rows
            if repeat_noop:
                compressed = engine.compress(compressed)
        fresh = {"role": "user", "content": "A genuinely new turn after compression."}
        engine.ingest(compressed + [fresh])
        rows = engine._store.get_session_messages("same-session")
        assert rows[:-1] == original_rows
        assert rows[-1]["content"] == fresh["content"]
    finally:
        engine.shutdown()