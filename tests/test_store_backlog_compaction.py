"""Regression target for upstream #585's store-vs-window compaction gap."""

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


@pytest.mark.xfail(
    strict=True,
    reason="upstream #585: an unreflected store prefix is absent from the assembled window",
)
def test_hidden_store_prefix_is_summarized_without_replaying_it_as_live_context(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=2,
            leaf_chunk_tokens=100,
            threshold_full_sweep_enabled=False,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start(
        "session", platform="telegram", context_length=10_000,
        conversation_id="conversation",
    )
    try:
        old_rows = [
            {"role": "user", "content": f"durable older turn {n}: " + "detail " * 30}
            for n in range(12)
        ]
        old_ids = engine._store.append_batch(
            "session", old_rows, [50] * len(old_rows),
            source="telegram", conversation_id="conversation",
        )
        active = [
            {"role": "system", "content": "You are helpful."},
            {"role": "user", "content": "recent request one"},
            {"role": "user", "content": "recent request two"},
        ]
        engine.threshold_tokens = 1

        def summarize_stub(chunk, **_kwargs):
            return chunk, 100, "Older durable turns summarized.", 1, 1

        monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", summarize_stub)
        assert engine.should_compress_preflight(active) is True
        result = engine.compress(active)

        leaves = [node for node in engine._dag.get_session_nodes("session") if node.depth == 0]
        assert leaves
        assert old_ids[0] in leaves[0].source_ids
        assert any("recent request two" in str(msg.get("content")) for msg in result)
        assert engine._store.get(old_ids[0])["content"] == old_rows[0]["content"]
        state = engine._lifecycle.get_by_conversation("conversation")
        assert state.current_frontier_store_id >= old_ids[0]
    finally:
        engine.shutdown()
