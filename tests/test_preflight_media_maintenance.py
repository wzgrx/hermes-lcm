"""Regression for upstream PR #516's media-heavy preflight trigger."""

import hermes_lcm.engine as lcm_engine

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
from hermes_lcm.tokens import count_messages_tokens


def test_externalized_replay_below_threshold_defers_summary_but_persists_payload(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=1,
            large_output_externalization_enabled=True,
            large_output_externalization_threshold_chars=50,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("media-session", platform="telegram", context_length=1_000_000)
    engine.threshold_tokens = 750_000
    messages = [
        {"role": "user", "content": "older media data:image/png;base64," + "A" * 1024},
        {"role": "user", "content": "current request"},
    ]
    summary_calls = []

    def summarize_spy(*args, **kwargs):
        summary_calls.append((args, kwargs))
        raise AssertionError("under-threshold cleanup must not call the summary provider")

    monkeypatch.setattr(lcm_engine, "summarize_with_escalation", summarize_spy)
    try:
        assert count_messages_tokens(messages) < engine.threshold_tokens
        assert engine.should_compress_preflight(messages) is False
        assert summary_calls == []
        assert engine._dag.get_session_node_count("media-session") == 0
        rows = engine._store.get_session_messages("media-session")
        assert len(rows) == 2
        assert "Externalized" in str(rows[0]["content"])
        assert rows[-1]["content"] == "current request"
        assert engine.should_compress_preflight(messages) is False
        assert engine._store.get_session_count("media-session") == 2
        # The same protected replay may still trigger a required pass when
        # actual context pressure reaches the configured threshold.
        engine.threshold_tokens = 1
        assert engine.should_compress_preflight(messages) is True
    finally:
        engine.shutdown()
