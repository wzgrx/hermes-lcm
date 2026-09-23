"""Regression target for upstream #585's store-vs-window compaction gap."""

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.dag import SummaryNode
from hermes_lcm.engine import LCMEngine
from hermes_lcm.externalize import externalize_ingest_payload


def test_hidden_store_prefix_bounds_stop_before_active_and_protected_tail(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=2,
            leaf_chunk_tokens=100,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        older = [
            {"role": "user", "content": f"older {n} " + "detail " * 20}
            for n in range(5)
        ]
        old_ids = engine._store.append_batch(
            "session", older, [40] * len(older),
            source="telegram", conversation_id="conversation",
        )
        engine._store.append(
            "other", {"role": "user", "content": "unrelated"}, token_estimate=1000,
        )
        active = [
            {"role": "system", "content": "system prompt"},
            {"role": "user", "content": "latest one"},
            {"role": "user", "content": "latest two"},
        ]
        engine._ingest_messages(active)

        bounds = engine._hidden_store_prefix_upper_bound(active)
        assert bounds is not None
        assert bounds["messages"] == len(older)
        assert bounds["estimated_tokens"] == 200
        assert bounds["first_store_id"] == old_ids[0]
        assert bounds["last_store_id"] == old_ids[-1]
        assert bounds["before_store_id"] == engine._store.get_session_tail("session", 3)[0]["store_id"]
        assert bounds["first_tail_store_id"] > bounds["before_store_id"]

        loaded = engine._load_hidden_store_leaf_chunk(active)
        assert loaded is not None
        chunk, direct_ids, chunk_bounds = loaded
        assert chunk_bounds == bounds
        assert direct_ids[id(chunk[0])] == old_ids[0]
        assert all(store_id in old_ids for store_id in direct_ids.values())
        assert all("latest" not in str(message.get("content")) for message in chunk)
    finally:
        engine.shutdown()


def test_repeated_active_text_maps_to_recent_store_copy(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        old_ids = engine._store.append_batch(
            "session",
            [{"role": "user", "content": "same literal"} for _ in range(10)],
            [10] * 10,
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "same literal"}]
        engine._ingest_messages(active)
        bounds = engine._hidden_store_prefix_upper_bound(active)
        assert bounds is not None
        assert bounds["messages"] == 10
        assert bounds["last_store_id"] == old_ids[-1]
        assert bounds["first_active_store_id"] > old_ids[-1]
    finally:
        engine.shutdown()


def test_large_active_window_without_hidden_prefix_has_no_store_candidate(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=4,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        active = [
            {"role": "user", "content": f"visible {n} " + "detail " * 20}
            for n in range(80)
        ]
        engine._ingest_messages(active)
        assert engine._hidden_store_prefix_upper_bound(active) is None
        assert engine._load_hidden_store_leaf_chunk(active) is None
    finally:
        engine.shutdown()


def test_hidden_store_chunk_keeps_assistant_tool_group_complete(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        group = [
            {
                "role": "assistant",
                "content": "calling both tools " + "detail " * 25,
                "tool_calls": [
                    {"id": "a", "function": {"name": "one", "arguments": "{}"}},
                    {"id": "b", "function": {"name": "two", "arguments": "{}"}},
                ],
            },
            {"role": "tool", "tool_call_id": "a", "content": "first result"},
            {"role": "tool", "tool_call_id": "b", "content": "second result"},
        ]
        group_ids = engine._store.append_batch(
            "session", group, [40, 5, 5],
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine._ingest_messages(active)
        loaded = engine._load_hidden_store_leaf_chunk(active)
        assert loaded is not None
        chunk, direct_ids, _bounds = loaded
        assert [message["role"] for message in chunk] == ["assistant", "tool", "tool"]
        assert [direct_ids[id(message)] for message in chunk] == group_ids
    finally:
        engine.shutdown()


def test_missing_externalized_sidecar_blocks_store_backed_leaf(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
            deferred_maintenance_enabled=True,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        placeholder = (
            "[Externalized payload: kind=raw_payload; role=user; "
            "chars=1000; bytes=1000; ref=missing-sidecar.json]"
        )
        old_id = engine._store.append(
            "session", {"role": "user", "content": placeholder},
            token_estimate=100, source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine.threshold_tokens = 1
        engine._ingest_messages(active)
        assert engine._load_hidden_store_leaf_chunk(active) is None
        assert engine.should_compress_preflight(active) is False
        assert engine._store.get(old_id)["content"] == placeholder
        assert engine._dag.get_session_node_count("session") == 0
    finally:
        engine.shutdown()


def test_valid_externalized_sidecar_is_restored_for_hidden_summary(tmp_path):
    hermes_home = str(tmp_path / "hermes")
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=hermes_home,
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        source_text = "DURABLE_SOURCE_NEEDLE " + "detail " * 100
        sidecar = externalize_ingest_payload(
            source_text,
            role="user",
            session_id="session",
            config=engine._config,
            hermes_home=hermes_home,
        )
        assert sidecar is not None
        engine._store.append(
            "session", {"role": "user", "content": sidecar["placeholder"]},
            token_estimate=100, source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine._ingest_messages(active)
        loaded = engine._load_hidden_store_leaf_chunk(active)
        assert loaded is not None
        chunk, _direct_ids, _bounds = loaded
        assert chunk[0]["content"] == source_text
    finally:
        engine.shutdown()


def test_hidden_store_chunk_skips_rows_already_covered_by_a_leaf(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        old_ids = engine._store.append_batch(
            "session",
            [{"role": "user", "content": f"older {n} " + "detail " * 20} for n in range(6)],
            [40] * 6,
            source="telegram", conversation_id="conversation",
        )
        engine._dag.add_node(SummaryNode(
            session_id="session", depth=0, summary="already covered",
            token_count=3, source_token_count=80,
            source_ids=old_ids[:2], source_type="messages",
        ))
        active = [{"role": "user", "content": "current"}]
        engine._ingest_messages(active)
        loaded = engine._load_hidden_store_leaf_chunk(active)
        assert loaded is not None
        chunk, direct_ids, _bounds = loaded
        assert direct_ids[id(chunk[0])] == old_ids[2]
        assert not set(direct_ids.values()).intersection(old_ids[:2])
    finally:
        engine.shutdown()


def test_hidden_store_chunk_keeps_late_tool_result_after_covered_parent(tmp_path):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        parent = {
            "role": "assistant", "content": "calling a tool",
            "tool_calls": [{"id": "late", "function": {"name": "lookup", "arguments": "{}"}}],
        }
        late_tool = {"role": "tool", "tool_call_id": "late", "content": "important late result " + "detail " * 30}
        ids = engine._store.append_batch(
            "session", [parent, late_tool], [10, 100],
            source="telegram", conversation_id="conversation",
        )
        engine._dag.add_node(SummaryNode(
            session_id="session", depth=0, summary="parent already covered",
            token_count=3, source_token_count=10,
            source_ids=ids[:1], source_type="messages",
        ))
        active = [{"role": "user", "content": "current"}]
        engine._ingest_messages(active)
        loaded = engine._load_hidden_store_leaf_chunk(active)
        assert loaded is not None
        chunk, direct_ids, _bounds = loaded
        assert chunk[0]["role"] == "tool"
        assert direct_ids[id(chunk[0])] == ids[1]
    finally:
        engine.shutdown()


def test_store_backed_rescue_cannot_publish_half_of_tool_group(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        parent = {
            "role": "assistant", "content": "calling a tool " + "detail " * 30,
            "tool_calls": [{"id": "call", "function": {"name": "lookup", "arguments": "{}"}}],
        }
        result = {"role": "tool", "tool_call_id": "call", "content": "tool result"}
        engine._store.append_batch(
            "session", [parent, result], [100, 5],
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine.threshold_tokens = 1

        def truncated_summary(chunk, **_kwargs):
            return chunk[:1], 30, "A truncated summary.", 1, 2

        monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", truncated_summary)
        with pytest.raises(RuntimeError, match="assistant tool group"):
            engine.compress(active)
        assert engine._dag.get_session_nodes("session") == []
        assert engine._lifecycle.get_by_conversation("conversation").current_frontier_store_id == 0
    finally:
        engine.shutdown()


def test_hidden_ignored_row_stays_out_of_summary_but_is_retained_raw(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
            ignore_message_patterns=["SECRET"],
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        rows = [
            {"role": "user", "content": "SECRET old ignored row " + "detail " * 30},
            {"role": "user", "content": "ordinary older context " + "detail " * 30},
        ]
        old_ids = engine._store.append_batch(
            "session", rows, [100, 100],
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine.threshold_tokens = 1
        seen = []

        def summarize_stub(chunk, **_kwargs):
            seen.extend(str(message.get("content")) for message in chunk)
            return chunk, 50, "Ordinary older context summarized.", 1, 1

        monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", summarize_stub)
        assert engine.should_compress_preflight(active) is True
        engine.compress(active)
        assert seen and all("SECRET" not in content for content in seen)
        leaves = [node for node in engine._dag.get_session_nodes("session") if node.depth == 0]
        assert old_ids[0] not in leaves[0].source_ids
        assert old_ids[1] in leaves[0].source_ids
        assert engine._store.get(old_ids[0])["content"] == rows[0]["content"]
    finally:
        engine.shutdown()


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


def test_hidden_store_prefix_keeps_debt_until_all_rows_are_covered(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=80,
            threshold_full_sweep_enabled=False,
            deferred_maintenance_enabled=True,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start(
        "session", platform="telegram", context_length=10_000,
        conversation_id="conversation",
    )
    try:
        old = [
            {"role": "user", "content": f"older {n}: " + "detail " * 20}
            for n in range(12)
        ]
        old_ids = engine._store.append_batch(
            "session", old, [40] * len(old),
            source="telegram", conversation_id="conversation",
        )
        active = [
            {"role": "system", "content": "stable system"},
            {"role": "user", "content": "current request"},
        ]
        engine.threshold_tokens = 1
        monkeypatch.setattr(
            engine, "_summarize_leaf_chunk_with_rescue",
            lambda chunk, **_kwargs: (chunk, 80, "Condensed older turns.", 1, 1),
        )
        monkeypatch.setattr(engine, "_maybe_condense", lambda **_kwargs: None)

        assert engine.should_compress_preflight(active) is True
        active = engine.compress(active)
        first_frontier = engine._lifecycle.get_by_conversation(
            "conversation"
        ).current_frontier_store_id
        assert old_ids[0] <= first_frontier < old_ids[-1]
        assert engine._lifecycle.get_by_conversation("conversation").debt_kind == "raw_backlog"

        for _ in range(12):
            if engine._lifecycle.get_by_conversation("conversation").current_frontier_store_id >= old_ids[-1]:
                break
            assert engine.should_compress_preflight(active) is True
            active = engine.compress(active)

        assert engine._lifecycle.get_by_conversation("conversation").current_frontier_store_id >= old_ids[-1]
        assert engine._hidden_store_prefix_upper_bound(active) is None
        assert any("current request" in str(msg.get("content")) for msg in active)
        covered_ids = {
            store_id
            for node in engine._dag.get_session_nodes("session")
            if node.depth == 0
            for store_id in node.source_ids
        }
        assert set(old_ids).issubset(covered_ids)
        assert engine._store.get_session_count("session") >= len(old_ids)
    finally:
        engine.shutdown()


def test_hidden_leaf_rescue_shrink_does_not_advance_past_unconsumed_rows(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=80,
            deferred_maintenance_enabled=True,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    try:
        old_ids = engine._store.append_batch(
            "session",
            [{"role": "user", "content": f"older {n} " + "detail " * 30} for n in range(5)],
            [40] * 5,
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine.threshold_tokens = 1

        def shrink_summary(chunk, **_kwargs):
            return chunk[:1], 40, "First row only.", 1, 2

        monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", shrink_summary)
        assert engine.should_compress_preflight(active) is True
        active = engine.compress(active)
        state = engine._lifecycle.get_by_conversation("conversation")
        assert state.current_frontier_store_id == old_ids[0]
        assert not any("older 1" in str(message.get("content")) for message in active)
        assert engine.should_compress_preflight(active) is True
        assert engine._store.get(old_ids[1])["content"].startswith("older 1")
    finally:
        engine.shutdown()


def test_restart_resumes_after_published_hidden_leaf_without_duplicate_sources(tmp_path, monkeypatch):
    config = LCMConfig(
        database_path=str(tmp_path / "lcm.db"),
        fresh_tail_count=1,
        leaf_chunk_tokens=80,
        deferred_maintenance_enabled=True,
    )
    hermes_home = str(tmp_path / "hermes")
    engine = LCMEngine(config=config, hermes_home=hermes_home)
    engine.on_session_start("session", platform="telegram", conversation_id="conversation")
    old_ids = engine._store.append_batch(
        "session",
        [{"role": "user", "content": f"older {n} " + "detail " * 30} for n in range(8)],
        [40] * 8,
        source="telegram", conversation_id="conversation",
    )
    active = [{"role": "user", "content": "current"}]
    monkeypatch.setattr(
        engine, "_summarize_leaf_chunk_with_rescue",
        lambda chunk, **_kwargs: (chunk, 80, "First published leaf.", 1, 1),
    )
    engine.threshold_tokens = 1
    try:
        assert engine.should_compress_preflight(active) is True
        active = engine.compress(active)
        first_node = [node for node in engine._dag.get_session_nodes("session") if node.depth == 0][0]
        first_sources = set(first_node.source_ids)
        first_frontier = engine._lifecycle.get_by_conversation("conversation").current_frontier_store_id
        assert old_ids[0] in first_sources
        assert first_frontier < old_ids[-1]
    finally:
        engine.shutdown()

    resumed = LCMEngine(config=config, hermes_home=hermes_home)
    resumed.on_session_start("session", platform="telegram", conversation_id="conversation")
    resumed.threshold_tokens = 1
    monkeypatch.setattr(
        resumed, "_summarize_leaf_chunk_with_rescue",
        lambda chunk, **_kwargs: (chunk, 80, "Second published leaf.", 1, 1),
    )
    try:
        assert resumed._lifecycle.get_by_conversation("conversation").current_frontier_store_id == first_frontier
        assert resumed.should_compress_preflight(active) is True
        resumed.compress(active)
        nodes = [node for node in resumed._dag.get_session_nodes("session") if node.depth == 0]
        assert len(nodes) >= 2
        assert first_sources.isdisjoint(nodes[1].source_ids)
        assert resumed._store.get(old_ids[0])["content"].startswith("older 0")
    finally:
        resumed.shutdown()


def test_session_rollover_during_hidden_summary_rolls_back_publication(tmp_path, monkeypatch):
    engine = LCMEngine(
        config=LCMConfig(
            database_path=str(tmp_path / "lcm.db"),
            fresh_tail_count=1,
            leaf_chunk_tokens=20,
        ),
        hermes_home=str(tmp_path / "hermes"),
    )
    engine.on_session_start(
        "session", platform="telegram", context_length=10_000,
        conversation_id="conversation",
    )
    try:
        old_ids = engine._store.append_batch(
            "session",
            [{"role": "user", "content": "older " + "detail " * 20} for _ in range(4)],
            [40] * 4,
            source="telegram", conversation_id="conversation",
        )
        active = [{"role": "user", "content": "current"}]
        engine.threshold_tokens = 1
        assert engine.should_compress_preflight(active) is True

        def summarize_after_rollover(chunk, **_kwargs):
            engine._lifecycle.bind_session("other", conversation_id="conversation")
            return chunk, 40, "stale summary", 1, 1

        monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", summarize_after_rollover)
        with pytest.raises(RuntimeError, match="session binding changed"):
            engine.compress(active)
        assert engine._dag.get_session_node_count("session") == 0
        assert engine._store.get(old_ids[0])["content"].startswith("older")
        assert engine._lifecycle.get_by_conversation("conversation").current_session_id == "other"
    finally:
        engine.shutdown()
