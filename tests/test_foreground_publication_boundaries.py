"""Fork hidden-backlog and cancellation boundaries for guarded publication."""

import copy
import sqlite3

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.dag import SummaryNode
from hermes_lcm.engine import LCMEngine
from hermes_lcm.rollup_store import RollupStore


@pytest.fixture
def engine(tmp_path):
    instance = LCMEngine(LCMConfig(
        database_path=str(tmp_path / "boundary.db"), fresh_tail_count=2,
        leaf_chunk_tokens=100, dynamic_leaf_chunk_enabled=False,
        threshold_full_sweep_enabled=False,
    ), hermes_home=str(tmp_path / "profile"))
    instance.on_session_start("session", platform="telegram", conversation_id="lane")
    instance.threshold_tokens = 1
    try:
        yield instance
    finally:
        instance.shutdown()


@pytest.mark.parametrize("timing", ["after_load", "during_model"])
def test_hidden_source_changes_reject_stale_publication(engine, monkeypatch, timing):
    ids = engine._store.append_batch(
        "session", [{"role": "user", "content": f"old {n} " + "detail " * 30}
                    for n in range(12)], [50] * 12,
        source="telegram", conversation_id="lane",
    )
    active = [{"role": "system", "content": "helpful"},
              {"role": "user", "content": "recent one"},
              {"role": "user", "content": "recent two"}]
    original = copy.deepcopy(active)
    calls = []

    def mutate():
        with sqlite3.connect(engine._store.db_path) as writer:
            writer.execute("UPDATE messages SET content='concurrent change' WHERE store_id=?", (ids[0],))

    load = engine._load_hidden_store_leaf_chunk

    def load_and_change(messages):
        result = load(messages)
        assert result is not None
        mutate()
        return result

    def summarize(chunk, **kwargs):
        calls.append(True)
        assert "old 0" in chunk[0]["content"]
        if timing == "during_model":
            mutate()
        return chunk, 100, "stale hidden summary", 1, 0

    if timing == "after_load":
        monkeypatch.setattr(engine, "_load_hidden_store_leaf_chunk", load_and_change)
    monkeypatch.setattr(engine, "_summarize_leaf_chunk_with_rescue", summarize)
    with pytest.raises(RuntimeError, match="changed"):
        engine.compress(active)
    assert bool(calls) == (timing == "during_model")
    assert engine._dag.get_session_nodes("session") == []
    assert engine._lifecycle.get_by_conversation("lane").current_frontier_store_id == 0
    assert engine._last_compacted_store_id == 0
    assert active == original
    assert engine._store.get(ids[0])["content"] == "concurrent change"


@pytest.mark.parametrize("field", ["_session_id", "_conversation_id", "_hermes_home"])
def test_runtime_rebinding_rejects_publication(engine, monkeypatch, field):
    ids = engine._store.append_batch(
        "session", [{"role": "user", "content": "source fact"}], [10],
        source="telegram", conversation_id="lane",
    )
    snapshot, validate = engine._prepare_summary_publication(ids, "messages")
    node = SummaryNode(session_id="session", summary="stale summary", source_ids=ids)
    with monkeypatch.context() as patch:
        patch.setattr(engine, field, "new-runtime")
        with pytest.raises(RuntimeError, match="runtime changed"):
            engine._dag.publish_node(node, snapshot, frontier_store_id=ids[0], validate_runtime=validate)
    assert node.node_id == 0
    assert engine._dag.get_session_nodes("session") == []
    assert engine._lifecycle.get_by_conversation("lane").current_frontier_store_id == 0
    assert not engine._dag._conn.in_transaction


def test_cancellation_after_insert_rolls_back_all_publication_effects(engine):
    ids = engine._store.append_batch(
        "session", [{"role": "user", "content": "source fact"}], [10],
        source="telegram", conversation_id="lane",
    )
    RollupStore(engine._store.db_path).close()
    snapshot = engine._dag.publication_snapshot(ids, "messages", "lane")
    node = SummaryNode(session_id="session", summary="faithful summary", source_ids=ids)
    validations = []

    def interrupt_before_commit():
        validations.append(True)
        if len(validations) == 2:
            # The transaction has inserted the node and updated its frontier.
            assert engine._dag._conn.execute("SELECT COUNT(*) FROM summary_nodes").fetchone()[0] == 1
            raise KeyboardInterrupt("cancel publication")

    with pytest.raises(KeyboardInterrupt, match="cancel publication"):
        engine._dag.publish_node(node, snapshot, frontier_store_id=ids[0], validate_runtime=interrupt_before_commit)
    with sqlite3.connect(engine._store.db_path) as reader:
        assert reader.execute("SELECT COUNT(*) FROM summary_nodes").fetchone()[0] == 0
        assert reader.execute("SELECT COUNT(*) FROM nodes_fts WHERE nodes_fts MATCH 'faithful'").fetchone()[0] == 0
        assert reader.execute("SELECT COUNT(*) FROM lcm_rollup_invalidations").fetchone()[0] == 0
    assert engine._lifecycle.get_by_conversation("lane").current_frontier_store_id == 0
    assert node.node_id == 0
    assert not engine._dag._conn.in_transaction
