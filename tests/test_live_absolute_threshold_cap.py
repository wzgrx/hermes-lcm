"""Consume host-assigned absolute trigger caps without changing LCM policy."""

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


@pytest.fixture
def engine(tmp_path):
    e = LCMEngine(config=LCMConfig(database_path=str(tmp_path / "lcm.db"),
        context_threshold=0.85, max_assembly_tokens=0, reserve_tokens_floor=0),
        hermes_home=str(tmp_path))
    e.update_model("test-model", 1_000_000, provider="test")
    yield e
    # Plugin unload shuts down process-wide recall workers/pools. These tests
    # own one engine, not the plugin lifetime shared by the following tests.
    e.shutdown(wait_for_background_work=True)


def test_live_cap_affects_trigger_and_removal_restores_ratio(engine):
    assert engine.threshold_tokens == 850_000
    engine.threshold_tokens_cap = engine._coerce_threshold_tokens_cap("500000")
    # Exact invalidation slots written by Hermes' live config function.
    engine._threshold_tokens = engine._tail_token_budget = None
    assert engine.threshold_tokens == 500_000
    assert engine.should_compress(500_000) is True
    assert engine.should_compress(499_999) is False
    engine.threshold_tokens_cap = None
    engine._threshold_tokens = engine._tail_token_budget = None
    assert engine.threshold_tokens == 850_000
    assert engine.should_compress(500_000) is False
    assert engine._config.context_threshold == 0.85


def test_cap_never_raises_stricter_assembly_trigger(engine):
    engine._config.max_assembly_tokens = 300_000
    engine.update_model("test-model", 1_000_000)
    engine.threshold_tokens_cap = 500_000
    assert engine.threshold_tokens == 300_000
    engine.threshold_tokens_cap = 250_000
    assert engine.threshold_tokens == 250_000
    # Trigger is not a hard assembly budget.
    assert engine._effective_assembly_token_cap() == 300_000
    engine.threshold_tokens_cap = None
    assert engine.threshold_tokens == 300_000


def test_model_switch_recomputes_ratio_but_preserves_global_cap(engine):
    engine.threshold_tokens_cap = 500_000
    engine.update_model("smaller-model", 200_000)
    assert engine.threshold_tokens == 170_000
    engine.update_model("larger-model", 2_000_000)
    assert engine.threshold_tokens == 500_000
    engine.threshold_tokens_cap = None
    assert engine.threshold_tokens == 1_700_000


def test_clone_copies_cap_but_not_mutable_policy(engine):
    engine.threshold_tokens_cap = 500_000
    clone = engine.clone_for_agent()
    try:
        assert clone.threshold_tokens == 500_000
        clone.threshold_tokens_cap = 600_000
        assert engine.threshold_tokens == 500_000
        assert clone.threshold_tokens == 600_000
    finally:
        clone.shutdown(wait_for_background_work=True)


def test_profile_rebind_clears_old_live_cap(engine, tmp_path):
    engine.threshold_tokens_cap = 500_000
    new_home = tmp_path / "profile-b"
    new_home.mkdir()
    assert engine._rebind_storage_for_home(str(new_home))
    assert engine.threshold_tokens == 850_000
    assert engine.threshold_tokens_cap is None


@pytest.mark.parametrize("value", [None, 0, -1, "bad", float("inf"), float("nan")])
def test_invalid_cap_does_not_destroy_base_threshold(engine, value):
    engine.threshold_tokens_cap = value
    assert engine.threshold_tokens == 850_000


def test_unknown_window_stays_disabled_despite_cap(engine):
    engine._set_context_length(0, source="unknown")
    engine.threshold_tokens_cap = 500_000
    assert engine.threshold_tokens == 0
    assert engine.should_compress(600_000) is False


def test_effective_cap_supports_host_status_display(engine):
    engine.threshold_tokens_cap = 2_000_000
    assert engine._effective_threshold_cap(engine.context_length) == 1_000_000
    engine.threshold_tokens_cap = None
    assert engine._effective_threshold_cap(engine.context_length) is None
