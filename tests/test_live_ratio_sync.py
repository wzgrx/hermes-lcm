"""Host live ratio adoption respects LCM authority and runtime isolation."""

import sys
import importlib.util
import logging
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


@pytest.fixture
def engine(tmp_path):
    cfg = LCMConfig(database_path=str(tmp_path / "lcm.db"), context_threshold=0.85,
        max_assembly_tokens=0, reserve_tokens_floor=0, fresh_tail_max_tokens=0)
    cfg.config_sources["context_threshold"] = "config_yaml:compression.threshold"
    e = LCMEngine(cfg, hermes_home=str(tmp_path))
    e.update_model("model-a", 1_000_000, provider="test")
    yield e
    e.shutdown(wait_for_background_work=True)


def sync(e, value, *, overrides=None, cap=None):
    # These are the real host's ordered assignments. The final invalidation is
    # the sync boundary; values are not a request to rewrite user configuration.
    e.model_thresholds = overrides or {}
    e._config_threshold_percent = e._configured_threshold_percent = value
    e._base_threshold_percent = value
    e.threshold_percent = value
    e.threshold_tokens_cap = cap
    e._threshold_tokens = e._tail_token_budget = None


def test_ratio_update_and_removed_key_default_affect_real_trigger(engine):
    sync(engine, 0.60)
    assert engine.threshold_tokens == 600_000
    assert engine.should_compress(600_000) is True
    assert engine.should_compress(599_999) is False
    assert engine.context_threshold == engine.threshold_percent == 0.60
    # The host resolves an absent threshold to its constructor/model default.
    sync(engine, 0.50)
    assert engine.threshold_tokens == 500_000
    assert engine._config.context_threshold == 0.85
    assert engine._context_threshold_source == "host_live_compression"


@pytest.mark.parametrize("source", ["env:LCM_CONTEXT_THRESHOLD", "config_yaml:lcm.context_threshold", "manual_or_default", ""])
def test_explicit_or_manual_lcm_ratio_remains_authoritative(engine, source):
    engine._config.config_sources["context_threshold"] = source
    sync(engine, 0.30)
    assert engine.context_threshold == engine.threshold_percent == 0.85
    assert engine.threshold_tokens == 850_000


@pytest.mark.parametrize("value", [None, True, False, float("nan"), float("inf"), float("-inf"), 0, -1, 2, "bad"])
def test_bad_live_ratio_preserves_last_valid_policy(engine, value):
    sync(engine, 0.65)
    sync(engine, value)
    assert engine.context_threshold == engine.threshold_percent == 0.65
    assert engine.threshold_tokens == 650_000


def test_stricter_cap_and_assembly_remain_independent(engine):
    engine._config.max_assembly_tokens = 400_000
    sync(engine, 0.70, cap=300_000)
    assert engine.threshold_tokens == 300_000
    engine.threshold_tokens_cap = None
    assert engine.threshold_tokens == 400_000
    assert engine.context_threshold == 0.70


def test_model_overrides_recompute_after_route_change_and_remove(engine, monkeypatch):
    module = ModuleType("agent.context_compressor")
    module.resolve_model_threshold = lambda model, overrides, default, provider="": overrides.get(model, default)
    monkeypatch.setitem(sys.modules, "agent.context_compressor", module)
    overrides = {"model-a": 0.60, "model-b": 0.75, "bad": float("inf")}
    sync(engine, 0.50, overrides=overrides)
    assert engine.threshold_tokens == 600_000
    overrides["model-a"] = 0.20  # caller dictionary must not mutate copied policy
    engine.update_model("model-b", 200_000, provider="test")
    assert engine.threshold_tokens == 150_000
    engine.update_model("model-a", 1_000_000, provider="test")
    assert engine.threshold_tokens == 600_000
    sync(engine, 0.50)
    assert engine.threshold_tokens == 500_000


def test_clone_inherits_independent_live_policy(engine):
    sync(engine, 0.60)
    clone = engine.clone_for_agent()
    try:
        assert clone.threshold_tokens == 600_000
        sync(clone, 0.70)
        assert clone.threshold_tokens == 700_000
        assert engine.threshold_tokens == 600_000
    finally:
        clone.shutdown(wait_for_background_work=True)


def test_profile_rebind_drops_live_ratio_and_stale_host_inputs(engine, tmp_path):
    sync(engine, 0.60)
    profile = tmp_path / "profile-b"
    profile.mkdir()
    assert engine._rebind_storage_for_home(str(profile))
    assert engine.threshold_tokens == 850_000
    engine._threshold_tokens = None  # stale invalidation must not revive old ratio
    assert engine.threshold_tokens == 850_000


def test_ratio_sync_preserves_cooldown_and_unknown_context(engine):
    engine.record_rejected_compaction()
    before = engine._host_rejection_snapshot()
    sync(engine, 0.60)
    assert engine._host_rejection_snapshot() == before
    engine._set_context_length(0, source="unknown")
    sync(engine, 0.75)
    assert engine.threshold_tokens == 0


def test_live_ratio_preserves_fresh_tail_floor(engine):
    engine._config.fresh_tail_max_tokens = 60_000
    engine.update_model("small-model", 100_000)
    sync(engine, 0.01)
    assert engine.context_threshold > 0.01
    assert engine._context_threshold_autoraised["guard"] == "fresh_tail_floor_guard"


@pytest.fixture
def host_sync(monkeypatch):
    path = Path(__file__).parent / "fixtures/hermes_live_sync_af90026.py"
    spec = importlib.util.spec_from_file_location("hermes_live_ratio_fixture", path)
    fixture = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(fixture)
    fixture.logger = logging.getLogger(__name__)
    fixture.is_truthy_value = bool
    fixture._compressor_ctor_default = lambda name, fallback: fallback
    fixture._default_threshold_tokens_cap = lambda: None
    fixture._derived_default_threshold_percent = lambda agent, compression: 0.50
    init = ModuleType("agent.agent_init")
    # Test host ratio semantics, not route/pin scoping or metadata discovery.
    init.config_context_length_for_runtime = lambda agent, cfg: None
    init.set_config_context_length = lambda agent, value: setattr(agent.context_compressor, "_config_context_length", value)
    compressor = ModuleType("agent.context_compressor")
    compressor.resolve_model_threshold = fixture.resolve_model_threshold
    monkeypatch.setitem(sys.modules, "agent.agent_init", init)
    monkeypatch.setitem(sys.modules, "agent.context_compressor", compressor)
    return fixture._apply_live_compression_config


def test_exact_host_function_updates_and_removes_ratio_and_cap(engine, host_sync):
    agent = SimpleNamespace(context_compressor=engine, model=engine.model, provider=engine.provider)
    host_sync(agent, {"compression":{"threshold":0.60, "threshold_tokens":400_000}})
    assert engine.threshold_tokens == 400_000
    assert engine.context_threshold == 0.60
    host_sync(agent, {"compression":{"threshold":0.70, "threshold_tokens":None}})
    assert engine.threshold_tokens == 700_000
    host_sync(agent, {})
    assert engine.threshold_tokens == 500_000


def test_exact_host_provider_scoped_override_does_not_leak(engine, host_sync):
    agent = SimpleNamespace(context_compressor=engine, model=engine.model, provider="test")
    cfg = {"compression":{"threshold":0.50, "model_thresholds":{"model":0.60,"test:model":0.75}}}
    host_sync(agent, cfg)
    assert engine.threshold_tokens == 750_000
    engine.update_model("model-a", 1_000_000, provider="other")
    assert engine.threshold_tokens == 600_000
    engine.update_model("unrelated", 1_000_000, provider="other")
    assert engine.threshold_tokens == 500_000


def test_exact_host_invalid_and_explicit_lcm_ratio(engine, host_sync):
    agent = SimpleNamespace(context_compressor=engine, model=engine.model, provider=engine.provider)
    host_sync(agent, {"compression":{"threshold":0.60}})
    host_sync(agent, {"compression":{"threshold":float("nan")}})
    assert engine.threshold_tokens == 600_000
    engine._config.config_sources["context_threshold"] = "config_yaml:lcm.context_threshold"
    host_sync(agent, {"compression":{"threshold":0.30}})
    assert engine.threshold_tokens == 850_000


def test_exact_host_codex_model_override_is_not_default_autoraised(engine, host_sync):
    engine.update_model("gpt-5.5", 1_000_000, provider="openai-codex")
    agent = SimpleNamespace(context_compressor=engine, model=engine.model, provider=engine.provider)
    host_sync(agent, {"compression":{"threshold":0.50, "model_thresholds":{"openai-codex:gpt-5.5":0.60}}})
    assert engine.context_threshold == 0.60
    assert engine.threshold_tokens == int(engine.context_length * 0.60)
    assert engine._context_threshold_source == "host_live_compression:model_threshold"
    assert engine._context_threshold_autoraised is None
