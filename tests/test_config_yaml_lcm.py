"""The entire public LCM configuration surface may be set from config.yaml."""

import yaml

from hermes_lcm.config import ENV_FIELD_SPECS, LCMConfig


def _home(tmp_path, monkeypatch, yaml_text: str):
    home = tmp_path / "hermes"
    home.mkdir()
    (home / "config.yaml").write_text(yaml_text)
    monkeypatch.setenv("HERMES_HOME", str(home))
    return home


def test_lcm_yaml_scalars_env_precedence_and_provenance(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, """lcm:
  fresh_tail_count: 48
  context_threshold: 0.72
  dynamic_leaf_chunk_enabled: true
  automatic_foreground_max_seconds: 19.5
  expansion_model: example-model
  summary_timeout_ms: 30000
  codex_gpt55_autoraise_enabled: false
  unknown_setting: 9
auxiliary:
  compression:
    timeout: 12
""")
    monkeypatch.setenv("LCM_FRESH_TAIL_COUNT", "20")
    for key in (
        "LCM_CONTEXT_THRESHOLD", "LCM_DYNAMIC_LEAF_CHUNK_ENABLED",
        "LCM_AUTOMATIC_FOREGROUND_MAX_SECONDS", "LCM_EXPANSION_MODEL",
        "LCM_SUMMARY_TIMEOUT_MS",
    ):
        monkeypatch.delenv(key, raising=False)

    config = LCMConfig.from_env()

    assert config.fresh_tail_count == 20
    assert config.context_threshold == 0.72
    assert config.dynamic_leaf_chunk_enabled is True
    assert config.automatic_foreground_max_seconds == 19.5
    assert config.expansion_model == "example-model"
    assert config.summary_timeout_ms == 30000
    assert config.codex_gpt55_autoraise_enabled is False
    assert config.config_sources["fresh_tail_count"] == "env:LCM_FRESH_TAIL_COUNT"
    assert config.config_sources["summary_timeout_ms"] == "config_yaml:lcm.summary_timeout_ms"
    assert config.config_sources["automatic_foreground_max_seconds"] == "config_yaml:lcm.automatic_foreground_max_seconds"
    assert config.ignored_config_yaml_lcm_keys == ["unknown_setting"]


def test_recall_policy_opt_in_respects_yaml_and_environment_precedence(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, "lcm:\n  recall_policy_enabled: true\n")
    monkeypatch.delenv("LCM_RECALL_POLICY_ENABLED", raising=False)
    config = LCMConfig.from_env()
    assert config.recall_policy_enabled is True
    assert config.config_sources["recall_policy_enabled"] == "config_yaml:lcm.recall_policy_enabled"

    monkeypatch.setenv("LCM_RECALL_POLICY_ENABLED", "false")
    config = LCMConfig.from_env()
    assert config.recall_policy_enabled is False
    assert config.config_sources["recall_policy_enabled"] == "env:LCM_RECALL_POLICY_ENABLED"


def test_lcm_yaml_lists_mapping_and_optional_age(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, """lcm:
  ignore_session_patterns:
    - cron-*
    - scratch-*
  summary_fallback_models:
    - model-a
    - model-b
  sensitive_patterns:
    - api_key
  recall_arm_weights:
    fts: 0.25
    summary: 0.75
  empty_lifecycle_gc_max_age_hours: null
""")
    for key in (
        "LCM_IGNORE_SESSION_PATTERNS", "LCM_SUMMARY_FALLBACK_MODELS",
        "LCM_SENSITIVE_PATTERNS", "LCM_RECALL_ARM_WEIGHTS",
        "LCM_EMPTY_LIFECYCLE_GC_MAX_AGE_HOURS",
    ):
        monkeypatch.delenv(key, raising=False)

    config = LCMConfig.from_env()

    assert config.ignore_session_patterns == ["cron-*", "scratch-*"]
    assert config.summary_fallback_models == ["model-a", "model-b"]
    assert config.sensitive_patterns == ["api_key"]
    assert config.recall_arm_weights == {"fts": 0.25, "summary": 0.75, "chunk": 1.0}
    assert config.empty_lifecycle_gc_max_age_hours is None
    assert config.ignore_session_patterns_source == "config_yaml:lcm.ignore_session_patterns"
    assert config.config_sources["recall_arm_weights"] == "config_yaml:lcm.recall_arm_weights"


def test_invalid_lcm_yaml_values_are_ignored_with_key_only_warnings(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, """lcm:
  fresh_tail_count: true
  automatic_foreground_max_seconds: .nan
  ignore_session_patterns: [7, cron-*]
  recall_arm_weights:
    fts: -1
""")
    for key in (
        "LCM_FRESH_TAIL_COUNT", "LCM_AUTOMATIC_FOREGROUND_MAX_SECONDS",
        "LCM_IGNORE_SESSION_PATTERNS", "LCM_RECALL_ARM_WEIGHTS",
    ):
        monkeypatch.delenv(key, raising=False)

    config = LCMConfig.from_env()

    assert config.fresh_tail_count == 32
    assert config.automatic_foreground_max_seconds == 120.0
    assert config.ignore_session_patterns == []
    assert config.recall_arm_weights["fts"] == 0.5
    assert len(config.config_source_warnings) == 4
    assert all("config_yaml:lcm." in warning for warning in config.config_source_warnings)


def test_env_list_and_valid_scalar_override_yaml_while_invalid_env_falls_back(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, """lcm:
  ignore_session_patterns: [yaml-*]
  fresh_tail_count: 44
  leaf_chunk_tokens: 18000
""")
    monkeypatch.setenv("LCM_IGNORE_SESSION_PATTERNS", "env-*")
    monkeypatch.setenv("LCM_FRESH_TAIL_COUNT", "21")
    monkeypatch.setenv("LCM_LEAF_CHUNK_TOKENS", "bad-value")

    config = LCMConfig.from_env()

    assert config.ignore_session_patterns == ["env-*"]
    assert config.fresh_tail_count == 21
    assert config.config_sources["ignore_session_patterns"] == "env:LCM_IGNORE_SESSION_PATTERNS"
    assert config.leaf_chunk_tokens == 18000
    assert config.config_sources["fresh_tail_count"] == "env:LCM_FRESH_TAIL_COUNT"
    assert config.config_sources["leaf_chunk_tokens"] == "config_yaml:lcm.leaf_chunk_tokens"
    assert any("LCM_LEAF_CHUNK_TOKENS" in warning for warning in config.config_source_warnings)


def test_invalid_lcm_threshold_falls_back_to_host_compression(tmp_path, monkeypatch):
    _home(tmp_path, monkeypatch, """lcm:
  context_threshold: true
compression:
  threshold: 0.66
""")
    monkeypatch.delenv("LCM_CONTEXT_THRESHOLD", raising=False)

    config = LCMConfig.from_env()

    assert config.context_threshold == 0.66
    assert config.config_sources["context_threshold"] == "config_yaml:compression.threshold"
    assert any("config_yaml:lcm.context_threshold" in warning for warning in config.config_source_warnings)


def test_every_scalar_env_field_has_a_typed_yaml_equivalent(tmp_path, monkeypatch):
    expected = {}
    for spec in ENV_FIELD_SPECS:
        monkeypatch.delenv(spec.env_key, raising=False)
        if spec.py_type is bool:
            expected[spec.name] = not getattr(LCMConfig(), spec.name)
        elif spec.py_type is int:
            expected[spec.name] = 7
        elif spec.py_type is float:
            expected[spec.name] = 0.75
        else:
            expected[spec.name] = "yaml-config-value"
    _home(tmp_path, monkeypatch, yaml.safe_dump({"lcm": expected}))

    config = LCMConfig.from_env()

    for name, value in expected.items():
        assert getattr(config, name) == value, name
        assert config.config_sources[name] == f"config_yaml:lcm.{name}"
    assert config.ignored_config_yaml_lcm_keys == []


def test_minimal_yaml_fallback_still_reads_scalar_lcm_keys(tmp_path, monkeypatch):
    import hermes_lcm.config as config_module

    _home(tmp_path, monkeypatch, """lcm:
  fresh_tail_count: 41
  dynamic_leaf_chunk_enabled: true
""")
    monkeypatch.setattr(config_module, "yaml", None)
    monkeypatch.delenv("LCM_FRESH_TAIL_COUNT", raising=False)
    monkeypatch.delenv("LCM_DYNAMIC_LEAF_CHUNK_ENABLED", raising=False)

    config = LCMConfig.from_env()

    assert config.fresh_tail_count == 41
    assert config.dynamic_leaf_chunk_enabled is True
    assert config.config_sources["fresh_tail_count"] == "config_yaml:lcm.fresh_tail_count"


def test_invalid_host_compression_threshold_does_not_leak_nonfinite_or_boolean(tmp_path, monkeypatch):
    home = _home(tmp_path, monkeypatch, "compression:\n  threshold: .nan\n")
    monkeypatch.delenv("LCM_CONTEXT_THRESHOLD", raising=False)

    nan_config = LCMConfig.from_env()
    assert nan_config.context_threshold == LCMConfig().context_threshold
    assert nan_config.config_sources["context_threshold"] == "default"

    (home / "config.yaml").write_text("compression:\n  threshold: true\n")
    bool_config = LCMConfig.from_env()
    assert bool_config.context_threshold == LCMConfig().context_threshold
    assert bool_config.config_sources["context_threshold"] == "default"


def test_from_env_uses_one_hermes_yaml_snapshot(monkeypatch):
    import hermes_lcm.config as config_module

    snapshots = iter([
        {
            "lcm": {"fresh_tail_count": 47},
            "compression": {"threshold": 0.61, "codex_gpt55_autoraise": False},
            "auxiliary": {"compression": {"timeout": 23}},
        },
        {
            "lcm": {"fresh_tail_count": 99},
            "compression": {"threshold": 0.9, "codex_gpt55_autoraise": True},
            "auxiliary": {"compression": {"timeout": 999}},
        },
    ])
    calls = []

    def load_snapshot():
        calls.append(1)
        return next(snapshots)

    monkeypatch.setattr(config_module, "_load_hermes_config_yaml", load_snapshot)
    for key in ("LCM_FRESH_TAIL_COUNT", "LCM_CONTEXT_THRESHOLD", "LCM_SUMMARY_TIMEOUT_MS"):
        monkeypatch.delenv(key, raising=False)

    config = LCMConfig.from_env()

    assert calls == [1]
    assert config.fresh_tail_count == 47
    assert config.context_threshold == 0.61
    assert config.codex_gpt55_autoraise_enabled is False
    assert config.summary_timeout_ms == 23000
