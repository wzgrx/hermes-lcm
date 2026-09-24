"""Fingerprint fences for opt-in background preparation."""

from __future__ import annotations

from dataclasses import replace

import pytest

from hermes_lcm.async_compaction_policy import policy_fingerprint, route_fingerprint
from hermes_lcm.config import LCMConfig


def _host_config():
    return {
        "model": {
            "provider": "opencode-go",
            "default": "deepseek-flash",
            "base_url": "https://example.test/v1",
            "api_key": "SECRET_1",
        },
        "auxiliary": {
            "compression": {
                "provider": "opencode-go",
                "model": "deepseek-flash",
                "base_url": "https://example.test/v1",
                "context_length": 200_000,
                "extra_body": {"reasoning_effort": "max"},
                "api_key": "SECRET_2",
            },
        },
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("fresh_tail_count", 64),
        ("fresh_tail_max_tokens", 1000),
        ("leaf_chunk_tokens", 8000),
        ("context_threshold", 0.95),
        ("ignore_message_patterns", ["secret"]),
        ("sensitive_patterns_enabled", True),
        ("custom_instructions", "Preserve identifiers"),
    ],
)
def test_policy_changes_invalidate_prepared_generation(field, value):
    baseline = LCMConfig()
    changed = replace(baseline, **{field: value})
    assert policy_fingerprint(changed) != policy_fingerprint(baseline)


def test_route_changes_invalidate_but_key_rotation_does_not():
    config = LCMConfig()
    host = _host_config()
    baseline = route_fingerprint(config, host)
    host["auxiliary"]["compression"]["api_key"] = "ROTATED_SECRET"
    host["model"]["api_key"] = "OTHER_SECRET"
    assert route_fingerprint(config, host) == baseline
    host["auxiliary"]["compression"]["model"] = "another-model"
    assert route_fingerprint(config, host) != baseline
    assert (
        route_fingerprint(
            replace(config, summary_fallback_models=["fallback"]),
            _host_config(),
        )
        != baseline
    )


def test_missing_task_route_needs_explicit_lcm_model():
    with pytest.raises(ValueError, match="unavailable"):
        route_fingerprint(LCMConfig(), {})
    assert route_fingerprint(LCMConfig(summary_model="explicit-model"), {})
