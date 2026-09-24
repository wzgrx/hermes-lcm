"""Deterministic, secret-free fences for prepared LCM summary generations."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Mapping

from .config import LCMConfig


_PROTOCOL = "lcm-async-leaf-v1"


def _digest(value: Mapping[str, Any]) -> str:
    serialized = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def policy_fingerprint(config: LCMConfig) -> str:
    """Fence all live settings that alter leaf selection or summary content."""
    return _digest(
        {
            "protocol": _PROTOCOL,
            "fresh_tail_count": config.fresh_tail_count,
            "fresh_tail_max_tokens": config.fresh_tail_max_tokens,
            "leaf_chunk_tokens": config.leaf_chunk_tokens,
            "dynamic_leaf_chunk_enabled": config.dynamic_leaf_chunk_enabled,
            "dynamic_leaf_chunk_max": config.dynamic_leaf_chunk_max,
            "context_threshold": config.context_threshold,
            "threshold_full_sweep_enabled": config.threshold_full_sweep_enabled,
            "ignore_message_patterns": list(config.ignore_message_patterns),
            "ignore_message_patterns_source": config.ignore_message_patterns_source,
            "sensitive_patterns_enabled": config.sensitive_patterns_enabled,
            "sensitive_patterns": list(config.sensitive_patterns),
            "sensitive_patterns_source": config.sensitive_patterns_source,
            "large_output_externalization_enabled": config.large_output_externalization_enabled,
            "large_output_externalization_threshold_chars": config.large_output_externalization_threshold_chars,
            "large_output_active_replay_stubbing_enabled": config.large_output_active_replay_stubbing_enabled,
            "large_output_active_replay_stub_threshold_tokens": config.large_output_active_replay_stub_threshold_tokens,
            "custom_instructions": config.custom_instructions,
            "l2_budget_ratio": config.l2_budget_ratio,
            "l3_truncate_tokens": config.l3_truncate_tokens,
            "summary_prefix_target_tokens": config.summary_prefix_target_tokens,
            "max_assembly_tokens": config.max_assembly_tokens,
            "reserve_tokens_floor": config.reserve_tokens_floor,
        }
    )


def route_fingerprint(config: LCMConfig, host_config: Mapping[str, Any]) -> str:
    """Fence the actual host compression route without persisting credentials.

    An empty LCM override delegates to Hermes' auxiliary compression task.
    In that case a missing host route is not a trustworthy preparation fence.
    """
    auxiliary = host_config.get("auxiliary")
    compression = auxiliary.get("compression") if isinstance(auxiliary, dict) else None
    if not config.summary_model and not isinstance(compression, dict):
        raise ValueError("host compression route is unavailable")
    compression = compression if isinstance(compression, dict) else {}
    primary = host_config.get("model")
    primary = primary if isinstance(primary, dict) else {}
    return _digest(
        {
            "protocol": _PROTOCOL,
            "summary_model": config.summary_model,
            "summary_fallback_models": list(config.summary_fallback_models),
            "host_compression": {
                key: compression.get(key)
                for key in (
                    "provider",
                    "model",
                    "base_url",
                    "context_length",
                    "extra_body",
                )
            },
            "host_default": {
                key: primary.get(key)
                for key in ("provider", "default", "base_url", "context_length")
            },
        }
    )
