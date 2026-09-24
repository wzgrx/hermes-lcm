#!/usr/bin/env python3
"""Measure turn latency shifted by staged leaf preparation, without a provider.

Each run uses a fresh temporary SQLite database and identical synthetic source
rows. A fixed sleep models provider latency; this is a scheduling benchmark,
not a model-quality or real-network benchmark. No live Hermes profile is read.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarking.replay import _ensure_hermes_lcm_package


def _run_once(root: Path, *, staged: bool, source_tokens: int,
              provider_delay_ms: float) -> dict:
    _ensure_hermes_lcm_package()
    from hermes_lcm.config import LCMConfig
    from hermes_lcm.engine import LCMEngine

    root.mkdir(parents=True, exist_ok=True)
    config = LCMConfig(
        database_path=str(root / "lcm.db"),
        async_background_compaction_enabled=staged,
        async_background_compaction_worker_enabled=False,
        fresh_tail_count=2,
        leaf_chunk_tokens=256,
        context_threshold=0.05,
        threshold_full_sweep_enabled=False,
        summary_model="synthetic-benchmark-summarizer",
    )
    engine = LCMEngine(config=config)
    engine.on_session_start(
        "benchmark-session", conversation_id="benchmark-conversation",
        platform="benchmark", context_length=200_000,
    )
    calls = 0

    def summarize(**_kwargs):
        nonlocal calls
        calls += 1
        time.sleep(provider_delay_ms / 1000)
        return "Synthetic summary retains the source fact.", 1

    try:
        engine._store.append(
            "benchmark-session", {"role": "system", "content": "system anchor"},
            conversation_id="benchmark-conversation",
        )
        source_id = engine._store.append(
            "benchmark-session",
            {"role": "user", "content": "synthetic fact " + (" x" * source_tokens)},
            conversation_id="benchmark-conversation",
        )
        for role, content in (("assistant", "fresh reply"), ("user", "fresh request")):
            engine._store.append(
                "benchmark-session", {"role": role, "content": content},
                conversation_id="benchmark-conversation",
            )
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("benchmark-session")
        ]
        with patch("hermes_lcm.engine.summarize_with_escalation", summarize):
            preparation_ms = 0.0
            if staged:
                started = time.perf_counter()
                batch = engine.prepare_background_compaction_once(host_config={})
                preparation_ms = (time.perf_counter() - started) * 1000
                if batch is None or batch["state"] != "ready":
                    raise AssertionError("synthetic background batch was not ready")
            calls_before_foreground = calls
            with patch("hermes_cli.config.load_config_readonly", return_value={}):
                started = time.perf_counter()
                output = engine.compress(messages, current_tokens=20_000)
                foreground_ms = (time.perf_counter() - started) * 1000
        nodes = engine._dag.get_session_nodes("benchmark-session")
        if len(nodes) != 1 or nodes[0].source_ids != [source_id]:
            raise AssertionError("benchmark modes did not publish the same raw source")
        if not any("Synthetic summary" in str(msg.get("content")) for msg in output):
            raise AssertionError("summary was absent from active context")
        return {
            "preparation_ms": preparation_ms,
            "foreground_ms": foreground_ms,
            "provider_calls_total": calls,
            "provider_calls_foreground": calls - calls_before_foreground,
            "raw_rows": len(engine._store.get_session_messages("benchmark-session")),
            "canonical_nodes": len(nodes),
        }
    finally:
        engine.shutdown()


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def run_benchmark(*, repeats: int = 5, source_tokens: int = 10_000,
                  provider_delay_ms: float = 100.0) -> dict:
    if repeats < 1 or source_tokens < 256 or provider_delay_ms < 0:
        raise ValueError("repeats >= 1, source_tokens >= 256, delay >= 0 required")
    samples: dict[str, list[dict]] = {"synchronous": [], "staged": []}
    with tempfile.TemporaryDirectory(prefix="lcm-async-benchmark-") as directory:
        root = Path(directory)
        for index in range(repeats):
            for mode in ("synchronous", "staged") if index % 2 == 0 else ("staged", "synchronous"):
                result = _run_once(
                    root / str(index) / mode,
                    staged=mode == "staged",
                    source_tokens=source_tokens,
                    provider_delay_ms=provider_delay_ms,
                )
                samples[mode].append(result)
    baseline = [item["foreground_ms"] for item in samples["synchronous"]]
    staged = [item["foreground_ms"] for item in samples["staged"]]
    baseline_median = statistics.median(baseline)
    staged_median = statistics.median(staged)
    return {
        "workload": "synthetic_single_old_leaf",
        "provider": "fixed_sleep_stub_no_network",
        "repeats": repeats,
        "source_tokens_requested": source_tokens,
        "provider_delay_ms": provider_delay_ms,
        "synchronous": {
            "foreground_median_ms": round(baseline_median, 3),
            "foreground_p95_ms": round(_percentile(baseline, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["synchronous"]),
        },
        "staged": {
            "preparation_median_ms": round(statistics.median(
                item["preparation_ms"] for item in samples["staged"]
            ), 3),
            "foreground_median_ms": round(staged_median, 3),
            "foreground_p95_ms": round(_percentile(staged, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["staged"]),
        },
        "foreground_median_reduction_percent": round(
            100 * (1 - staged_median / baseline_median), 2
        ) if baseline_median > 0 else None,
        "invariants": {
            "one_canonical_leaf_each_run": all(
                item["canonical_nodes"] == 1 for mode in samples.values() for item in mode
            ),
            "all_raw_rows_retained": all(
                item["raw_rows"] == 4 for mode in samples.values() for item in mode
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--source-tokens", type=int, default=10_000)
    parser.add_argument("--provider-delay-ms", type=float, default=100.0)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(
        repeats=args.repeats,
        source_tokens=args.source_tokens,
        provider_delay_ms=args.provider_delay_ms,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
