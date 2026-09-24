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
              provider_delay_ms: float, old_messages: int,
              max_batches: int) -> dict:
    _ensure_hermes_lcm_package()
    from hermes_lcm.config import LCMConfig
    from hermes_lcm.engine import LCMEngine

    root.mkdir(parents=True, exist_ok=True)
    config = LCMConfig(
        database_path=str(root / "lcm.db"),
        async_background_compaction_enabled=staged,
        async_background_compaction_worker_enabled=False,
        async_background_compaction_max_batches=max_batches,
        fresh_tail_count=2,
        leaf_chunk_tokens=256,
        context_threshold=0.05 if old_messages == 1 else 0.005,
        dynamic_leaf_chunk_enabled=old_messages > 1,
        threshold_full_sweep_enabled=False,
        summary_model="synthetic-benchmark-summarizer",
    )
    engine = LCMEngine(config=config)
    engine.on_session_start(
        "benchmark-session", conversation_id="benchmark-conversation",
        platform="benchmark", context_length=200_000,
    )
    calls = 0
    call_depths: list[int] = []

    def summarize(**kwargs):
        nonlocal calls
        calls += 1
        call_depths.append(int(kwargs.get("depth", -1)))
        time.sleep(provider_delay_ms / 1000)
        return "Synthetic summary retains the source fact.", 1

    try:
        engine._store.append(
            "benchmark-session", {"role": "system", "content": "system anchor"},
            conversation_id="benchmark-conversation",
        )
        source_ids = [
            engine._store.append(
                "benchmark-session",
                {"role": "user", "content": f"synthetic fact {index} " + (" x" * source_tokens)},
                conversation_id="benchmark-conversation",
            )
            for index in range(old_messages)
        ]
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
            prepared_ids: set[str] = set()
            if staged:
                started = time.perf_counter()
                for _ in range(max_batches):
                    batch = engine.prepare_background_compaction_once(host_config={})
                    if batch is None or batch["state"] != "ready":
                        break
                    if batch["batch_id"] in prepared_ids:
                        break
                    prepared_ids.add(batch["batch_id"])
                preparation_ms = (time.perf_counter() - started) * 1000
                if not prepared_ids:
                    raise AssertionError("synthetic background batch was not ready")
            calls_before_foreground = calls
            with patch("hermes_cli.config.load_config_readonly", return_value={}):
                started = time.perf_counter()
                output = engine.compress(
                    messages, current_tokens=max(20_000, old_messages * source_tokens),
                )
                foreground_ms = (time.perf_counter() - started) * 1000
        nodes = engine._dag.get_session_nodes("benchmark-session")
        covered_ids = sorted({
            source_id for node in nodes if node.depth == 0 for source_id in node.source_ids
        })
        if old_messages == 1 and covered_ids != source_ids:
            raise AssertionError("benchmark modes did not publish the same raw source")
        if not any("Synthetic summary" in str(msg.get("content")) for msg in output):
            raise AssertionError("summary was absent from active context")
        return {
            "preparation_ms": preparation_ms,
            "foreground_ms": foreground_ms,
            "provider_calls_total": calls,
            "provider_calls_foreground": calls - calls_before_foreground,
            "foreground_provider_depths": call_depths[calls_before_foreground:],
            "prepared_batches": len(prepared_ids),
            "raw_rows": len(engine._store.get_session_messages("benchmark-session")),
            "canonical_nodes": len(nodes),
            "covered_source_ids": covered_ids,
            "old_source_ids": source_ids,
        }
    finally:
        engine.shutdown()


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def run_benchmark(*, repeats: int = 5, source_tokens: int = 10_000,
                  provider_delay_ms: float = 100.0, old_messages: int = 1,
                  max_batches: int = 2) -> dict:
    if (repeats < 1 or source_tokens < 256 or provider_delay_ms < 0
            or old_messages < 1 or max_batches < 1):
        raise ValueError("repeats, old_messages, max_batches >= 1; source_tokens >= 256; delay >= 0 required")
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
                    old_messages=old_messages,
                    max_batches=max_batches,
                )
                samples[mode].append(result)
    baseline = [item["foreground_ms"] for item in samples["synchronous"]]
    staged = [item["foreground_ms"] for item in samples["staged"]]
    baseline_median = statistics.median(baseline)
    staged_median = statistics.median(staged)
    coverage_matches = all(
        samples["synchronous"][index]["covered_source_ids"]
        == samples["staged"][index]["covered_source_ids"]
        for index in range(repeats)
    )
    return {
        "workload": "synthetic_single_old_leaf" if old_messages == 1 else "synthetic_multi_old_messages",
        "provider": "fixed_sleep_stub_no_network",
        "repeats": repeats,
        "source_tokens_requested": source_tokens,
        "old_messages": old_messages,
        "max_prepared_batches": max_batches,
        "provider_delay_ms": provider_delay_ms,
        "synchronous": {
            "foreground_median_ms": round(baseline_median, 3),
            "foreground_p95_ms": round(_percentile(baseline, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["synchronous"]),
            "foreground_provider_depths_per_run": [
                item["foreground_provider_depths"] for item in samples["synchronous"]
            ],
            "covered_old_messages_per_run": [
                len(set(item["covered_source_ids"]) & set(item["old_source_ids"]))
                for item in samples["synchronous"]
            ],
        },
        "staged": {
            "preparation_median_ms": round(statistics.median(
                item["preparation_ms"] for item in samples["staged"]
            ), 3),
            "foreground_median_ms": round(staged_median, 3),
            "foreground_p95_ms": round(_percentile(staged, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["staged"]),
            "foreground_provider_depths_per_run": [
                item["foreground_provider_depths"] for item in samples["staged"]
            ],
            "prepared_batches": sum(item["prepared_batches"] for item in samples["staged"]),
            "covered_old_messages_per_run": [
                len(set(item["covered_source_ids"]) & set(item["old_source_ids"]))
                for item in samples["staged"]
            ],
        },
        "foreground_median_reduction_percent": round(
            100 * (1 - staged_median / baseline_median), 2
        ) if baseline_median > 0 and coverage_matches else None,
        "source_coverage_comparable": coverage_matches,
        "invariants": {
            "one_canonical_leaf_each_run": all(
                item["canonical_nodes"] == 1 for mode in samples.values() for item in mode
            ) if old_messages == 1 else None,
            "source_coverage_matches_baseline": coverage_matches,
            "all_raw_rows_retained": all(
                item["raw_rows"] == old_messages + 3
                for mode in samples.values() for item in mode
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--source-tokens", type=int, default=10_000)
    parser.add_argument("--provider-delay-ms", type=float, default=100.0)
    parser.add_argument("--old-messages", type=int, default=1)
    parser.add_argument("--max-batches", type=int, default=2)
    args = parser.parse_args()
    print(json.dumps(run_benchmark(
        repeats=args.repeats,
        source_tokens=args.source_tokens,
        provider_delay_ms=args.provider_delay_ms,
        old_messages=args.old_messages,
        max_batches=args.max_batches,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
