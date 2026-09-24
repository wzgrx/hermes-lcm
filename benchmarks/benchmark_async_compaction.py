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
              max_batches: int, turns: int,
              missing_token_estimates: bool) -> dict:
    _ensure_hermes_lcm_package()
    from hermes_lcm.config import LCMConfig
    from hermes_lcm.engine import LCMEngine
    from hermes_lcm.tokens import count_message_tokens, count_messages_tokens

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
        def append_initial(message: dict) -> int:
            return engine._store.append(
                "benchmark-session", message,
                token_estimate=(
                    0 if missing_token_estimates else count_message_tokens(message)
                ),
                conversation_id="benchmark-conversation",
            )

        append_initial({"role": "system", "content": "system anchor"})
        source_ids = [
            append_initial(
                {"role": "user", "content": f"synthetic fact {index} " + (" x" * source_tokens)},
            )
            for index in range(old_messages)
        ]
        for role, content in (("assistant", "fresh reply"), ("user", "fresh request")):
            append_initial({"role": role, "content": content})
        messages = [
            engine._store.to_openai_msg(row)
            for row in engine._store.get_session_messages("benchmark-session")
        ]
        with patch("hermes_lcm.engine.summarize_with_escalation", summarize):
            preparation_ms = 0.0
            prepared_ids: set[str] = set()
            preparation_ms_by_turn: list[float] = []
            foreground_ms_by_turn: list[float] = []
            foreground_depths_by_turn: list[list[int]] = []
            covered_old_messages_by_turn: list[int] = []
            prepared_batches_by_turn: list[int] = []
            foreground_calls = 0
            foreground_depths: list[int] = []
            output = messages
            for turn in range(turns):
                if turn:
                    messages = output + [
                        {"role": "assistant", "content": f"fresh reply {turn}"},
                        {"role": "user", "content": f"fresh request {turn}"},
                    ]
                    # Model the post-LLM ingest boundary before the next turn's
                    # off-turn preparation, without starting the automatic worker.
                    engine.ingest(messages)
                prepared_this_turn: set[str] = set()
                new_prepared_this_turn = 0
                prep_started = time.perf_counter()
                if staged:
                    for _ in range(max_batches):
                        batch = engine.prepare_background_compaction_once(host_config={})
                        if batch is None or batch["state"] != "ready":
                            break
                        if batch["batch_id"] in prepared_this_turn:
                            break
                        prepared_this_turn.add(batch["batch_id"])
                        if batch["batch_id"] not in prepared_ids:
                            prepared_ids.add(batch["batch_id"])
                            new_prepared_this_turn += 1
                    if turn == 0 and not prepared_ids:
                        raise AssertionError("synthetic background batch was not ready")
                turn_preparation_ms = (
                    (time.perf_counter() - prep_started) * 1000 if staged else 0.0
                )
                preparation_ms += turn_preparation_ms
                preparation_ms_by_turn.append(turn_preparation_ms)
                prepared_batches_by_turn.append(new_prepared_this_turn)
                calls_before_foreground = calls
                with patch("hermes_cli.config.load_config_readonly", return_value={}):
                    started = time.perf_counter()
                    output = engine.compress(
                        messages,
                        current_tokens=(
                            max(20_000, old_messages * source_tokens)
                            if turns == 1 else count_messages_tokens(messages)
                        ),
                    )
                    turn_foreground_ms = (time.perf_counter() - started) * 1000
                foreground_ms_by_turn.append(turn_foreground_ms)
                turn_depths = call_depths[calls_before_foreground:]
                foreground_depths_by_turn.append(turn_depths)
                foreground_depths.extend(turn_depths)
                foreground_calls += len(turn_depths)
                turn_nodes = engine._dag.get_session_nodes("benchmark-session")
                covered_old_messages_by_turn.append(len(set(source_ids) & {
                    source_id for node in turn_nodes if node.depth == 0
                    for source_id in node.source_ids
                }))
            foreground_ms = sum(foreground_ms_by_turn)
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
            "foreground_ms_by_turn": foreground_ms_by_turn,
            "preparation_ms_by_turn": preparation_ms_by_turn,
            "provider_calls_total": calls,
            "provider_calls_foreground": foreground_calls,
            "foreground_provider_depths": foreground_depths,
            "foreground_provider_depths_by_turn": foreground_depths_by_turn,
            "prepared_batches": len(prepared_ids),
            "prepared_batches_by_turn": prepared_batches_by_turn,
            "covered_old_messages_by_turn": covered_old_messages_by_turn,
            "raw_rows": len(engine._store.get_session_messages("benchmark-session")),
            "canonical_nodes": len(nodes),
            "leaf_source_groups": sorted(
                (list(node.source_ids) for node in nodes if node.depth == 0),
                key=lambda ids: ids[0] if ids else -1,
            ),
            "covered_source_ids": covered_ids,
            "old_source_ids": source_ids,
        }
    finally:
        engine.shutdown()


def _percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def _source_coverage(samples: dict[str, list[dict]]) -> tuple[bool, bool]:
    """Report both parity and completeness; parity alone can hide lost work."""
    synchronous = samples["synchronous"]
    staged = samples["staged"]
    matches = all(
        left["covered_source_ids"] == right["covered_source_ids"]
        for left, right in zip(synchronous, staged, strict=True)
    )
    complete = all(
        set(item["covered_source_ids"]) == set(item["old_source_ids"])
        for mode in samples.values() for item in mode
    )
    return matches, complete


def _source_partition_matches(samples: dict[str, list[dict]]) -> bool:
    """A latency comparison should summarize the same leaves, not just their union."""
    return all(
        left["leaf_source_groups"] == right["leaf_source_groups"]
        for left, right in zip(samples["synchronous"], samples["staged"], strict=True)
    )


def run_benchmark(*, repeats: int = 5, source_tokens: int = 10_000,
                  provider_delay_ms: float = 100.0, old_messages: int = 1,
                  max_batches: int = 4, turns: int = 1,
                  missing_token_estimates: bool = False) -> dict:
    if (repeats < 1 or source_tokens < 256 or provider_delay_ms < 0
            or old_messages < 1 or max_batches < 1 or turns < 1):
        raise ValueError("repeats, old_messages, max_batches, turns >= 1; source_tokens >= 256; delay >= 0 required")
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
                    turns=turns,
                    missing_token_estimates=missing_token_estimates,
                )
                samples[mode].append(result)
    baseline = [item["foreground_ms"] for item in samples["synchronous"]]
    staged = [item["foreground_ms"] for item in samples["staged"]]
    baseline_median = statistics.median(baseline)
    staged_median = statistics.median(staged)
    coverage_matches, coverage_complete = _source_coverage(samples)
    partition_matches = _source_partition_matches(samples)
    comparable = coverage_matches and coverage_complete and partition_matches
    return {
        "workload": (
            "synthetic_multi_turns" if turns > 1 else
            "synthetic_single_old_leaf" if old_messages == 1 else
            "synthetic_multi_old_messages"
        ),
        "provider": "fixed_sleep_stub_no_network",
        "repeats": repeats,
        "source_tokens_requested": source_tokens,
        "old_messages": old_messages,
        "turns": turns,
        "missing_token_estimates": missing_token_estimates,
        "max_prepared_batches": max_batches,
        "provider_delay_ms": provider_delay_ms,
        "synchronous": {
            "foreground_median_ms": round(baseline_median, 3),
            "foreground_p95_ms": round(_percentile(baseline, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["synchronous"]),
            "provider_calls_total": sum(item["provider_calls_total"] for item in samples["synchronous"]),
            "foreground_provider_depths_per_run": [
                item["foreground_provider_depths"] for item in samples["synchronous"]
            ],
            "foreground_ms_by_turn_per_run": [
                item["foreground_ms_by_turn"] for item in samples["synchronous"]
            ],
            "covered_old_messages_by_turn_per_run": [
                item["covered_old_messages_by_turn"] for item in samples["synchronous"]
            ],
            "covered_old_messages_per_run": [
                len(set(item["covered_source_ids"]) & set(item["old_source_ids"]))
                for item in samples["synchronous"]
            ],
            "leaf_source_groups_per_run": [
                item["leaf_source_groups"] for item in samples["synchronous"]
            ],
        },
        "staged": {
            "preparation_median_ms": round(statistics.median(
                item["preparation_ms"] for item in samples["staged"]
            ), 3),
            "foreground_median_ms": round(staged_median, 3),
            "foreground_p95_ms": round(_percentile(staged, 0.95), 3),
            "provider_calls_foreground": sum(item["provider_calls_foreground"] for item in samples["staged"]),
            "provider_calls_total": sum(item["provider_calls_total"] for item in samples["staged"]),
            "provider_calls_off_turn": sum(
                item["provider_calls_total"] - item["provider_calls_foreground"]
                for item in samples["staged"]
            ),
            "foreground_provider_depths_per_run": [
                item["foreground_provider_depths"] for item in samples["staged"]
            ],
            "foreground_provider_depths_by_turn_per_run": [
                item["foreground_provider_depths_by_turn"] for item in samples["staged"]
            ],
            "foreground_ms_by_turn_per_run": [
                item["foreground_ms_by_turn"] for item in samples["staged"]
            ],
            "covered_old_messages_by_turn_per_run": [
                item["covered_old_messages_by_turn"] for item in samples["staged"]
            ],
            "prepared_batches": sum(item["prepared_batches"] for item in samples["staged"]),
            "prepared_batches_by_turn_per_run": [
                item["prepared_batches_by_turn"] for item in samples["staged"]
            ],
            "covered_old_messages_per_run": [
                len(set(item["covered_source_ids"]) & set(item["old_source_ids"]))
                for item in samples["staged"]
            ],
            "leaf_source_groups_per_run": [
                item["leaf_source_groups"] for item in samples["staged"]
            ],
        },
        "foreground_median_reduction_percent": round(
            100 * (1 - staged_median / baseline_median), 2
        ) if baseline_median > 0 and comparable else None,
        "source_coverage_comparable": comparable,
        "invariants": {
            "one_canonical_leaf_each_run": all(
                item["canonical_nodes"] == 1 for mode in samples.values() for item in mode
            ) if old_messages == 1 else None,
            "source_coverage_matches_baseline": coverage_matches,
            "source_partition_matches_baseline": partition_matches,
            "all_old_sources_covered": coverage_complete,
            "all_raw_rows_retained": all(
                item["raw_rows"] == old_messages + 3 + 2 * (turns - 1)
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
    parser.add_argument("--max-batches", type=int, default=4)
    parser.add_argument("--turns", type=int, default=1)
    parser.add_argument("--missing-token-estimates", action="store_true")
    args = parser.parse_args()
    print(json.dumps(run_benchmark(
        repeats=args.repeats,
        source_tokens=args.source_tokens,
        provider_delay_ms=args.provider_delay_ms,
        old_messages=args.old_messages,
        max_batches=args.max_batches,
        turns=args.turns,
        missing_token_estimates=args.missing_token_estimates,
    ), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
