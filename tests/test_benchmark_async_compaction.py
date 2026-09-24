"""Smoke-gate the offline async-compaction latency benchmark."""

from benchmarks.benchmark_async_compaction import run_benchmark


def test_benchmark_preserves_sources_and_moves_provider_off_foreground():
    report = run_benchmark(
        repeats=2, source_tokens=256, provider_delay_ms=0,
    )
    assert report["provider"] == "fixed_sleep_stub_no_network"
    assert report["source_coverage_comparable"] is True
    assert report["synchronous"]["provider_calls_foreground"] == 2
    assert report["staged"]["provider_calls_foreground"] == 0
    assert report["invariants"] == {
        "one_canonical_leaf_each_run": True,
        "source_coverage_matches_baseline": True,
        "all_raw_rows_retained": True,
    }


def test_multi_leaf_benchmark_exposes_queue_capacity_tradeoff():
    small_queue = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=4, max_batches=2,
    )
    full_queue = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=4, max_batches=4,
    )
    assert small_queue["source_coverage_comparable"] is True
    assert full_queue["source_coverage_comparable"] is True
    assert small_queue["invariants"]["all_raw_rows_retained"] is True
    assert full_queue["invariants"]["all_raw_rows_retained"] is True
    assert full_queue["staged"]["covered_old_messages_per_run"] == [4]
    assert full_queue["staged"]["foreground_provider_depths_per_run"] == [[1]]
    assert (
        full_queue["staged"]["provider_calls_foreground"]
        < small_queue["staged"]["provider_calls_foreground"]
    )
