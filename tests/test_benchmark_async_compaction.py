"""Smoke-gate the offline async-compaction latency benchmark."""

from benchmarks.benchmark_async_compaction import _source_coverage, run_benchmark


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
        "all_old_sources_covered": True,
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
    assert full_queue["invariants"]["all_old_sources_covered"] is True
    assert full_queue["staged"]["covered_old_messages_per_run"] == [4]
    assert full_queue["staged"]["foreground_provider_depths_per_run"] == [[1]]
    assert (
        full_queue["staged"]["provider_calls_foreground"]
        < small_queue["staged"]["provider_calls_foreground"]
    )


def test_matching_but_partial_source_coverage_is_not_comparable():
    samples = {
        mode: [{"covered_source_ids": [1], "old_source_ids": [1, 2]}]
        for mode in ("synchronous", "staged")
    }
    assert _source_coverage(samples) == (True, False)


def test_large_backlog_reports_partial_promotion_without_speedup_claim():
    report = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=8, max_batches=8,
    )
    assert report["source_coverage_comparable"] is (
        report["invariants"]["source_coverage_matches_baseline"]
        and report["invariants"]["all_old_sources_covered"]
    )
    if not report["invariants"]["all_old_sources_covered"]:
        assert report["source_coverage_comparable"] is False
        assert report["foreground_median_reduction_percent"] is None
