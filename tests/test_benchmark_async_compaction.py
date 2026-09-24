"""Smoke-gate the offline async-compaction latency benchmark."""

import pytest

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


@pytest.mark.parametrize("missing_token_estimates", [False, True])
def test_multi_turn_benchmark_batches_stamped_and_legacy_backlog(
    missing_token_estimates,
):
    report = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=8, max_batches=2, turns=2,
        missing_token_estimates=missing_token_estimates,
    )
    assert report["source_coverage_comparable"] is True
    assert report["invariants"]["all_raw_rows_retained"] is True
    assert report["invariants"]["all_old_sources_covered"] is True
    assert report["synchronous"]["provider_calls_total"] == 3
    assert report["staged"]["provider_calls_total"] == 2
    assert report["staged"]["provider_calls_foreground"] == 0
    assert report["staged"]["provider_calls_off_turn"] == 2
    assert report["staged"]["covered_old_messages_by_turn_per_run"] == [[8, 8]]
    assert report["staged"]["prepared_batches_by_turn_per_run"] == [[2, 0]]


def test_matching_but_partial_source_coverage_is_not_comparable():
    samples = {
        mode: [{"covered_source_ids": [1], "old_source_ids": [1, 2]}]
        for mode in ("synchronous", "staged")
    }
    assert _source_coverage(samples) == (True, False)


def test_incomplete_coverage_suppresses_latency_percentage(monkeypatch):
    monkeypatch.setattr(
        "benchmarks.benchmark_async_compaction._source_coverage",
        lambda _samples: (True, False),
    )
    report = run_benchmark(
        repeats=1, source_tokens=256, provider_delay_ms=0,
    )
    assert report["source_coverage_comparable"] is False
    assert report["foreground_median_reduction_percent"] is None
