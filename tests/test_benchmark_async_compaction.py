"""Smoke-gate the offline async-compaction latency benchmark."""

import pytest

from benchmarks.benchmark_async_compaction import (
    _source_coverage,
    _source_partition_matches,
    run_benchmark,
)


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
        "source_partition_matches_baseline": True,
        "all_old_sources_covered": True,
        "all_raw_rows_retained": True,
    }


@pytest.mark.parametrize("missing_token_estimates", [False, True])
def test_multi_turn_benchmark_batches_stamped_and_legacy_backlog(
    missing_token_estimates,
):
    report = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=8, max_batches=4, turns=2,
        missing_token_estimates=missing_token_estimates,
    )
    assert report["source_coverage_comparable"] is True
    assert report["invariants"]["all_raw_rows_retained"] is True
    assert report["invariants"]["all_old_sources_covered"] is True
    assert report["synchronous"]["provider_calls_total"] == 3
    assert report["staged"]["provider_calls_total"] == 3
    assert report["staged"]["provider_calls_foreground"] == 0
    assert report["staged"]["provider_calls_off_turn"] == 3
    assert report["staged"]["covered_old_messages_by_turn_per_run"] == [[8, 8]]
    assert report["staged"]["prepared_batches_by_turn_per_run"] == [[3, 0]]


def test_default_queue_cap_covers_three_leaf_backlog():
    from hermes_lcm.config import LCMConfig

    assert LCMConfig().async_background_compaction_max_batches == 4
    report = run_benchmark(
        repeats=1, source_tokens=3_000, provider_delay_ms=0,
        old_messages=16, turns=5,
    )
    assert report["max_prepared_batches"] == 4
    assert report["source_coverage_comparable"] is True
    assert (
        report["staged"]["leaf_source_groups_per_run"]
        == report["synchronous"]["leaf_source_groups_per_run"]
    )
    assert report["staged"]["prepared_batches_by_turn_per_run"] == [[3, 0, 0, 0, 0]]
    assert report["staged"]["provider_calls_foreground"] == 0
    assert report["staged"]["provider_calls_off_turn"] == 3
    assert report["synchronous"]["provider_calls_foreground"] == 3


def test_matching_but_partial_source_coverage_is_not_comparable():
    samples = {
        mode: [{"covered_source_ids": [1], "old_source_ids": [1, 2]}]
        for mode in ("synchronous", "staged")
    }
    assert _source_coverage(samples) == (True, False)


def test_matching_union_with_different_leaf_groups_is_not_comparable():
    samples = {
        "synchronous": [{"leaf_source_groups": [[1, 2], [3, 4]]}],
        "staged": [{"leaf_source_groups": [[1], [2, 3, 4]]}],
    }
    assert _source_partition_matches(samples) is False


def test_different_leaf_groups_suppress_latency_percentage(monkeypatch):
    monkeypatch.setattr(
        "benchmarks.benchmark_async_compaction._source_partition_matches",
        lambda _samples: False,
    )
    report = run_benchmark(repeats=1, source_tokens=256, provider_delay_ms=0)
    assert report["invariants"]["source_partition_matches_baseline"] is False
    assert report["source_coverage_comparable"] is False
    assert report["foreground_median_reduction_percent"] is None


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
