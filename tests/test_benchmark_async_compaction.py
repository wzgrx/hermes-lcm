"""Smoke-gate the offline async-compaction latency benchmark."""

from benchmarks.benchmark_async_compaction import run_benchmark


def test_benchmark_preserves_sources_and_moves_provider_off_foreground():
    report = run_benchmark(
        repeats=2, source_tokens=256, provider_delay_ms=0,
    )
    assert report["provider"] == "fixed_sleep_stub_no_network"
    assert report["synchronous"]["provider_calls_foreground"] == 2
    assert report["staged"]["provider_calls_foreground"] == 0
    assert report["invariants"] == {
        "one_canonical_leaf_each_run": True,
        "all_raw_rows_retained": True,
    }
