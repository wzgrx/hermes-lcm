# 2026-10-04 — bounded session-end contention tolerance

Based on [upstream issue #662](https://github.com/stephenschoettler/hermes-lcm/issues/662),
reviewed against main `8d1b1e6d3d63f5fc7b209e8d7ec1dc9b814f2e54`.
Maintenance after 1.0.0-rc.2, not a new tagged release.

## Reproduced, not inferred from live data

Two independent SQLite connections reproduce the report: a competing writer
holding BEGIN IMMEDIATE for about 150ms makes the old 50ms bound skip either
final raw-message ingestion or lifecycle finalization. Three of four new tests
fail on the old fork; the persistent-lock/idempotence guard already passes.
No lock warnings were observed in the current short idle Gateway journal;
this change does not claim to repair already-missing historical messages.

Increase only the existing temporary session-end busy timeout to 500ms. Keep
the current fail-open lock handling, unrelated-error propagation, interruption
handling, stale-owner checks and original timeout restoration. No worker,
automatic retry, global timeout, settings surface, schema or provider change.

The value bounds each SQLite busy wait, not the total duration of a hook with
multiple operations or a very large transcript. A persistent lock still logs
the skip and returns. A later ordinary session-end call can persist/finalize
once the lock clears; tests verify raw rows are not duplicated. In the older
persistent-lock test only the timing assertion changes from 300ms to 900ms
to include the new 500ms allowance plus scheduler headroom; all warning and
timeout-restoration assertions remain.

Four new tests exercise transient ingest/finalize locks, persistent contention
plus later idempotent finalization, and scoped 500ms values with restoration on
an unrelated exception. Tests use real SQLite writers and disposable profiles;
no real messages or databases are modified.

Latest reviewed official Hermes `9cf7960f274ea2bdfe672d64d26389e9954dfeb6`
only adds a process_registry fix after the previous reviewed snapshot. It does
not change these lifecycle hooks. Deployed core stays `0764e9165721` in this
plugin-only round. Existing SQLite POSIX-lock guards, hidden-backlog gap rules,
atomic publication and replay protections remain mandatory in full/low-FD gates.

Retire the carried timeout change when upstream ships equivalent bounded
contention tolerance and the transient/persistent lock regressions pass. This
does not solve indefinite contention, every open database report, or archived
corruption. Retain existing backups and raw history.

## Local verification

All functional release checks pass, including dependency contract, Python/shell
checks, focused pytest, benchmark/stress smoke, full pytest, low-FD pytest and
release stress. Both full runs report **3533 passed,
6 skipped, 12 expected failures**. Four new cases pass; three failed before
the change (two real finalization/data failures plus the timeout contract).
Ruff/whitespace checks pass. The original full runner exited 1 only because
CHANGELOG/README/the new maintenance document were authored concurrently and
changed its git-status snapshot. Its original failure receipt is retained.
Code/tests were unchanged; a subsequent frozen-workspace smoke gate passed,
including source stability. The full/low-FD logs and frozen gate remain separate
evidence, not an invented successful full-run exit. Exact-commit hosted CI is
required before deployment.
