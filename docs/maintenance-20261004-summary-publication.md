# 2026-10-04 — guarded atomic summary publication

Adapted from [upstream PR #658](https://github.com/stephenschoettler/hermes-lcm/pull/658),
reviewed head `2e6fd721a189cedf6d45b36a858ed1df79152051`, by the3asic / 3ASiC.
Maintenance after 1.0.0-rc.2, not a new tagged release.

## Evidence and behavior

The unchanged maintained fork failed twelve of thirteen upstream regression
tests. During a slow summarization call, raw rows, tool-call arguments, summary
children, existing parents or lifecycle ownership could change. A separate
frontier failure could also leave a committed node with unadvanced progress.
This is isolated reproduction, not a claim of live historical corruption.

Capture source rows, their covering parents and the lifecycle binding before
model work. Recheck under BEGIN IMMEDIATE, then insert the node, run existing
FTS/rollup-outbox triggers and advance eligible frontier progress in one commit.
Rollback on exceptions, including cancellation before commit. The in-memory
node ID and cursor advance only after the commit. Model calls occur outside
this write transaction; no lock is held over a provider request.

The maintained fork additionally validates hidden backlog messages materialized
by its loader against captured rows, using schema column names rather than
migration-dependent offsets. Active-source identities are frozen before model
work and rechecked after preparation. Rescue-selected subsets keep their original
lineage. A newer active leaf still does not jump over an older hidden gap.
Legacy add_node_with_frontier remains available; production leaf publication
uses the guarded transaction. Raw history and prior local replay/lock safeguards
are retained, with no schema or dependency migration and no provider retuning.

Six extra tests cover hidden-source mutation after loading and during model
work, session/conversation/profile rebinding, and KeyboardInterrupt after the
insert/frontier update. Independent SQLite readers verify rollback of summary,
FTS, frontier and invalidation outbox; the caller's messages remain unchanged.

Full-suite integration exposed an older test that expected a slow summary to
publish after its concurrent session end had already released the lifecycle
binding. The same nonblocking thread/claim-lock assertions remain; it now also
requires a rejected publication, no new summary/cursor, and preserved raw rows.
Session-binding drift keeps a specific diagnostic compatible with the existing
hidden-backlog rollover test. No race is hidden by relaxing the publication guard.

## Limits and upstream review

This guard is not a host-wide atomic cancellation-admission protocol. Runtime
identity checks bracket publication but do not synchronize every possible host
thread. It does not add automatic retries, rewrite old sessions or repair old
orphan summaries. Issue [#662](https://github.com/stephenschoettler/hermes-lcm/issues/662)
(session-end busy timeout), broader UID-bridge PR #659 and other SQLite-lock
reports stay separately scoped, not declared solved by this patch.

Latest reviewed official Hermes is `ea81748579ee1732d214ccb75f91d22208ed623d`;
its new local-model/PM and WhatsApp changes do not alter context-engine lifecycle
hooks. Local integration uses deployed overlay `0764e9165721` (official base
`24b9f0f8c5df`). The newer core was source-reviewed, not deployed in this round.
Retire the adaptation when upstream provides equivalent guarded publication
and the full maintained hidden-backlog/replay/low-FD tests pass unchanged.

## Local verification

Complete release gate passes: dependency contract, Python/shell checks, focused
pytest, benchmark/stress smoke, full pytest, low-FD pytest and release stress.
Both full runs report **3529 passed, 6 skipped, 12 expected failures**. Nineteen
targeted tests pass; twelve of thirteen upstream tests failed before the fix.
The final gate includes the session-end race contract update described above.
Ruff/whitespace checks pass. Hosted CI and deployment health remain separate
acceptance layers, not provider or real-chat E2E evidence.
