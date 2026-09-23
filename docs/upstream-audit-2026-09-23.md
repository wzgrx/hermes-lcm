# Upstream issue/PR audit — 2026-09-23

This is a point-in-time maintainer record for the maintained fork. Check the
current upstream diff and this fork's tests before taking any later revision.

| Upstream | Fork implementation | Verification/decision |
| --- | --- | --- |
| [#625 explicit `/new`](https://github.com/stephenschoettler/hermes-lcm/pull/625) | `05ec069` | Clear only the outgoing conversation's automatic carry; retain raw rows and historical DAG nodes for recall. Late old-session finalization is fenced. |
| [#626 lazy clone binding](https://github.com/stephenschoettler/hermes-lcm/pull/626) | `4aabbb0`, `ceab1c8`, `01dffae` | Clone without SQLite I/O, serialize shared reads, and fence first-time optional schema creation. Covered by lazy-storage and concurrent-bind tests. |
| [#621 foreground compaction budget](https://github.com/stephenschoettler/hermes-lcm/pull/621) | `d74cbdb` | Automatic pass/time ceilings are configurable; forced/manual compaction retains its separate budget. |
| [#516 under-threshold opportunistic maintenance](https://github.com/stephenschoettler/hermes-lcm/pull/516) | `5f9dfbb` and `tests/test_preflight_media_maintenance.py` | This fork already defers divergent-replay leaf maintenance below the context threshold, while persisting externalized payloads during preflight. The focused regression also proves a required threshold trigger still runs. No extra pressure-ratio knob is needed for this behavior. |
| [#614 tiny leaf](https://github.com/stephenschoettler/hermes-lcm/issues/614) | `4c0b9df` | Skip wasteful provider calls for tiny leaves and preserve healthy summary circuits. |
| [#620 orphaned externalized refs](https://github.com/stephenschoettler/hermes-lcm/pull/620) | Existing read-only integrity scan | Do **not** transplant the proposed row-deletion routine: deleting a message because its sidecar is missing destroys the surviving transcript and can leave DAG/chunk/lifecycle evidence inconsistent. In this fork both externalization writers return `None` on an `OSError`, so ingest retains the original inline content; `tests/test_ingest_protection.py` covers this failure path. `/lcm doctor` already reports missing refs without deleting rows. Repair should first recover sidecars from verified backups or original host state, then validate referential integrity. |
| [#585 durable backlog visibility](https://github.com/stephenschoettler/hermes-lcm/issues/585) | Bounded store-backed hidden-prefix compaction implemented; broader cases remain under audit | Preflight now detects an eligible hidden prefix after successful ingest; the compactor loads bounded, ordered, uncovered store rows before the first active raw row/protected tail, resolves sidecars, preserves tool groups, filters ignored content, publishes exact source IDs and the frontier atomically, and keeps debt while rows remain. When active-window raw is already eligible, it is compacted first to lower live prompt pressure; that newer D0 node does not move the lifecycle cursor past an older hidden gap, even when the gap is below one leaf chunk. `tests/test_store_backlog_compaction.py` covers this ordering plus duplicate text, rescue shrinkage, rollover, restart, and sidecar failures. `lcm_status.store.post_frontier` remains an upper-bound diagnostic. Deliberate limits and blocked-row behavior are in `docs/store-backed-compaction-design.md`; these require observation before calling all variants of #585 resolved. |
| [#622 async background compaction](https://github.com/stephenschoettler/hermes-lcm/issues/622) | Not integrated | Reference branch is a large subsystem based on an older fork point. Its five publication/visibility invariants need a current-main implementation and concurrency proof before deployment. The bounded foreground policy above remains the active latency control. |

For a live database, keep diagnostics read-only while the gateway is running.
Before any repair, take a consistent SQLite backup and verify it with
`PRAGMA quick_check`; never delete a message merely to silence a missing-ref
warning.

Local concurrency regression found during the #585 follow-up: two lazy
`AssertionStore` initializers could expose a missing
`lcm_assertion_source_insert_guard` between `DROP TRIGGER` and the subsequent
`executescript`. The assertion-family DDL now rebuilds that guard inside one
`BEGIN IMMEDIATE` / `COMMIT` transaction. The parallel initializer test covers
eight opens and was repeated 20 times locally; this is narrower than upstream
[#601](https://github.com/stephenschoettler/hermes-lcm/issues/601)'s broader
WAL/handle incident, which needs its own ongoing audit.
