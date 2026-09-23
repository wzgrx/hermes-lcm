# Upstream issue/PR audit — 2026-09-23

This is a point-in-time maintainer record for the maintained fork. Check the
current upstream diff and this fork's tests before taking any later revision.

| Upstream | Fork implementation | Verification/decision |
| --- | --- | --- |
| [#625 explicit `/new`](https://github.com/stephenschoettler/hermes-lcm/pull/625) | `05ec069` | Clear only the outgoing conversation's automatic carry; retain raw rows and historical DAG nodes for recall. Late old-session finalization is fenced. |
| [#626 lazy clone binding](https://github.com/stephenschoettler/hermes-lcm/pull/626) | `4aabbb0`, `ceab1c8`, `01dffae` | Clone without SQLite I/O, serialize shared reads, and fence first-time optional schema creation. Covered by lazy-storage and concurrent-bind tests. |
| [#621 foreground compaction budget](https://github.com/stephenschoettler/hermes-lcm/pull/621) | `d74cbdb` | Automatic pass/time ceilings are configurable; forced/manual compaction retains its separate budget. |
| [#614 tiny leaf](https://github.com/stephenschoettler/hermes-lcm/issues/614) | `4c0b9df` | Skip wasteful provider calls for tiny leaves and preserve healthy summary circuits. |
| [#620 orphaned externalized refs](https://github.com/stephenschoettler/hermes-lcm/pull/620) | Existing read-only integrity scan | Do **not** transplant the proposed row-deletion routine: deleting a message because its sidecar is missing destroys the surviving transcript and can leave DAG/chunk/lifecycle evidence inconsistent. In this fork both externalization writers return `None` on an `OSError`, so ingest retains the original inline content; `tests/test_ingest_protection.py` covers this failure path. `/lcm doctor` already reports missing refs without deleting rows. Repair should first recover sidecars from verified backups or original host state, then validate referential integrity. |
| [#585 durable backlog visibility](https://github.com/stephenschoettler/hermes-lcm/issues/585) | Open design gap | Current `_leaf_compaction_candidate_status`, `_refresh_raw_backlog_debt`, and `_should_run_deferred_maintenance` still use only the assembled window's middle slice. A store-frontier token query alone would improve debt telemetry but not make hidden rows compactable: the compaction loop also selects from `working_messages`. A complete fix must assemble a bounded, ordered store-backed candidate with source lineage and fresh-tail protection, then advance the frontier atomically. Do not claim that foreground budget caps solve this backlog-visibility issue. |
| [#622 async background compaction](https://github.com/stephenschoettler/hermes-lcm/issues/622) | Not integrated | Reference branch is a large subsystem based on an older fork point. Its five publication/visibility invariants need a current-main implementation and concurrency proof before deployment. The bounded foreground policy above remains the active latency control. |

For a live database, keep diagnostics read-only while the gateway is running.
Before any repair, take a consistent SQLite backup and verify it with
`PRAGMA quick_check`; never delete a message merely to silence a missing-ref
warning.
