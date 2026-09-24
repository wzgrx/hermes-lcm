# Opt-in async/background compaction with atomic publish

Design spike for preparing old stable chunks off the turn-critical path while keeping current LCM behavior unchanged unless explicitly enabled.

Refs:

- Hermes LCM #622 — opt-in async background compaction and five publication invariants
- Lossless Claw #807 — prepare incremental summaries in the background and publish atomically
- Lossless Claw #942 — live config must beat stale persisted thresholds/debt
- Lossless Claw #902 — summary failure/backoff must not wedge compaction debt forever

## Problem

Today `LCMEngine.compress()` does the expensive work synchronously: ingest, select the oldest raw backlog outside the fresh tail, call the summarizer, write canonical DAG nodes, optionally condense, then assemble the active context. That preserves the important cache-friendly property: active context changes only at threshold/full-sweep boundaries. The downside is foreground latency, especially with slow local summarizers or serial leaf chains.

The safe target is not “write summaries in another thread and flip a boolean.” The target is a two-phase lifecycle:

1. **Prepare** non-canonical summaries for old, stable raw-message chunks in the background.
2. **Promote** a complete, still-valid batch atomically when normal foreground compaction would have run.

Until promotion, active context, search, recall, expansion, transcript GC, and doctor integrity checks behave as if the prepared summaries do not exist.

## Non-goals for the first slice

- No default behavior change.
- No mandatory background thread in normal installs.
- No prebuilt condensation layers in the first slice. Leaf-only promotion is already useful and is easier to prove atomic.
- No replacement for foreground compaction. If prepared work is absent, incomplete, stale, or invalid, foreground compaction falls back to today’s path.
- No persisted threshold override that can win over live config. Persisted metadata is evidence to validate against live policy, not policy itself.

## Proposed flags

Add config fields, all disabled by default:

| Field | Env | Default | Meaning |
| --- | --- | ---: | --- |
| `async_background_compaction_enabled` | `LCM_BACKGROUND_COMPACTION_ENABLED` | `false` | Enables the feature surface; matches the upstream issue's master flag. |
| `async_background_compaction_worker_enabled` | `LCM_ASYNC_BACKGROUND_COMPACTION_WORKER_ENABLED` | `false` | Allows automatic background preparation. Tests and hosts may still call one-shot prep manually when the feature is enabled. |
| `async_background_compaction_max_batches` | `LCM_ASYNC_BACKGROUND_COMPACTION_MAX_BATCHES` | `2` | Backpressure cap per conversation. |
| `async_background_compaction_retry_backoff_seconds` | `LCM_ASYNC_BACKGROUND_COMPACTION_RETRY_BACKOFF_SECONDS` | `300` | Cooldown after summary failures. |

The enable flag should guard all writes to the new tables and all promotion attempts. Reader filters must still be robust if old pending rows exist after the flag is later disabled.

## Storage model

Add tables separate from canonical `summary_nodes`:

### `lcm_compaction_batches`

One row per prepared generation.

Suggested columns:

- `batch_id TEXT PRIMARY KEY`
- `conversation_id TEXT NOT NULL`
- `session_id TEXT NOT NULL`
- `state TEXT NOT NULL` — `pending`, `preparing`, `ready`, `promoting`, `promoted`, `rejected`, `failed`, `superseded`
- `frontier_start_store_id INTEGER NOT NULL`
- `source_frontier_start_store_id INTEGER NOT NULL` — normally the same as the
  lifecycle frontier; for the first leaf it may skip only a contiguous leading
  system-anchor prefix, which is not summary source material.
- `frontier_end_store_id INTEGER NOT NULL`
- `fresh_tail_count INTEGER NOT NULL`
- `leaf_chunk_tokens INTEGER NOT NULL`
- `policy_fingerprint TEXT NOT NULL`
- `summary_route_fingerprint TEXT NOT NULL`
- `source_coverage_hash TEXT NOT NULL`
- `source_ids_json TEXT NOT NULL` — exact ordered source IDs planned in the batch
- `source_identity_hashes_json TEXT NOT NULL` — one identity digest per planned ID
- `preparer_identity TEXT NOT NULL DEFAULT ''` — Linux boot/PID-namespace/process-start identity of the current owner; refreshed when another process claims preparation, with the boot ID stored only as a digest
- `expected_leaf_count INTEGER NOT NULL`
- `prepared_leaf_count INTEGER NOT NULL DEFAULT 0`
- `failure_count INTEGER NOT NULL DEFAULT 0`
- `next_retry_at REAL`
- `last_error TEXT`
- `created_at REAL NOT NULL`
- `updated_at REAL NOT NULL`
- `promoted_at REAL`
- `rejected_reason TEXT`

Indexes:

- `(conversation_id, state, created_at)`
- `(session_id, state, created_at)`
- partial `(state, preparer_identity)` for active, owned claims only
- `(next_retry_at, state)`

### `lcm_pending_summary_nodes`

Prepared but non-canonical leaf summaries.

Suggested columns:

- `pending_id TEXT PRIMARY KEY`
- `batch_id TEXT NOT NULL REFERENCES compaction_batches(batch_id)`
- `conversation_id TEXT NOT NULL`
- `session_id TEXT NOT NULL`
- `depth INTEGER NOT NULL DEFAULT 0`
- `summary TEXT NOT NULL`
- `token_count INTEGER NOT NULL`
- `source_token_count INTEGER NOT NULL`
- `source_ids TEXT NOT NULL`
- `source_identity_hashes TEXT NOT NULL`
- `source_range_start_store_id INTEGER NOT NULL`
- `source_range_end_store_id INTEGER NOT NULL`
- `previous_pending_ids TEXT NOT NULL DEFAULT '[]'`
- `created_at REAL NOT NULL`
- `earliest_at REAL`
- `latest_at REAL`
- `expand_hint TEXT DEFAULT ''`

Indexes:

- `(batch_id, source_range_start_store_id)`
- `(conversation_id, state)` is intentionally on the batch table; pending nodes should not have independent canonical state.

Do **not** add pending rows to `summary_nodes`. Reusing the canonical table with a lifecycle flag would make every reader, FTS query, and integrity check a footgun. Keeping pending rows in a separate table gives active-only defaults naturally.

### Implementation status on `feature/async-background-compaction`

The isolated branch now has default-off config flags, an optional store bound
only when enabled, transactional schema creation, source-identity snapshots,
staging with overlap checks, exact ready-state coverage validation, and a
single-transaction canonical publisher with frontier compare-and-swap. The
publisher rejects stale source/policy/route/fresh-tail inputs and canonical
overlap; publication failure rolls back nodes, frontier, and batch state.

`LCMEngine.prepare_background_compaction_once()` is a manual, off-turn
entry point for one old leaf. It fences live policy and Hermes' compression
route, claims one active batch per frontier, calls the summarizer outside any
SQLite transaction, and records typed failures with backoff. Its first slice
is conservative: it requires an established frontier beyond any leading
system anchor, excludes ignored-message-pattern sessions and unresolved
externalized refs, and only prepares a complete tool-call group outside the
stored fresh tail. It leaves the canonical DAG and active context unchanged.

The normal foreground leaf loop now attempts publication only when a ready
batch's ordered source IDs match an exact active raw prefix outside the live
fresh tail. It skips provider summarization on success, then uses the existing
condensation and active-context assembly path. A stale route or source falls
back to the synchronous leaf path. Status and Doctor expose separate queue
counts; default-off does not materialize optional tables.

With both master and worker flags enabled, successful post-turn ingest now
queues a bounded, deduplicated worker keyed by database and conversation/session.
The worker opens private SQLite helpers and uses an immutable binding snapshot;
it does not run the provider on the foreground engine. Plugin unload drains
accepted work, while retirement of an individual foreground engine leaves its
already accepted preparation intact. On Linux, an incomplete claim owned by
a provably exited process is released at store reopen (or the next recovery
pass). If the process identity cannot be verified, the lease fallback releases
claims older than twice the provider timeout (minimum five minutes). A live
other process is not reclaimed by the immediate dead-owner check; the
time-based lease still applies if a provider stalls beyond its bound. Ready
batches remain durable and are revalidated at promotion. The immediate recovery
probe reads active identities without a write lock and only enters a write
transaction for a provably exited owner; that update also checks the owner
identity has not changed since the probe.

Before the automatic worker opens its private SQLite connection, it shares the
read-only background-write preflight used by temporal rollups: same-UID
deleted-handle scan plus targeted integrity checks for `metadata` and
`lcm_migration_state`. A failed or inconclusive check defers preparation and
backs off repeated attempts for five minutes; a transient SQLite lock skips
that pass. This contains the upstream #601 failure class but does not establish
its full root cause. The explicit one-shot preparation API remains a separate
operator/test seam; the automatic worker is the guarded path.

The worker can now prepare a consecutive ready chain ahead of the live frontier
(default at most two batches; at most four preparations in one scheduled pass).
Each future claim validates a ready predecessor and source identities inside
its write transaction. Foreground promotion still consumes one batch at a time
in frontier order; a future batch remains inactive until its predecessor has
committed. The next preparation pass retires queue claims left unreachable by
a session reset, failed preparation, or rejected ancestor. A provider result
arriving after a reset or predecessor rejection cannot become ready.

Emergency overflow recovery may reuse a ready leaf only when that leaf covers
the **entire** old raw candidate outside the protected fresh tail. If it covers
only a prefix, the existing bounded foreground emergency summary path remains
in charge so the rest of the old candidate is not silently dropped from the
emergency summary. Simultaneous preparers are fenced by the unique active
frontier claim, and a foreground publisher that wins while a provider is still
running prevents that late result from becoming ready.

The first leaf can now prepare behind a stored system anchor without advancing
the lifecycle frontier early: the batch records a separate source-selection
frontier, and both readiness and publication revalidate that every skipped row
is still a system anchor. The one publication transaction still moves the
canonical frontier from its original value to the leaf end.

This remains an **experimental isolated branch**, not installed in the live
Gateway. It still needs broader queue-race and partial-emergency stress tests,
plus long-session performance coverage before deployment. The offline
single-leaf scheduling microbenchmark in
`benchmarks/benchmark_async_compaction.py` verifies foreground provider calls
move from one per run to zero when a complete ready leaf exists; it is not a
long-session quality or real-provider measurement. The worker flag is off by
default even when the master feature flag is enabled.

The same offline harness accepts `--old-messages`, `--max-batches`, and
`--turns` to stress a multi-leaf backlog across successive turn boundaries.
It compares cumulative foreground time only when both modes cover
the same raw source IDs **and every requested old source**; matching partial
coverage is not a valid speedup comparison. Neither check establishes equal
summary quality. The report distinguishes coverage parity from completeness
and leaves `foreground_median_reduction_percent` unset when either fails.
The initial multi-leaf fixture inserted rows with `token_estimate=0` (the
`MessageStore.append()` default), unlike the production ingest path, which
persists per-message estimates. Dynamic preparation therefore saw a zero-token
backlog, selected its minimum 256-token target, and wasted one provider call
per old message. The previous four-message cap-2/cap-4 table was a legacy-row
stress case, **not** a representative production performance comparison. The
preparer now adds a bounded, read-only token count from at most 64 of the
already-fetched oldest unstamped rows (up to one million content characters) before choosing
its dynamic target. It does not rewrite historical rows or scan the full
transcript. The preparation policy fingerprint changed to v2, so old planned
batches are revalidated rather than silently reused after this selection change.

On 2026-09-24, five runs of **two turns** with eight approximately 3,000-token
old messages, a queue cap of two, and a fixed 50 ms summarizer stub yielded:

| Initial old-row estimates | Cumulative foreground median, staged / sync | Off-turn preparation median | Provider calls over five runs, staged / sync |
| --- | ---: | ---: | ---: |
| Present (normal ingest) | 6.0 / 159.8 ms | 105.8 ms | 10 (all off-turn) / 15 |
| Missing (legacy/direct insert) | 6.1 / 160.6 ms | 105.9 ms | 10 (all off-turn) / 15 |

Both modes covered all eight old source IDs by the first turn and retained all
raw rows; the second turn adds a fresh synthetic reply/request. These are
fixed-delay scheduling numbers, not real-provider latency, summary quality,
token spend, or a recommendation to enable the worker globally. A run with
`--missing-token-estimates` exercises the legacy case explicitly; the default
models production ingest. The deliberately low threshold forces compaction and
does not forecast the live Hermes threshold or network latency.

## Executable acceptance coverage

The original `test_async_background_compaction_design.py` spike called an
obsolete positional preparation API and marked every scenario as an expected
failure. The following executable tests replace its checks against the current
API; no design-only expected failures remain:

| Scenario | Executable test |
| --- | --- |
| Default-off has no optional store/tables | `test_disabled_store_does_not_create_database_or_optional_tables`, `test_disabled_preparation_is_inert` |
| Pending summary is absent from DAG/search | `test_staged_leaf_is_durable_but_invisible_to_canonical_dag`, `test_pending_summary_text_is_absent_from_active_search` |
| Source rewrite rejects preparation/publication | `test_source_rewrite_during_summary_preparation_fails_closed`, `test_foreground_rejects_rewritten_source_then_summarizes_current_rows` |
| Live policy and threshold beat staged metadata | `test_promotion_rejects_stale_policy_route_or_fresh_tail`, `test_foreground_uses_live_threshold_policy_over_prepared_batch` |
| Live summary route beats staged metadata | `test_foreground_falls_back_when_summary_route_changes` |
| Foreground and background publication race | `test_foreground_winner_fences_inflight_background_provider`, `test_two_publishers_serialize_and_publish_once`, `test_two_processes_publish_once_without_partial_canonical_state` (three independent databases with a simultaneous release gate) |
| Provider failure/backoff leaves foreground usable | `test_summary_failure_records_type_only_and_enforces_backoff`, `test_background_failure_backoff_does_not_block_foreground_compaction` |
| Automatic worker skips unsafe SQLite state, then resumes after recovery | `test_background_preparation_honors_sqlite_integrity_gate` |
| Restart recovery | `test_live_other_process_is_preserved_then_dead_owner_recovers_on_open` (Linux dead-owner immediate recovery; live owner preserved), `test_restart_recovery_releases_only_abandoned_incomplete_claims` (lease fallback for unknown ownership) |
| Atomic success and rollback | `test_promotion_publishes_nodes_frontier_and_batch_in_one_transaction`, `test_mid_publication_failure_rolls_back_all_canonical_changes` |
| Status/Doctor counters | `test_manual_preparation_calls_provider_outside_sqlite_transaction` |

The remaining work is not represented as a passing claim: broader multi-process
stress and long-session latency/quality measurements still require evidence.

## Fingerprints and validation inputs

A prepared batch is valid only for the exact policy and source frontier it was created for.

### Policy fingerprint

Hash a normalized JSON object of compaction policy inputs that affect chunking or active-context semantics:

- schema/protocol version (`lcm-async-leaf-v2`)
- `fresh_tail_count`
- `leaf_chunk_tokens`
- `context_threshold` / effective preflight threshold policy
- `dynamic_leaf_chunk_enabled`
- `dynamic_leaf_chunk_max`
- `ignore_message_patterns` plus their source
- sensitive-pattern config that changes stored/summarized text
- large-output externalization settings that change serializer input
- `custom_instructions`
- `l2_budget_ratio`, `l3_truncate_tokens`
- `incremental_max_depth` only if the batch later supports pending condensation

### Summary route fingerprint

Hash the effective summarizer contract:

- `summary_model`
- `summary_fallback_models`
- provider/model route after parsing, if available
- summarizer timeout class only if it changes produced summaries or failure policy
- plugin version/protocol version

A model/config change should not necessarily delete pending rows immediately, but promotion must reject rows whose fingerprints no longer match live config.

### Source coverage hash

For every source row in the prepared range, hash a canonical tuple:

```text
store_id | session_id | conversation_id | role | content_sha256 | tool_call_id | tool_calls_sha256 | tool_name | timestamp
```

Promotion validates both:

- the ordered set of `store_id`s is exactly what the batch claims;
- the identity hash for every row still matches.

This catches transcript reconciliation, late ingest of missing rows inside the range, externalization/GC rewrites, and accidental ordinal drift.

## Preparation lifecycle

Background preparation should operate only on raw messages that are outside the fresh tail at preparation time.

1. Resolve live config and compute the compactable prefix using the same filtering as foreground compaction.
2. Choose the oldest chunk(s) up to the current publishable frontier.
3. Create or resume a `pending`/`preparing` batch for that frontier.
4. Generate leaf summaries into `pending_summary_nodes`, optionally using previous pending summaries from the same batch as continuity context.
5. Mark the batch `ready` only when `prepared_leaf_count == expected_leaf_count` and every expected range is covered exactly once.

Preparation must not mutate:

- `summary_nodes`
- active replay context
- compaction count
- frontier markers
- transcript GC state
- generated placeholder ordinals

## Atomic promotion

Foreground compaction remains the owner of canonical changes. When `should_compress_preflight()` says compaction is needed, `compress()` may attempt prepared promotion before doing synchronous summarization.

Promotion runs in one SQLite transaction on a single connection, using `BEGIN IMMEDIATE` so foreground/background writers serialize.

The promotion path should not call existing helpers that perform their own commits on separate SQLite connections. Canonical node inserts, lifecycle frontier updates, batch state changes, and superseding older batches must share one transaction boundary. In-process markers such as `_last_compacted_store_id` should update only after the transaction commits.

Validation inside the transaction:

1. Feature flag still enabled.
2. Batch is `ready` for this `conversation_id` and `session_id`.
3. Live policy fingerprint equals batch policy fingerprint.
4. Live summary route fingerprint equals batch summary route fingerprint.
5. Current lifecycle frontier equals the batch’s expected start frontier.
6. Fresh-tail boundary still permits promoting the full prepared range. If the boundary moved such that only a prefix is safe, reject the batch for v1 rather than partially publish.
7. Source rows for the claimed range still exist, are ordered, and match every source identity hash.
8. Pending nodes cover the full source range without gaps or overlaps.
9. No canonical `summary_nodes` already cover the same source IDs. This handles a foreground compaction race that won before promotion.

Publish steps in the same transaction:

1. Insert canonical `summary_nodes` copied from pending rows.
2. Advance lifecycle frontier to the promoted end store id.
3. Mark the batch `promoted` with `promoted_at`.
4. Mark older pending/ready batches for the same conversation `superseded`.
5. Commit.

Only after commit may the engine assemble active context from canonical summaries. If any validation step fails, mark the batch `rejected` with a reason in a transaction that does **not** insert canonical summaries or advance the frontier, then fall back to today’s foreground compaction path.

## Race semantics

### Foreground compaction wins first

If a synchronous foreground pass inserts canonical nodes and advances the frontier while a background batch is pending, later promotion sees either frontier mismatch or canonical source overlap. It rejects/supersedes the stale batch and leaves the foreground result intact.

### Background ready wins first

Foreground promotion takes `BEGIN IMMEDIATE`, validates current source/frontier/fingerprints, publishes, then assembles. A concurrent background preparer trying to write the same batch waits or fails with normal SQLite busy behavior and must reload batch state before continuing.

### Background failure/backoff

Summary failure increments `failure_count`, stores a compact `last_error`, and sets `next_retry_at`. It must not create compaction debt that blocks foreground recovery. Foreground compaction can always ignore failed/pending work and use the current synchronous path.

### Restart

At optional-store open, `pending`/`preparing` rows owned by a provably exited
Linux process become `failed` with an immediately eligible retry. On the next
recovery pass, older unverifiable or stalled claims are also marked `failed`
after the configured lease. A live owner is preserved by the immediate check.
`ready` rows remain durable but require fresh validation before promotion.
Pending rows never enter active context. Canonical publication uses one SQLite
transaction; a crash cannot expose half-published nodes or frontier state.

## Reader and diagnostics rules

Active readers default to canonical rows only:

- `lcm_grep` summary search ignores pending rows.
- `lcm_expand(node_id=...)` cannot expand pending IDs through the canonical node path.
- `lcm_describe` active DAG overview excludes pending rows.
- transcript GC eligibility ignores pending rows.
- doctor active-context integrity checks ignore pending rows unless checking async health specifically.

Add an explicit async section to status/doctor instead:

```json
"async_compaction": {
  "enabled": false,
  "worker_enabled": false,
  "pending_batches": 0,
  "preparing_batches": 0,
  "prepared_batches": 0,
  "promoted_batches": 0,
  "rejected_batches": 0,
  "failed_batches": 0,
  "superseded_batches": 0,
  "pending_summaries": 0,
  "oldest_pending_age_seconds": null,
  "last_rejected_reason": null,
  "last_error": null
}
```

Doctor should warn, not fail, for normal disabled state. It should warn on:

- stale `preparing` batches beyond recovery timeout;
- ready batches whose live policy fingerprint no longer matches;
- failed batches whose backoff has expired but no worker has retried;
- pending rows whose batch is missing.

## Implementation sequence

1. **Acceptance tests first** for stale rejection, config change rejection, foreground/background race, summary failure/backoff, restart recovery, successful atomic promotion, pending invisibility, and status/doctor counts.
2. Schema only: create the two tables and reader filters, with feature disabled and no behavior change.
3. Manual one-shot preparer behind the flag, no automatic worker yet.
4. Atomic promotion path in `compress()` before foreground summarization, with fallback on any reject.
5. Status/doctor async counts.
6. Optional worker loop with backpressure and retry policy.
7. Later: pending condensed layers, if leaf-only promotion leaves too much foreground work.

## Test matrix

These are mirrored in `tests/test_async_background_compaction_design.py` as xfailed RED spike tests until the implementation exists.

| Test | Proves |
| --- | --- |
| `test_pending_summaries_are_invisible_until_atomic_promotion` | Pending rows do not affect active assembly/search/status counters except async diagnostics. |
| `test_atomic_promotion_rejects_stale_source_identity` | Transcript reconciliation or row rewrite invalidates the batch without canonical mutation. |
| `test_atomic_promotion_rejects_live_config_change` | Live config/policy wins over persisted prepared metadata. |
| `test_foreground_compaction_race_supersedes_pending_batch` | Foreground compaction and background promotion cannot double-compact or mix generations. |
| `test_summary_failure_backoff_does_not_wedge_foreground_compaction` | Failed background prep backs off but foreground synchronous compaction remains available. |
| `test_restart_recovers_or_discards_pending_batches_safely` | Restart never makes pending rows canonical and cleans stale lifecycle states. |
| `test_default_disabled_async_compaction_is_inert` | Default-off config performs no background preparation and reports zero async counts. |
| `test_atomic_promotion_rejects_summary_route_change` | Model/route changes reject stale prepared summaries before canonical mutation. |
| `test_atomic_promotion_rejects_live_threshold_policy_change` | Live threshold policy changes beat persisted prepared metadata. |
| `test_successful_atomic_promotion_is_all_or_nothing` | Canonical node insert, frontier advance, and batch promotion commit together. |
| `test_atomic_promotion_rolls_back_partial_publish_failure` | A mid-promotion failure leaves no canonical node/frontier/batch half-state. |
| `test_status_and_doctor_report_async_compaction_counts` | Operators see pending/prepared/promoted/rejected/failed counts. |

## Open questions

- Should v1 reject a ready batch when only a prefix is still publishable, or support prefix promotion? Recommendation: reject in v1. Prefix promotion makes continuity and expected leaf counts more complex.
- Should automatic workers live inside `LCMEngine`, a plugin lifecycle helper, or a host-managed scheduler? Recommendation: start with a manual one-shot preparer and make the automatic worker a later slice.
- Should route fingerprint include fallback model order? Recommendation: yes. Different fallback order can change output after partial failures.
- Should summary timeout changes reject prepared work? Recommendation: no unless timeout policy changes the output contract; include route/model/policy version, not operational timing knobs.
