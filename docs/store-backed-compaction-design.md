# Store-backed leaf compaction: #585 implementation contract

The observed failure in [upstream #585](https://github.com/stephenschoettler/hermes-lcm/issues/585)
is not a missing threshold flag: the active `messages` list can contain less than
one leaf chunk while an older, unreflected prefix is durable in `messages`.
`tests/test_store_backlog_compaction.py` is now a passing end-to-end gate:
preflight requests work, hidden source rows enter D0 nodes in bounded passes,
the current request stays in returned context, and raw rows remain recoverable.

## Implemented path and invariants

1. After a successful ingest, use the bound session's persisted compaction
   frontier and the effective count/token-bounded fresh-tail boundary. Never
   borrow a frontier from a side-channel or another session. A status-only
   `post_frontier` aggregate is an upper bound, not an eligibility decision.
2. Identify a genuinely hidden prefix **before** the first active raw message
   aligned backward to recent store rows. Do not widen from the whole session or reintroduce
   already summarized rows, synthetic summary scaffolding, ignored placeholders,
   or a past user request as a fresh anchor. Keep the original active message
   list as the anchor source during assembly.
   When the active middle itself has an eligible leaf, compact it first to
   reduce the live prompt. A newer active leaf may publish its exact D0 source
   IDs, but it must **not** leap the monotonic lifecycle frontier across the
   older hidden gap. A later store-backed pass covers that prefix, skipping
   the already-published active source IDs.
3. Load the oldest prefix in store order using a bounded page/actual-token
   budget. Preserve assistant/tool-result groups at the boundary. Resolve
   externalized payloads according to the existing ingest and pre-compaction
   rules; never summarize only a sidecar path as though it were source text.
   A late tool result after a covered assistant call remains raw source text;
   it may begin the serialized leaf when earlier D0 rows prove the parent was
   already covered. A summary rescue that stops inside an assistant tool group
   aborts before node/frontier publication.
4. Run the same ignore/dependent-reply sanitation as an ordinary leaf pass.
   Publish exact `store_id` source lineage for the consumed chunk, retain raw
   rows, and move the frontier only through rows actually consumed. A retry
   that shrinks the chunk must not advance past its unconsumed suffix.
5. Publish node and frontier as one durable SQLite transaction, guarded by the current
   session binding. A crash or session rollover between publication and cursor
   advance must not duplicate a node or mark uncovered rows as covered.
6. Respect the configured foreground pass/time budget and summary spend guard.
   If more hidden rows remain, retain maintenance debt instead of reporting
   `raw_prefix_drained` or clearing debt based on the smaller active window.

## Evidence and release gate

- Live database, read-only measurement on 2026-09-23: the largest session had
  41,767 rows; the indexed `(session_id, store_id)` diagnostic aggregate took
  about 26 ms. This bounds the status-query cost, **not** the cost of loading
  message bodies or provider summarization.
- A SQLite online-backup copy of the active database (264,493 rows) exercised
  the hidden-prefix selector without gateway writes. For a 29,883-row session
  and a 128-message active-tail fixture, range selection took about 0.33 s;
  the first eligible leaf contained 14 raw rows after already-covered D0 rows,
  including a late tool result. This validates selector mechanics, not model
  quality or live-turn latency.
- The existing [#553](https://github.com/stephenschoettler/hermes-lcm/pull/553)
  closed proposal advances the same frontier on every ingest and widens from
  the store; it changes the frontier from *compacted through* to *ingested
  through*, so it is not a safe direct transplant into this fork.
- Tests now cover duplicate text, valid/missing externalized sidecars,
  ignored rows, tool groups, rescue shrinkage, concurrent rollover,
  atomic rollback, restart idempotence, and a large active window with no
  hidden prefix. Full Python-version CI and a copied SQLite fixture are
  release gates for each implementation revision.

## Deliberate bounds

The hidden-store path scans at most 16,384 source rows and 4 million restored
characters per pass, and aligns active windows of at most 5,000 messages.
An unresolved sidecar, unpaired new tool group, or unmapped newest active message
leaves the durable rows in place rather than publishing an ambiguous summary.
These bounds can leave some sessions with visible debt and no automatic leaf;
`lcm_status.store.post_frontier` remains an upper-bound diagnostic, not proof
of eligible work. Inspect the blocking row/identity and fix its source before
expanding limits.
