# Store-backed leaf compaction: #585 implementation contract

The observed failure in [upstream #585](https://github.com/stephenschoettler/hermes-lcm/issues/585)
is not a missing threshold flag: the active `messages` list can contain less than
one leaf chunk while an older, unreflected prefix is durable in `messages`.
`tests/test_store_backlog_compaction.py` is an expected-failure end-to-end gate:
preflight must request work, the first hidden source row must enter a D0 node,
the current request must stay in the returned context, and the raw row must
remain recoverable. Remove its xfail only when all assertions pass.

## Required path

1. After a successful ingest, use the bound session's persisted compaction
   frontier and the effective count/token-bounded fresh-tail boundary. Never
   borrow a frontier from a side-channel or another session. A status-only
   `post_frontier` aggregate is an upper bound, not an eligibility decision.
2. Identify a genuinely hidden prefix **before** the first active raw message
   mapped to a store row. Do not widen from the whole session or reintroduce
   already summarized rows, synthetic summary scaffolding, ignored placeholders,
   or a past user request as a fresh anchor. Keep the original active message
   list as the anchor source during assembly.
3. Load the oldest prefix in store order using a bounded page/actual-token
   budget. Preserve assistant/tool-result groups at the boundary. Resolve
   externalized payloads according to the existing ingest and pre-compaction
   rules; never summarize only a sidecar path as though it were source text.
4. Run the same ignore/dependent-reply sanitation as an ordinary leaf pass.
   Publish exact `store_id` source lineage for the consumed chunk, retain raw
   rows, and move the frontier only through rows actually consumed. A retry
   that shrinks the chunk must not advance past its unconsumed suffix.
5. Publish node and frontier as one durable operation, guarded by the current
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
- The existing [#553](https://github.com/stephenschoettler/hermes-lcm/pull/553)
  closed proposal advances the same frontier on every ingest and widens from
  the store; it changes the frontier from *compacted through* to *ingested
  through*, so it is not a safe direct transplant into this fork.
- Before deployment, add tests for duplicate text, mixed media/externalized
  rows, ignored messages, tool groups, rescue shrinkage, concurrent rollover,
  crash/retry idempotence, and a large active window with no hidden prefix.
  Run full Python-version CI and exercise the new path against a copied SQLite
  fixture, never a live writable database.
