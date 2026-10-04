# 2026-10-04 — persisted-output lookup maintenance

## Reviewed source and bounded choices

Official Hermes main remains `24b9f0f8c5df5ec6d3d5c10ad9b27c3346bbc925`;
the tested local host is that snapshot plus existing overlays (`0764e9165721`).
LCM upstream main remains `8d1b1e6d3d63f5fc7b209e8d7ec1dc9b814f2e54`.
This is a maintained-fork commit after 1.0.0-rc.2, not a newly tagged release.

| Upstream proposal | Review decision |
|---|---|
| [#655](https://github.com/stephenschoettler/hermes-lcm/pull/655), head `fae8530a6ec8bd63426b74f23fec611d8b9fb40d` | Adapt routing-path index and invalidation, plus damaged-input handling below. |
| [#663](https://github.com/stephenschoettler/hermes-lcm/pull/663), #597, #647 | Existing fork already has 0600 no-open handling, O_PATH tightening, fail-closed non-O_PATH behavior and lock-safe Linux creation. Keep these stronger existing protections. |
| [#659](https://github.com/stephenschoettler/hermes-lcm/pull/659) | Durable Core UID bridge is a separate ingestion-identity change. Reviewed, not imported in this bounded output-lookup update. |
| [#657](https://github.com/stephenschoettler/hermes-lcm/pull/657) | Atomic active-tool-occurrence budgeting changes compaction selection; requires a separate data/host acceptance gate. |
| [#502](https://github.com/stephenschoettler/hermes-lcm/pull/502), [#540](https://github.com/stephenschoettler/hermes-lcm/pull/540) | Reasoning controls and Codex context resolution are separate provider-policy work, not needed to solve the reproduced output lookup defects. |

Open reports are not proof of a new local incident. No claim is made that all
upstream proposals were merged or every issue is solved.

## Implemented behavior

Previously every persisted-output lookup parsed every JSON payload in its
storage directory, including unrelated large outputs. Reconciliation can repeat
that operation many times. The adapted process-local index stores only routing
metadata and signatures, not tool-output text. Warm lookups stat the indexed
files, then freshly parse candidate payloads and retain the existing marker,
generation, session and content validation.

- Eight-directory LRU bound; metadata within each directory still grows with
  file count. This is reduced JSON reading, not a constant-time lookup claim.
- Internal writes/session reassignment invalidate the index. Directory and file
  signatures detect external create/remove/replace and in-place routing changes.
- Cold scans retry instability up to three times, then use the original full
  candidate scan for that lookup. Damaged JSON is retried after later repair.
- Added tolerant candidate reads: non-object JSON and invalid UTF-8 now skip
  that damaged artifact rather than aborting discovery of other valid results.
  This also protects the unstable-index full-scan path. No artifact is deleted.
- Original messages, source output files, database schema, dependencies,
  provider credentials, thresholds and reasoning settings remain unchanged.

Retirement condition: upstream ships equivalent candidate selection plus
invalidation/invalid-input tests, and the maintained-fork regression suite stays
green after removing this adaptation.

## Reproducible verification

Fourteen focused cases pass. Against unchanged fork code, eight failed: one
warm-scan performance assertion, four malformed-entry failures, and three new
index API contracts not yet implemented. Six compatibility cases already
passed. Tests include in-place route mutation, external writes, cold-scan
instability, parse repair, cache bound, two sessions sharing one call ID, fresh
content/deletion, and marker-proof rejection.

The deterministic read-count fixture has 40 unrelated files plus one candidate:
a warm lookup now reads only the candidate instead of all 41 payloads. This is
an offline operation-count result, not a measured production latency ratio.

Full isolated suite against the selected Hermes: **3496 passed, 6 skipped,
12 expected failures** under a 1024-descriptor limit. Ruff and whitespace checks
pass. Full release-validation, exact-commit hosted CI and runtime deployment
are recorded separately after their gates complete. No provider calls or live
profile edits are part of tests. Hosted minimal-host tests and actual-host local
tests are distinct evidence.

## Operator notes

Update via the maintained Hermes plugin/PM workflow in `INSTALL.md`, then restart
the host to load code. Keep a consistent SQLite backup and previous plugin ref.
An index is in-memory only and begins cold after restart; no migration, model
download, cache purge, conversation rewrite or configuration retuning is needed.
This round also corrects fork README badges and the runtime dependency claim
to match this fork's existing `pyproject.toml`; no dependency was added.


The complete `scripts/validate_release.sh --full --keep-going` gate passes:
dependency contract, compile/shell checks, focused pytest, benchmark smoke,
stress smoke, full pytest, low-FD pytest and release stress. Both full invocations
report 3496 passed, 6 skipped and 12 expected failures. README/maintenance prose
was synchronized afterwards; source and regression tests are unchanged.
