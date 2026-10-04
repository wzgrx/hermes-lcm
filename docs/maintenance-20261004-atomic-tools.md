# 2026-10-04 — atomic active-context tool occurrences

Adapted from [upstream PR #657](https://github.com/stephenschoettler/hermes-lcm/pull/657),
reviewed head `58b7690cf785676067b096242bc119482f9a7003`, by the3asic.
This is maintenance after 1.0.0-rc.2, not a new tagged release.

## Why

The maintained fork reproduced four failures in the nine upstream tests:
an incomplete exchange selected under budget became an empty final context;
a sole irreducible user request disappeared; dropping protected tool context
was twice reported as successful overflow recovery. Five compatibility cases
already passed. These are isolated reproductions, not claims of observed damage
to the operator's live conversation.

## Behavior and boundaries

Group an assistant carrying tool calls with only its contiguous tool results.
Select or omit the whole occurrence; a reused ID in another turn does not supply
its results. Preserve existing non-contiguous-tail/objective-scaffold rules to
avoid duplicating raw user rows on replay. A sole oversized user request stays
visible and reports overflow instead of being silently removed.

If the newest post-user tool occurrence is omitted, recovery stays unsuccessful
even when the shortened payload fits. Diagnostics explicitly report the protected
group flag. This flag resets on every assembly, including uncapped assemblies.
Original messages, raw SQLite storage and summary DAG data are not changed by
selection. No database migration, provider change, threshold retuning, model
download, new dependency, or live conversation rewrite is part of this patch.

The upstream nine tests are supplemented by five maintained-fork checks: every
integer budget boundary, bounded/uncapped state reset, truthful diagnostics and
SQLite close/reopen ingestion that adds only the new user row. Existing restart,
externalized-output and session-boundary regressions remain mandatory.

Local tests use deployed Hermes `0764e9165721` (official base `24b9f0f8c5df`
plus reviewed overlays). New official main `ea81748579ee` changes local model/PM
and WhatsApp paths, not agent/context-engine lifecycle files; it was source-reviewed,
not deployed by this plugin-only update. Broader Core-UID bridge PR #659 and
atomic summary-publication PR #658 remain separate work, not silently merged.

Retirement: when upstream includes equivalent occurrence selection plus honest
overflow reporting and all maintained replay/assembly gates pass without this
adaptation. Full local/low-FD release gates, exact-commit CI and deployment health
are recorded after completion; synthetic tests are not provider E2E evidence.

## Local verification

The complete release-validation gate passes: dependency contract, Python/shell
checks, focused pytest, benchmark/stress smoke, full pytest, low-FD pytest and
release stress. Both full invocations report **3510 passed, 6 skipped, 12 expected
failures**. Fourteen focused cases pass; four of the original nine upstream
checks failed against the unchanged maintained fork. Ruff and whitespace checks
pass. Hosted CI and exact-code deployment are distinct following gates.
