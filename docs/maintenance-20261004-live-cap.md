# Host-assigned absolute trigger caps — 2026-10-04

## Finding

Providing `_coerce_threshold_tokens_cap` fixes a missing-method exception, but
does not make a host-written cap affect LCM. The existing trigger was a plain
integer. Core writes `threshold_tokens_cap`, then invalidates private caches
that LCM does not use; both preflight and `should_compress` still read the old
integer. This patch retains an uncapped trigger and derives its minimum with
the normalized host cap on access. No need to impersonate native cache slots.

Removing/invalidating a cap restores the original trigger, not a previously
capped number. Model switches recompute the base; clone copies the independent
cap value; profile rebinding clears it. An unknown window stays disabled.
`_effective_threshold_cap` also supports the host's startup status presentation.
Existing finite coercion behavior is retained.

## Scope and deliberate exclusions

Reviewed [upstream #595](https://github.com/stephenschoettler/hermes-lcm/pull/595)
and [the3asic #7](https://github.com/the3asic/hermes-lcm/pull/7), including review
findings about initially pinned windows and stale profile pins. This is an
independent small adaptation of the absolute-cap boundary, not a cherry-pick of
the complete live-policy implementation. Dynamic ratio/model-threshold reload,
initial native-config cap import, and context-pin add/remove need their own
profile/route tests and are **not implemented by this patch**. The host must
actually assign the cap; merely changing an unused field is not evidence.

Effective trigger = min(existing ratio/assembly-derived trigger, positive live
cap, known context window). Manual ratio remains unchanged; a separately set
absolute cap can intentionally lower the trigger. `max_assembly_tokens` and
reserve floors are unchanged. Cooldowns, no-progress guards and automatic
attempt limits are not reset. No schema/dependency/provider/model/config edits,
no session rewrites, and no raw-history or SQLite deletion.

Thirteen focused regressions cover enable/remove, stricter assembly caps,
model switches, clones, profile rebinds, bad values, unknown windows and native
status compatibility; six fail before the patch. Full suite and release gates
are separate from live-provider/Feishu acceptance.

## Verification

Full release runner passes, including **3556 passed, 6 skipped, 12 expected
failures** in both ordinary and low-FD suites. Ruff, dependency, compile, shell,
focused, benchmark and stress gates pass. The first exploratory run exposed a
new test-cleanup bug: plugin-wide unload closed the shared recall pool for later
tests. Fixture teardown now closes only its own engine/clone; no production
retrieval code or assertions were weakened. Hosted CI is a separate gate.
