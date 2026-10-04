# Non-finite context input handling — 2026-10-04

Maintenance on user main after 1.0.0-rc.2; not a new release tag.

Reviewed fresh upstream discussions in [#615](https://github.com/stephenschoettler/hermes-lcm/pull/615),
[#595](https://github.com/stephenschoettler/hermes-lcm/pull/595),
[#637](https://github.com/stephenschoettler/hermes-lcm/issues/637),
[#647](https://github.com/stephenschoettler/hermes-lcm/pull/647) and
[#629](https://github.com/stephenschoettler/hermes-lcm/pull/629).
The maintained fork already has the missing coercion method, valid PM metadata,
SQLite creation/permission lock guards and linear replay scan. Those remain in
place; duplicate patches are not blindly applied.

## New reproduction and narrow fix

The compatibility method and provider-context parser catch TypeError/ValueError,
but int(float('inf')) raises OverflowError. Positive and negative infinity can
therefore abort configuration adoption or context-window resolution. Both parsers
now catch this conversion error as another invalid input. A malformed threshold
cap yields None (the existing invalid-cap contract); an invalid provider window
returns False before replacing any current model/window/threshold state.

Ten focused tests: four positive/negative-infinity failures before the change,
plus six passing NaN/finite-contract guards. No database, provider call, live
configuration write, threshold retuning or migration is required. Ordinary
numeric strings, integer truncation and nonpositive no-cap semantics are unchanged.
This extends invalid-input handling beyond the mirrored host implementation;
it does not claim to implement every live host compression setting or override
LCM's assembly-cap policy. In particular, presence of the compatibility setter
surface alone is not proof that every native controller knob has LCM semantics.

## Verification boundary

Review latest Hermes snapshot `af90026aa09949579bd423d24def3d38f743cde0` for interface
changes; keep deployed core `0764e9165721` in this plugin-only round. Local release
gates use the existing isolated developer interpreter; actual managed PM runtime
checks happen separately after publication. Keep full/low-FD tests, POSIX-lock
guards, packaging checks, compile/shell, benchmark and stress validation enabled.
The idle journal had no lock warning or traceback; this is a synthetic regression,
not evidence that the user's configured 1M window was invalid.

## Local verification

Full release-validation runner passes, including dependency contract, compilation,
shell, focused tests, benchmark/smoke stress and release stress. Ordinary and
low-FD suites each report **3543 passed, 6 skipped, 12 expected failures**.
All ten new focused cases pass; four failed on the original implementation.
Ruff and whitespace checks pass. No concurrent source/document edits during the
release gate; exact-commit hosted CI is still required before deployment.
