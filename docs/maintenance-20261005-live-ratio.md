# Live inherited ratios — 2026-10-05 (Asia/Shanghai)

## Reproduced defect

The host writes threshold_percent and its configuration/model-override fields,
then invalidates `_threshold_tokens`. LCM previously ignored this invalidation:
the visible percent could change while its effective trigger retained the old
ratio. The existing absolute-cap fix alone did not fix this inconsistency.

The invalidation is now an adoption boundary, not an emulated second cache.
Ratios must be finite and in (0, 1]; malformed input keeps the last valid live
policy. The effective percentage and token trigger are recomputed together.
LCM config is not rewritten. Only inherited sources (`default` and
`config_yaml:compression.threshold`) adopt host ratios. Explicit LCM YAML/env
or manual config remains authoritative. Model overrides are sanitized and
copied, then resolved for the active model/provider by the already-loaded
official resolver. No host bootstrap/import or network call is introduced.
An explicit matching model rule takes precedence over the Codex default ratio
raise; the arithmetic fresh-tail floor guard still applies.

Clones copy live policy independently. A profile rebind clears adopted ratios,
copied overrides and pending host-input attributes, restoring the engine's own
baseline config. It does not reload every field from another profile's YAML.
The next host sync supplies that runtime's ratio. Rejection cooldowns and data
remain untouched. Stricter assembly/absolute caps and fresh-tail guards remain.

## Research and acceptance boundaries

Reviewed [upstream #595](https://github.com/stephenschoettler/hermes-lcm/pull/595)
and [the3asic #7](https://github.com/the3asic/hermes-lcm/pull/7), including the
initial-context-pin and cross-profile-pin review findings. This is an independent
ratio fix, not adoption of the complete context-pin patch. The current core does
not give the plugin an authoritative unpinned window at every construction path.
Pin removal, initial-pin re-inference, full profile reload and initial host cap
import still need their own route-aware integration work. Do not report them as
fixed by this change or manufacture the unpinned window from the old pin.

Tests include exact selected `_apply_live_compression_config` and model-resolver
functions from official Hermes `af90026aa09949579bd423d24def3d38f743cde0` (MIT).
The fixture uses isolated host imports/defaults, real LCM engines and temporary
SQLite. It tests inherited ratios, missing-key defaults, explicit null caps,
provider-scoped overrides and malformed values. Scope/pin metadata helpers are
stubbed: this is not a full real-agent bootstrap or a production message test.

Card remains 0.20.16. Official tool-progress callbacks still omit unique call IDs
even though other native tool callbacks carry IDs; merging those different event
feeds needs correlation/ordering tests. No speculative pairing or V1 UI changes
are shipped here. Hermes core, credentials, models and live config stay unchanged.

## Verification

All selected release gates pass. Both ordinary and low-FD full suites report
**3581 passed, 6 skipped, 12 expected failures**. The new focused file has 25
passing cases; 18 of the first 20 cases failed against the old implementation.
The final gate includes compile, dependency-contract, shell, focused, benchmark
and stress checks. Ruff passes independently. Exact-commit GitHub CI and managed
runtime deployment are separate gates; this is not a live Feishu conversation
or a context-pin restoration acceptance result.
