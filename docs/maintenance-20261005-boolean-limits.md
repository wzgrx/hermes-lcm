# Boolean token-limit hardening — 2026-10-05

Python's `bool` is an `int` subclass. The previous `int(value)` normalizer turned
`threshold_tokens: true` into a one-token trigger; direct false/true model-window
updates could clear/shrink a valid context. The shared token-cap normalizer now
treats both booleans as no valid cap. `_set_context_length` rejects them without
changing the last valid window. Actual integer 1, numeric strings, and the existing
explicit integer-zero behavior are preserved. No live configuration is rewritten.

This is an intentional stricter boundary than official Hermes `af90026aa`'s
`ContextCompressor._coerce_max_tokens`, which also accepts booleans. Reviewed
[upstream #615](https://github.com/stephenschoettler/hermes-lcm/pull/615) and
[#595](https://github.com/stephenschoettler/hermes-lcm/pull/595) address host-method
compatibility; no complete PR or context-pin restoration patch is adopted here.
Eight isolated tests cover invalid booleans, preserved model/window/threshold and
valid numeric caps. Three failed against the original code. Database, providers,
summaries, dependencies and the prior live-ratio patch are unchanged.

Context-pin removal/re-inference and full routed-profile configuration reload
remain separate work. Tests and service health are not a million-token real turn.

## Validation

Full release gates pass, including **3589 passed, 6 skipped, 12 expected failures**
in both ordinary and low-FD suites. Dependency, compile, shell, focused, benchmark
and stress gates pass. Ruff passes separately. Exact-commit GitHub CI and managed
runtime smoke remain separate publication/deployment gates.
