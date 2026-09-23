# Maintained fork workflow

This fork follows stephenschoettler/hermes-lcm while carrying a small set of reviewed fixes required by the local Hermes deployment.

## Remotes

- origin: wzgrx/hermes-lcm, the maintained fork
- upstream: stephenschoettler/hermes-lcm, the source project

Never force-push the maintained main branch. Bring upstream changes into a
short-lived local sync branch, run the complete validation matrix, then
fast-forward and push the maintained `main` directly, as requested by the
maintainer. Do not open a pull request for routine fork maintenance.

## Update procedure

1. Fetch origin and upstream with prune enabled.
2. Create sync/upstream-YYYYMMDD from origin/main.
3. Merge upstream/main into the sync branch. Preserve upstream authorship and resolve conflicts against current behavior rather than blindly preferring either side.
4. Retire a carried patch when upstream contains an equivalent tested fix.
5. Run the validation commands below.
6. Fast-forward local `main` to the validated sync branch, then push `main`
   directly to origin (without rewriting history).
7. Confirm GitHub Actions and the local validation gate pass. If CI fails,
   fix forward on `main` and re-run the gate.
8. Update the installed checkout and restart Hermes through its drain-aware
   gateway command after checking active work.

## Deploy the maintained main branch

Keep the live checkout and updater comparison ref aligned to `main` and
`origin/main`. Validate the exact pushed commit, run Plugin Doctor and SQLite
health checks, then restart Hermes after its active work is drained. Record the
loaded commit and keep the previous commit as a rollback reference.

`hermes plugins install --ref` is suitable for a one-commit immutable pin only:
its `--ref` value must be a full 40-character commit SHA. Use the Git-tracking
`main` branch for ongoing updates.

## Required validation

    python scripts/validate_dependency_contract.py --report-environment
    pytest tests/test_lcm_core.py tests/test_lcm_engine.py tests/test_packaging_install.py -q
    pytest -q
    bash -lc 'ulimit -n 1024 && pytest -q'
    python -m compileall -q .
    python -m py_compile scripts/import_lossless_claw.py
    bash -n scripts/install.sh scripts/update.sh
    git diff --check

For the installed Hermes integration, also run:

    hermes plugins doctor hermes-lcm
    sqlite3 ~/.hermes/lcm.db 'PRAGMA quick_check; PRAGMA journal_mode;'

On the current Hermes host, a healthy Plugin Doctor reports 15 tools and 5
hooks, with registration and discovery passing. Treat `--ci` warnings as a
manifest/runtime drift signal; the count alone is not a compatibility test.

## Carried patch policy

Every fork-only change must have:

- a focused regression test;
- an issue, upstream pull request, or reproducible local failure explaining why it exists;
- a clear retirement condition;
- an entry in the commit and upstream audit that introduced or retained it.

Current carried changes cover threshold-aware preflight maintenance, SQLite POSIX-lock preservation while enforcing private file modes, public lifecycle-hook registration for current Hermes hosts, the reviewed mutated-tail replay reconciliation from upstream PR #613, and the native-Anthropic tool-schema compatibility fix from PR #618.

## Existing duplicate rows

The replay repair prevents future whole-transcript re-ingest; it is not a destructive migration for rows already present. Audit the live database read-only and compare counts across new turns before concluding that replay growth continues.

Do not deduplicate `messages` by content alone. Store IDs can be referenced by rollups, assertions, trajectories, FTS tables, and lineage. A cleanup change must create a SQLite backup, build a deterministic old-ID to canonical-ID map, update every reference in one transaction, rebuild FTS, and pass integrity plus replay tests. An identity unique index is not sufficient for legacy rows with NULL `observed_at`, because SQLite permits multiple NULL values in a unique index.
