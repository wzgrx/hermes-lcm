# Maintained fork workflow

This fork follows stephenschoettler/hermes-lcm while carrying a small set of reviewed fixes required by the local Hermes deployment.

## Remotes

- origin: wzgrx/hermes-lcm, the maintained fork
- upstream: stephenschoettler/hermes-lcm, the source project

Never force-push the maintained main branch. Bring upstream changes into a short-lived sync branch, run the complete validation matrix, and merge them through a pull request.

## Update procedure

1. Fetch origin and upstream with prune enabled.
2. Create sync/upstream-YYYYMMDD from origin/main.
3. Merge upstream/main into the sync branch. Preserve upstream authorship and resolve conflicts against current behavior rather than blindly preferring either side.
4. Retire a carried patch when upstream contains an equivalent tested fix.
5. Run the validation commands below.
6. Push the sync branch and open a pull request against origin/main.
7. Merge only after GitHub Actions and the local validation gate pass.
8. Update the installed checkout and restart Hermes through its drain-aware gateway command.

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

The healthy Plugin Doctor result for the current integration is 15 tools and 4 hooks.

## Carried patch policy

Every fork-only change must have:

- a focused regression test;
- an issue, upstream pull request, or reproducible local failure explaining why it exists;
- a clear retirement condition;
- an entry in the pull request that introduced or retained it.

Current carried changes cover deferred below-threshold preflight maintenance, SQLite POSIX-lock preservation while enforcing private file modes, and public lifecycle-hook registration for current Hermes hosts.
