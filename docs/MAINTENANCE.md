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

## Deploy a pull-request branch for review

Keep the live checkout, the updater comparison ref, and the documented branch
identity aligned. A branch deployment is review state, not a permanent fork of
history.

1. Start the review branch from `origin/main`, commit the scoped change, push it,
   and open a pull request against the maintained fork's `main`.
2. In the live checkout, fetch that exact branch and switch to a local tracking
   branch of the same name. Do not rebase or force-push it after deployment.
3. Temporarily configure the local updater to compare that checkout with
   `origin/<review-branch>`. Comparing the live PR checkout with `origin/main`
   creates false ahead/behind results and can replay the wrong commits.
4. Run the required validation below, Plugin Doctor, and SQLite health checks,
   then restart Hermes and confirm the reported loaded branch/commit.
5. After merge, move the checkout and updater together back to
   `main`/`origin/main`. Preserve a backup ref for rollback.

`hermes plugins install --ref` is suitable for a one-commit immutable pin only:
its `--ref` value must be a full 40-character commit SHA. Use a Git tracking
branch when the requirement is to follow an open pull-request branch.

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
