# Managed installation — maintained Hermes-LCM fork

The maintained deployment is `wzgrx/hermes-lcm` main. Keep the development
checkout separate from `$HERMES_HOME/plugins/hermes-lcm`. Run through the durable
Hermes launcher, not a legacy `hermes-agent/venv` pip command.

```bash
hermes plugins install wzgrx/hermes-lcm --enable --force
hermes pm install
hermes plugins doctor hermes-lcm --ci
```

Installation/reinstallation is interactive: approve the declared Python
requirements. `--enable` grants no dependency consent. On current Hermes,
noninteractive replacement without consent leaves the old active installation
and dependency generation untouched. Select `context.engine: lcm` in the existing
configuration; preserve any configured threshold and profile-specific settings.

## Updates

Review the incoming commit, upstream issues and CI, then check Gateway work and
use a stopped or idle maintenance window. Adopt self-cloned directories once:

```bash
hermes plugins adopt hermes-lcm  # only when no install provenance exists
hermes plugins update hermes-lcm
hermes pm install
hermes plugins doctor hermes-lcm --ci
```

CAUTION findings can include reviewed permission helpers, docs and test fixtures.
Upstream currently lacks a working confirmation path on Git updates. The
maintained Hermes overlay adds interactive CAUTION consent and the explicit
`plugins update NAME --force` review flag; check `--help` before using it.
It accepts CAUTION only, not dangerous code or new dependency consent. Never
switch global scanning off. Preserve Git worktree registrations and user-owned
files during publication; verify installed Git SHA and provenance afterwards.

`./scripts/update.sh` delegates a managed checkout to this PM update workflow.
Only a separate development checkout uses its legacy Git pull/link workflow.

`--ref` pins survive a same-source reinstall even if the new command omits it.
To unpin, back up checkout and plugin configuration/grants, remove the plugin
through the managed CLI, reinstall interactively from main without `--ref`, and
restore custom entries using Hermes config APIs. Verify `pinned: false` in the
installer-owned `.install-metadata.json`; an explicit full SHA just moves a pin.

## Packaging and verification

Upstream issue #637 is already covered here: root `pyproject.toml` has a valid
virtual `[project]`, version and runtime dependency declaration. Do not pip-install
the tool-only directory or remove that metadata. PM resolves its dependencies into
the enabled plugin union without installing a fictitious standalone package.

Before restart run both plugin/core diagnostics and read-only SQLite health
checks (`mode=ro`, `PRAGMA query_only=ON`, `PRAGMA quick_check`). Keep models,
secrets, conversations and the live `lcm.db` intact. Replay fixes prevent future
duplication; they are not a destructive deduplication migration. Preserve rollback
refs and prior PM generation facts, then verify Gateway/Feishu and loaded code.

### Exact reviewed snapshot (maintained Hermes overlay)

```bash
hermes plugins update hermes-lcm --force --expected-revision REVIEWED_40_CHARACTER_SHA
```

This guard aborts before publication if the remote moves beyond the reviewed
commit. It applies to full custom Git checkouts; catalog and subdirectory
installs use their own provenance workflow. `--force` accepts only CAUTION
findings and does not grant new Python dependency or capability consent.
