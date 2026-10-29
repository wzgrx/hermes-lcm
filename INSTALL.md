# Managed installation — maintained Hermes-LCM fork

The maintained deployment is `wzgrx/hermes-lcm` main. Keep development checkouts
separate from `$HERMES_HOME/plugins/hermes-lcm`. Use the durable `hermes` launcher
and Hermes PM for runtime dependencies; do not pip-install into a serving runtime.

```bash
hermes plugins install wzgrx/hermes-lcm --enable --force
hermes pm install
hermes plugins doctor hermes-lcm --ci
```

Installation/reinstallation is interactive. Review source, scan findings, declared
capabilities and Python requirements before approving. `--enable` is not dependency
consent. Preserve existing `context.engine: lcm`, provider settings, thresholds,
custom grants and profile-specific configuration.

## Reviewed updates on current official Hermes

1. Review upstream changes and the maintained fork diff. Run tests and require
   successful CI for the exact selected main SHA; record the old and reviewed SHAs.
2. Confirm Gateway idle immediately before stopping it gracefully. Stop any other
   writers to the same LCM database. Back up the database with SQLite's backup API;
   record plugin install metadata, configuration and PM generation facts privately.
3. Confirm the remote main still equals the reviewed SHA, then update through PM:

```bash
hermes gateway stop
hermes plugins update --help
hermes plugins update hermes-lcm
hermes pm install
hermes plugins doctor hermes-lcm --ci
```

Read the installed CLI help: recent official Hermes (verified on main ef75bc70886)
supports `plugins update --force hermes-lcm` for a reviewed CAUTION scan verdict;
a DANGEROUS verdict still blocks. Without that option, a terminal asks first.
There is no `--expected-revision` flag. Older hosts without update confirmation
can use the official interactive same-source install procedure above. New
requirements and capabilities still need their own consent. Inspect scan findings
before approving; never disable scanning globally or restore a core overlay.
Recheck provenance, custom settings and Git worktree registrations after either path.

4. Verify the installed SHA equals the reviewed SHA **before starting Gateway**.
   Remote checks are not an atomic pin: if main moved during publication, keep
   Gateway stopped and review/restore the selected snapshot instead of starting
   unexpected code. Verify `pinned: false` and source in installer metadata.
5. Run read-only SQLite `quick_check`, plugin diagnostics and, when Card is enabled,
   its current `verify`/`status` commands. Start Gateway once, then verify loaded
   plugin version, connection and fresh logs. Service health is not a real chat test.

`./scripts/update.sh` delegates a managed checkout to this PM workflow. A separate
non-serving development checkout may use its legacy Git pull/link workflow.
Adopt a self-cloned install through `hermes plugins adopt hermes-lcm` only when
install provenance is missing. Preserve Git worktree registrations and user files.

## Card 1.x coexistence

Read the installed Card's INSTALL.md before changing Hermes core. Current Card
owns reversible source injections and plugin compatibility code. Do not replay
retired core overlays or commit Card-generated hooks into official Hermes main.
An LCM-only update does not require upgrading Card or rewriting its hooks. For a
core upgrade follow Card's uninstall/update/PM/verify/install sequence separately.

## Pins, packaging and rollback

A `--ref` pin can survive same-source reinstallation. To unpin, preserve checkout,
config/grants and metadata, use managed removal and interactive install from main,
restore custom settings through Hermes APIs, and verify provenance. Never edit
installer metadata to pretend a pin has disappeared.

Root `pyproject.toml` is intentionally a virtual PM dependency project. Its
placeholder version is not the plugin version, which is declared in `plugin.yaml`.
Do not remove that metadata or pip-install the tool-only directory. Fastembed
has an Android-only environment exclusion so Hermes's universal PM resolution does
not request unavailable ONNX wheels there; Linux, macOS and Windows retain it.

Version 1.0.0-rc.3 adds a nullable reasoning column without rewriting historical
messages. Preserve backups and prior PM generations. For rollback, keep all
writers stopped, restore the recorded code/config/dependency generation through
managed tooling and validate before restart. If restoring data, use the consistent
pre-upgrade database snapshot and quarantine the failed database with its own
WAL/SHM together; never combine different snapshot generations. Do not replace a
newer database merely to roll back code without considering intervening messages.
