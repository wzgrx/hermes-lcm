"""Backup and rotate maintenance operations for the LCM store.

These are the data-layer maintenance primitives behind ``/lcm backup`` and
``/lcm rotate``: they flush the engine's SQLite connections and snapshot the
store to a timestamped or rolling backup file. They are pure functions that
take the engine so the command layer (``command.py``) keeps only the text
formatting, and the store/dag/lifecycle connection handling lives in one place.
"""

from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import sqlite3
import stat
from uuid import uuid4
from typing import Any

from .sqlite_util import (
    _prepare_private_sqlite_file,
    _restrict_existing_sqlite_artifacts,
)


_FCHMOD = getattr(os, "fchmod", None)


def _prepare_private_backup_directory(path: Path) -> None:
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    expected = os.lstat(path)
    if stat.S_ISLNK(expected.st_mode) or not stat.S_ISDIR(expected.st_mode):
        raise OSError(f"backup directory is not a real directory: {path}")

    if os.name != "posix":  # pragma: no cover - Windows compatibility fallback
        path.chmod(0o700)
        return

    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        opened = os.fstat(fd)
        if (
            not stat.S_ISDIR(opened.st_mode)
            or (opened.st_dev, opened.st_ino) != (expected.st_dev, expected.st_ino)
        ):
            raise OSError(f"backup directory changed during validation: {path}")
        if callable(_FCHMOD):
            _FCHMOD(fd, 0o700)
        elif os.chmod in getattr(os, "supports_fd", ()):
            os.chmod(fd, 0o700)
        else:
            raise OSError("descriptor-based directory chmod is unavailable")
    finally:
        os.close(fd)

def flush_engine_connections(engine) -> None:
    """Commit pending writes on every SQLite connection the engine owns.

    Shared by ``backup_database`` (timestamped backup) and
    ``rotate_backup_database`` (rolling backup) so the connection-flush
    contract stays in one place.
    """
    engine._store.commit()
    engine._dag._conn.commit()
    lifecycle_conn = getattr(getattr(engine, "_lifecycle", None), "_conn", None)
    if lifecycle_conn is not None:
        lifecycle_conn.commit()
    assertion_store = getattr(engine, "_assertions", None)
    if assertion_store is not None:
        # AssertionStore owns a multi-statement publication transaction. Its
        # lock-taking API must serialize this flush with publish_source() so a
        # backup cannot commit a half-written receipt behind the publisher.
        assertion_store.commit()
    query_views = getattr(engine, "_query_views", None)
    if query_views is not None:
        query_views.commit()


def _verify_backup_integrity(path: Path) -> None:
    """Checkpoint, close, then validate the main file without WAL sidecars."""
    settle = sqlite3.connect(str(path))
    try:
        checkpoint = settle.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint and int(checkpoint[0]) != 0:
            raise sqlite3.DatabaseError("backup WAL checkpoint was busy")
    finally:
        settle.close()

    # immutable=1 deliberately ignores sidecars: the published main file must
    # be independently restorable after its staging name changes.
    check = sqlite3.connect(path.resolve().as_uri() + "?mode=ro&immutable=1", uri=True)
    try:
        row = check.execute("PRAGMA integrity_check").fetchone()
    finally:
        # sqlite3.Connection context managers commit but do not close. A
        # leaked WAL reader must never survive the staging-file rename.
        check.close()
    result = str(row[0]) if row else "missing-result"
    if result != "ok":
        raise sqlite3.DatabaseError(f"backup integrity_check failed: {result}")


def _cleanup_staged_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        try:
            path.with_name(path.name + suffix).unlink(missing_ok=True)
        except OSError:
            pass


def _cleanup_staged_backup(path: Path) -> None:
    _cleanup_staged_sidecars(path)
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


def _reject_existing_backup_sidecars(path: Path) -> None:
    for suffix in ("-wal", "-shm", "-journal"):
        sidecar = path.with_name(path.name + suffix)
        if os.path.lexists(sidecar):
            raise sqlite3.DatabaseError(f"rolling backup sidecar exists: {sidecar}")


def backup_database(engine) -> dict[str, Any]:
    db_path = Path(engine._store.db_path)
    if not db_path.exists():
        return {
            "ok": False,
            "db_path": db_path,
            "error": "database file does not exist",
        }

    backup_dir = engine.backup_dir()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    backup_path = backup_dir / f"{db_path.stem}-{timestamp}.sqlite3"
    tmp_path = backup_dir / f".{backup_path.name}.{os.getpid()}.{uuid4().hex}.tmp"

    try:
        _prepare_private_backup_directory(backup_dir)
        flush_engine_connections(engine)
        _prepare_private_sqlite_file(tmp_path)
        dest = sqlite3.connect(str(tmp_path))
        try:
            engine._store.backup(dest)
        finally:
            dest.close()
        _restrict_existing_sqlite_artifacts(tmp_path)
        _verify_backup_integrity(tmp_path)
        _cleanup_staged_sidecars(tmp_path)
        # The final timestamped file appears only after it is complete and
        # verified. An existing restore point is never used as staging.
        if backup_path.exists():
            raise FileExistsError(f"backup destination already exists: {backup_path}")
        tmp_path.replace(backup_path)
        _restrict_existing_sqlite_artifacts(backup_path)
    except (OSError, sqlite3.Error) as exc:
        _cleanup_staged_backup(tmp_path)
        return {
            "ok": False,
            "db_path": db_path,
            "backup_path": backup_path,
            "error": str(exc),
        }

    return {
        "ok": True,
        "db_path": db_path,
        "backup_path": backup_path,
        "backup_size": backup_path.stat().st_size,
        "integrity_check": "ok",
    }


def rotate_backup_database(engine) -> dict[str, Any]:
    """Write a rolling rotate-latest SQLite snapshot of the LCM store.

    Atomic via tmp-then-rename so the slot is never half-written. Unlike
    ``backup_database`` which produces timestamped files, this overwrites a
    single rolling slot so disk usage stays bounded across repeated rotates.
    """
    db_path = Path(engine._store.db_path)
    if not db_path.exists():
        return {
            "ok": False,
            "db_path": db_path,
            "error": "database file does not exist",
        }

    backup_path = engine.rotate_backup_path()
    backup_dir = backup_path.parent
    tmp_path = backup_path.with_name(f".{backup_path.name}.{os.getpid()}.{uuid4().hex}.tmp")

    try:
        _prepare_private_backup_directory(backup_dir)
        _restrict_existing_sqlite_artifacts(backup_path)
        # Replacing only the main file while old-name WAL/SHM files remain
        # could pair the new snapshot with the previous SQLite generation.
        # Preserve the old slot and let the operator inspect those sidecars.
        _reject_existing_backup_sidecars(backup_path)
        flush_engine_connections(engine)

        # A unique staging path keeps concurrent rotate attempts isolated;
        # never delete a fixed-name temp that another process may be writing.
        _prepare_private_sqlite_file(tmp_path)
        dest = sqlite3.connect(str(tmp_path))
        try:
            engine._store.backup(dest)
        finally:
            dest.close()
        _restrict_existing_sqlite_artifacts(tmp_path)
        _verify_backup_integrity(tmp_path)
        _cleanup_staged_sidecars(tmp_path)
        _reject_existing_backup_sidecars(backup_path)
        # Atomic replace so the rolling slot is never half-written.
        tmp_path.replace(backup_path)
        _restrict_existing_sqlite_artifacts(backup_path)
    except (OSError, sqlite3.Error) as exc:
        # Best-effort cleanup of the staging file and its SQLite sidecars.
        _cleanup_staged_backup(tmp_path)
        return {
            "ok": False,
            "db_path": db_path,
            "backup_path": backup_path,
            "error": str(exc),
        }

    backup_size = backup_path.stat().st_size if backup_path.exists() else 0
    return {
        "ok": True,
        "db_path": db_path,
        "backup_path": backup_path,
        "backup_size": backup_size,
        "integrity_check": "ok",
    }
