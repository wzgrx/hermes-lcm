"""Read-only diagnosis of orphaned SQLite handles after WAL unlink races."""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest

from hermes_lcm.diagnostics import inspect_orphaned_sqlite_handles


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires /proc/self/fd")
def test_detects_only_exact_database_artifacts(tmp_path: Path):
    db = tmp_path / "lcm.db"
    db.write_bytes(b"")
    target = Path(str(db) + "-wal")
    other = tmp_path / "other.db-wal"
    descriptors = []
    try:
        for path in (target, other):
            fd = os.open(path, os.O_CREAT | os.O_RDWR | os.O_EXCL, 0o600)
            descriptors.append(fd)
            path.unlink()
        result = inspect_orphaned_sqlite_handles(db)
        assert result == {
            "status": "fail",
            "scope": "current_process",
            "orphaned": [{"fd": descriptors[0], "artifact": "wal"}],
        }
    finally:
        for fd in descriptors:
            os.close(fd)
    assert inspect_orphaned_sqlite_handles(db)["status"] == "pass"


def test_reports_unavailable_without_claiming_a_clean_scan(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    assert inspect_orphaned_sqlite_handles(tmp_path / "lcm.db") == {
        "status": "unavailable",
        "scope": "current_process",
        "orphaned": [],
    }
