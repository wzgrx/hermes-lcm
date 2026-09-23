"""Read-only diagnosis of orphaned SQLite handles after WAL unlink races."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

import hermes_lcm.diagnostics as diagnostics_module
from hermes_lcm.diagnostics import (
    _may_use_lcm_when_fds_are_unreadable,
    inspect_orphaned_sqlite_handles,
)


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
        assert result["status"] == "fail"
        assert result["scope"] == "same_uid_accessible_processes"
        assert result["orphaned"] == [
            {"pid": os.getpid(), "fd": descriptors[0], "artifact": "wal"}
        ]
    finally:
        for fd in descriptors:
            os.close(fd)
    assert inspect_orphaned_sqlite_handles(db)["status"] == "pass"


def test_reports_unavailable_without_claiming_a_clean_scan(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(sys, "platform", "darwin")
    assert inspect_orphaned_sqlite_handles(tmp_path / "lcm.db") == {
        "status": "unavailable",
        "scope": "same_uid_accessible_processes",
        "orphaned": [],
    }


def test_unreadable_process_classification_keeps_relevant_and_unknown_processes(tmp_path: Path):
    proc = tmp_path / "123"
    proc.mkdir()
    command = proc / "cmdline"
    command.write_bytes(b"/usr/lib/systemd/systemd\x00--user\x00")
    assert _may_use_lcm_when_fds_are_unreadable(str(proc)) is False
    command.write_bytes(b"/home/user/.hermes/venv/bin/python\x00-m\x00hermes_cli.main\x00")
    assert _may_use_lcm_when_fds_are_unreadable(str(proc)) is True
    command.unlink()
    assert _may_use_lcm_when_fds_are_unreadable(str(proc)) is True


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires /proc")
def test_empty_process_scan_is_unavailable_not_clean(tmp_path: Path, monkeypatch):
    original_scandir = os.scandir
    monkeypatch.setattr(diagnostics_module.os, "scandir", lambda path: original_scandir(tmp_path))
    result = inspect_orphaned_sqlite_handles(tmp_path / "lcm.db")
    assert result["status"] == "unavailable"
    assert result["scanned_processes"] == 0


@pytest.mark.skipif(not sys.platform.startswith("linux"), reason="requires /proc")
def test_detects_orphaned_wal_held_by_another_same_user_process(tmp_path: Path):
    db = tmp_path / "lcm.db"
    db.write_bytes(b"")
    wal = Path(str(db) + "-wal")
    child = subprocess.Popen(
        [sys.executable, "-u", "-c", "import os,sys,time; p=sys.argv[1]; "
         "fd=os.open(p,os.O_CREAT|os.O_RDWR|os.O_EXCL,0o600); os.unlink(p); "
         "print(fd,flush=True); time.sleep(30)", str(wal)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout is not None
        child_fd = int(child.stdout.readline().strip())
        result = inspect_orphaned_sqlite_handles(db)
        assert result["status"] == "fail"
        assert {"pid": child.pid, "fd": child_fd, "artifact": "wal"} in result["orphaned"]
    finally:
        child.terminate()
        child.wait(timeout=5)
