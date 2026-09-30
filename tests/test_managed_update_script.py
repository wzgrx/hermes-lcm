"""A managed update wrapper delegates without changing the live checkout."""
import os
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.skipif(shutil.which("bash") is None, reason="bash unavailable")
@pytest.mark.parametrize("option", [[], ["--force"]])
def test_managed_update_delegates_to_durable_launcher(tmp_path, option):
    home = tmp_path / "Hermes home"
    root = home / "plugins" / "hermes-lcm"
    (root / "scripts").mkdir(parents=True)
    source = Path(__file__).resolve().parents[1] / "scripts" / "update.sh"
    script = root / "scripts" / "update.sh"
    shutil.copy2(source, script)
    marker = root / "user-owned.txt"; marker.write_text("preserve")
    bin_dir = tmp_path / "bin"; bin_dir.mkdir()
    record = tmp_path / "argv.txt"
    launcher = bin_dir / "hermes"
    launcher.write_text('#!/bin/sh\nprintf "%s\\n" "$@" > "$RECORD"\n')
    launcher.chmod(0o755)
    env = {**os.environ, "PATH": str(bin_dir) + os.pathsep + os.environ["PATH"],
           "HERMES_HOME": str(home), "RECORD": str(record)}
    env.pop("HERMES_PROFILE", None)
    subprocess.run([shutil.which("bash"), str(script), *option], env=env, check=True, capture_output=True)
    assert record.read_text().splitlines() == ["plugins", "update", "hermes-lcm", *option]
    assert marker.read_text() == "preserve"
    assert not (home / "skills").exists()
