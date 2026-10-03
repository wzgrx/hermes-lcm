"""Test collection must not fall back to an operator's real SQLite store."""
import os
from pathlib import Path
import subprocess
import sys


def test_collection_sandboxes_home_even_when_profile_override_is_removed(tmp_path):
    operator = tmp_path / "operator"
    operator.mkdir()
    env = {**os.environ, "HOME": str(operator), "USERPROFILE": str(operator), "HERMES_HOME": str(operator / ".hermes")}
    conftest = Path(__file__).with_name("conftest.py")
    code = """
import os, runpy, sys
from pathlib import Path
from types import SimpleNamespace
operator = Path(sys.argv[1])
runpy.run_path(sys.argv[2])
assert Path.home() != operator, "test collection retained the operator HOME"
os.environ.pop("HERMES_HOME", None)
from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine
path = LCMEngine._resolve_db_path(SimpleNamespace(_config=LCMConfig()))
assert path == Path.home() / ".hermes" / "lcm.db"
assert not path.is_relative_to(operator)
assert not (operator / ".hermes").exists()
"""
    result = subprocess.run([sys.executable, "-c", code, str(operator), str(conftest)], env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
