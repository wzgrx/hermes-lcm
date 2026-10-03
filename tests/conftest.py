"""Test configuration for hermes-lcm plugin tests.

Patches the plugin modules so they can be imported both as a package
(relative imports during plugin loading) and directly during testing.
"""
import sys
import os
import importlib
import tempfile

import pytest
from pathlib import Path

# Test imports must never exec into the host PM runtime.
os.environ["HERMES_DISABLE_LAZY_INSTALLS"] = "1"

# Some tests deliberately clear HERMES_HOME. Isolate the process HOME before
# importing plugin modules so their default SQLite path never reaches user data.
# The TemporaryDirectory is retained until interpreter shutdown, including
# child-process tests; this does not change the invoking shell environment.
_TEST_HOME = tempfile.TemporaryDirectory(prefix="hermes-lcm-tests-")
os.environ["HOME"] = _TEST_HOME.name
if os.name == "nt":
    os.environ["USERPROFILE"] = _TEST_HOME.name
os.environ["HERMES_HOME"] = str(Path(_TEST_HOME.name) / ".hermes")

@pytest.fixture(autouse=True)
def isolate_home_per_test(tmp_path_factory, monkeypatch):
    """Keep auxiliary profile files from leaking between unrelated tests."""
    # Do not populate a test's own tmp_path: benchmark output contracts can
    # require that directory to be empty before their first write.
    home = tmp_path_factory.mktemp("lcm-isolated-user")
    monkeypatch.setenv("HOME", str(home))
    if os.name == "nt":
        monkeypatch.setenv("USERPROFILE", str(home))
    monkeypatch.setenv("HERMES_HOME", str(home / ".hermes"))


# Make the repo root importable (for agent.context_engine etc.)
repo_root = str(Path(__file__).resolve().parent.parent.parent.parent)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

# Register the plugin directory as a proper package
plugin_dir = Path(__file__).resolve().parent.parent
pkg_name = "hermes_lcm"

if pkg_name not in sys.modules:
    spec = importlib.util.spec_from_file_location(
        pkg_name,
        str(plugin_dir / "__init__.py"),
        submodule_search_locations=[str(plugin_dir)],
    )
    mod = importlib.util.module_from_spec(spec)
    mod.__path__ = [str(plugin_dir)]
    mod.__package__ = pkg_name
    sys.modules[pkg_name] = mod
    # Don't exec the module (it tries to register with ctx)
    # Just make submodules importable

    # Register each submodule
    for py_file in plugin_dir.glob("*.py"):
        if py_file.name == "__init__.py":
            continue
        sub_name = f"{pkg_name}.{py_file.stem}"
        if sub_name not in sys.modules:
            sub_spec = importlib.util.spec_from_file_location(
                sub_name, str(py_file),
            )
            sub_mod = importlib.util.module_from_spec(sub_spec)
            sub_mod.__package__ = pkg_name
            sys.modules[sub_name] = sub_mod
            setattr(mod, py_file.stem, sub_mod)
            try:
                sub_spec.loader.exec_module(sub_mod)
            except Exception:
                pass  # some modules may fail (e.g. engine needs agent)
