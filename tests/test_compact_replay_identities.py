"""Large tool replay identities retain exact matching with bounded strings."""

import pytest

from hermes_lcm.config import LCMConfig
from hermes_lcm.engine import LCMEngine


@pytest.fixture
def engine(tmp_path):
    instance = LCMEngine(config=LCMConfig(database_path=str(tmp_path / "memory.db")), hermes_home=str(tmp_path / "home"))
    instance._session_id = "example-session"
    try:
        yield instance
    finally:
        instance.shutdown()


def identity(engine, content, role="tool", call="example-call"):
    return engine._message_replay_identity({"role": role, "content": content, "tool_call_id": call})


def test_large_tool_identity_is_bounded_and_content_exact(engine):
    content = "a" * 70_000
    original = identity(engine, content)
    assert len(original[1]) < 200
    assert original == identity(engine, content)
    assert original != identity(engine, content[:-1] + "b")
    assert original != identity(engine, content, call="different-call")
    assert original != identity(engine, original[1])
    assert original != identity(engine, original[1][1:])


def test_large_unicode_content_matches_by_utf8_digest(engine):
    content = "🍀" * 70_000
    result = identity(engine, content)
    assert len(result[1]) < 200
    assert "bytes=280000" in result[1]
    assert result != identity(engine, content[:-1] + "🌱")


def test_short_tool_and_nontool_content_keep_original_identity(engine):
    assert identity(engine, "short")[1] == "sshort"
    assert identity(engine, "a" * 70_000, role="assistant")[1] == "s" + "a" * 70_000


@pytest.mark.parametrize("content", ["x" * 70_000 + "\ud800", "[LCM compact tool replay identity: \udcff]"], ids=["long-surrogate", "marker-surrogate"])
def test_surrogate_tool_output_has_a_stable_distinct_identity(engine, content):
    result = identity(engine, content)
    assert len(result[1]) < 200
    assert result == identity(engine, content)
    assert result != identity(engine, content.replace("\ud800", "\ufffd").replace("\udcff", "\ufffd"))
