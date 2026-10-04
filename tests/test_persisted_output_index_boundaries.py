"""Indexing must tolerate damaged entries and retain fresh, scoped reads."""
import json

import pytest

from hermes_lcm import externalize
from hermes_lcm.config import LCMConfig


def payload(session="s", call="c", content="current"):
    return {"kind": "tool_result", "role": "tool", "session_id": session,
            "tool_call_id": call, "content": content}


@pytest.mark.parametrize("bad", [b"[]", b"null", b'"text"', b"\xff\xfe", b"{partial"])
def test_bad_json_entry_does_not_hide_valid_result(tmp_path, bad):
    (tmp_path / "0-invalid.json").write_bytes(bad)
    (tmp_path / "1-valid.json").write_text(json.dumps(payload()), encoding="utf-8")
    config = LCMConfig(large_output_externalization_path=str(tmp_path))
    for _ in range(2):
        assert externalize.find_externalized_tool_result_content_for_call(
            tool_call_id="c", session_id="s", config=config) == "current"


def test_same_call_in_two_sessions_remains_scoped_and_reads_fresh_content(tmp_path):
    for session in ("s", "other"):
        (tmp_path / f"{session}.json").write_text(json.dumps(payload(session=session, content=session)))
    config = LCMConfig(large_output_externalization_path=str(tmp_path))
    def lookup(session):
        return externalize.find_externalized_tool_result_content_for_call(
            tool_call_id="c", session_id=session, config=config)
    assert lookup("s") == "s" and lookup("other") == "other"
    (tmp_path / "s.json").write_text(json.dumps(payload(content="updated")))
    assert lookup("s") == "updated" and lookup("other") == "other"
    (tmp_path / "s.json").unlink()
    assert lookup("s") is None and lookup("other") == "other"


def test_ambiguous_candidate_still_requires_matching_marker(tmp_path):
    (tmp_path / "x.json").write_text(json.dumps(payload(content="not proven")))
    config = LCMConfig(large_output_externalization_path=str(tmp_path))
    assert externalize.find_externalized_tool_result_content_for_call(
        tool_call_id="c", session_id="s", expected_chars=10,
        persisted_output_preview_sha256="0" * 64, config=config) is None
