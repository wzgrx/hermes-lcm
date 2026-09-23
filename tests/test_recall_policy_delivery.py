"""Policy lives once in the product-owned request prefix, not replayed users."""

import pytest
from pathlib import Path
import sys

from hermes_lcm.config import LCMConfig
from hermes_lcm.recall_policy_delivery import rewrite_recall_policy_request


POLICY = "## Hermes-LCM Recall Policy\nUse exact refs when available."


def _wire_count(request):
    if isinstance(request, str):
        return request.count(POLICY)
    if isinstance(request, dict):
        return sum(_wire_count(value) for value in request.values())
    if isinstance(request, list):
        return sum(_wire_count(value) for value in request)
    return 0


def test_policy_defaults_off_and_accepts_explicit_environment_opt_in(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("LCM_RECALL_POLICY_ENABLED", raising=False)
    assert LCMConfig.from_env().recall_policy_enabled is False
    monkeypatch.setenv("LCM_RECALL_POLICY_ENABLED", "true")
    assert LCMConfig.from_env().recall_policy_enabled is True


def test_legacy_user_policy_replay_is_removed_without_mutating_history():
    request = {
        "messages": [
            {"role": "system", "content": "stable instructions"},
            {"role": "user", "content": "first request\n\n" + POLICY},
            {"role": "assistant", "content": "first answer"},
            {"role": "user", "content": "second request\n\n" + POLICY},
            {"role": "user", "content": "current request"},
        ],
        "model": "deepseek-flash",
    }
    clean = rewrite_recall_policy_request(request, POLICY, enabled=False)

    assert _wire_count(clean) == 0
    assert [message["content"] for message in clean["messages"] if message["role"] == "user"] == [
        "first request", "second request", "current request",
    ]
    assert _wire_count(request) == 2
    assert clean["messages"][0] is request["messages"][0]
    assert clean["messages"][2] is request["messages"][2]


def test_opt_in_places_one_policy_in_system_prefix_and_is_idempotent():
    request = {
        "messages": [
            {"role": "system", "content": "stable instructions"},
            {"role": "user", "content": "first request\n\n" + POLICY},
            {"role": "user", "content": "second request\n\n" + POLICY + "\n\n[evidence]"},
        ]
    }
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=True)
    repeated = rewrite_recall_policy_request(rewritten, POLICY, enabled=True)

    assert _wire_count(rewritten) == 1
    assert rewritten["messages"][0]["content"] == "stable instructions\n\n" + POLICY
    assert rewritten["messages"][1]["content"] == "first request"
    assert rewritten["messages"][2]["content"] == "second request\n\n[evidence]"
    assert repeated is rewritten
    assert _wire_count(request) == 2


def test_anthropic_system_blocks_and_multimodal_user_parts():
    request = {
        "system": [{"type": "text", "text": "static"}],
        "messages": [{
            "role": "user",
            "content": [
                {"type": "text", "text": "photo request\n\n" + POLICY},
                {"type": "image", "source": {"type": "base64", "data": "AAAA"}},
            ],
        }],
    }
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=True)

    assert _wire_count(rewritten) == 1
    assert rewritten["system"][-1] == {"type": "text", "text": POLICY}
    assert rewritten["messages"][0]["content"][0]["text"] == "photo request"
    assert rewritten["messages"][0]["content"][1] is request["messages"][0]["content"][1]
    assert _wire_count(request) == 1


def test_old_multimodal_context_part_is_removed_but_plain_policy_quote_is_preserved():
    request = {
        "messages": [
            {"role": "user", "content": "Please explain this text: " + POLICY},
            {"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "data": "AAAA"}},
                {"type": "text", "text": POLICY},
            ]},
        ],
    }
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=False)

    assert rewritten["messages"][0]["content"] == "Please explain this text: " + POLICY
    assert rewritten["messages"][1]["content"] == [
        {"type": "image", "source": {"type": "base64", "data": "AAAA"}},
    ]
    assert _wire_count(rewritten) == 1


def test_responses_instructions_take_precedence_over_input_message():
    request = {
        "instructions": "stable instruction prefix",
        "input": [{"role": "user", "content": "question\n\n" + POLICY}],
    }
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=True)

    assert _wire_count(rewritten) == 1
    assert rewritten["instructions"] == "stable instruction prefix\n\n" + POLICY
    assert rewritten["input"][0]["content"] == "question"


def test_chat_without_system_gets_one_prefix_and_preserves_user_quote():
    request = {
        "messages": [
            {"role": "user", "content": "What does this mean: " + POLICY},
            {"role": "user", "content": "old request\n\n" + POLICY},
        ]
    }
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=True)

    assert rewritten["messages"][0] == {"role": "system", "content": POLICY}
    assert rewritten["messages"][1] is request["messages"][0]
    assert rewritten["messages"][2]["content"] == "old request"
    assert rewrite_recall_policy_request(rewritten, POLICY, enabled=True) is rewritten


def test_unknown_request_shape_stays_unchanged():
    request = {"prompt": "plain completion"}
    assert rewrite_recall_policy_request(request, POLICY, enabled=True) is request


def test_installed_host_two_turn_api_content_replay_has_one_product_policy():
    # The plugin's tools.py shadows the installed host's tools package when
    # pytest prepends this checkout to sys.path. Restore paths after import.
    plugin_root = Path(__file__).resolve().parents[1]
    shadowing_paths = [
        entry for entry in sys.path
        if Path(entry or ".").resolve() == plugin_root
    ]
    for entry in shadowing_paths:
        sys.path.remove(entry)
    shadowed_tools = sys.modules.pop("tools", None)
    try:
        host = pytest.importorskip("agent.turn_context")
    finally:
        if shadowed_tools is not None:
            sys.modules["tools"] = shadowed_tools
        sys.path[:0] = shadowing_paths
    stored = [
        {
            "role": "user",
            "content": text,
            "api_content": host.compose_user_api_content(text, "", POLICY),
        }
        for text in ("first request", "second request")
    ]
    wire = [dict(message) for message in stored]
    for message in wire:
        host.substitute_api_content(message)
    assert _wire_count(wire) == 2

    request = {"messages": [{"role": "system", "content": "stable prefix"}, *wire]}
    rewritten = rewrite_recall_policy_request(request, POLICY, enabled=True)

    assert _wire_count(rewritten) == 1
    assert [message["content"] for message in rewritten["messages"][1:]] == [
        "first request", "second request",
    ]
    assert [message["content"] for message in stored] == ["first request", "second request"]
    assert all(POLICY in message["api_content"] for message in stored)
