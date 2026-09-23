"""One-copy, product-owned recall guidance at the provider request boundary.

Older LCM releases returned the policy from ``pre_llm_call``. Hermes persists
that hook context in each user row's ``api_content`` and replays it on every
later request. This middleware removes those exact historical copies from the
wire and, when opted in, adds one copy to the high-trust request prefix.
Stored user messages and sidecars are deliberately left untouched.
"""

from __future__ import annotations

from typing import Any


def _without_legacy_policy(text: str, policy: str) -> str:
    boundary = "\n\n" + policy
    if boundary not in text:
        return text
    # The old user-context composer separated the policy with two newlines.
    # Match that shape, not an arbitrary user-authored mention of the policy.
    return text.replace(boundary, "")


def _scrub_user_content(content: Any, policy: str) -> Any:
    if isinstance(content, str):
        return _without_legacy_policy(content, policy)
    if not isinstance(content, list):
        return content
    changed = False
    result = []
    for part in content:
        if isinstance(part, dict) and isinstance(part.get("text"), str):
            if part["text"] == policy:
                changed = True
                continue
            if part["text"].startswith(policy + "\n\n"):
                clean = part["text"][len(policy) + 2:]
            else:
                clean = _without_legacy_policy(part["text"], policy)
            if clean != part["text"]:
                part = {**part, "text": clean}
                changed = True
        elif isinstance(part, str):
            clean = _without_legacy_policy(part, policy)
            if clean != part:
                part = clean
                changed = True
        result.append(part)
    return (result or "") if changed else content


def _scrub_user_messages(messages: Any, policy: str) -> Any:
    if not isinstance(messages, list):
        return messages
    changed = False
    result = []
    for message in messages:
        if isinstance(message, dict) and message.get("role") == "user":
            content = message.get("content")
            clean = _scrub_user_content(content, policy)
            if clean is not content:
                message = {**message, "content": clean}
                changed = True
        result.append(message)
    return result if changed else messages


def _append_policy(text: str, policy: str) -> str:
    if policy in text:
        return text
    return text + ("\n\n" if text else "") + policy


def _inject_into_messages(messages: list[Any], policy: str) -> list[Any]:
    for index, message in enumerate(messages):
        if not isinstance(message, dict) or message.get("role") not in {"system", "developer"}:
            continue
        content = message.get("content")
        if isinstance(content, str):
            updated = _append_policy(content, policy)
        elif isinstance(content, list):
            if any(isinstance(part, dict) and policy in str(part.get("text") or "") for part in content):
                return messages
            updated = [*content, {"type": "text", "text": policy}]
        else:
            continue
        if updated == content:
            return messages
        result = list(messages)
        result[index] = {**message, "content": updated}
        return result
    return [{"role": "system", "content": policy}, *messages]


def rewrite_recall_policy_request(
    request: dict[str, Any], policy: str, *, enabled: bool,
) -> dict[str, Any]:
    """Return a copy-on-write provider request with zero or one policy copy.

    Handles OpenAI-compatible chat ``messages``, Responses/Codex ``input`` +
    ``instructions``, and Anthropic ``messages`` + ``system``. Unknown request
    shapes are left alone rather than receiving an unsupported field.
    """
    if not policy:
        return request
    out = request
    for key in ("messages", "input"):
        original = out.get(key)
        clean = _scrub_user_messages(original, policy)
        if clean is not original:
            if out is request:
                out = dict(request)
            out[key] = clean
    if not enabled:
        return out

    instructions = out.get("instructions")
    if isinstance(instructions, str) or ("input" in out and instructions is None):
        updated = _append_policy(instructions or "", policy)
        if updated != instructions:
            if out is request:
                out = dict(request)
            out["instructions"] = updated
        return out

    system = out.get("system")
    if isinstance(system, str):
        updated = _append_policy(system, policy)
        if updated != system:
            if out is request:
                out = dict(request)
            out["system"] = updated
        return out
    if isinstance(system, list):
        if any(isinstance(part, dict) and policy in str(part.get("text") or "") for part in system):
            return out
        if out is request:
            out = dict(request)
        out["system"] = [*system, {"type": "text", "text": policy}]
        return out

    for key in ("messages", "input"):
        messages = out.get(key)
        if isinstance(messages, list):
            updated = _inject_into_messages(messages, policy)
            if updated is not messages:
                if out is request:
                    out = dict(request)
                out[key] = updated
            return out
    return out
