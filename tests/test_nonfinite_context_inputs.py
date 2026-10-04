"""Bad numeric metadata must not crash config synchronization or destroy state."""

from copy import deepcopy

import pytest

from hermes_lcm.engine import LCMEngine


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_nonfinite_host_threshold_cap_is_ignored(value):
    assert LCMEngine._coerce_threshold_tokens_cap(value) is None


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_nonfinite_context_length_preserves_existing_runtime(value):
    # Invalid input must return before touching any runtime/model state.
    engine = object.__new__(LCMEngine)
    engine.raw_context_length = engine.context_length = 1_000_000
    engine.threshold_tokens = 850_000
    engine._context_length_source = "configured"
    old = deepcopy(engine.__dict__)
    assert engine._set_context_length(value, source="provider") is False
    assert engine.__dict__ == old


@pytest.mark.parametrize(("value", "expected"), [("850000", 850000), (7.9, 7), (0, None), (-1, None)])
def test_finite_cap_contract_unchanged(value, expected):
    assert LCMEngine._coerce_threshold_tokens_cap(value) == expected
