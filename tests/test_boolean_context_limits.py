"""YAML booleans must not become one-token compression/context limits."""

import pytest

from hermes_lcm.engine import LCMEngine
from hermes_lcm.config import LCMConfig


@pytest.fixture
def engine(tmp_path):
    cfg=LCMConfig(database_path=str(tmp_path/'lcm.db'),context_threshold=0.85,
                  max_assembly_tokens=0,reserve_tokens_floor=0)
    instance=LCMEngine(cfg,hermes_home=str(tmp_path))
    instance.update_model('synthetic',1000000,provider='test')
    yield instance
    instance.shutdown(wait_for_background_work=True)


@pytest.mark.parametrize('value',[True,False])
def test_bool_is_not_a_token_cap(engine,value):
    assert engine._coerce_threshold_tokens_cap(value) is None
    engine.threshold_tokens_cap=value
    assert engine.threshold_tokens==850000
    assert not engine.should_compress(1)


@pytest.mark.parametrize('value',[True,False])
def test_bool_context_update_preserves_current_window(engine,value):
    before=(engine.raw_context_length,engine.context_length,engine.threshold_tokens)
    assert engine._set_context_length(value,source='synthetic-config') is False
    assert (engine.raw_context_length,engine.context_length,engine.threshold_tokens)==before


@pytest.mark.parametrize('value',[1,'1',500000,'500000'])
def test_valid_numeric_cap_remains_supported(value):
    assert LCMEngine._coerce_threshold_tokens_cap(value)==int(value)
