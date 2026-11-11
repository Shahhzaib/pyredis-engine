"""Rehearse expected traces locally; these results never count as Redis parity."""
from __future__ import annotations

import pytest
from _support.acceptance_cases import TRACES, assert_expected, assert_ttl_window

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.clock import ManualClock
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Bulk, Integer


@pytest.mark.parametrize("name", TRACES)
def test_local_acceptance_trace(name: str) -> None:
    engine = Executor(ServerConfig(), clock=ManualClock(1_000_000))
    for command, expected in TRACES[name]:
        assert_expected(engine.execute(list(command)).reply, expected, command)


@pytest.mark.parametrize("ttl,pttl", [(Integer(2), Integer(1499)), (Integer(1), Integer(1498))])
def test_ttl_observation_bracket(ttl: Integer, pttl: Integer) -> None:
    assert_ttl_window(2500, 1000, 1002, ttl, pttl)


@pytest.mark.parametrize("ttl,pttl", [
    (Bulk(b"1"), Integer(1499)), (Integer(1), Bulk(b"1499")),
    (Integer(0), Integer(1499)), (Integer(2), Integer(1400)),
])
def test_ttl_bracket_rejects_wrong_type_or_value(ttl: object, pttl: object) -> None:
    with pytest.raises(AssertionError):
        assert_ttl_window(2500, 1000, 1002, ttl, pttl)  # type: ignore[arg-type]
