from dataclasses import replace

import pytest

from pyredis_engine.config import ServerConfig


@pytest.mark.parametrize('name', ['port', 'max_clients', 'max_bulk_bytes', 'max_frame_bytes',
    'max_args', 'max_header_bytes', 'max_depth', 'max_input_bytes', 'max_output_bytes',
    'max_reply_bytes', 'maxmemory_bytes'])
@pytest.mark.parametrize('value', [True, 1.5, '100'])
def test_core_resource_limits_require_integers(name: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(ServerConfig(), **{name: value})


@pytest.mark.parametrize('name', ['read_timeout', 'write_timeout', 'expiry_interval',
                                 'shutdown_timeout'])
@pytest.mark.parametrize('value', [True, float('nan'), float('inf'), 0, '1'])
def test_deadlines_require_finite_positive_numbers(name: str, value: object) -> None:
    with pytest.raises(ValueError):
        replace(ServerConfig(), **{name: value})


@pytest.mark.parametrize('value', [0, -1])
def test_maxmemory_cannot_disable_the_budget(value: int) -> None:
    with pytest.raises(ValueError):
        ServerConfig(maxmemory_bytes=value)


@pytest.mark.parametrize('options', [('--maxmemory', '0'), ('--maxmemory', '1.5'),
    ('--write-timeout', 'nan'), ('--shutdown-timeout', '0'), ('--host', '0.0.0.0'),
    ('--log-level', 'DEBUG')])
def test_cli_rejects_unsafe_limits_before_listening(monkeypatch, options: tuple[str, ...]) -> None:
    import sys

    from pyredis_engine.__main__ import main

    monkeypatch.setattr(sys, 'argv', ['pyredis-engine', *options])
    with pytest.raises(SystemExit) as error:
        main()
    assert error.value.code == 2
