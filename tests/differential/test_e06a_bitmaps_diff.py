from __future__ import annotations

import random

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize('commands', [
    [(b'SETBIT', b'k', b'0', b'1'), (b'SETBIT', b'k', b'15', b'1'),
     (b'GETBIT', b'k', b'15'), (b'GETBIT', b'k', b'99'), (b'GET', b'k'),
     (b'BITCOUNT', b'k'), (b'BITCOUNT', b'k', b'-1', b'-1'),
     (b'BITCOUNT', b'k', b'1', b'14', b'BIT')],
    [(b'SET', b'k', b'\xff\xff'), (b'BITPOS', b'k', b'0'),
     (b'BITPOS', b'k', b'0', b'0', b'-1'), (b'BITPOS', b'k', b'1', b'1'),
     (b'BITPOS', b'k', b'0', b'0', b'7', b'BIT'),
     (b'BITPOS', b'missing', b'0'), (b'BITPOS', b'missing', b'1')],
    [(b'SET', b'a', b'\xff\x00'), (b'SET', b'b', b'\x0f'),
     (b'BITOP', b'AND', b'd', b'a', b'b'), (b'GET', b'd'),
     (b'BITOP', b'OR', b'a', b'a', b'b'), (b'GET', b'a'),
     (b'BITOP', b'XOR', b'd', b'a', b'b'), (b'GET', b'd'),
     (b'BITOP', b'NOT', b'd', b'b'), (b'GET', b'd'),
     (b'BITOP', b'OR', b'd', b'missing'), (b'GET', b'd')],
    [(b'BITFIELD', b'k', b'SET', b'i8', b'0', b'127', b'OVERFLOW', b'FAIL',
      b'INCRBY', b'i8', b'0', b'1', b'GET', b'i8', b'0'),
     (b'BITFIELD', b'k', b'OVERFLOW', b'WRAP', b'INCRBY', b'i8', b'0', b'1',
      b'SET', b'u7', b'#2', b'127', b'GET', b'u7', b'14'),
     (b'BITFIELD_RO', b'k', b'GET', b'i8', b'0', b'GET', b'u7', b'14'),
     (b'GET', b'k')],
    [(b'BITFIELD', b'k', b'OVERFLOW', b'SAT', b'SET', b'u3', b'3', b'9',
      b'INCRBY', b'i4', b'12', b'-10'), (b'GET', b'k')],
    [(b'HSET', b'h', b'f', b'v'), (b'GETBIT', b'h', b'0'),
     (b'SETBIT', b'h', b'0', b'1'), (b'BITCOUNT', b'h'),
     (b'BITPOS', b'h', b'1'), (b'BITOP', b'OR', b'd', b'h'),
     (b'BITFIELD_RO', b'h', b'GET', b'u8', b'0'),
     (b'BITCOUNT', b'h', b'bad', b'0'), (b'BITPOS', b'h', b'1', b'bad'),
     (b'BITFIELD', b'h'), (b'BITFIELD_RO', b'h')],
    [(b'GETBIT', b'k', b'-1'), (b'SETBIT', b'k', b'0', b'2'),
     (b'BITFIELD', b'k', b'GET', b'u64', b'0'),
     (b'BITOP', b'NOT', b'd', b'a', b'b')],
    [(b'BITPOS', b'missing', b'0', b'5'),
     (b'BITPOS', b'missing', b'0', b'5', b'7'),
     (b'BITPOS', b'missing', b'0', b'-3', b'-1', b'BIT'),
     (b'BITCOUNT', b'missing', b'0', b'-1', b'BIT'),
     (b'SET', b'k', b'\xff'), (b'BITPOS', b'k', b'0', b'8'),
     (b'BITPOS', b'k', b'0', b'-20'), (b'BITPOS', b'k', b'1', b'-20', b'-1')],
    [(b'BITFIELD', b'k', b'OVERFLOW', b'SAT',
      b'SET', b'i3', b'0', b'-9', b'GET', b'i3', b'0'),
     (b'BITFIELD', b'k', b'OVERFLOW', b'FAIL', b'INCRBY', b'u3', b'#1', b'-1'),
     (b'GET', b'k')],
    [(b'BITCOUNT', b'k', b'1'), (b'BITCOUNT', b'k', b'0', b'-1', b'BAD'),
     (b'BITPOS', b'k', b'2'), (b'BITPOS', b'k', b'bad'),
     (b'BITFIELD_RO', b'k', b'SET', b'u8', b'0', b'1'),
     (b'BITFIELD', b'k', b'GET', b'i0', b'0'), (b'BITFIELD', b'k', b'GET', b'u8', b'-1')],
    [(b'SET', b'k', b'\x01'), (b'PEXPIRE', b'k', b'600000'),
     (b'SETBIT', b'k', b'0', b'1'), (b'PERSIST', b'k'),
     (b'PEXPIRE', b'k', b'600000'), (b'BITFIELD', b'k', b'SET', b'u8', b'0', b'3'),
     (b'PERSIST', b'k'), (b'SET', b'd', b'old'), (b'PEXPIRE', b'd', b'600000'),
     (b'BITOP', b'NOT', b'd', b'k'), (b'PERSIST', b'd')],
])
async def test_bitmaps_against_redis(
    reference_port: int, commands: list[tuple[bytes, ...]]
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        for command in commands:
            expected = await reference.call(*command)
            result = await actual.call(*command)
            assert result == expected, (command, expected, result)


async def test_failed_field_write_does_not_extend_intentional_difference(
    reference_port: int,
) -> None:
    from pyredis_engine.protocol import Bulk

    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        command = (b'BITFIELD', b'k', b'OVERFLOW', b'FAIL', b'SET', b'u3', b'0', b'8')
        assert await reference.call(*command) == await actual.call(*command)
        assert await reference.call(b'GET', b'k') == Bulk(b'\0')
        assert await actual.call(b'GET', b'k') == Bulk(None)


async def test_seeded_bitmap_ranges_and_fields_against_redis(reference_port: int) -> None:
    rng = random.Random(60106)
    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        for _ in range(100):
            data = rng.randbytes(rng.randrange(3, 24))
            setup = (b'SET', b'k', data)
            assert await reference.call(*setup) == await actual.call(*setup)
            unit = rng.choice((b'BIT', b'BYTE'))
            start, end = (str(rng.randrange(-40, 40)).encode() for _ in range(2))
            commands = [
                (b'BITCOUNT', b'k', start, end, unit),
                (b'BITPOS', b'k', str(rng.randrange(2)).encode(), start, end, unit),
                (b'BITFIELD', b'k', b'OVERFLOW', rng.choice((b'WRAP', b'SAT', b'FAIL')),
                 b'INCRBY', rng.choice((b'i3', b'u7', b'i16')), b'1',
                 str(rng.randrange(-100, 100)).encode()),
                (b'GET', b'k'),
            ]
            for command in commands:
                expected, result = await reference.call(*command), await actual.call(*command)
                assert result == expected, (command, data, expected, result)
