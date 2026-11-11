from __future__ import annotations

import pytest
from _support.comparison import peer

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


@pytest.mark.parametrize('commands', [
    [(b'PFADD', b'a'), (b'PFCOUNT', b'a'), (b'PFADD', b'a', b'one', b'two', b'one'),
     (b'PFADD', b'a', b'one'), (b'PFCOUNT', b'a'), (b'PFADD', b'b', b'three'),
     (b'PFCOUNT', b'a', b'b'), (b'PFMERGE', b'a', b'b'), (b'PFCOUNT', b'a')],
    [(b'PFMERGE', b'd'), (b'PFCOUNT', b'd'), (b'PEXPIRE', b'd', b'600000'),
     (b'PFADD', b'd', b'a'), (b'PERSIST', b'd'),
     (b'PEXPIRE', b'd', b'600000'), (b'PFMERGE', b'd', b'missing'), (b'PERSIST', b'd')],
    [(b'GEOADD', b'g', b'13.361389', b'38.115556', b'Palermo',
      b'15.087269', b'37.502669', b'Catania'), (b'ZSCORE', b'g', b'Palermo'),
     (b'GEODIST', b'g', b'Palermo', b'Catania', b'KM'),
     (b'GEOHASH', b'g', b'Palermo', b'Catania', b'none'),
     (b'GEOPOS', b'g', b'Palermo', b'Catania', b'none')],
    [(b'GEOADD', b'g', b'NX', b'0', b'0', b'a'),
     (b'GEOADD', b'g', b'NX', b'1', b'1', b'a'),
     (b'GEOADD', b'g', b'XX', b'CH', b'1', b'1', b'a'),
     (b'GEOADD', b'g', b'XX', b'1', b'1', b'b'),
     (b'ZRANGE', b'g', b'0', b'-1', b'WITHSCORES'), (b'ZREM', b'g', b'a')],
    [(b'GEOADD', b'g', b'13.361389', b'38.115556', b'Palermo',
      b'15.087269', b'37.502669', b'Catania'),
     (b'GEOSEARCH', b'g', b'FROMMEMBER', b'Palermo', b'BYRADIUS', b'200', b'KM', b'ASC'),
     (b'GEOSEARCH', b'g', b'FROMLONLAT', b'15', b'37', b'BYBOX', b'400', b'400',
      b'KM', b'ASC', b'WITHDIST', b'WITHHASH', b'WITHCOORD')],
    [(b'GEOADD', b'g', b'179.99', b'0', b'east', b'-179.99', b'0', b'west'),
     (b'GEOSEARCH', b'g', b'FROMMEMBER', b'east', b'BYRADIUS', b'10', b'KM', b'ASC'),
     (b'GEOSEARCH', b'g', b'FROMLONLAT', b'179.99', b'0', b'BYBOX', b'10', b'10', b'KM', b'ASC')],
    [(b'GEOADD', b'g', b'180', b'85.05112878', b'upper',
      b'-180', b'-85.05112878', b'lower'),
     (b'ZSCORE', b'g', b'upper'), (b'ZSCORE', b'g', b'lower'),
     (b'GEOPOS', b'g', b'upper', b'lower')],
    [(b'GEOADD', b'g', b'CH', b'0', b'0', b'a', b'1', b'1', b'a'),
     (b'GEOADD', b'g', b'CH', b'2', b'2', b'a', b'3', b'3', b'a'),
     (b'ZSCORE', b'g', b'a')],
    [(b'SET', b'bad', b'not-a-hll'), (b'PFADD', b'bad', b'item'),
     (b'PFCOUNT', b'bad'), (b'PFMERGE', b'd', b'bad'), (b'GET', b'bad')],
    [(b'HSET', b'h', b'f', b'v'), (b'PFADD', b'h', b'v'), (b'PFCOUNT', b'h'),
     (b'PFMERGE', b'd', b'h'), (b'GEOADD', b'h', b'0', b'0', b'a'),
     (b'GEOPOS', b'h', b'a')],
    [(b'GEOADD', b'g', b'181', b'0', b'a'), (b'GEOADD', b'g', b'0', b'86', b'a'),
     (b'GEOADD', b'g', b'NX', b'XX', b'0', b'0', b'a'),
     (b'GEODIST', b'g', b'a', b'b', b'BAD')],
    [(b'GEOADD', b'g', b'0', b'0', b'a', b'0.1', b'0.1', b'b', b'1', b'1', b'c'),
     (b'GEOSEARCH', b'g', b'FROMLONLAT', b'0', b'0', b'BYRADIUS', b'200', b'KM',
      b'DESC', b'COUNT', b'2', b'WITHDIST'),
     (b'GEOSEARCH', b'g', b'FROMMEMBER', b'a', b'BYBOX', b'50', b'50', b'KM',
      b'ASC', b'COUNT', b'2', b'WITHCOORD')],
    [(b'GEOADD', b'g', b'1', b'1', b'WITHCOORD', b'1.01', b'1', b'\0\xff'),
     (b'GEOSEARCH', b'g', b'FROMMEMBER', b'WITHCOORD', b'BYRADIUS', b'2', b'KM',
      b'ASC', b'WITHDIST'), (b'GEOPOS', b'g', b'\0\xff')],
    [(b'GEOADD', b'g', b'0', b'0', b'a'), (b'PEXPIRE', b'g', b'600000'),
     (b'GEOADD', b'g', b'1', b'1', b'a'), (b'PERSIST', b'g')],
])
async def test_reference(
    reference_port: int, commands: list[tuple[bytes, ...]],
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        for command in commands:
            expected, result = await reference.call(*command), await actual.call(*command)
            assert result == expected, (command, expected, result)


async def test_hll_seeded_cardinalities(reference_port: int) -> None:
    from pyredis_engine.protocol import Integer

    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        for start in range(0, 20000, 500):
            elements = tuple(f'item-{i}'.encode() for i in range(start, start + 500))
            await reference.call(b'PFADD', b'hll', *elements)
            await actual.call(b'PFADD', b'hll', *elements)
            count = start + 500
            if count in (500, 1000, 10000, 20000):
                expected = await reference.call(b'PFCOUNT', b'hll')
                result = await actual.call(b'PFCOUNT', b'hll')
                assert isinstance(expected, Integer) and isinstance(result, Integer)
                print(f'HLL {count}: Redis={expected.payload} PyRedis={result.payload}')
                assert abs(result.payload - count) < count * 0.03
                assert abs(result.payload - expected.payload) < count * 0.03
        for number in range(20000, 20200):
            element = f'item-{number}'.encode()
            assert await reference.call(b'PFADD', b'hll', element) == (
                await actual.call(b'PFADD', b'hll', element))


async def test_geosearch_any_count_is_bounded(reference_port: int) -> None:
    from pyredis_engine.protocol import Array, Bulk

    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        setup = (b'GEOADD', b'g', b'0', b'0', b'a', b'0.1', b'0.1', b'b')
        assert await reference.call(*setup) == await actual.call(*setup)
        command = (b'GEOSEARCH', b'g', b'FROMMEMBER', b'a', b'BYRADIUS', b'100',
                   b'KM', b'COUNT', b'1', b'ANY')
        for reply in (await reference.call(*command), await actual.call(*command)):
            assert isinstance(reply, Array) and reply.payload is not None
            assert len(reply.payload) == 1
            assert reply.payload[0] in (Bulk(b'a'), Bulk(b'b'))


async def test_geo_exact_coordinate_corners(reference_port: int) -> None:
    corners = ((b'180', b'85.05112878', b'ne'),
               (b'-180', b'-85.05112878', b'sw'),
               (b'180', b'-85.05112878', b'se'),
               (b'-180', b'85.05112878', b'nw'))
    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        for lon, lat, member in corners:
            for command in ((b'GEOADD', b'all', lon, lat, member),
                            (b'GEOADD', member, lon, lat, member)):
                assert await actual.call(*command) == await reference.call(*command)
        commands = [(b'ZRANGE', b'all', b'0', b'-1', b'WITHSCORES'),
                    (b'GEOHASH', b'all', *(corner[2] for corner in corners)),
                    (b'GEOPOS', b'all', *(corner[2] for corner in corners))]
        commands += [(b'GEOSEARCH', member, b'FROMMEMBER', member, b'BYRADIUS',
                      b'1', b'KM', b'WITHHASH', b'WITHCOORD')
                     for _, _, member in corners]
        for command in commands:
            expected, result = await reference.call(*command), await actual.call(*command)
            assert result == expected, (command, expected, result)


@pytest.mark.parametrize('origin', [
    (b'FROMMEMBER', b'west'), (b'FROMMEMBER', b'east'),
    (b'FROMLONLAT', b'-180', b'0'), (b'FROMLONLAT', b'180', b'0'),
])
@pytest.mark.parametrize('shape', [(b'BYRADIUS', b'0', b'KM'),
                                  (b'BYBOX', b'0', b'0', b'KM')])
async def test_geo_zero_size_at_both_dateline_endpoints(
    reference_port: int, origin: tuple[bytes, ...], shape: tuple[bytes, ...],
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with (server, peer(reference_port) as reference,
                peer(server.sockets[0].getsockname()[1]) as actual):
        await reference.call(b'FLUSHDB')
        setup = (b'GEOADD', b'g', b'-180', b'0', b'west', b'180', b'0', b'east')
        assert await actual.call(*setup) == await reference.call(*setup)
        command = (b'GEOSEARCH', b'g', *origin, *shape, b'ASC', b'COUNT', b'1',
                   b'WITHDIST', b'WITHHASH', b'WITHCOORD')
        assert await actual.call(*command) == await reference.call(*command)


async def test_resp3_geo_and_hll_wire(reference_port: int) -> None:
    import asyncio

    from pyredis_engine.protocol import Array, Bulk, serialize_reply

    async def read_frame(reader: asyncio.StreamReader) -> bytes:
        header = await asyncio.wait_for(reader.readline(), 3)
        if header[:1] == b',':
            # RESP3 doubles are numeric; decimal spellings may differ but must
            # round-trip to the same binary64 value and retain the double type.
            return b',' + repr(float(header[1:-2])).encode('ascii') + b'\r\n'
        if header[:1] == b'$':
            length = int(header[1:-2])
            return header if length < 0 else header + await reader.readexactly(length + 2)
        if header[:1] in (b'*', b'%', b'~', b'>'):
            count = int(header[1:-2]) * (2 if header[:1] == b'%' else 1)
            for _ in range(max(0, count)):
                header += await read_frame(reader)
        return header

    server = await start_server(ServerConfig(port=0))
    async with server:
        peers = [await asyncio.open_connection('127.0.0.1', port) for port in
                 (reference_port, server.sockets[0].getsockname()[1])]
        try:
            async def call(*args: bytes) -> list[bytes]:
                request = serialize_reply(Array(tuple(Bulk(arg) for arg in args)))
                for _, writer in peers:
                    writer.write(request)
                    await writer.drain()
                return [await read_frame(reader) for reader, _ in peers]

            assert all(item.startswith(b'%') for item in await call(b'HELLO', b'3'))
            assert len(set(await call(b'FLUSHDB'))) == 1
            for command in ((b'PFADD', b'h', b'a', b'b'), (b'PFCOUNT', b'h'),
                            (b'GEOADD', b'g', b'13.361389', b'38.115556', b'p'),
                            (b'GEOPOS', b'g', b'p'), (b'GEOHASH', b'g', b'p'),
                            (b'GEODIST', b'g', b'p', b'p'),
                            (b'GEOSEARCH', b'g', b'FROMMEMBER', b'p', b'BYRADIUS', b'1',
                             b'KM', b'WITHDIST', b'WITHHASH', b'WITHCOORD')):
                reference, actual = await call(*command)
                assert actual == reference, (command, reference, actual)
        finally:
            for _, writer in peers:
                writer.close()
                await writer.wait_closed()
