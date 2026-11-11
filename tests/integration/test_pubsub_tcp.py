from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, serialize_reply
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


async def test_binary_channels_duplicate_counts_delivery_and_return_to_commands() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as subscriber, peer(port) as publisher:
            assert await subscriber.call(b'SUBSCRIBE', b'\x00\xff', b'\x00\xff', b'') == ack(
                b'subscribe', b'\x00\xff', 1)
            assert await read(subscriber, 2) == [ack(b'subscribe', b'\x00\xff', 1),
                                                ack(b'subscribe', b'', 2)]
            assert await publisher.call(b'PUBLISH', b'\x00\xff', b'\x00\r\n\xff') == Integer(1)
            assert await read(subscriber) == [message(b'\x00\xff', b'\x00\r\n\xff')]
            assert await subscriber.call(b'PING') == Array((Bulk(b'pong'), Bulk(b'')))
            assert await subscriber.call(b'PING', b'\x00') == Array((Bulk(b'pong'), Bulk(b'\x00')))
            assert await subscriber.call(b'UNSUBSCRIBE', b'\x00\xff', b'\x00\xff') == ack(
                b'unsubscribe', b'\x00\xff', 1)
            assert await read(subscriber) == [ack(b'unsubscribe', b'\x00\xff', 1)]
            assert await subscriber.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', b'', 0)
            assert await subscriber.call(b'PING') == Simple(b'PONG')
            assert await subscriber.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', None, 0)
            assert await subscriber.call(b'SET', b'k', b'v') == Simple(b'OK')
            assert await subscriber.call(b'GET', b'k') == Bulk(b'v')


@pytest.mark.parametrize('command', [(b'GET', b'k'), (b'PUBLISH', b'c', b'm'), (b'MULTI',),
                                    (b'ECHO', b'v')])
async def test_subscribed_command_restrictions_leave_connection_usable(
    command: tuple[bytes, ...],
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as subscriber:
        assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
        expected = (b"ERR Can't execute '" + command[0].lower()
            + b"': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET"
              b" are allowed in this context")
        assert await subscriber.call(*command) == Error(expected)
        assert await subscriber.call(b'QUIT') == Simple(b'OK')
        assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''


async def test_variadic_subscribe_exec_preserves_observed_resp2_value_sequence() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        replies = await client.pipeline([(b'MULTI',), (b'SUBSCRIBE', b'a', b'b'),
            (b'PUBLISH', b'a', b'm'), (b'SET', b'k', b'v'), (b'EXEC',)])
        assert replies[:4] == [Simple(b'OK'), Simple(b'QUEUED'), Simple(b'QUEUED'),
                              Simple(b'QUEUED')]
        assert replies[4] == Array((ack(b'subscribe', b'a', 1),
                                    ack(b'subscribe', b'b', 2), Integer(1)))
        assert await read(client, 2) == [Simple(b'OK'), message(b'a', b'm')]
        assert await client.call(b'UNSUBSCRIBE', b'a', b'b') == ack(b'unsubscribe', b'a', 1)
        assert await read(client) == [ack(b'unsubscribe', b'b', 0)]
        assert await client.call(b'GET', b'k') == Bulk(b'v')


async def test_fragmented_subscription_and_pipelined_control_order() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server, peer(server.sockets[0].getsockname()[1]) as client:
        command = Array((Bulk(b'SUBSCRIBE'), Bulk(b'\xff\r\n')))
        for byte in serialize_reply(command):
            client.writer.write(bytes((byte,)))
            await client.writer.drain()
        assert await read(client) == [ack(b'subscribe', b'\xff\r\n', 1)]
        assert await client.pipeline([(b'PING', b'one'), (b'UNSUBSCRIBE', b'\xff\r\n'),
                                      (b'PING',)]) == [
            Array((Bulk(b'pong'), Bulk(b'one'))), ack(b'unsubscribe', b'\xff\r\n', 0),
            Simple(b'PONG')]


async def test_many_publishers_have_one_consistent_order_for_every_subscriber() -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as first, peer(port) as second, peer(port) as third:
            for subscriber in (first, second, third):
                assert await subscriber.call(b'SUBSCRIBE', b'\x00\xff') == ack(
                    b'subscribe', b'\x00\xff', 1)

            async def publish(index: int) -> None:
                async with peer(port) as publisher:
                    commands = [(b'PUBLISH', b'\x00\xff', f'{index}:{n}'.encode() + b'\x00\r\n')
                                for n in range(5)]
                    assert await publisher.pipeline(commands) == [Integer(3)] * 5

            received = [asyncio.create_task(read(subscriber, 125))
                        for subscriber in (first, second, third)]
            await asyncio.gather(*(publish(index) for index in range(25)))
            streams = await asyncio.gather(*received)
            assert streams[0] == streams[1] == streams[2]
            expected = {message(b'\x00\xff', f'{index}:{n}'.encode() + b'\x00\r\n')
                        for index in range(25) for n in range(5)}
            assert set(streams[0]) == expected and len(streams[0]) == len(expected)
            for index in range(25):
                own = [reply for reply in streams[0] if isinstance(reply, Array)
                       and reply.payload is not None and isinstance(reply.payload[2], Bulk)
                       and reply.payload[2].payload.startswith(f'{index}:'.encode())]
                assert own == [message(b'\x00\xff', f'{index}:{n}'.encode() + b'\x00\r\n')
                               for n in range(5)]
            assert await first.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', b'\x00\xff', 0)
            async with peer(port) as publisher:
                assert await publisher.call(b'FLUSHDB') == Simple(b'OK')
                assert await publisher.call(b'PUBLISH', b'\x00\xff', b'tail') == Integer(2)
                assert await publisher.call(b'PUBLISH', b'other', b'no one') == Integer(0)
            assert await read(second) == await read(third) == [message(b'\x00\xff', b'tail')]
            assert await first.call(b'PING') == Simple(b'PONG')  # no post-unsubscribe push
    assert not server.executor.pubsub.channels and not server.executor.pubsub.clients


async def test_subscriber_idle_is_allowed_but_partial_frames_still_time_out() -> None:
    config = ServerConfig(port=0, read_timeout=0.05)
    server = await start_server(config)
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as subscriber:
            assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
            await asyncio.sleep(0.15)  # exceed the ordinary idle limit, not a latency assertion
            async with peer(port) as publisher:
                assert await publisher.call(b'PUBLISH', b'c', b'still listening') == Integer(1)
            assert await read(subscriber) == [message(b'c', b'still listening')]
            subscriber.writer.write(b'*')
            await subscriber.writer.drain()
            assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
        async with peer(port) as publisher:
            assert await publisher.call(b'PUBLISH', b'c', b'gone') == Integer(0)


@pytest.mark.parametrize('frame', [b'!invalid\r\n', b'*1\r\n$-1\r\n'])
async def test_malformed_subscriber_frame_removes_registration(frame: bytes) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        port = server.sockets[0].getsockname()[1]
        async with peer(port) as subscriber:
            await subscriber.call(b'SUBSCRIBE', b'c')
            subscriber.writer.write(frame)
            await subscriber.writer.drain()
            assert await read(subscriber) == [Error(b'ERR Protocol error')]
            assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''
        async with peer(port) as publisher:
            assert await publisher.call(b'PUBLISH', b'c', b'gone') == Integer(0)
