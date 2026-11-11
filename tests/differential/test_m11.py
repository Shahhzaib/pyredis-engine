"""Deferred strict acceptance: RESP types, subscription state and pushed wire order."""
from __future__ import annotations

import asyncio

import pytest
from _support.comparison import peer
from _support.pubsub import ack, message, read

from pyredis_engine.config import ServerConfig
from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.differential]


async def test_binary_duplicates_unsubscribe_and_subscribed_ping(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as subscriber, peer(port) as publisher:
                assert await subscriber.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', None, 0)
                replies = await subscriber.pipeline([
                    (b'SUBSCRIBE', b'\xff\x00\r\n', b'\xff\x00\r\n', b''), (b'PING',)])
                assert replies == [ack(b'subscribe', b'\xff\x00\r\n', 1)] * 2
                assert await read(subscriber, 2) == [ack(b'subscribe', b'', 2),
                                                    Array((Bulk(b'pong'), Bulk(b'')))]
                assert await publisher.call(b'PUBLISH', b'', b'\x00\xff\r\n') == Integer(1)
                assert await read(subscriber) == [message(b'', b'\x00\xff\r\n')]
                assert await subscriber.call(b'PING', b'\xff') == Array(
                    (Bulk(b'pong'), Bulk(b'\xff')))
                assert await subscriber.call(b'UNSUBSCRIBE', b'\xff\x00\r\n') == ack(
                    b'unsubscribe', b'\xff\x00\r\n', 1)
                assert await subscriber.call(b'UNSUBSCRIBE') == ack(b'unsubscribe', b'', 0)
                assert await subscriber.call(b'PING') == Simple(b'PONG')


@pytest.mark.parametrize('command', [(b'GET', b'k'), (b'PUBLISH', b'c', b'm'),
                                      (b'MULTI',), (b'ECHO', b'm')])
async def test_subscribed_command_restrictions_and_quit(
    reference_port: int, command: tuple[bytes, ...],
) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as subscriber:
                assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
                assert await subscriber.call(*command) == Error(
                    b"ERR Can't execute '" + command[0].lower() + b"': only (P|S)SUBSCRIBE / "
                    b'(P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context')
                assert await subscriber.call(b'QUIT') == Simple(b'OK')
                assert await asyncio.wait_for(subscriber.reader.read(), 5) == b''


async def test_two_subscribers_receive_the_same_pipeline_order(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as first, peer(port) as second, peer(port) as publisher:
                for subscriber in (first, second):
                    assert await subscriber.call(b'SUBSCRIBE', b'c') == ack(b'subscribe', b'c', 1)
                assert await publisher.pipeline([(b'PUBLISH', b'c', bytes([n]))
                                                 for n in range(5)]) == [Integer(2)] * 5
                expected = [message(b'c', bytes([n])) for n in range(5)]
                assert await read(first, 5) == await read(second, 5) == expected
                assert await first.call(b'UNSUBSCRIBE', b'c', b'c', b'missing') == ack(
                    b'unsubscribe', b'c', 0)
                assert await read(first, 2) == [ack(b'unsubscribe', b'c', 0),
                                               ack(b'unsubscribe', b'missing', 0)]
                assert await publisher.call(b'PUBLISH', b'c', b'tail') == Integer(1)
                assert await read(second) == [message(b'c', b'tail')]


async def test_exec_variadic_subscription_reply_sequence(reference_port: int) -> None:
    server = await start_server(ServerConfig(port=0))
    async with server:
        for port in (server.sockets[0].getsockname()[1], reference_port):
            async with peer(port) as connection:
                replies = await connection.pipeline([(b'MULTI',), (b'SUBSCRIBE', b'a', b'b'),
                    (b'PUBLISH', b'a', b'm'), (b'SET', b'm11-key', b'v'), (b'EXEC',)])
                assert replies == [Simple(b'OK'), *([Simple(b'QUEUED')] * 3),
                    Array((ack(b'subscribe', b'a', 1), ack(b'subscribe', b'b', 2), Integer(1)))]
                assert await read(connection, 2) == [Simple(b'OK'), message(b'a', b'm')]
