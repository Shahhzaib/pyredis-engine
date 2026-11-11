from __future__ import annotations

import asyncio
import hashlib
from dataclasses import replace

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.replication.wire import (
    HEADER,
    OVERHEAD,
    ReplicationError,
    SnapshotInfo,
    packet,
    receive,
)

INFO = SnapshotInfo(b'a' * 40, 123, b's' * 16, b'g' * 16, 100, 1000)


def test_versioned_snapshot_handshake_roundtrip() -> None:
    assert SnapshotInfo.decode(INFO.encode(), ServerConfig()) == INFO


@pytest.mark.parametrize('index,value', [(0, b'FULLRESYNC'), (1, b'2'), (2, b'x' * 40),
    (3, b'-1'), (3, b'1' * 21), (4, b'../path'), (5, b'ff' * 17),
    (6, str(2**64).encode()), (7, b'0'), (7, str(2**64).encode())])
def test_invalid_handshake_fields(index: int, value: bytes) -> None:
    parts = INFO.encode().split(b' ')
    parts[index] = value
    with pytest.raises(ReplicationError):
        SnapshotInfo.decode(b' '.join(parts), ServerConfig())


@pytest.mark.asyncio
async def test_fragmented_binary_frames_and_heartbeat_preserve_exact_offsets() -> None:
    reader = asyncio.StreamReader()
    payload = b'\x00\r\n\xff\x00'
    end = 100 + OVERHEAD + len(payload)
    data = packet(b'E', end, payload) + packet(b'H', end)

    async def feed() -> None:
        for byte in data:
            reader.feed_data(bytes((byte,)))
            await asyncio.sleep(0)
        reader.feed_eof()

    feeding = asyncio.create_task(feed())
    assert await receive(reader, ServerConfig(), 100) == (b'E', end, payload)
    assert await receive(reader, ServerConfig(), end) == (b'H', end, b'')
    await feeding


@pytest.mark.asyncio
@pytest.mark.parametrize('damage', ['kind', 'length', 'heartbeat_payload', 'gap', 'duplicate',
    'checksum', 'truncated_header', 'truncated_body', 'truncated_digest'])
async def test_invalid_stream_frames_fail_before_application(damage: str) -> None:
    config = ServerConfig()
    data = packet(b'E', OVERHEAD + 5, b'abcde')
    if damage == 'kind':
        data = packet(b'X', 0)
    elif damage == 'length':
        data = HEADER.pack(b'E', 0, config.aof_max_record_bytes + 1)
    elif damage == 'heartbeat_payload':
        data = packet(b'H', 0, b'abcde')
    elif damage == 'gap':
        data = packet(b'E', OVERHEAD + 6, b'abcde')
    elif damage == 'duplicate':
        data = packet(b'E', 0, b'abcde')
    elif damage == 'checksum':
        data = data[:-1] + bytes((data[-1] ^ 1,))
    elif damage == 'truncated_header':
        data = data[:2]
    elif damage == 'truncated_body':
        data = data[:HEADER.size + 2]
    else:
        data = data[:-1]
    reader = asyncio.StreamReader()
    reader.feed_data(data)
    reader.feed_eof()
    with pytest.raises(ReplicationError):
        await receive(reader, config, 0)


@pytest.mark.asyncio
async def test_idle_stream_has_an_explicit_timeout() -> None:
    with pytest.raises(TimeoutError):
        await receive(asyncio.StreamReader(), replace(ServerConfig(), replication_timeout=0.01,
            replication_heartbeat_interval=0.001), 0)


@pytest.mark.parametrize('options', [dict(replication_enabled=1), dict(replication_enabled=True),
    dict(replica_host='127.0.0.1'), dict(replica_host='127.0.0.1', replica_port=1234),
    dict(max_replicas=0), dict(replication_queue_frames=True), dict(replication_queue_bytes=0),
    dict(replication_timeout=float('inf')), dict(replication_heartbeat_interval=10)])
def test_replication_configuration_dependencies_and_limits(options: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        ServerConfig(**options)


@pytest.mark.parametrize('host,port', [('example.com', 6379), ('192.0.2.1', 6379),
                                      ('127.0.0.1', 0), ('::1', 65536), ('::1', True)])
def test_upstream_requires_loopback_and_valid_port(host: str, port: int) -> None:
    with pytest.raises(ValueError):
        ServerConfig(aof_path='test.aof', snapshot_enabled=True, replication_enabled=True,
                     replica_host=host, replica_port=port)


def test_checksums_cover_header_as_well_as_payload() -> None:
    data = packet(b'E', 50, b'abcde')
    assert hashlib.sha256(data[:-32]).digest() == data[-32:]
