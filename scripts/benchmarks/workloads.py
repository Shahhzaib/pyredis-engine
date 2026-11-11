"""Synthetic workloads and independent response/final-state checks."""
from __future__ import annotations

from dataclasses import dataclass

from pyredis_engine.protocol import Array, Bulk, Integer, Simple

from .client import Peer


@dataclass(frozen=True)
class Case:
    command: str
    clients: int
    keys: int
    payload_bytes: int = 32
    mode: str = 'memory'

    def validate(self) -> None:
        if any(not isinstance(value, int) or isinstance(value, bool)
               for value in (self.clients, self.keys, self.payload_bytes)):
            raise ValueError('benchmark case sizes must be integers')
        if self.command not in ('GET', 'SET', 'INCR', 'HSET', 'RPUSH', 'SADD', 'ZADD', 'ZRANGE'):
            raise ValueError('unsupported benchmark command')
        if (not 1 <= self.clients <= 100 or not 1 <= self.keys <= 4096
                or not 1 <= self.payload_bytes <= 65536
                or self.keys * self.payload_bytes > 40 * 1024 * 1024):
            raise ValueError('benchmark case exceeds safety limits')
        if self.mode not in ('memory', 'none', 'everysec', 'always', 'compact', 'replication'):
            raise ValueError('unsupported benchmark persistence mode')


def key(index: int) -> bytes:
    return f'k:{index:06d}'.encode()


async def initialize(peer: Peer, case: Case) -> None:
    payload = b'x' * case.payload_bytes
    for start in range(0, case.keys, 100):
        indexes = range(start, min(start + 100, case.keys))
        if case.command in ('GET', 'SET', 'INCR'):
            args = [part for index in indexes for part in
                    (key(index), b'0' if case.command == 'INCR' else payload)]
            assert await peer.call(b'MSET', *args) == Simple(b'OK')
        elif case.command == 'HSET':
            args = [part for index in indexes for part in (key(index), payload)]
            assert await peer.call(b'HSET', b'collection', *args) == Integer(len(args) // 2)
        elif case.command == 'RPUSH':
            count = min(start + 100, case.keys)
            reply = await peer.call(b'RPUSH', b'collection', *([payload] * (count - start)))
            assert reply == Integer(count)
        elif case.command == 'SADD':
            args = [key(index) for index in indexes]
            assert await peer.call(b'SADD', b'collection', *args) == Integer(len(args))
        else:
            args = [part for index in indexes for part in (str(index + .25).encode(), key(index))]
            assert await peer.call(b'ZADD', b'collection', *args) == Integer(len(args) // 2)


async def operation(peer: Peer, case: Case, client: int, sequence: int) -> None:
    index = (client + sequence) % case.keys
    payload = b'x' * case.payload_bytes
    if case.command == 'GET':
        assert await peer.call(b'GET', key(index)) == Bulk(payload)
    elif case.command == 'SET':
        assert await peer.call(b'SET', key(index), payload) == Simple(b'OK')
    elif case.command == 'INCR':
        result = await peer.call(b'INCR', key(index))
        assert isinstance(result, Integer) and result.payload > 0
    elif case.command == 'HSET':
        assert await peer.call(b'HSET', b'collection', key(index), payload) == Integer(0)
    elif case.command == 'RPUSH':
        result = await peer.call(b'RPUSH', b'collection', payload)
        assert isinstance(result, Integer) and result.payload > case.keys
    elif case.command == 'SADD':
        member = f'new:{client}:{sequence}'.encode()
        assert await peer.call(b'SADD', b'collection', member) == Integer(1)
    elif case.command == 'ZADD':
        score = b'0.125' if sequence % 2 else b'0.375'
        assert await peer.call(b'ZADD', b'collection', score, key(client % case.keys)) == Integer(0)
    else:
        expected = tuple(part for index in range(min(10, case.keys))
                         for part in (Bulk(key(index)), Bulk(str(index + .25).encode())))
        reply = await peer.call(b'ZRANGE', b'collection', b'0', b'9', b'WITHSCORES')
        assert reply == Array(expected)


async def verify(peer: Peer, case: Case, completed: list[int]) -> None:
    total = sum(completed)
    if case.command in ('GET', 'SET', 'INCR'):
        increments = [0] * case.keys
        if case.command == 'INCR':
            for client, count in enumerate(completed):
                for sequence in range(count):
                    increments[(client + sequence) % case.keys] += 1
        for start in range(0, case.keys, 100):
            indexes = range(start, min(start + 100, case.keys))
            result = await peer.call(b'MGET', *(key(index) for index in indexes))
            assert result == Array(tuple(Bulk(str(increments[index]).encode()
                if case.command == 'INCR' else b'x' * case.payload_bytes) for index in indexes))
    elif case.command == 'HSET':
        result = await peer.call(b'HGETALL', b'collection')
        assert isinstance(result, Array) and result.payload is not None
        assert len(result.payload) == 2 * case.keys
        actual = dict(zip(result.payload[::2], result.payload[1::2], strict=True))
        assert actual == {Bulk(key(index)): Bulk(b'x' * case.payload_bytes)
                          for index in range(case.keys)}
    elif case.command == 'RPUSH':
        assert await peer.call(b'LLEN', b'collection') == Integer(case.keys + total)
        # Check all contents in bounded response ranges, not just final cardinality.
        for start in range(0, case.keys + total, 100):
            count = min(100, case.keys + total - start)
            assert await peer.call(b'LRANGE', b'collection', str(start).encode(),
                                   str(start + count - 1).encode()) == Array(
                tuple(Bulk(b'x' * case.payload_bytes) for _ in range(count)))
    elif case.command == 'SADD':
        result = await peer.call(b'SMEMBERS', b'collection')
        assert isinstance(result, Array) and result.payload is not None
        expected = {Bulk(key(index)) for index in range(case.keys)}
        expected.update(Bulk(f'new:{client}:{sequence}'.encode())
                        for client, count in enumerate(completed) for sequence in range(count))
        assert len(result.payload) == len(expected) and set(result.payload) == expected
    elif case.command == 'ZADD':
        assert await peer.call(b'ZCARD', b'collection') == Integer(case.keys)
        for index in range(case.keys):
            scores = [b'0.125' if (count - 1) % 2 else b'0.375'
                      for client, count in enumerate(completed) if client % case.keys == index]
            result = await peer.call(b'ZSCORE', b'collection', key(index))
            # Colliding clients may finish at different sequence numbers.
            assert result in [Bulk(score) for score in scores] if scores else result == Bulk(
                str(index + .25).encode())
    else:
        for start in range(0, case.keys, 100):
            indexes = range(start, min(start + 100, case.keys))
            range_expected = tuple(part for index in indexes
                             for part in (Bulk(key(index)), Bulk(str(index + .25).encode())))
            reply = await peer.call(b'ZRANGE', b'collection', str(start).encode(),
                str(min(start + 99, case.keys - 1)).encode(), b'WITHSCORES')
            assert reply == Array(range_expected)
