from __future__ import annotations

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.engine.executor import Executor
from pyredis_engine.protocol import Array, Bulk, Error, Integer


def test_hll_bounded_dedup_and_union() -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([b'PFADD', b'a', b'\0\xff', b'\0\xff']).reply == Integer(1)
        assert engine.execute([b'PFADD', b'a', b'\0\xff']).reply == Integer(0)
        assert engine.execute([b'PFCOUNT', b'a']).reply == Integer(1)
        assert engine.execute([b'PFADD', b'b', b'next']).reply == Integer(1)
        assert engine.execute([b'PFCOUNT', b'a', b'b']).reply == Integer(2)
        assert engine.execute([b'PFMERGE', b'a', b'b']).reply.payload == b'OK'
        assert engine.execute([b'PFCOUNT', b'a']).reply == Integer(2)
        encoded = engine.store.get_string(b'a') or b''
        assert len(encoded) < 17000
        engine.execute([b'SET', b'copy', encoded])
        assert engine.execute([b'PFCOUNT', b'copy']).reply == Integer(2)
    finally:
        engine.close()


def test_geo_sorted_set_interoperability_and_validation() -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([b'GEOADD', b'g', b'13.361389', b'38.115556', b'Palermo',
                               b'15.087269', b'37.502669', b'Catania']).reply == Integer(2)
        assert engine.execute([b'ZSCORE', b'g', b'Palermo']).reply == Bulk(b'3479099956230698')
        assert engine.execute([b'ZREM', b'g', b'Palermo']).reply == Integer(1)
        assert engine.execute([b'GEOPOS', b'g', b'Palermo']).reply == Array((Array(None),))
        assert isinstance(engine.execute([b'GEOADD', b'g', b'181', b'0', b'x']).reply, Error)
    finally:
        engine.close()


@pytest.mark.parametrize(('longitude', 'latitude', 'score'), [
    (b'-180', b'-85.05112878', b'0'),
    (b'-180', b'85.05112878', b'4503599627370496'),
    (b'180', b'-85.05112878', b'9007199254740992'),
    (b'180', b'85.05112878', b'13510798882111488'),
])
def test_geo_redis_exact_corner_scores(longitude: bytes, latitude: bytes, score: bytes) -> None:
    engine = Executor(ServerConfig())
    try:
        assert engine.execute([b'GEOADD', b'g', longitude, latitude, b'corner']).reply == Integer(1)
        assert engine.execute([b'ZSCORE', b'g', b'corner']).reply == Bulk(score)
    finally:
        engine.close()


def test_hll_corruption_and_accounting_are_safe() -> None:
    from pyredis_engine.commands.hyperloglog import MAGIC, REGISTERS

    engine = Executor(ServerConfig())
    try:
        engine.execute([b'SET', b'bad', MAGIC + b'\xff' * REGISTERS])
        before = engine.store.memory_bytes, engine.store.mutation_revision
        assert isinstance(engine.execute([b'PFADD', b'bad', b'x']).reply, Error)
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
        engine.execute([b'SET', b'huge', MAGIC + bytes([51]) * REGISTERS])
        assert isinstance(engine.execute([b'PFCOUNT', b'huge']).reply, Error)
        assert engine.execute([b'PING']).reply.payload == b'PONG'
    finally:
        engine.close()


def test_hll_and_geo_memory_rejections_preserve_existing_state() -> None:
    engine = Executor(ServerConfig(maxmemory_bytes=256))
    try:
        engine.execute([b'SET', b'old', b'v'])
        before = engine.store.memory_bytes, engine.store.mutation_revision
        assert isinstance(engine.execute([b'PFADD', b'h', b'a']).reply, Error)
        assert engine.execute([b'GET', b'h']).reply == Bulk(None)
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
        assert isinstance(engine.execute([b'GEOADD', b'g', b'0', b'0', b'm']).reply, Error)
        assert (engine.store.memory_bytes, engine.store.mutation_revision) == before
    finally:
        engine.close()


def test_hll_union_work_limit_is_bounded() -> None:
    engine = Executor(ServerConfig(max_collection_bytes=16384))
    try:
        assert engine.execute([b'PFADD', b'a', b'value']).reply == Integer(1)
        result = engine.execute([b'PFCOUNT', b'a', b'a']).reply
        assert isinstance(result, Error) and b'work limit' in result.payload
        assert engine.execute([b'PFCOUNT', b'a']).reply == Integer(1)
    finally:
        engine.close()


@pytest.mark.parametrize('protocol', [2, 3])
def test_exec_reply_rejection_discards_hll_and_geo(protocol: int) -> None:
    engine = Executor(ServerConfig(max_reply_bytes=128))
    client = engine.new_client()
    client.protocol_version = protocol
    try:
        engine.execute([b'SET', b'large', b'x' * 100], client)
        engine.execute([b'MULTI'], client)
        engine.execute([b'PFADD', b'h', b'a'], client)
        engine.execute([b'GEOADD', b'g', b'0', b'0', b'p'], client)
        engine.execute([b'GET', b'large'], client)
        engine.execute([b'GET', b'large'], client)
        result = engine.execute([b'EXEC'], client).reply
        assert isinstance(result, Error) and result.payload.startswith(b'EXECABORT')
        assert engine.execute([b'GET', b'h'], client).reply == Bulk(None)
        assert engine.execute([b'TYPE', b'g'], client).reply.payload == b'none'
    finally:
        engine.close()
