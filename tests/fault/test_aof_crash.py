from __future__ import annotations

import asyncio
import os
import queue
import subprocess
import sys
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest
from _support.comparison import peer
from _support.redis_reference import ReferenceError

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.errors import TornTail
from pyredis_engine.protocol import Array, Bulk, Integer, Simple
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.persistence]


@contextmanager
def child(
    path: Path, *, torn: bool = False
) -> Iterator[tuple[subprocess.Popen[str], int, queue.Queue[str]]]:
    root = Path(__file__).resolve().parents[2]
    command = [sys.executable, "-u", str(root / "tests" / "_support" / "aof_child.py"), str(path)]
    if torn:
        command.append("torn")
    environment = dict(os.environ, PYTHONPATH=str(root / "src"))
    process = subprocess.Popen(
        command,
        cwd=root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    lines: queue.Queue[str] = queue.Queue()

    def drain() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            lines.put(line.strip())

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        line = lines.get(timeout=10)
        assert line.startswith("PORT "), line
        yield process, int(line.split()[1]), lines
    finally:
        if process.poll() is None:
            process.kill()
        process.wait(timeout=5)
        reader.join(timeout=5)
        assert not reader.is_alive()
        assert process.stdout is not None
        process.stdout.close()


async def test_process_kill_after_always_ack_recovers_entire_exec(tmp_path: Path) -> None:
    path = tmp_path / "journal.aof"
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            replies = await connection.pipeline(
                [(b"MULTI",), (b"SET", b"a", b"\x00\xff"), (b"SET", b"b", b"v"), (b"EXEC",)]
            )
            assert replies[-1] == Array((Simple(b"OK"), Simple(b"OK")))
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"MGET", b"a", b"b") == Array((Bulk(b"\x00\xff"), Bulk(b"v")))


async def test_process_kill_after_bitmap_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'bitmap-journal.aof'
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            replies = await connection.pipeline([
                (b'MULTI',), (b'SETBIT', b'k', b'0', b'1'),
                (b'BITFIELD', b'k', b'SET', b'u8', b'8', b'255'), (b'EXEC',),
            ])
            assert replies[-1] == Array((Integer(0), Array((Integer(0),))))
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync='always'))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'GET', b'k') == Bulk(b'\x80\xff')


async def test_process_kill_after_hll_geo_transaction(tmp_path: Path) -> None:
    path = tmp_path / 'hll-geo-journal.aof'
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            replies = await connection.pipeline([
                (b'MULTI',), (b'PFADD', b'h', b'\0\xff', b'two'),
                (b'GEOADD', b'g', b'13.361389', b'38.115556', b'p'), (b'EXEC',),
            ])
            assert replies[-1] == Array((Integer(1), Integer(1)))
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync='always'))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b'PFCOUNT', b'h') == Integer(2)
        assert await connection.call(b'ZSCORE', b'g', b'p') == Bulk(b'3479099956230698')


async def test_process_kill_after_stream_ack_preserves_ids_and_fields(tmp_path: Path) -> None:
    path = tmp_path / "stream-journal.aof"
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            assert await connection.call(b"XADD", b"s", b"7-1", b"\x00", b"\xff") == Bulk(b"7-1")
            assert await connection.call(b"XADD", b"s", b"8-0", b"f", b"v") == Bulk(b"8-0")
            assert await connection.call(b"XDEL", b"s", b"7-1") == Integer(1)
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"XRANGE", b"s", b"-", b"+") == Array(
            (Array((Bulk(b"8-0"), Array((Bulk(b"f"), Bulk(b"v"))))),)
        )
        assert await connection.call(b"XADD", b"s", b"8-1", b"f", b"next") == Bulk(b"8-1")


async def test_process_kill_preserves_group_pending_entries(tmp_path: Path) -> None:
    path = tmp_path / "group-journal.aof"
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            assert await connection.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
            assert await connection.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
            assert isinstance(
                await connection.call(b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"s", b">"),
                Array,
            )
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        pending = await connection.call(b"XPENDING", b"s", b"g")
        assert isinstance(pending, Array) and pending.payload is not None
        assert pending.payload[0] == Integer(1)
        assert await connection.call(b"XACK", b"s", b"g", b"1-0") == Integer(1)


async def test_process_kill_preserves_autoclaim_owner(tmp_path: Path) -> None:
    path = tmp_path / "claimed-journal.aof"
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            assert await connection.call(b"XADD", b"s", b"1-0", b"f", b"v") == Bulk(b"1-0")
            assert await connection.call(b"XGROUP", b"CREATE", b"s", b"g", b"0") == Simple(b"OK")
            assert isinstance(
                await connection.call(b"XREADGROUP", b"GROUP", b"g", b"a", b"STREAMS", b"s", b">"),
                Array,
            )
            assert await connection.call(
                b"XAUTOCLAIM", b"s", b"g", b"b", b"0", b"0-0", b"COUNT", b"1", b"JUSTID"
            ) == Array((Bulk(b"0-0"), Array((Bulk(b"1-0"),)), Array(())))
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"XPENDING", b"s", b"g") == Array(
            (Integer(1), Bulk(b"1-0"), Bulk(b"1-0"), Array((Array((Bulk(b"b"), Bulk(b"1"))),)))
        )


async def test_process_kill_preserves_multi_stream_exec_delivery(tmp_path: Path) -> None:
    path = tmp_path / "multi-group.aof"
    with child(path) as (process, port, _):
        async with peer(port) as connection:
            for key in (b"a", b"b"):
                assert await connection.call(b"XADD", key, b"1-0", b"f", b"v") == Bulk(b"1-0")
                assert await connection.call(b"XGROUP", b"CREATE", key, b"g", b"0") == Simple(b"OK")
            assert await connection.call(b"MULTI") == Simple(b"OK")
            assert await connection.call(
                b"XREADGROUP", b"GROUP", b"g", b"c", b"STREAMS", b"a", b"b", b">", b">"
            ) == Simple(b"QUEUED")
            assert isinstance(await connection.call(b"EXEC"), Array)
        process.kill()
        process.wait(timeout=5)
    server = await start_server(ServerConfig(port=0, aof_path=str(path), aof_fsync="always"))
    async with server, peer(server.sockets[0].getsockname()[1]) as connection:
        for key in (b"a", b"b"):
            pending = await connection.call(b"XPENDING", key, b"g")
            assert isinstance(pending, Array) and pending.payload is not None
            assert pending.payload[0] == Integer(1)


async def test_kill_during_transaction_write_never_replays_partial_group(tmp_path: Path) -> None:
    path = tmp_path / "journal.aof"
    with child(path, torn=True) as (process, port, lines):
        async with peer(port) as connection:
            assert await connection.call(b"SET", b"safe", b"v") == Simple(b"OK")
            pending = asyncio.create_task(
                connection.pipeline(
                    [
                        (b"MULTI",),
                        (b"SET", b"a", b"TORN_MARKER"),
                        (b"SET", b"b", b"new"),
                        (b"EXEC",),
                    ]
                )
            )
            assert await asyncio.to_thread(lines.get, True, 5) == "TORN"
            assert not pending.done()
            process.kill()
            process.wait(timeout=5)
            with pytest.raises((ReferenceError, OSError, AssertionError)):
                await pending
    before = path.read_bytes()
    with pytest.raises(TornTail):
        await start_server(ServerConfig(port=0, aof_path=str(path)))
    assert path.read_bytes() == before
    repaired = await start_server(ServerConfig(port=0, aof_path=str(path), aof_repair_tail=True))
    async with repaired, peer(repaired.sockets[0].getsockname()[1]) as connection:
        assert await connection.call(b"GET", b"safe") == Bulk(b"v")
        assert await connection.call(b"MGET", b"a", b"b") == Array((Bulk(None), Bulk(None)))
