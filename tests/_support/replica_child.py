"""Owned subprocess; stdin/stdout expose test barriers, never replica protocol ACKs."""
from __future__ import annotations

import asyncio
import sys
import threading

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.generations import GenerationFiles
from pyredis_engine.server import start_server


async def main() -> None:
    path, upstream, stage = sys.argv[1:4]

    def stop(self: GenerationFiles, name: str) -> None:
        if name == stage:
            print('STAGE ' + stage, flush=True)
            threading.Event().wait()

    GenerationFiles._stage = stop
    server = await start_server(ServerConfig(port=0, aof_path=path, snapshot_enabled=True,
        aof_fsync='always', replication_enabled=True, replica_host='127.0.0.1',
        replica_port=int(upstream)))
    print('PORT', server.sockets[0].getsockname()[1], flush=True)
    async with server:
        assert server.replication is not None
        await server.replication.wait_ready()
        print('READY', flush=True)
        while line := await asyncio.to_thread(sys.stdin.readline):
            await server.replication.wait_offset(int(line))
            print('OFFSET', server.replication.offset, flush=True)


if __name__ == '__main__':
    asyncio.run(main())
