"""Owned kill-test server; named boundaries block until the parent kills it."""
from __future__ import annotations

import asyncio
import sys
import threading

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence.generations import GenerationFiles
from pyredis_engine.server import start_server


async def main() -> None:
    path, target, policy = sys.argv[1:4]

    def stage(self: GenerationFiles, name: str) -> None:
        # Always let the parent put committed effects into the post-pin tail.
        if name == 'snapshot_write':
            print('PINNED', flush=True)
            assert sys.stdin.readline().strip() == 'resume'
        if name == target:
            print('STAGE ' + name, flush=True)
            threading.Event().wait()

    GenerationFiles._stage = stage
    server = await start_server(ServerConfig(port=0, aof_path=path, aof_fsync=policy,
                                             snapshot_enabled=True))
    print('PORT', server.sockets[0].getsockname()[1], flush=True)
    async with server:
        assert await asyncio.to_thread(sys.stdin.readline) == 'compact\n'
        assert server.persistence is not None
        result = await server.persistence.compact()
        print('COMPACTED', result.activated, flush=True)
        await server.serve_forever()


if __name__ == '__main__':
    asyncio.run(main())
