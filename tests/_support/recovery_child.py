"""Owned M10 primary/replica child; test pipe progress is separate from wire ACKs."""
from __future__ import annotations

import asyncio
import sys

from pyredis_engine.config import ServerConfig
from pyredis_engine.replication.recovery import RecoveryReplication
from pyredis_engine.server import start_server


async def main() -> None:
    path, role, port = sys.argv[1:4]
    upstream = int(sys.argv[4]) if role == 'replica' else None
    server = await start_server(ServerConfig(port=int(port), aof_path=path, aof_fsync='always',
        snapshot_enabled=True, replication_enabled=True, replication_recovery=True,
        replica_host='127.0.0.1' if upstream is not None else None, replica_port=upstream))
    print('PORT', server.sockets[0].getsockname()[1], flush=True)
    async with server:
        state = server.replication
        assert isinstance(state, RecoveryReplication)
        if role == 'replica':
            await state.wait_ready()
            print('READY', state.last_sync, flush=True)
        while line := await asyncio.to_thread(sys.stdin.readline):
            if role == 'replica':
                await state.wait_offset(int(line))
                print('OFFSET', state.offset, flush=True)


if __name__ == '__main__':
    asyncio.run(main())
