"""Owned fault-test subprocess: stdlib-only server with an optional torn write hook."""
from __future__ import annotations

import asyncio
import os
import sys
import threading

from pyredis_engine.config import ServerConfig
from pyredis_engine.persistence import journal
from pyredis_engine.server import start_server


async def main() -> None:
    server = await start_server(ServerConfig(port=0, aof_path=sys.argv[1], aof_fsync="always"))
    if len(sys.argv) > 2:
        original = journal._write_all

        def torn(fd: int, data: bytes) -> None:
            if b"TORN_MARKER" in data:
                os.write(fd, data[:len(data) // 2])
                print("TORN", flush=True)
                threading.Event().wait()
            else:
                original(fd, data)

        journal._write_all = torn
    print("PORT", server.sockets[0].getsockname()[1], flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
