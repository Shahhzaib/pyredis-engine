"""Windows signal test: raise a real handled signal in an owned child, no console changes."""
from __future__ import annotations

import asyncio
import logging
import signal
import sys

from pyredis_engine.__main__ import run_server
from pyredis_engine.config import ServerConfig


async def main() -> None:
    async def notify() -> None:
        name = await asyncio.to_thread(sys.stdin.readline)
        signal.raise_signal(getattr(signal, name.strip()))

    config = ServerConfig(port=0, aof_path=sys.argv[1], aof_fsync=sys.argv[2],
                          maxmemory_bytes=5000)
    await asyncio.gather(run_server(config), notify())


logging.basicConfig(level=logging.INFO)
asyncio.run(main())
