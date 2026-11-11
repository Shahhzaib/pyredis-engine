"""Optional smoke acceptance with the official client; never installs tools."""
from __future__ import annotations

import asyncio
import shutil

import pytest

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server


@pytest.mark.asyncio
@pytest.mark.integration
async def test_redis_cli_supported_commands() -> None:
    cli = shutil.which("redis-cli")
    if cli is None:
        pytest.skip("redis-cli is not installed")
    server = await start_server(ServerConfig(port=0))
    try:
        port = str(server.sockets[0].getsockname()[1])
        for args, stdin, expected in [
            (["PING"], None, b"PONG\n"),
            (["-x", "ECHO"], b"\x00\r\n\xff", b"\x00\r\n\xff\n"),
            (["QUIT"], None, b"OK\n"),
        ]:
            process = await asyncio.create_subprocess_exec(
                cli, "-h", "127.0.0.1", "-p", port, "--raw", *args,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(stdin), 5)
                assert process.returncode == 0, stderr
                assert stdout == expected
            finally:
                if process.returncode is None:
                    process.kill()
                    await process.wait()
    finally:
        server.close()
        await server.wait_closed()
