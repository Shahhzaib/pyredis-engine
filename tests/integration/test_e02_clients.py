from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import pytest
from _support.acl import PASSWORD, bootstrap
from _support.tls_fixtures import generate

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server

pytestmark = [pytest.mark.asyncio, pytest.mark.integration]


@pytest.fixture(scope="module")
def tls_certs(tmp_path_factory: pytest.TempPathFactory) -> Path:
    configured = os.environ.get("PYREDIS_TEST_TLS_DIR")
    if configured:
        return Path(configured)
    if not shutil.which("openssl"):
        pytest.fail("OpenSSL or PYREDIS_TEST_TLS_DIR is required for TLS integration tests")
    path = tmp_path_factory.mktemp("e02-tls")
    generate(path)
    return path


async def test_sync_client_tls_pool_pipeline_watch_and_reconnect(
    tmp_path: Path, tls_certs: Path
) -> None:
    import redis

    settings = ServerConfig(
        port=0,
        acl_file=bootstrap(tmp_path / "acl.json", ["on", "+@all", "~*", "&*"]),
        tls_cert_file=str(tls_certs / "server.pem"),
        tls_key_file=str(tls_certs / "server.key"),
    )
    server = await start_server(settings)
    async with server:
        port = server.sockets[0].getsockname()[1]

        def exercise() -> None:
            client = redis.Redis(
                host="localhost",
                port=port,
                username="app",
                password=PASSWORD,
                ssl=True,
                ssl_ca_certs=str(tls_certs / "ca.pem"),
                ssl_check_hostname=True,
                socket_timeout=5,
                health_check_interval=1,
                protocol=2,
            )
            try:
                assert client.ping()
                assert client.client_setname("sync-client")
                assert client.client_getname() == "sync-client"
                assert client.set(b"binary\x00", b"\x00\xff")
                with client.pipeline(transaction=True) as pipe:
                    pipe.watch(b"binary\x00")
                    assert pipe.get(b"binary\x00") == b"\x00\xff"
                    pipe.multi()
                    pipe.set(b"binary\x00", b"updated")
                    pipe.get(b"binary\x00")
                    assert pipe.execute() == [True, b"updated"]
                client.connection_pool.disconnect()
                assert client.get(b"binary\x00") == b"updated"
                with pytest.raises(redis.ResponseError):
                    client.execute_command("HELLO", 4)
            finally:
                client.close()

        await asyncio.wait_for(asyncio.to_thread(exercise), 20)


async def test_async_client_pubsub_pool_and_binary_values(tmp_path: Path) -> None:
    from redis.asyncio import Redis

    server = await start_server(
        ServerConfig(port=0, acl_file=bootstrap(tmp_path / "acl.json", ["on", "+@all", "~*", "&*"]))
    )
    async with server:
        client = Redis(
            host="127.0.0.1",
            port=server.sockets[0].getsockname()[1],
            username="app",
            password=PASSWORD,
            protocol=2,
            socket_timeout=5,
        )
        try:
            assert await client.ping()
            async with client.pipeline(transaction=False) as pipe:
                pipe.set(b"key", b"\xff\x00")
                pipe.get(b"key")
                assert await pipe.execute() == [True, b"\xff\x00"]
            async with client.pubsub() as sub:
                await sub.subscribe(b"chan\x00")
                assert (await sub.get_message(timeout=5))["type"] == "subscribe"
                assert await client.publish(b"chan\x00", b"\xff\x00") == 1
                message = await sub.get_message(timeout=5)
                assert message is not None and message["data"] == b"\xff\x00"
                await sub.unsubscribe()
            await client.connection_pool.disconnect()
            assert await client.get(b"key") == b"\xff\x00"
        finally:
            await client.aclose()
