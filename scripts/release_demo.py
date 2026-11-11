"""Exercise a private local primary, replica and restart with redis-py."""
from __future__ import annotations

import argparse
import asyncio
import tempfile
from pathlib import Path

from redis.asyncio import Redis

from pyredis_engine.config import ServerConfig
from pyredis_engine.server import start_server


async def demo(directory: Path) -> None:
    primary_config = ServerConfig(port=0, aof_path=str(directory / 'primary.aof'),
                                  aof_fsync='always', snapshot_enabled=True,
                                  replication_enabled=True, replication_recovery=True)
    primary = await start_server(primary_config)
    async with primary:
        port = primary.sockets[0].getsockname()[1]
        client = Redis(host='127.0.0.1', port=port, protocol=2, socket_timeout=5)
        try:
            assert await client.ping()
            assert await client.set(b'example', b'\x00\xff')
            assert await client.get(b'example') == b'\x00\xff'
            async with client.pipeline(transaction=True) as pipeline:
                result = await pipeline.set(b'transaction', b'value').get(b'transaction').execute()
                assert result == [True, b'value']
            print('transaction: committed and read back')

            assert await client.set(b'temporary', b'value', px=25)
            async with asyncio.timeout(3):
                while await client.get(b'temporary') is not None:
                    await asyncio.sleep(0.01)
            print('expiration: key expired')

            entry = await client.xadd(b'events', {b'kind': b'demo'}, id='1-0')
            assert await client.xgroup_create(b'events', b'workers', id='0')
            delivered = await client.xreadgroup(b'workers', b'consumer', {b'events': b'>'})
            assert delivered == [[b'events', [(entry, {b'kind': b'demo'})]]]
            assert await client.xack(b'events', b'workers', entry) == 1
            print('consumer group: delivered and acknowledged')

            replica_config = ServerConfig(port=0, aof_path=str(directory / 'replica.aof'),
                aof_fsync='always', snapshot_enabled=True, replication_enabled=True,
                replication_recovery=True, replica_host='127.0.0.1', replica_port=port)
            replica = await start_server(replica_config)
            async with replica:
                assert primary.replication is not None and replica.replication is not None
                await replica.replication.wait_ready()
                assert await client.set(b'replicated', b'after-sync')
                await replica.replication.wait_offset(primary.replication.offset)
                follower = Redis(host='127.0.0.1', port=replica.sockets[0].getsockname()[1],
                                 protocol=2, socket_timeout=5)
                try:
                    assert await follower.get(b'replicated') == b'after-sync'
                finally:
                    await follower.aclose()
                print('replica: snapshot and subsequent write verified')
            print(f'diagnostics: {primary.metrics()}')
        finally:
            await client.aclose()

    restarted = await start_server(primary_config)
    async with restarted:
        client = Redis(host='127.0.0.1', port=restarted.sockets[0].getsockname()[1],
                       protocol=2, socket_timeout=5)
        try:
            assert await client.get(b'example') == b'\x00\xff'
            assert await client.get(b'replicated') == b'after-sync'
            assert await client.xlen(b'events') == 1
            print('restart: journal and stream state recovered')
        finally:
            await client.aclose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--work-dir', type=Path, help='parent for an isolated temporary directory')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='pyredis-demo-', dir=args.work_dir) as temporary:
        asyncio.run(demo(Path(temporary)))


if __name__ == '__main__':
    main()
