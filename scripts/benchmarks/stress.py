"""Finite resource pressure and allocation retention observations, not a long soak."""
from __future__ import annotations

import asyncio
import time
from typing import Any

from pyredis_engine.protocol import Array, Bulk, Error, Integer, Simple, read_value

from .client import READ_LIMITS, Child, Peer


async def stress() -> dict[str, Any]:
    async with Child({'maxmemory': 1024 * 1024, 'max_clients': 8}) as child:
        main = await Peer.open(child.port)
        try:
            # Warm allocator/caches before tracking five equal create/delete rounds.
            history: list[dict[str, Any]] = []
            await child.control('trace_start')
            for _cycle in range(6):
                for index in range(200):
                    reply = await main.call(b'SET', f'cycle:{index}'.encode(), b'x' * 1024)
                    assert reply == Simple(b'OK')
                for start in range(0, 200, 100):
                    assert await main.call(b'DEL', *(f'cycle:{n}'.encode()
                        for n in range(start, start + 100))) == Integer(100)
                state = await child.control('collect')
                assert state['metrics']['memory_accounted_bytes'] == 0
                assert state['metrics']['keys_physical'] == state['metrics']['keys_expiring'] == 0
                history.append(state)
            trace = await child.control('trace_stop')
            # Exhaust only the small accounting budget, never OS memory.
            accepted = 0
            for index in range(1100):
                reply = await main.call(b'SET', f'quota:{index}'.encode(), b'x' * 1024)
                if isinstance(reply, Error):
                    assert reply.payload.startswith(b'OOM ')
                    break
                assert reply == Simple(b'OK')
                accepted += 1
            else:
                raise AssertionError('stress quota did not reject within its finite bound')
            quota = await child.control('metrics')
            assert quota['metrics']['memory_accounted_bytes'] <= 1024 * 1024
            for index in range(accepted):
                assert await main.call(b'GET', f'quota:{index}'.encode()) == Bulk(b'x' * 1024)
            assert await main.call(b'FLUSHDB') == Simple(b'OK')
            # Pressure admission while keeping the main client responsive.
            pressure = [await Peer.open(child.port) for _ in range(24)]
            try:
                replies = await asyncio.gather(*(peer.call(b'PING') for peer in pressure),
                                               return_exceptions=True)
                assert any(isinstance(reply, Error) or isinstance(reply, Exception)
                           for reply in replies)
                assert await main.call(b'PING') == Simple(b'PONG')
                connections = await child.control('metrics')
                assert connections['metrics']['connections_active'] <= 8
                assert connections['metrics']['client_tasks'] <= 16
            finally:
                for peer in pressure:
                    await peer.close()
            async with asyncio.timeout(5):
                while (await child.control('metrics'))['metrics']['connections_active'] != 1:
                    await asyncio.sleep(.01)
            # Explicit slow-reader scenario: fast subscriber drains each message.
            slow, fast = await Peer.open(child.port), await Peer.open(child.port)
            try:
                for peer in (slow, fast):
                    assert await peer.call(b'SUBSCRIBE', b'c') == Array(
                        (Bulk(b'subscribe'), Bulk(b'c'), Integer(1)))
                payload = b'p' * 65536
                dropped_at = None
                for index in range(128):
                    count = await main.call(b'PUBLISH', b'c', payload)
                    if count == Integer(1):
                        dropped_at = index
                    else:
                        assert count == Integer(2)
                    expected = Array((Bulk(b'message'), Bulk(b'c'), Bulk(payload)))
                    pushed = await asyncio.wait_for(read_value(fast.reader, READ_LIMITS), 5)
                    assert pushed == expected
                    if dropped_at is not None:
                        break
                assert dropped_at is not None, 'slow subscriber did not reach bounded backpressure'
            finally:
                await slow.close()
                await fast.close()
            # Sustained bounded increments: verify final exact counter after every round.
            started = time.perf_counter()
            for index in range(3000):
                assert await main.call(b'INCR', b'counter') == Integer(index + 1)
            assert await main.call(b'GET', b'counter') == Bulk(b'3000')
            sustained = {'operations': 3000, 'seconds': time.perf_counter() - started}
        finally:
            await main.close()
        async with asyncio.timeout(5):
            while True:
                final = await child.control('collect')
                if final['metrics']['connections_active'] == final['metrics']['client_tasks'] == 0:
                    break
                await asyncio.sleep(.01)
        assert final['metrics']['channels'] == final['metrics']['subscriptions'] == 0
        return {'correctness': 'passed', 'create_delete_cycles': history, 'trace': trace,
                'quota_keys_accepted': accepted, 'quota': quota, 'connection_pressure': connections,
                'observed_removal_after_publish_requests': dropped_at + 1,
                'retained_traced_growth_after_warmup_bytes': history[-1]['traced_current_bytes']
                    - history[0]['traced_current_bytes'],
                'sustained': sustained, 'final': final,
                'limitations': 'Short bounded run; stable cleanup does not prove no leaks.'}
