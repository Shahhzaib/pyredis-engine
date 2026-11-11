"""Private bounded replication frames; not Redis PSYNC/RDB wire format."""
from __future__ import annotations

import asyncio
import hashlib
import struct
from dataclasses import dataclass

from ..config import ServerConfig
from ..protocol import ProtocolError

HEADER = struct.Struct('>cQI')
OVERHEAD = HEADER.size + 32
MAX_OFFSET = 2**64 - 1
REQUEST = [b'PYREDIS.SYNC', b'1']


class ReplicationError(ProtocolError):
    pass


class ReplicationEOF(ReplicationError):
    """Transport loss preserves the last fully applied recovery boundary."""


@dataclass(frozen=True)
class SnapshotInfo:
    replication_id: bytes
    offset: int
    store_id: bytes
    generation: bytes
    sequence: int
    size: int

    def encode(self) -> bytes:
        return b' '.join((b'PYREDIS-FULLSYNC', b'1', self.replication_id,
            str(self.offset).encode(), self.store_id.hex().encode(), self.generation.hex().encode(),
            str(self.sequence).encode(), str(self.size).encode()))

    @classmethod
    def decode(cls, raw: bytes, config: ServerConfig) -> SnapshotInfo:
        parts = raw.split(b' ')
        if len(parts) != 8 or parts[:2] != [b'PYREDIS-FULLSYNC', b'1']:
            raise ReplicationError('incompatible full-sync handshake')
        for value, size in ((parts[2], 40), (parts[4], 32), (parts[5], 32)):
            if len(value) != size or any(c not in b'0123456789abcdef' for c in value):
                raise ReplicationError('invalid replication identity')
        for value in (parts[3], parts[6], parts[7]):
            if not value or len(value) > 20 or not value.isdigit():
                raise ReplicationError('invalid replication number')
        offset, sequence, size = int(parts[3]), int(parts[6]), int(parts[7])
        if max(offset, sequence) > MAX_OFFSET or not 92 <= size <= config.snapshot_max_bytes:
            raise ReplicationError('replication metadata exceeds limits')
        return cls(parts[2], offset, bytes.fromhex(parts[4].decode('ascii')),
                   bytes.fromhex(parts[5].decode('ascii')), sequence, size)


def packet(kind: bytes, offset: int, payload: bytes = b'') -> bytes:
    header = HEADER.pack(kind, offset, len(payload))
    digest = hashlib.sha256(header)
    digest.update(payload)
    return header + payload + digest.digest()


async def receive(reader: asyncio.StreamReader, config: ServerConfig,
                  offset: int, *, version: int = 1) -> tuple[bytes, int, bytes]:
    async with asyncio.timeout(config.replication_timeout):
        try:
            header = await reader.readexactly(HEADER.size)
            kind, end, length = HEADER.unpack(header)
            if (kind not in ((b'E', b'H', b'R') if version == 2 else (b'E', b'H'))
                    or kind == b'H' and length != (32 if version == 2 else 0)
                    or kind == b'R' and length != 32
                    or kind == b'E' and not 5 <= length <= config.aof_max_record_bytes):
                raise ReplicationError('invalid replication frame kind/length')
            expected = offset if kind in (b'H', b'R') else offset + OVERHEAD + length
            if end != expected or expected > MAX_OFFSET:
                raise ReplicationError('replication offset gap or duplicate')
            digest = hashlib.sha256(header)
            body = bytearray()
            while len(body) < length:
                chunk = await reader.readexactly(min(65536, config.max_input_bytes,
                                                    length - len(body)))
                body.extend(chunk)
                digest.update(chunk)
            if await reader.readexactly(32) != digest.digest():
                raise ReplicationError('replication checksum mismatch')
            return kind, end, bytes(body)
        except asyncio.IncompleteReadError as error:
            raise ReplicationEOF('replication stream ended inside a frame') from error
