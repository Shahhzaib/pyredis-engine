"""Version-2 negotiation and bounded reverse-direction acknowledgement frames."""
from __future__ import annotations

import asyncio
import hashlib
from collections import OrderedDict
from dataclasses import dataclass

from ..config import ServerConfig
from .backlog import advance
from .wire import HEADER, MAX_OFFSET, ReplicationError, SnapshotInfo, packet

POLICIES = {'none': b'N', 'everysec': b'E', 'always': b'A'}


def hex_bytes(value: bytes, size: int) -> bytes | None:
    if len(value) != size * 2 or any(c not in b'0123456789abcdef' for c in value):
        return None
    return bytes.fromhex(value.decode('ascii'))


def number(value: bytes) -> int | None:
    if not value or len(value) > 20 or not value.isdigit():
        return None
    result = int(value)
    return result if result <= MAX_OFFSET else None


@dataclass(frozen=True)
class ResumeRequest:
    history_id: bytes
    offset: int
    proof: bytes

    @classmethod
    def decode(cls, args: list[bytes]) -> ResumeRequest | None:
        if len(args) != 5 or any(len(value) > 128 for value in args[2:]):
            raise ReplicationError('invalid recovery request shape')
        history, offset, proof = args[2:]
        position, digest = number(offset), hex_bytes(proof, 32)
        if hex_bytes(history, 20) is None or position is None or digest is None:
            return None
        return cls(history, position, digest)


def full_greeting(info: SnapshotInfo, proof: bytes, target: int) -> bytes:
    parts = info.encode().split(b' ')
    parts[1] = b'2'
    return b' '.join((*parts, proof.hex().encode(), str(target).encode()))


def parse_full(raw: bytes, config: ServerConfig) -> tuple[SnapshotInfo, bytes, int]:
    parts = raw.split(b' ')
    if len(parts) != 10 or parts[:2] != [b'PYREDIS-FULLSYNC', b'2']:
        raise ReplicationError('incompatible recovery snapshot greeting')
    proof, target = hex_bytes(parts[8], 32), number(parts[9])
    if proof is None or target is None:
        raise ReplicationError('invalid full-sync recovery boundary')
    info = SnapshotInfo.decode(b' '.join((parts[0], b'1', *parts[2:8])), config)
    if target < info.offset:
        raise ReplicationError('reversed recovery boundary')
    return info, proof, target


def continuation(history: bytes, offset: int, proof: bytes, target: int) -> bytes:
    return b' '.join((b'PYREDIS-CONTINUE', b'2', history, str(offset).encode(),
                      proof.hex().encode(), str(target).encode()))


def parse_continuation(raw: bytes, request: ResumeRequest | None) -> int:
    parts = raw.split(b' ')
    if request is None or len(parts) != 6 or parts[:2] != [b'PYREDIS-CONTINUE', b'2']:
        raise ReplicationError('unexpected partial synchronization')
    position, proof, target = number(parts[3]), hex_bytes(parts[4], 32), number(parts[5])
    if (parts[2] != request.history_id or position != request.offset or proof != request.proof
            or target is None or target < request.offset):
        raise ReplicationError('partial synchronization history diverged')
    return target


def acknowledgement(history: bytes, offset: int, proof: bytes, policy: str) -> bytes:
    return packet(b'A', offset, history + proof + POLICIES[policy])


async def receive_ack(reader: asyncio.StreamReader, config: ServerConfig) -> tuple[int, bytes]:
    async with asyncio.timeout(config.replication_ack_timeout):
        try:
            header = await reader.readexactly(HEADER.size)
            kind, offset, length = HEADER.unpack(header)
            if kind != b'A' or length != 73:
                raise ReplicationError('invalid ACK frame kind/length')
            payload = await reader.readexactly(length)
            if await reader.readexactly(32) != hashlib.sha256(header + payload).digest():
                raise ReplicationError('ACK checksum mismatch')
            return offset, payload
        except asyncio.IncompleteReadError as error:
            raise ConnectionError('replica disconnected during ACK') from error


class AckTracker:
    def __init__(self, history: bytes, base: int, proof: bytes, limit: int) -> None:
        self.history, self.limit = history, limit
        self.offered, self.proof = base, proof
        self.acknowledged: int | None = None
        self.policy: bytes | None = None
        self.pending: OrderedDict[int, bytes] = OrderedDict(((base, proof),))

    def offer(self, frame: bytes) -> None:
        kind, end, _ = HEADER.unpack_from(frame)
        if kind != b'E':
            return
        if len(self.pending) - int(self.acknowledged in self.pending) >= self.limit:
            raise ReplicationError('unacknowledged frame boundary limit exceeded')
        if end != self.offered + len(frame):
            raise ReplicationError('offered replication history is not contiguous')
        self.proof = advance(self.proof, frame[-32:])
        self.offered = end
        self.pending[end] = self.proof

    def accept(self, offset: int, payload: bytes) -> None:
        if (len(payload) != 73 or payload[:40] != self.history
                or payload[-1:] not in POLICIES.values()
                or self.policy is not None and self.policy != payload[-1:]
                or self.acknowledged is not None and offset < self.acknowledged
                or self.pending.get(offset) != payload[40:72]):
            raise ReplicationError('invalid, stale or incompatible ACK')
        self.acknowledged, self.policy = offset, payload[-1:]
        while self.pending and next(iter(self.pending)) < offset:
            self.pending.popitem(last=False)
