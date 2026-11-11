"""No redis-cli required: run with M00 server already listening."""
from __future__ import annotations

import socket

with socket.create_connection(("127.0.0.1", 6380), timeout=3) as sock:
    sock.sendall(b"*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n")
    reply = sock.recv(1024)
    if b"+PONG\r\n" not in reply:
        raise RuntimeError(f"Unexpected response: {reply!r}")
    if b"$5\r\nhello\r\n" not in reply:
        more = sock.recv(1024)
        reply += more
        if b"$5\r\nhello\r\n" not in reply:
            raise RuntimeError(f"Unexpected ECHO response: {reply!r}")
print("PASS: PING and ECHO served over TCP")
