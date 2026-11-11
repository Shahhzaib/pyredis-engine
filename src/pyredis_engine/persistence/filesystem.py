"""Local publication primitives; Windows namespace limits remain explicit."""
from __future__ import annotations

import os
import sys
from pathlib import Path


def sync_directory(path: Path) -> bool:
    if sys.platform == "win32":
        return False
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return True


def atomic_replace(source: Path, target: Path, *, existing: bool = False) -> None:
    if source.parent != target.parent:
        raise ValueError("publication must stay in one directory")
    if not existing and target.exists():
        raise FileExistsError(str(target))
    if sys.platform == "win32":
        import ctypes
        from ctypes import wintypes

        move = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
        move.argtypes = [wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD]
        move.restype = wintypes.BOOL
        if not move(str(source), str(target), 8 | (1 if existing else 0)):
            raise ctypes.WinError(ctypes.get_last_error())
    else:
        os.replace(source, target)
