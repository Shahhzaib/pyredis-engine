"""Indexed minimum heap: exactly one deadline node per expiring key."""
from __future__ import annotations


class ExpirationIndex:
    def __init__(self) -> None:
        self._heap: list[tuple[int, bytes]] = []
        self._positions: dict[bytes, int] = {}

    def __len__(self) -> int:
        return len(self._heap)

    def deadline(self, key: bytes) -> int | None:
        index = self._positions.get(key)
        return None if index is None else self._heap[index][0]

    def first(self) -> tuple[int, bytes] | None:
        return self._heap[0] if self._heap else None

    def _swap(self, left: int, right: int) -> None:
        self._heap[left], self._heap[right] = self._heap[right], self._heap[left]
        self._positions[self._heap[left][1]] = left
        self._positions[self._heap[right][1]] = right

    def _restore(self, index: int) -> None:
        if index and self._heap[index] < self._heap[(index - 1) // 2]:
            while index:
                parent = (index - 1) // 2
                if self._heap[parent] <= self._heap[index]:
                    break
                self._swap(parent, index)
                index = parent
        else:
            while 2 * index + 1 < len(self._heap):
                child = 2 * index + 1
                if child + 1 < len(self._heap) and self._heap[child + 1] < self._heap[child]:
                    child += 1
                if self._heap[index] <= self._heap[child]:
                    break
                self._swap(index, child)
                index = child

    def set(self, key: bytes, deadline: int) -> None:
        index = self._positions.get(key)
        if index is None:
            index = len(self._heap)
            self._positions[key] = index
            self._heap.append((deadline, key))
        else:
            self._heap[index] = (deadline, key)
        self._restore(index)

    def discard(self, key: bytes) -> None:
        index = self._positions.pop(key, None)
        if index is None:
            return
        last = self._heap.pop()
        if index < len(self._heap):
            self._heap[index] = last
            self._positions[last[1]] = index
            self._restore(index)

    def clear(self) -> None:
        self._heap.clear()
        self._positions.clear()
