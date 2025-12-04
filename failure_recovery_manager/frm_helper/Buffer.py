from typing import Generic, TypeVar, Dict, Optional, List
from datetime import datetime
from collections import OrderedDict
import threading

T = TypeVar('T')

class BufferEntry(Generic[T]):
    def __init__(self, key: str, data: T, isDirty: bool = False):
        self._key = key
        self._data = data
        self._isDirty = isDirty
        self._lastAccessed = datetime.now()
        self._pinCount = 0

    def getKey(self) -> str:
        return self._key

    def getData(self) -> T:
        return self._data

    def isDirty(self) -> bool:
        return self._isDirty

    def getLastAccessed(self) -> datetime:
        return self._lastAccessed

    def getPinCount(self) -> int:
        return self._pinCount

    def markDirty(self) -> None:
        self._isDirty = True

    def markClean(self) -> None:
        self._isDirty = False

    def pin(self) -> None:
        self._pinCount += 1

    def unpin(self) -> None:
        if self._pinCount > 0:
            self._pinCount -= 1

    def updateAccessTime(self) -> None:
        self._lastAccessed = datetime.now()


class Buffer(Generic[T]):
    def __init__(self, maxSize: int = 100, emergencyFlushCallback=None):
        self._maxSize = maxSize
        self._bufferPool: Dict[str, BufferEntry[T]] = {}
        self._accessOrder: OrderedDict[str, None] = OrderedDict()
        self._lock = threading.RLock()
        self._emergencyFlushCallback = emergencyFlushCallback

    def get(self, key: str) -> Optional[T]:
        with self._lock:
            if key not in self._bufferPool:
                return None

            entry = self._bufferPool[key]
            entry.updateAccessTime()
            self._accessOrder.move_to_end(key)

            return entry.getData()

    def put(self, key: str, data: T, isDirty: bool = False) -> None:
        with self._lock:
            if key in self._bufferPool:
                self._bufferPool[key]._data = data

                if isDirty:
                    self._bufferPool[key].markDirty()
                self._bufferPool[key].updateAccessTime()

                if key in self._accessOrder:
                    self._accessOrder.move_to_end(key)
                return

            if self.isFull():
                evicted_key = self.evictEntry()
                if evicted_key is None:
                    if self._emergencyFlushCallback is not None:
                        # print("[Buffer] Buffer full with dirty entries, triggering emergency flush...")
                        try:
                            self._lock.release()
                            self._emergencyFlushCallback()
                            self._lock.acquire()
                            # print("[Buffer] Emergency flush completed, retrying eviction...")

                            # Try eviction again after flush
                            evicted_key = self.evictEntry()
                            if evicted_key is None:
                                raise RuntimeError(
                                    "Buffer still full after emergency flush. "
                                    "Flush callback may not be working properly."
                                )
                        except Exception as e:
                            if not self._lock._is_owned():
                                self._lock.acquire()
                            raise RuntimeError(f"Emergency flush failed: {e}")
                    else:
                        raise RuntimeError(
                            "Buffer full with dirty entries. "
                            "Cannot add new entry without data loss. "
                            "Call flush_buffer_to_disk() first or configure emergencyFlushCallback."
                        )

            entry = BufferEntry(key, data, isDirty)
            self._bufferPool[key] = entry
            self._accessOrder[key] = None

    def remove(self, key: str) -> None:
        with self._lock:
            if key in self._bufferPool:
                del self._bufferPool[key]
            if key in self._accessOrder:
                del self._accessOrder[key]

    def isFull(self) -> bool:
        with self._lock:
            return len(self._bufferPool) >= self._maxSize

    def isNearlyFull(self, threshold: float = 0.9) -> bool:
        with self._lock:
            return len(self._bufferPool) >= self._maxSize * threshold

    def getDirtyEntries(self) -> List[BufferEntry[T]]:
        with self._lock:
            return [entry for entry in self._bufferPool.values() if entry.isDirty()]

    def flushDirtyEntries(self) -> List[BufferEntry[T]]:
        with self._lock:
            dirty_entries = self.getDirtyEntries()
            for entry in dirty_entries:
                entry.markClean()
            return dirty_entries

    def evictEntry(self) -> Optional[str]:
        with self._lock:
            for key in self._accessOrder:
                entry = self._bufferPool[key]
                if entry.getPinCount() == 0 and not entry.isDirty():
                    self.remove(key)
                    # print(f"[Buffer] Evicted clean entry: {key}")
                    return key

            # print(f"[Buffer CRITICAL] Cannot evict: all unpinned entries are dirty!")
            # print(f"[Buffer CRITICAL] Buffer full with dirty data - MUST flush first")
            # print(f"[Buffer CRITICAL] Call flush_buffer_to_disk() before continuing")

            return None

    def clear(self) -> None:
        with self._lock:
            self._bufferPool.clear()
            self._accessOrder.clear()

    def getSize(self) -> int:
        with self._lock:
            return len(self._bufferPool)

    def getMaxSize(self) -> int:
        return self._maxSize

    def pinEntry(self, key: str) -> bool:
        with self._lock:
            if key in self._bufferPool:
                self._bufferPool[key].pin()
                return True
            return False

    def unpinEntry(self, key: str) -> bool:
        with self._lock:
            if key in self._bufferPool:
                self._bufferPool[key].unpin()
                return True
            return False
