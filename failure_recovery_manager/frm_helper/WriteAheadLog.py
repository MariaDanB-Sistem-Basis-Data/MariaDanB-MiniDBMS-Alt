from typing import List, Optional
from datetime import datetime
import os
import threading
from pathlib import Path

from frm_model.LogEntry import LogEntry, LogEntryType
from frm_model.Checkpoint import Checkpoint
from frm_helper.LogSerializer import LogSerializer
from frm_helper.Singleton import singleton
from frm_helper.Buffer import Buffer


@singleton
class WriteAheadLog:

    def __init__(self, logFilePath: str = "frm_logs/wal.log"):
        if not hasattr(self, 'initialized'):
            if not os.path.isabs(logFilePath):
                import inspect
                frames = inspect.stack()
                is_test = any('unittest' in frame.filename or 'UnitTest' in frame.filename for frame in frames)

                if is_test:
                    frm_root = Path(__file__).resolve().parent.parent
                    logFilePath = str(frm_root / logFilePath)
                else:
                    current = Path(__file__).resolve().parent
                    while current.name != 'MariaDanB-MiniDBMS-Alt' and current.parent != current:
                        current = current.parent
                    project_root = current
                    logFilePath = str(project_root / logFilePath)

            self.logFilePath = logFilePath
            self._logSerializer = LogSerializer(logFilePath)
            self._currentLogId = 0
            self._currentCheckpointId = 0
            self._logBuffer: Buffer[LogEntry] = Buffer()
            self._logIdLock = threading.Lock()
            self._checkpointIdLock = threading.Lock()

            self.initialized = True
            self._loadCurrentState()

    def _loadCurrentState(self) -> None:
        entries = self._logSerializer.readLogs()
        if entries:
            self._currentLogId = max(e.getLogId() for e in entries)
        checkpoints = self._logSerializer.readCheckpoints()
        if checkpoints:
            self._currentCheckpointId = max(cp.getCheckpointId() for cp in checkpoints)

    def appendLog(self, entry: LogEntry, forceFlush: bool = False) -> None:
        key = str(entry.getLogId())
        self._logBuffer.put(key, entry, isDirty=True)

        if forceFlush or entry.getEntryType() in (LogEntryType.COMMIT, LogEntryType.ABORT, LogEntryType.END):
            # print(f"[WAL] Force flushing {entry.getEntryType().value} log for durability")
            self.flushBuffer()


    def getNextLogId(self) -> int:
        with self._logIdLock:
            self._currentLogId += 1
            return self._currentLogId

    def getLogsForTransaction(self, transactionId: int) -> List[LogEntry]:
        entries = self._logSerializer.readLogs()
        return [e for e in entries if e.getTransactionId() == transactionId]

    def getLogsSinceCheckpoint(self, checkpointId: int) -> List[LogEntry]:
        checkpoints = self._logSerializer.readCheckpoints()
        checkpoint = next((cp for cp in checkpoints if cp.getCheckpointId() == checkpointId), None)
        if not checkpoint:
            return []
        lastLogId = checkpoint.getLastLogId()
        entries = self._logSerializer.readLogs()
        return [e for e in entries if e.getLogId() > lastLogId]

    def getLatestCheckpoint(self) -> Optional[Checkpoint]:
        cps = self._logSerializer.readCheckpoints()
        if not cps:
            return None
        return max(cps, key=lambda cp: cp.getCheckpointId())

    def createCheckpoint(self, activeTransactions: List[int]) -> Checkpoint:
        # Atomically read and increment checkpoint ID
        with self._checkpointIdLock:
            self._currentCheckpointId += 1
            checkpoint_id = self._currentCheckpointId

        # Atomically read current log ID
        with self._logIdLock:
            last_log_id = self._currentLogId

        checkpoint = Checkpoint(
            checkpointId=checkpoint_id,
            timestamp=datetime.now(),
            activeTransactions=activeTransactions,
            lastLogId=last_log_id,  # Last assigned log ID
        )
        self._logSerializer.writeLogEntry(checkpoint.toDict())
        return checkpoint

    def getAllLogsBackward(self, fromLogId: Optional[int] = None) -> List[LogEntry]:
        entries = self._logSerializer.readLogs()
        if fromLogId is not None:
            entries = [e for e in entries if e.getLogId() <= fromLogId]
        return sorted(entries, key=lambda e: e.getLogId(), reverse=True)

    def flushBuffer(self) -> None:
        dirty_entries = self._logBuffer.flushDirtyEntries()
        for buf_entry in dirty_entries:
            entry: LogEntry = buf_entry.getData()
            self._logSerializer.writeLogEntry(entry.toDict())
            self._logBuffer.remove(buf_entry.getKey())

    def needsFlush(self, bufferSizeThreshold: int = 50) -> bool:
        return self._logBuffer.getSize() >= bufferSizeThreshold or self._logBuffer.isNearlyFull()

    def truncateBeforeCheckpoint(self, checkpointId: int) -> None:
        checkpoints = self._logSerializer.readCheckpoints()
        checkpoint = next((cp for cp in checkpoints if cp.getCheckpointId() == checkpointId), None)
        if not checkpoint:
            print(f"[WAL WARNING] Checkpoint {checkpointId} not found, skipping truncate")
            return

        backup_path = f"frm_logs/wal_backup_cp{checkpointId}.log"

        try:
            # print(f"[WAL] Creating backup before truncate: {backup_path}")
            self._logSerializer.backupLogs(backup_path)
            # print(f"[WAL] Backup verified successfully")
        except Exception as e:
            print(f"[WAL ERROR] Backup failed - ABORTING truncate to prevent data loss!")
            print(f"[WAL ERROR] Error: {e}")
            raise RuntimeError(f"Cannot truncate logs: backup failed - {e}")

        try:
            # print(f"[WAL] Backup verified, proceeding with truncate...")
            self._logSerializer.truncateLogsBefore(checkpoint.getLastLogId())
            # print(f"[WAL] Truncate completed successfully")
        except Exception as e:
            print(f"[WAL ERROR] Truncate failed (backup is safe at {backup_path}): {e}")
            raise RuntimeError(f"Truncation failed: {e}")

    def verifyLogIntegrity(self) -> bool:
        entries = self._logSerializer.readLogs()
        if not entries:
            return True
        sortedLogs = sorted(entries, key=lambda e: e.getLogId())
        for i in range(1, len(sortedLogs)):
            if sortedLogs[i].getLogId() != sortedLogs[i-1].getLogId() + 1:
                return False
        return True

    def getLogStatistics(self) -> dict:
        entries = self._logSerializer.readLogs()
        checkpoint_dicts = self._logSerializer.readCheckpoints()
        return {
            "totalEntries": len(entries) + len(checkpoint_dicts),
            "currentLogId": self._currentLogId,
            "currentCheckpointId": self._currentCheckpointId,
            "bufferSize": self._logBuffer.getSize(),
            "checkpointCount": len(checkpoint_dicts),
            "operationCount": sum(1 for e in entries if e.getEntryType() == LogEntryType.UPDATE),
        }
