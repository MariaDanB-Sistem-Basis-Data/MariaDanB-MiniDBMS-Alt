from typing import List, Optional, Dict, Any
from datetime import datetime

from frm_model.LogEntry import LogEntry, LogEntryType
from frm_model.Checkpoint import Checkpoint
from frm_model.ExecutionResult import ExecutionResult
from frm_helper.LogSerializer import LogSerializer
from frm_helper.Singleton import singleton
from frm_helper.Buffer import Buffer


@singleton
class WriteAheadLog:
    """Singleton Write-Ahead Log for transaction logging."""

    def __init__(self, logFilePath: str = "frm_logs/wal.json"):
        if not hasattr(self, 'initialized'):
            self._logSerializer = LogSerializer(logFilePath)
            self._currentLogId = 0
            self._currentCheckpointId = 0
            self._logBuffer: Buffer[LogEntry] = Buffer()
            self.initialized = True
            self._loadCurrentState()

    def _loadCurrentState(self) -> None:
        # Load last logId and checkpointId from existing logs
        entries = self._logSerializer.readLogs()
        if entries:
            self._currentLogId = max(e.getLogId() for e in entries)
        checkpoints = self._logSerializer.readCheckpoints()
        if checkpoints:
            self._currentCheckpointId = max(cp.getCheckpointId() for cp in checkpoints)

    def appendLog(self, entry: LogEntry) -> None:
        # Add log entry to WAL buffer
        key = str(entry.getLogId())
        self._logBuffer.put(key, entry, isDirty=True)

    def appendLogFromExecution(self, executionResult: ExecutionResult) -> LogEntry:
        # Create and append log entry from ExecutionResult
        logId = self.getNextLogId()
        entry = LogEntry(
            logId=logId,
            transactionId=executionResult.getTransactionId(),
            timestamp=datetime.now(),
            entryType=LogEntryType.UPDATE,
            dataItem=executionResult.getQuery() if hasattr(executionResult, "getQuery") else None,
            oldValue=None,
            newValue=executionResult.getData() if hasattr(executionResult, "getData") else None,
        )
        self.appendLog(entry)
        return entry

    def getNextLogId(self) -> int:
        # Generate next sequential log ID
        self._currentLogId += 1
        return self._currentLogId

    def getLogsForTransaction(self, transactionId: int) -> List[LogEntry]:
        # Retrieve all log entries for specific transaction
        entries = self._logSerializer.readLogs()
        return [e for e in entries if e.getTransactionId() == transactionId]

    def getLogsSinceCheckpoint(self, checkpointId: int) -> List[LogEntry]:
        # Retrieve all log entries since specified checkpoint
        checkpoints = self._logSerializer.readCheckpoints()
        checkpoint = next((cp for cp in checkpoints if cp.getCheckpointId() == checkpointId), None)
        if not checkpoint:
            return []
        lastLogId = checkpoint.getLastLogId()
        entries = self._logSerializer.readLogs()
        return [e for e in entries if e.getLogId() > lastLogId]

    def getLatestCheckpoint(self) -> Optional[Checkpoint]:
        # Retrieve most recent checkpoint from log
        cps = self._logSerializer.readCheckpoints()
        if not cps:
            return None
        return max(cps, key=lambda cp: cp.getCheckpointId())

    def createCheckpoint(self, activeTransactions: List[int]) -> Checkpoint:
        # Create new checkpoint entry in WAL
        self._currentCheckpointId += 1
        now = datetime.now()
        lastLogId = self._currentLogId
        checkpoint = Checkpoint(
            checkpointId=self._currentCheckpointId,
            timestamp=now,
            activeTransactions=activeTransactions,
            lastLogId=lastLogId,
        )
        self._logSerializer.writeLogEntry(checkpoint.toDict())
        return checkpoint

    def getAllLogsBackward(self, fromLogId: Optional[int] = None) -> List[LogEntry]:
        # Retrieve logs in reverse order for undo operations
        entries = self._logSerializer.readLogs()
        if fromLogId is not None:
            entries = [e for e in entries if e.getLogId() <= fromLogId]
        return sorted(entries, key=lambda e: e.getLogId(), reverse=True)

    def flushBuffer(self) -> None:
        # Write buffered log entries to persistent storage
        dirty_entries = self._logBuffer.flushDirtyEntries()
        for buf_entry in dirty_entries:
            entry: LogEntry = buf_entry.getData()
            self._logSerializer.writeLogEntry(entry.toDict())
            self._logBuffer.remove(buf_entry.getKey())

    def needsFlush(self, bufferSizeThreshold: int = 50) -> bool:
        # Check if log buffer should be flushed
        return self._logBuffer.getSize() >= bufferSizeThreshold or self._logBuffer.isNearlyFull()

    def truncateBeforeCheckpoint(self, checkpointId: int) -> None:
        # Remove log entries before specified checkpoint (log maintenance)
        checkpoints = self._logSerializer.readCheckpoints()
        checkpoint = next((cp for cp in checkpoints if cp.getCheckpointId() == checkpointId), None)
        if not checkpoint:
            return
        self._logSerializer.truncateLogsBefore(checkpoint.getLastLogId())

    def verifyLogIntegrity(self) -> bool:
        # Verify log sequence integrity (no gaps in log IDs)
        entries = self._logSerializer.readLogs()
        if not entries:
            return True
        sortedLogs = sorted(entries, key=lambda e: e.getLogId())
        for i in range(1, len(sortedLogs)):
            if sortedLogs[i].getLogId() != sortedLogs[i-1].getLogId() + 1:
                return False
        return True

    def getLogStatistics(self) -> dict:
        # Return statistics about WAL (size, entry count, etc.)
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
