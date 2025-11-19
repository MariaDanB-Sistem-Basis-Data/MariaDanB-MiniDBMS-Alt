import json
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import shutil

from frm_model import LogEntry, LogEntryType, Checkpoint


class LogSerializer:
    def __init__(self, logFilePath: str = "logs/wal.json"):
        self._logFilePath = Path(logFilePath)
        self._ensureLogDirectory()

    def _ensureLogDirectory(self) -> None:
        # Create log directory if it doesn't exist
        self._logFilePath.parent.mkdir(parents=True, exist_ok=True)
        if not self._logFilePath.exists():
            self._logFilePath.write_text("[]")

    def writeLogEntry(self, entryDict: Dict[str, Any]) -> None:
        # Append single log entry to JSON file
        entries = self.readAllLogs()
        entries.append(entryDict)
        self._logFilePath.write_text(json.dumps(entries, indent=2))

    def writeLogEntries(self, entries: List[Dict[str, Any]]) -> None:
        # Append multiple log entries to JSON file
        existingEntries = self.readAllLogs()
        existingEntries.extend(entries)
        self._logFilePath.write_text(json.dumps(existingEntries, indent=2))

    def readAllLogs(self) -> List[Dict[str, Any]]:
        # Read all log entries from JSON file
        if not self._logFilePath.exists():
            return []
        try:
            return json.loads(self._logFilePath.read_text())
        except json.JSONDecodeError:
            return []

    def readLogs(self) -> List[LogEntry]:
        # Return all LogEntry items (without checkpoints)
        raw = self.readAllLogs()
        result: List[LogEntry] = []
        for d in raw:
            et = d.get("entryType") or d.get("entry_type")
            if et in {t.value for t in LogEntryType} and et != LogEntryType.CHECKPOINT.value:
                if "logId" not in d and "log_id" in d:
                    d = {**d, "logId": d["log_id"]}
                if "transactionId" not in d and "transaction_id" in d:
                    d = {**d, "transactionId": d["transaction_id"]}
                if "timestamp" in d and isinstance(d["timestamp"], datetime):
                    d = {**d, "timestamp": d["timestamp"].isoformat()}
                result.append(LogEntry.fromDict(d))
        return result

    def readCheckpoints(self) -> List[Checkpoint]:
        # Return all checkpoint records from log file
        raw = self.readAllLogs()
        cps: List[Checkpoint] = []
        for d in raw:
            if d.get("type") == "checkpoint":
                cps.append(Checkpoint.fromDict(d))
            else:
                et = d.get("entryType") or d.get("entry_type")
                if et == LogEntryType.CHECKPOINT.value and "timestamp" in d:
                    cps.append(Checkpoint.fromDict({
                        "checkpointId": d.get("checkpointId", 0),
                        "timestamp": d["timestamp"],
                        "activeTransactions": d.get("activeTransactions", []),
                        "lastLogId": d.get("lastLogId", d.get("logId", d.get("log_id", 0)))
                    }))
        return cps

    def readLogsSince(self, logId: int) -> List[Dict[str, Any]]:
        # Read log entries starting from specified log_id
        allLogs = self.readAllLogs()
        return [log for log in allLogs if log.get("log_id", 0) >= logId]

    def readLogsBetween(self, startId: int, endId: int) -> List[Dict[str, Any]]:
        # Read log entries within specified range
        allLogs = self.readAllLogs()
        return [log for log in allLogs if startId <= log.get("log_id", 0) <= endId]

    def clearLogs(self) -> None:
         # Clear all log entries
        self._logFilePath.write_text("[]")

    def truncateLogsBefore(self, logId: int) -> None:
        # Remove log entries before specified log_id (after checkpoint)
        allLogs = self.readAllLogs()
        filteredLogs = [log for log in allLogs if log.get("log_id", 0) >= logId]
        self._logFilePath.write_text(json.dumps(filteredLogs, indent=2))

    def backupLogs(self, backupPath: str) -> None:
        # Create backup copy of log file
        backupFile = Path(backupPath)
        backupFile.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._logFilePath, backupFile)

    def restoreLogs(self, backupPath: str) -> None:
        # Restore logs from backup file
        backupFile = Path(backupPath)
        if backupFile.exists():
            shutil.copy2(backupFile, self._logFilePath)

    def _serializeDatetime(self, dt: datetime) -> str:
        # Convert datetime to ISO format string
        return dt.isoformat()

    def _deserializeDatetime(self, dtStr: str) -> datetime:
        # Convert ISO format string to datetime
        return datetime.fromisoformat(dtStr)

    def getLogFileSize(self) -> int:
        # Return log file size in bytes
        if self._logFilePath.exists():
            return self._logFilePath.stat().st_size
        return 0

    def isLogFileLarge(self, thresholdMb: float = 10.0) -> bool:
        # Check if log file exceeds size threshold
        sizeBytes = self.getLogFileSize()
        sizeMb = sizeBytes / (1024 * 1024)
        return sizeMb > thresholdMb
