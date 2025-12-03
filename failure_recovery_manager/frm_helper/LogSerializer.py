import json
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import shutil

from frm_model.LogEntry import LogEntry, LogEntryType
from frm_model.Checkpoint import Checkpoint


class LogSerializer:
    def __init__(self, logFilePath: str = "frm_logs/wal.log"):
        self._logFilePath = Path(logFilePath)
        self._ensureLogDirectory()

    def _ensureLogDirectory(self) -> None:
        self._logFilePath.parent.mkdir(parents=True, exist_ok=True)
        if not self._logFilePath.exists():
            self._logFilePath.touch()

    def writeLogEntry(self, entryDict: Dict[str, Any]) -> None:
        with self._logFilePath.open('a', encoding='utf-8') as f:
            json.dump(entryDict, f, ensure_ascii=False)
            f.write('\n')

    def writeLogEntries(self, entries: List[Dict[str, Any]]) -> None:
        with self._logFilePath.open('a', encoding='utf-8') as f:
            for entry in entries:
                json.dump(entry, f, ensure_ascii=False)
                f.write('\n')

    def readAllLogs(self) -> List[Dict[str, Any]]:
        if not self._logFilePath.exists():
            return []

        entries = []
        try:
            with self._logFilePath.open('r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            entries.append(json.loads(line))
                        except json.JSONDecodeError:
                            continue
        except Exception:
            return []

        return entries

    def readLogs(self) -> List[LogEntry]:
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
        allLogs = self.readAllLogs()
        return [log for log in allLogs if log.get("log_id", log.get("logId", 0)) >= logId]

    def readLogsBetween(self, startId: int, endId: int) -> List[Dict[str, Any]]:
        allLogs = self.readAllLogs()
        return [log for log in allLogs if startId <= log.get("log_id", log.get("logId", 0)) <= endId]

    def clearLogs(self) -> None:
        self._logFilePath.write_text("")

    def truncateLogsBefore(self, logId: int) -> None:
        allLogs = self.readAllLogs()
        filteredLogs = [log for log in allLogs if log.get("log_id", log.get("logId", 0)) >= logId]

        # Rewrite file with filtered logs
        self._logFilePath.write_text("")
        self.writeLogEntries(filteredLogs)

    def backupLogs(self, backupPath: str) -> None:
        backupFile = Path(backupPath)
        backupFile.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(self._logFilePath, backupFile)

    def restoreLogs(self, backupPath: str) -> None:
        backupFile = Path(backupPath)
        if backupFile.exists():
            shutil.copy2(backupFile, self._logFilePath)

    def _serializeDatetime(self, dt: datetime) -> str:
        return dt.isoformat()

    def _deserializeDatetime(self, dtStr: str) -> datetime:
        return datetime.fromisoformat(dtStr)

    def getLogFileSize(self) -> int:
        if self._logFilePath.exists():
            return self._logFilePath.stat().st_size
        return 0

    def isLogFileLarge(self, thresholdMb: float = 10.0) -> bool:
        sizeBytes = self.getLogFileSize()
        sizeMb = sizeBytes / (1024 * 1024)
        return sizeMb > thresholdMb
