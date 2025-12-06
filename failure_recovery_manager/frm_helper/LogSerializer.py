import json
import os
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import shutil
import threading
import hashlib

from frm_model.LogEntry import LogEntry, LogEntryType
from frm_model.Checkpoint import Checkpoint


class LogSerializer:
    def __init__(self, logFilePath: str = "frm_logs/wal.log"):
        self._logFilePath = Path(logFilePath)
        self._fileLock = threading.RLock()  # File-level lock
        self._ensureLogDirectory()

    def _ensureLogDirectory(self) -> None:
        self._logFilePath.parent.mkdir(parents=True, exist_ok=True)
        if not self._logFilePath.exists():
            self._logFilePath.touch()

    def writeLogEntry(self, entryDict: Dict[str, Any]) -> None:
        with self._fileLock:
            with self._logFilePath.open('a', encoding='utf-8') as f:
                json.dump(entryDict, f, ensure_ascii=False)
                f.write('\n')
                f.flush()
                os.fsync(f.fileno())

    def writeLogEntries(self, entries: List[Dict[str, Any]]) -> None:
        with self._fileLock:
            with self._logFilePath.open('a', encoding='utf-8') as f:
                for entry in entries:
                    json.dump(entry, f, ensure_ascii=False)
                    f.write('\n')
                f.flush()
                os.fsync(f.fileno())

    def readAllLogs(self) -> List[Dict[str, Any]]:
        if not self._logFilePath.exists():
            return []

        with self._fileLock:
            entries = []
            corrupted_count = 0

            try:
                with self._logFilePath.open('r', encoding='utf-8') as f:
                    line_number = 0
                    for line in f:
                        line_number += 1
                        line = line.strip()
                        if line:
                            try:
                                entries.append(json.loads(line))
                            except json.JSONDecodeError as e:
                                corrupted_count += 1
                                # print(f"[LogSerializer WARNING] Corrupted log entry at line {line_number}: {e}")
                                # print(f"[LogSerializer WARNING] Corrupted line content: {line[:100]}...")
                                continue

            except FileNotFoundError:
                print(f"[LogSerializer ERROR] Log file not found: {self._logFilePath}")
                return []
            except PermissionError as e:
                print(f"[LogSerializer ERROR] Permission denied reading log file: {e}")
                raise RuntimeError(f"Cannot read log file due to permissions: {e}")
            except Exception as e:
                print(f"[LogSerializer ERROR] Unexpected error reading logs: {e}")
                raise RuntimeError(f"Failed to read log file: {e}")

            if corrupted_count > 0:
                print(f"[LogSerializer WARNING] Found {corrupted_count} corrupted log entries (skipped)")

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
        with self._fileLock:
            allLogs = self.readAllLogs()
            filteredLogs = [
                log for log in allLogs
                if log.get("type") == "checkpoint" or log.get("log_id", log.get("logId", 0)) >= logId
            ]

            temp_path = self._logFilePath.with_suffix('.tmp')

            try:
                with temp_path.open('w', encoding='utf-8') as f:
                    for entry in filteredLogs:
                        json.dump(entry, f, ensure_ascii=False)
                        f.write('\n')
                    f.flush()
                    os.fsync(f.fileno())  # Force write to disk

                if not temp_path.exists() or temp_path.stat().st_size == 0:
                    raise RuntimeError("Temp file creation failed or empty")

                temp_path.replace(self._logFilePath)

                # # print(f"[LogSerializer] Truncated logs before ID {logId} (kept {len(filteredLogs)} entries)")

            except Exception as e:
                if temp_path.exists():
                    temp_path.unlink()
                print(f"[LogSerializer ERROR] Truncate failed: {e}")
                raise RuntimeError(f"Failed to truncate logs atomically: {e}")

    def backupLogs(self, backupPath: str) -> None:
        with self._fileLock:
            backupFile = Path(backupPath)
            backupFile.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(self._logFilePath, backupFile)

            original_size = self._logFilePath.stat().st_size
            backup_size = backupFile.stat().st_size

            if original_size != backup_size:
                backupFile.unlink()
                raise RuntimeError(
                    f"Backup verification failed: size mismatch "
                    f"(original={original_size}, backup={backup_size})"
                )

            original_checksum = self._computeFileChecksum(self._logFilePath)
            backup_checksum = self._computeFileChecksum(backupFile)

            if original_checksum != backup_checksum:
                backupFile.unlink()
                raise RuntimeError(
                    f"Backup verification failed: checksum mismatch "
                    f"(original={original_checksum[:8]}..., backup={backup_checksum[:8]}...)"
                )

            # # print(f"[LogSerializer] Backup created and verified: {backupPath} ({original_size} bytes)")

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

    def _computeFileChecksum(self, filePath: Path) -> str:
        sha256 = hashlib.sha256()
        with filePath.open('rb') as f:
            for chunk in iter(lambda: f.read(4096), b''):
                sha256.update(chunk)
        return sha256.hexdigest()
