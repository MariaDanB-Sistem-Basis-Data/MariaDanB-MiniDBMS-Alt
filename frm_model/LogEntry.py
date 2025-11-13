from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum

from .Serializable import Serializable


class LogEntryType(Enum):
    START = "start"
    COMMIT = "commit"
    ABORT = "abort"
    UPDATE = "update"
    COMPENSATION = "compensation"  # Redo-only log record
    CHECKPOINT = "checkpoint"


class LogEntry(Serializable):
    """Log entry for WAL. Format: <Ti start> | <Ti, Xj, V1, V2> | <Ti commit> | <Ti abort>"""

    def __init__(
        self,
        logId: int,
        transactionId: int,
        timestamp: datetime,
        entryType: LogEntryType,
        dataItem: Optional[str] = None,  # Xj
        oldValue: Optional[Any] = None,   # V1 (for undo)
        newValue: Optional[Any] = None    # V2 (for redo)
    ):
        self._logId = logId
        self._transactionId = transactionId
        self._timestamp = timestamp
        self._entryType = entryType
        self._dataItem = dataItem
        self._oldValue = oldValue
        self._newValue = newValue

    def getLogId(self) -> int:
        return self._logId

    def getTransactionId(self) -> int:
        return self._transactionId

    def getTimestamp(self) -> datetime:
        return self._timestamp

    def getEntryType(self) -> LogEntryType:
        return self._entryType

    def getDataItem(self) -> Optional[str]:
        return self._dataItem

    def getOldValue(self) -> Optional[Any]:
        return self._oldValue

    def getNewValue(self) -> Optional[Any]:
        return self._newValue

    def toDict(self) -> Dict[str, Any]:
        #TODO: Serialize to JSON
        pass

    @staticmethod
    def fromDict(data: Dict[str, Any]) -> 'LogEntry':
        #TODO: Deserialize from JSON
        pass

    def performUndo(self) -> Any:
        #TODO: Restore old value (Write V1 to Xj)
        pass

    def performRedo(self) -> Any:
        #TODO: Apply new value (Write V2 to Xj)
        pass

    def toString(self) -> str:
        if self._entryType == LogEntryType.START:
            return f"<T{self._transactionId} start>"
        elif self._entryType == LogEntryType.COMMIT:
            return f"<T{self._transactionId} commit>"
        elif self._entryType == LogEntryType.ABORT:
            return f"<T{self._transactionId} abort>"
        elif self._entryType == LogEntryType.UPDATE:
            return f"<T{self._transactionId}, {self._dataItem}, {self._oldValue}, {self._newValue}>"
        elif self._entryType == LogEntryType.COMPENSATION:
            return f"<T{self._transactionId}, {self._dataItem}, {self._newValue}>"
        else:
            return f"<checkpoint>"
