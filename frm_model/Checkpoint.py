from datetime import datetime
from typing import List, Dict, Any

from .Serializable import Serializable


class Checkpoint(Serializable):
    """Checkpoint for recovery. Format: <checkpoint L> where L = active transaction list"""

    def __init__(
        self,
        checkpointId: int,
        timestamp: datetime,
        activeTransactions: List[int],
        lastLogId: int
    ):
        self._checkpointId = checkpointId
        self._timestamp = timestamp
        self._activeTransactions = activeTransactions
        self._lastLogId = lastLogId

    def getCheckpointId(self) -> int:
        return self._checkpointId

    def getTimestamp(self) -> datetime:
        return self._timestamp

    def getActiveTransactions(self) -> List[int]:
        return self._activeTransactions

    def getLastLogId(self) -> int:
        return self._lastLogId

    def hasActiveTransactions(self) -> bool:
        return len(self._activeTransactions) > 0

    def toDict(self) -> Dict[str, Any]:
        #TODO: Serialize to JSON
        pass

    @staticmethod
    def fromDict(data: Dict[str, Any]) -> 'Checkpoint':
        #TODO: Deserialize from JSON
        pass

    def toString(self) -> str:
        return f"<checkpoint {self._activeTransactions}>"
