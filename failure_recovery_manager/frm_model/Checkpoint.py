from datetime import datetime
from typing import List, Dict, Any


class Checkpoint:
    # Format: <checkpoint L> where L = active transaction list

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
        return {
            "type": "checkpoint",
            "checkpointId": self._checkpointId,
            "timestamp": self._timestamp.isoformat(),
            "activeTransactions": self._activeTransactions,
            "lastLogId": self._lastLogId
        }

    @staticmethod
    def fromDict(data: Dict[str, Any]) -> 'Checkpoint':
        return Checkpoint(
            checkpointId=data["checkpointId"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            activeTransactions=data["activeTransactions"],
            lastLogId=data["lastLogId"]
        )

    def toString(self) -> str:
        return f"<checkpoint {self._activeTransactions}>"
