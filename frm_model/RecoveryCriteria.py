from datetime import datetime
from typing import Optional


class RecoveryCriteria:
    def __init__(
        self,
        timestamp: Optional[datetime] = None,
        transactionId: Optional[int] = None
    ):
        self._timestamp = timestamp
        self._transactionId = transactionId

    def getTimestamp(self) -> Optional[datetime]:
        return self._timestamp

    def getTransactionId(self) -> Optional[int]:
        return self._transactionId

    def matchesEntry(self, entryTimestamp: datetime, entryTransactionId: int) -> bool:
        if self._timestamp is not None and entryTimestamp < self._timestamp:
            return False
        if self._transactionId is not None and entryTransactionId != self._transactionId:
            return False
        return True

    def isValid(self) -> bool:
        return self._timestamp is not None or self._transactionId is not None
