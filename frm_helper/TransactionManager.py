from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum
import threading

from .Singleton import singleton


class TransactionState(Enum):
    ACTIVE = "active"
    PARTIALLY_COMMITTED = "partially_committed"
    COMMITTED = "committed"
    ABORTED = "aborted"
    FAILED = "failed"


class Transaction:
    def __init__(self, transactionId: int, startTimestamp: datetime):
        self._transactionId = transactionId
        self._state = TransactionState.ACTIVE
        self._startTimestamp = startTimestamp
        self._lastAccessTimestamp = startTimestamp

    def getTransactionId(self) -> int:
        return self._transactionId

    def getState(self) -> TransactionState:
        return self._state

    def setState(self, state: TransactionState) -> None:
        #TODO: Update state and last access timestamp
        pass

    def getStartTimestamp(self) -> datetime:
        return self._startTimestamp

    def getLastAccessTimestamp(self) -> datetime:
        return self._lastAccessTimestamp

    def updateLastAccessTimestamp(self, timestamp: datetime) -> None:
        #TODO: Update last access timestamp
        pass

    def isActive(self) -> bool:
        #TODO: Check if state is ACTIVE
        pass

    def isCommitted(self) -> bool:
        #TODO: Check if state is COMMITTED
        pass

    def isAborted(self) -> bool:
        #TODO: Check if state is ABORTED
        pass

    def canBeAborted(self) -> bool:
        #TODO: Check if state allows abortion (ACTIVE or PARTIALLY_COMMITTED)
        pass


@singleton
class TransactionManager:
    """Thread-safe singleton for transaction tracking and state management."""

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self._transactions: Dict[int, Transaction] = {}
            self._transactionLock = threading.RLock()
            self.initialized = True

    def beginTransaction(self, transactionId: int) -> Transaction:
        #TODO: Create Transaction with current timestamp and add to dict
        pass

    def getTransaction(self, transactionId: int) -> Optional[Transaction]:
        #TODO: Return transaction from dict or None
        pass

    def hasTransaction(self, transactionId: int) -> bool:
        #TODO: Check if transactionId exists in dict
        pass

    def commitTransaction(self, transactionId: int) -> bool:
        #TODO: Set transaction state to COMMITTED
        pass

    def abortTransaction(self, transactionId: int) -> bool:
        #TODO: Set transaction state to ABORTED if allowed
        pass

    def getActiveTransactions(self) -> List[Transaction]:
        #TODO: Return list of ACTIVE transactions
        pass

    def getActiveTransactionIds(self) -> List[int]:
        #TODO: Return list of active transaction IDs
        pass

    def removeTransaction(self, transactionId: int) -> bool:
        #TODO: Remove transaction from dict
        pass

    def clearCompletedTransactions(self) -> int:
        #TODO: Remove committed/aborted transactions, return count
        pass

    def getTransactionCount(self) -> int:
        #TODO: Return transaction dict size
        pass

    def getActiveTransactionCount(self) -> int:
        #TODO: Count ACTIVE transactions
        pass

    def getAllTransactions(self) -> Dict[int, Transaction]:
        return self._transactions

    def getStatistics(self) -> Dict[str, int]:
        #TODO: Return dict with transaction state counts
        pass

    def clear(self) -> None:
        #TODO: Clear all data (WARNING: Only for testing/init)
        pass
