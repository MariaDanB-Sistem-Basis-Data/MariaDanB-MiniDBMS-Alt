from datetime import datetime
from typing import Union, List, Dict, Any

from .Serializable import Serializable


class Rows:
    def __init__(self):
        self._data: List[Dict[str, Any]] = []
        self._rowsCount: int = 0

    def getData(self) -> List[Dict[str, Any]]:
        return self._data

    def getRowsCount(self) -> int:
        return self._rowsCount

    def addRow(self, row: Dict[str, Any]) -> None:
        #TODO: Add row to data list
        pass

    def clear(self) -> None:
        #TODO: Clear all rows
        pass


class ExecutionResult(Serializable):
    """Execution result from query processing."""

    def __init__(
        self,
        transactionId: int,
        timestamp: datetime,
        message: str,
        data: Union[Rows, int],
        query: str
    ):
        self._transactionId = transactionId
        self._timestamp = timestamp
        self._message = message
        self._data = data
        self._query = query

    def getTransactionId(self) -> int:
        return self._transactionId

    def getTimestamp(self) -> datetime:
        return self._timestamp

    def getMessage(self) -> str:
        return self._message

    def getData(self) -> Union[Rows, int]:
        return self._data

    def getQuery(self) -> str:
        return self._query

    def toDict(self) -> Dict[str, Any]:
        #TODO: Convert ExecutionResult to dictionary for serialization
        pass

    @staticmethod
    def fromDict(data: Dict[str, Any]) -> 'ExecutionResult':
        #TODO: Create ExecutionResult from dictionary
        pass
