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
        self._data.append(row)
        self._rowsCount += 1

    def clear(self) -> None:
        self._data.clear()
        self._rowsCount = 0


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
        data_dict = None
        if isinstance(self._data, Rows):
            data_dict = {
                "type": "rows",
                "data": self._data.getData(),
                "rowsCount": self._data.getRowsCount()
            }
        else:
            data_dict = {
                "type": "int",
                "value": self._data
            }

        return {
            "transactionId": self._transactionId,
            "timestamp": self._timestamp.isoformat(),
            "message": self._message,
            "data": data_dict,
            "query": self._query
        }

    @staticmethod
    def fromDict(data: Dict[str, Any]) -> 'ExecutionResult':
        data_obj = None
        if data["data"]["type"] == "rows":
            rows = Rows()
            for row in data["data"]["data"]:
                rows.addRow(row)
            data_obj = rows
        else:
            data_obj = data["data"]["value"]

        return ExecutionResult(
            transactionId=data["transactionId"],
            timestamp=datetime.fromisoformat(data["timestamp"]),
            message=data["message"],
            data=data_obj,
            query=data["query"]
        )
