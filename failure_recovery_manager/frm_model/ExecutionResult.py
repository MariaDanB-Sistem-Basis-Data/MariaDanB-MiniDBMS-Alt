from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Union

from .Rows import Rows


@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    message: str
    data: Union[Rows, int]
    query: str

    def to_json_dict(self):
        if isinstance(self.data, Rows):
            data_value = {
                "type": "Rows",
                "rows_count": self.data.rows_count,
                "data": [item.__dict__ if hasattr(item, '__dict__') else item for item in self.data.data]
            }
        else:
            data_value = self.data

        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "message": self.message,
            "data": data_value,
            "query": self.query
        }
