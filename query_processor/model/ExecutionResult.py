from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, List, TypeVar, Union

from .Rows import Rows

@dataclass
class ExecutionResult:
    transaction_id: int
    timestamp: datetime
    message: str
    data: Union[Rows, int] # data can be either rows  or an int
    query: str

    def to_json_dict(self):
        
        # Handle the 'data' field
        if isinstance(self.data, Rows):
            # Convert Rows object to a dictionary
            data_value = {
                "type": "Rows",
                "rows_count": self.data.rows_count,
                # Convert the list of generic items (T) into a list of dictionaries
                "data": [item.__dict__ if hasattr(item, '__dict__') else item for item in self.data.data]
            }
        else:
            # data is a simple int (e.g., affected rows count)
            data_value = self.data
            
        return {
            "transaction_id": self.transaction_id,
            # Convert datetime to a standard string format (ISO 8601)
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
            "message": self.message,
            "data": data_value,
            "query": self.query
        }
