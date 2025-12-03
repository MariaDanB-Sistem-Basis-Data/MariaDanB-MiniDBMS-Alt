from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime
from ccm_helper.Resource import Resource
from ccm_helper.Operation import Operation
from ccm_model.Response import Response

class Timestamp:
    def __init__(self):
        self.read_ts: Optional[datetime] = None
        self.write_ts: Optional[datetime] = None
    
class TimestampManager:
    def __init__(self):
        self.timestampTable: Dict[str, Timestamp] = {}

    def create_timestamp(self, object_id: str) -> Timestamp:
        if object_id not in self.timestampTable:
            self.timestampTable[object_id] = Timestamp()
        return self.timestampTable[object_id]

    def validate_operation(self, operation: Operation, tx_ts: datetime) -> Response:
        timestamp = self.create_timestamp(operation.resource_id)
        action_type = operation.operation_type.lower()

        if action_type == 'read' or action_type == 'r':
            return self.check_read(timestamp, tx_ts)
        elif action_type == 'write' or action_type == 'w':
            return self.check_write(timestamp, tx_ts)
        else:
            raise ValueError("Action type harus 'read' atau 'write'")


    def check_read(self, timestamp: Timestamp, tx_ts: datetime) -> Response:
        if timestamp.write_ts is not None and tx_ts < timestamp.write_ts:
            return Response(False, "Read conflict: trying to read a value that has been overwritten by a newer transaction.") 
        
        timestamp.read_ts = max(timestamp.read_ts, tx_ts) if timestamp.read_ts else tx_ts
        return Response(True, "Read successful")
    
    def check_write(self, timestamp: Timestamp, tx_ts: datetime) -> Response:
        if timestamp.read_ts is not None and tx_ts < timestamp.read_ts:
            return Response(False, "Write conflict: trying to write a value that has been read by a newer transaction.")        
        if timestamp.write_ts is not None and tx_ts < timestamp.write_ts:
            return Response(False, "Write conflict: trying to write a value that has been written by a newer transaction.")

        timestamp.write_ts = tx_ts
        return Response(True, "Write successful")