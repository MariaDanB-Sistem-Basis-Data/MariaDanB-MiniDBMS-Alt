from typing import Dict, List, Tuple, Optional
from ccm_helper.Resource import Resource
from ccm_helper.Operation import Operation
from ccm_model.Response import Response

"""
    perlu timestamp pada class Transcation
    perlu value pada request_write
    perlu output version yang dibaca atau ditulis pada response
"""

class DataVersion:
    def __init__(self, value, write_timestamp, read_timestamp):
        self.value = value
        self.write_timestamp = write_timestamp
        self.read_timestamp = read_timestamp
        
class Multiversion():
    def __init__(self):
        self.version: Dict[str, List[DataVersion]] = {}

    def request_read(self, operation: Operation, tx_timestamp: float) -> Optional[DataVersion]:
        r_id = operation.resource_id

        if r_id not in self.version():
            self.version[r_id] = [DataVersion(None, 0, 0)]

        selected_version = None
        for version in self.version[r_id]:
            if version.write_timestamp <= tx_timestamp:
                selected_version = version
                break

        if selected_version is None:
            return Response(False, "No suitable version found")
        
        if selected_version.read_timestamp < tx_timestamp:
            selected_version.read_timestamp = tx_timestamp

        return Response(True, f"Read successful for T{operation.transaction_id} on {r_id}, version: {selected_version.value}")

        
    def request_write(self, operation: Operation, tx_timestamp: float, value) -> Response:
        r_id = operation.resource_id

        if r_id not in self.version:
            self.version[r_id] = [DataVersion(None, 0, 0)]

        selected_version = None
        selected_index = None

        for i, version in enumerate(self.version[r_id]):
            if version.write_timestamp <= tx_timestamp:
                selected_version = version
                selected_index = i
                break
        
        if selected_version is None:
            return Response(False, "No suitable version found for writing")
        
        if tx_timestamp < selected_version.read_timestamp:
            return Response(False, "Rollback required: T{operation.transaction_id}")
        
        if tx_timestamp == selected_version.write_timestamp:
            selected_version.value = value
            return Response(True, f"Write successful for T{operation.transaction_id} on {r_id}, version: {selected_version.value}")
        
        new_version = DataVersion(value, tx_timestamp, tx_timestamp)
        self.version[r_id].insert(selected_index, new_version)
        return Response(True, f"Write successful for T{operation.transaction_id} on {r_id}, version: {new_version.value}")
    
    def get_versions(self, resource_id: str) -> List[DataVersion]:
        return self.version.get(resource_id, [])
    
