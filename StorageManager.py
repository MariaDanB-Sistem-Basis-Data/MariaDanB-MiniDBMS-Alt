import os
import math
from model.data_retrieval import DataRetrieval
from model.data_write import DataWrite
from model.data_deletion import DataDeletion
from model.statistic import Statistic


class StorageManager:
    def __init__(self, storage_path="./data"):
        self.storage_path = storage_path
        self.tables_metadata = {}
        self.indexes = {}
        
    def read_block(self, data_retrieval):
        raise NotImplementedError("read_block belum diimplementasikan")
    
    def write_block(self, data_write):
        raise NotImplementedError("write_block belum diimplementasikan")
    
    def delete_block(self, data_deletion):
        raise NotImplementedError("delete_block belum diimplementasikan")
    
    def set_index(self, table, column, index_type):
        raise NotImplementedError("set_index belum diimplementasikan")
    
    def get_stats(self, table=None):
        if not table:
            
        else:
            columns = []
            for _ in range(num_columns):
                name_bytes, dtype_bytes, length = struct.unpack("20s20si", f.read(44))
                name = name_bytes.decode('utf-8').rstrip('\x00')
                dtype = dtype_bytes.decode('utf-8').rstrip('\x00')
                columns.append((name, dtype, length))
            print("Metadata:", columns)
        raise NotImplementedError("get_stats belum diimplementasikan")
    