import os
from helper.row_serializer import RowSerializer
from helper.schema_manager import SchemaManager
from helper.slotted_page import SlottedPage, PAGE_SIZE
from model.condition import Condition
from model.data_retrieval import DataRetrieval

class StorageManager:
    def __init__(self, base_path="data"):
        self.base_path = base_path
        self.row_serializer = RowSerializer()
        self.schema_manager = SchemaManager()
        self.schema_manager.load_schemas()

    def read_block(self, data_retrieval: DataRetrieval):
        table = data_retrieval.table
        columns = data_retrieval.column
        conditions = data_retrieval.conditions or []

        schema = self.schema_manager.get_table_schema(table)
        if schema is None:
            raise ValueError(f"Tabel '{table}' tidak ditemukan")

        schema_attrs = [attr["name"] for attr in schema.get_attributes()]

        if columns != "*" and columns is not None:
            if isinstance(columns, str):
                columns = [columns]
            for col in columns:
                if col not in schema_attrs:
                    raise ValueError(f"Kolom '{col}' tidak ada di tabel '{table}'")

        for cond in conditions:
            if cond.column not in schema_attrs:
                raise ValueError(f"Kolom '{cond.column}' tidak ada di tabel '{table}'")

        table_path = os.path.join(self.base_path, f"{table}.dat")
        if not os.path.exists(table_path):
            raise FileNotFoundError(f"File data '{table_path}' tidak ditemukan")

        results = []

        with open(table_path, "rb") as f:
            while True:
                page_bytes = f.read(PAGE_SIZE)
                if not page_bytes:
                    break
                if len(page_bytes) < PAGE_SIZE:
                    page_bytes = page_bytes.ljust(PAGE_SIZE, b"\x00")

                page = SlottedPage()
                page.load(page_bytes)

                for slot_idx in range(page.record_count):
                    try:
                        record_bytes = page.get_record(slot_idx)
                        row = self.row_serializer.deserialize(schema, record_bytes)
                    except Exception as e:
                        raise ValueError(f"Gagal decode record: {e}")

                    if not self._match_all(row, conditions):
                        continue

                    results.append(self._project(row, columns))

        return results

    def _match_all(self, row, conditions):
        for cond in conditions:
            if not self._match(row, cond):
                return False
        return True

    def _match(self, row, cond: Condition):
        a = row.get(cond.column)
        b = cond.operand
        op = cond.operation

        if isinstance(a, (int, float)) and isinstance(b, str):
            s = b.strip()
            if s.replace('.', '', 1).lstrip('+-').isdigit():
                b = float(s) if '.' in s else int(s)

        if op == "=": return a == b
        if op in ("<>", "!="): return a != b
        if op == ">": return a > b
        if op == ">=": return a >= b
        if op == "<": return a < b
        if op == "<=": return a <= b
        return False

    def _project(self, row, columns):
        if columns == "*" or columns is None:
            return row
        if isinstance(columns, str):
            columns = [columns]
        return {c: row[c] for c in columns}

# ===================================================================

    def write_block(self, data_write):
        
        pass

    def delete_block(self, data_deletion):
        # Implementation for deleting a block of data based on the data_deletion parameters
        pass

    def set_index(self, table, column, index_type):
        # Implementation for setting an index on a specified table and column
        pass

    def get_stats(self):
        # Implementation for retrieving statistics about the storage
        pass