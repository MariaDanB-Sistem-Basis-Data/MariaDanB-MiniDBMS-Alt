from datetime import datetime
import uuid

from typing import Any, Callable, Union, List, cast
import re
import os

from qp_model.ExecutionResult import ExecutionResult
from qp_model.Rows import Rows
from qp_helper.query_utils import *
from qp_helper.condition_adapter import NormalizedCondition

from storage_manager.StorageManager import StorageManager as sm
from storage_manager.storagemanager_model.data_retrieval import DataRetrieval as dr
from storage_manager.storagemanager_model.data_write import DataWrite as dw
from storage_manager.storagemanager_model.condition import Condition as cond
from storage_manager.storagemanager_helper.schema import Schema as sch
from query_optimizer.QueryOptimizer import OptimizationEngine as oe
from query_optimizer.model.query_tree import QueryTree as qt

class QueryProcessor:
    def __init__(
        self,
        optimization_engine: oe,
        storage_manager: sm,
        data_retrieval_factory: Callable[..., dr],
        data_write_factory: Callable[..., dw],
        condition_factory: Callable[..., cond],
        schema_factory: Callable[[], sch],
    ) -> None:
        self.optimization_engine = optimization_engine
        self.storage_manager = storage_manager
        self._data_retrieval_factory = data_retrieval_factory
        self._data_write_factory = data_write_factory
        self._condition_factory = condition_factory
        self._schema_factory = schema_factory
        self._transaction_active = False
        self._transaction_changes: list = []
        self._table_aliases: dict = {}

    def execute_query(self, query : str) -> ExecutionResult:

        transaction_uuid = uuid.uuid4()
        transaction_id = cast(int, transaction_uuid.int)

        query_type = get_query_type(query)

        try:
            # NOTE: harusnya yang handle keywords kaya FROM, JOIN, WHERE itu dari optimizer karena kan itu keyword bagian dari select atau query lain. soalnya disini cuma ngeidentifikasi tipe query dari keyword awalnya doang (CMIIW)

            # NOTE: feel free to adjust, kode ini dibikin dalam kondisi mengantuk kurang tidur

            result_data = Rows.from_list([])
            if query_type == QueryType.SELECT:
                result_data = self.execute_select(query)

            elif query_type == QueryType.UPDATE:
                result_data = self.execute_update(query)

            elif query_type == QueryType.DELETE:
                result_data = self.execute_delete(query)

            elif query_type == QueryType.INSERT_INTO:
                result_data = self.execute_insert(query)

            elif query_type == QueryType.CREATE_TABLE:
                result_data = self.execute_create_table(query)

            elif query_type == QueryType.DROP_TABLE:
                result_data = self.execute_drop_table(query)

            elif query_type == QueryType.BEGIN_TRANSACTION:
                result_data =  self.execute_begin_transaction(query)
            
            elif query_type == QueryType.COMMIT:
                result_data = self.execute_commit(query)

            elif query_type == QueryType.ABORT:
                result_data = self.execute_abort(query)

            elif query_type == QueryType.ROLLBACK:
                result_data = self.execute_rollback(query)

            elif query_type in DATA_QUERIES or query_type in TRANSACTION_QUERIES:
                return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message=f"Cek helper/query_utils.py, harusnya ini query type dari bonus yg belum consider dikerjain (query_type: {query_type})", data=0, query=query)

            else: # query_type == QueryType.UNKNOWN
                return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message="Error: unknown query syntax", data=-1, query=query)

            # if result_data != error maybe

            return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message="Success", data=result_data, query=query)

        except Exception as e:
            error_message = f"Error: {str(e)}"
            return ExecutionResult(transaction_id=transaction_id, timestamp=datetime.now(), message=error_message, data=-1, query=query)

    # execute SELECT query:
    # 1. parse query menggunakan query optimizer
    # 2. optimize query tree
    # 3. execute query tree dan retrieve data dari storage manager
    def execute_select(self, query: str) -> Union[Rows, int]:
        self._table_aliases = {}
        parsed_query = self.optimization_engine.parse_query(query)
        optimized_query = self.optimization_engine.optimize_query(parsed_query)
        if optimized_query.query_tree is None:
            raise ValueError("SELECT parsing failed - optimizer produced empty query tree")
        
        self._extract_table_aliases(optimized_query.query_tree)
        result_data = self._execute_query_tree(optimized_query.query_tree)
        
        return result_data

    def _extract_table_aliases(self, node: qt):
        if node is None:
            return
        
        if node.type == "TABLE":
            val_str = str(node.val)
            if hasattr(node.val, 'name'):
                val_str = str(node.val.name)
            
            parts = val_str.split()
            if len(parts) == 2:
                table_name, alias = parts
                self._table_aliases[alias] = table_name
            elif len(parts) == 1 and hasattr(node.val, 'alias') and node.val.alias:
                self._table_aliases[node.val.alias] = parts[0]
        
        for child in node.childs:
            self._extract_table_aliases(child)

    # execute UPDATE query:
    # 1. parse query menggunakan query optimizer
    # 2. extract table, columns, and conditions dari query tree
    # 3. execute update via storage manager
    def execute_update(self, query: str) -> Union[Rows, int]:
        parsed_query = self.optimization_engine.parse_query(query)
        if parsed_query.query_tree is None:
            raise ValueError("UPDATE parsing failed - optimizer produced empty query tree")
        result = self._execute_update_tree(parsed_query.query_tree)
        
        return Rows.from_list([f"Updated {result} rows"])

    # recursively execute query tree untuk SELECT operations
    # traverse dari root ke leaf (TABLE node) dan apply operations saat return
    def _execute_query_tree(self, node: qt) -> Rows:
        if node is None:
            return Rows.from_list([])
        
        if node.type == "TABLE":
            val_str = str(node.val)
            if hasattr(node.val, 'name'):
                val_str = str(node.val.name)
            parts = val_str.split()
            table_name = parts[0]
            return self._fetch_table_data(table_name)
        
        child_results = []
        for child in node.childs:
            child_result = self._execute_query_tree(child)
            child_results.append(child_result)
        
        if node.type == "PROJECT":
            return self._apply_projection(child_results[0], node.val)
        
        elif node.type == "SIGMA":
            return self._apply_selection(child_results[0], node.val)
        
        elif node.type == "JOIN" or node.type == "NATURAL_JOIN" or node.type == "THETA_JOIN":
            left_with_prefix = self._add_table_prefixes(child_results[0], self._get_table_name(node.childs[0]))
            right_with_prefix = self._add_table_prefixes(child_results[1], self._get_table_name(node.childs[1]))
            return self._apply_join(left_with_prefix, right_with_prefix, node.val, node.type)
        
        elif node.type == "CARTESIAN":
            left_with_prefix = self._add_table_prefixes(child_results[0], self._get_table_name(node.childs[0]))
            right_with_prefix = self._add_table_prefixes(child_results[1], self._get_table_name(node.childs[1]))
            return self._apply_cartesian(left_with_prefix, right_with_prefix)
        
        elif node.type == "SORT":
            return self._apply_sort(child_results[0], node.val)
        
        elif node.type == "LIMIT":
            return self._apply_limit(child_results[0], node.val)
        
        elif node.type == "GROUP":
            return self._apply_group(child_results[0], node.val)
        
        elif node.type == "OR":
            combined_data = []
            for result in child_results:
                combined_data.extend(result.data)
            unique_data = list(set(combined_data))
            return Rows.from_list(unique_data)
        
        else:
            return child_results[0] if child_results else Rows.from_list([])

    # execute UPDATE query tree
    # returns jumlah rows yang ter-update
    def _execute_update_tree(self, node: qt) -> int:
        if node is None:
            return 0
        
        update_operations = []
        conditions = []
        table_name = None
        
        current = node
        while current:
            if current.type == "UPDATE":
                update_operations.append(current.val)
            elif current.type == "SIGMA":
                conditions.append(current.val)
            elif current.type == "TABLE":
                table_name = current.val
                break
            
            current = current.childs[0] if current.childs else None
        
        if table_name and update_operations:
            return self._perform_update(table_name, update_operations, conditions)
        
        return 0

    def _fetch_table_data(self, table_name: Any) -> Rows:
        if hasattr(table_name, 'name'):
            table_str = str(table_name.name)
        else:
            table_str = str(table_name)
        
        data_retrieval = self._data_retrieval_factory(table=table_str, column="*", conditions=[])
        result = self.storage_manager.read_block(data_retrieval)
        
        if result is not None and isinstance(result, list):
            return Rows.from_list(result)
        else:
            return Rows.from_list([])

    def _resolve_column_name(self, column: str) -> str:
        if '.' in column:
            prefix, col_name = column.split('.', 1)
            if prefix in self._table_aliases:
                return f"{self._table_aliases[prefix]}.{col_name}"
        return column

    def _get_table_name(self, node: qt) -> str:
        if node is None:
            return ""
        if node.type == "TABLE":
            val_str = str(node.val)
            if hasattr(node.val, 'name'):
                val_str = str(node.val.name)
            return val_str.split()[0]
        for child in node.childs:
            name = self._get_table_name(child)
            if name:
                return name
        return ""
    
    def _add_table_prefixes(self, data: Rows, table_name: str) -> Rows:
        if not table_name or data.rows_count == 0:
            return data
        
        prefixed_data = []
        for row in data.data:
            if isinstance(row, dict):
                prefixed_row = {}
                for key, value in row.items():
                    if '.' in key or key.startswith('_'):
                        prefixed_row[key] = value
                    else:
                        prefixed_row[f"{table_name}.{key}"] = value
                prefixed_data.append(prefixed_row)
            else:
                prefixed_data.append(row)
        
        return Rows.from_list(prefixed_data)

    # apply PROJECT operation - select specific columns
    def _apply_projection(self, data: Rows, columns: Any) -> Rows:
        if isinstance(columns, str):
            if columns.strip() == "*":
                return data
            col_list = [col.strip() for col in columns.split(",") if col.strip()]
        elif isinstance(columns, (list, tuple)):
            if len(columns) == 1 and str(columns[0]).strip() == "*":
                return data
            col_list = [str(col).strip() for col in columns if str(col).strip()]
        else:
            return data
        
        if not col_list:
            return data
        
        # Validate columns exist in first row (if data exists)
        if data.rows_count > 0:
            first_row = list(data.data)[0] if data.data else None
            if first_row and isinstance(first_row, dict):
                available_cols = set(first_row.keys())
                # Also check for simple column names without table prefix
                available_simple_cols = set()
                for col in available_cols:
                    if '.' in col:
                        available_simple_cols.add(col.split('.', 1)[1])
                    else:
                        available_simple_cols.add(col)
                
                for req_col in col_list:
                    col_name = req_col.split('.', 1)[1] if '.' in req_col else req_col
                    if req_col not in available_cols and col_name not in available_simple_cols:
                        raise ValueError(f"Column '{req_col}' does not exist in table")
        
        projected_data = []
        for row in data.data:
            if isinstance(row, dict):
                projected_row = {}
                for original_col in col_list:
                    if '.' in original_col:
                        prefix, col_name = original_col.split('.', 1)
                        resolved_prefix = self._table_aliases.get(prefix, prefix)
                        
                        found = False
                        full_col = f"{resolved_prefix}.{col_name}"
                        if full_col in row:
                            projected_row[original_col] = row[full_col]
                            found = True
                        elif original_col in row:
                            projected_row[original_col] = row[original_col]
                            found = True
                        elif col_name in row:
                            projected_row[original_col] = row[col_name]
                            found = True
                        if not found:
                            for key in row.keys():
                                if key.endswith('.' + col_name) or key == col_name:
                                    projected_row[original_col] = row[key]
                                    found = True
                                    break
                    else:
                        if original_col in row:
                            projected_row[original_col] = row[original_col]
                        else:
                            for key in row.keys():
                                if key.endswith('.' + original_col) or key == original_col:
                                    projected_row[original_col] = row[key]
                                    break
                
                if projected_row:
                    projected_data.append(projected_row)
            else:
                projected_data.append(row)
        
        return Rows.from_list(projected_data)

    # apply SIGMA operation - filter rows based on WHERE condition
    def _apply_selection(self, data: Rows, condition: Any) -> Rows:
        # Normalize condition to internal format
        normalized = NormalizedCondition.normalize(condition)
        if not normalized:
            return data
        
        col_name = self._resolve_column_name(normalized.column)
        operator = normalized.operator
        value = normalized.value
        
        is_column_comparison = False
        value_col_name = None
        
        def strip_prefix(col):
            if '.' in col:
                return col.split('.', 1)[1]
            return col
        
        if '.' in value or (value and not value[0].isdigit() and "'" not in value and '"' not in value):
            value_col_name = self._resolve_column_name(value)
            sample_row = list(data.data)[0] if data.rows_count > 0 else None
            if sample_row and isinstance(sample_row, dict):
                if value_col_name in sample_row or strip_prefix(value_col_name) in sample_row or value in sample_row or strip_prefix(value) in sample_row:
                    is_column_comparison = True
        
        filtered_data = []
        
        for row in data.data:
            if not isinstance(row, dict):
                continue
                
            left_value = None
            if col_name in row:
                left_value = row[col_name]
            elif strip_prefix(col_name) in row:
                left_value = row[strip_prefix(col_name)]
            else:
                continue
            
            if is_column_comparison:
                right_value = None
                if value_col_name in row:
                    right_value = row[value_col_name]
                elif strip_prefix(value_col_name) in row:
                    right_value = row[strip_prefix(value_col_name)]
                elif value in row:
                    right_value = row[value]
                elif strip_prefix(value) in row:
                    right_value = row[strip_prefix(value)]
                else:
                    continue
            else:
                right_value = value
            
            try:
                left_num = float(left_value)
                right_num = float(right_value)
                
                if operator == "=" and left_num == right_num:
                    filtered_data.append(row)
                elif operator == "!=" and left_num != right_num:
                    filtered_data.append(row)
                elif operator == ">" and left_num > right_num:
                    filtered_data.append(row)
                elif operator == "<" and left_num < right_num:
                    filtered_data.append(row)
                elif operator == ">=" and left_num >= right_num:
                    filtered_data.append(row)
                elif operator == "<=" and left_num <= right_num:
                    filtered_data.append(row)
            except (ValueError, TypeError):
                left_str = str(left_value)
                right_str = str(right_value)
                
                if operator == "=" and left_str == right_str:
                    filtered_data.append(row)
                elif operator == "!=" and left_str != right_str:
                    filtered_data.append(row)
        
        return Rows.from_list(filtered_data)

    # apply join operation - support JOIN, NATURAL_JOIN, and THETA_JOIN
    def _apply_join(self, left_data: Rows, right_data: Rows, condition: str, join_type: str) -> Rows:
        if not left_data.data or not right_data.data:
            return Rows.from_list([])
        
        result = []
        
        if join_type == "JOIN":
            # inner join: hanya me-return baris yang memenuhi kondisi join
            result = self._theta_join(left_data.data, right_data.data, condition)
        
        elif join_type == "NATURAL_JOIN":
            # natural join: join berdasarkan kolom dengan value yang sama
            result = self._natural_join(left_data.data, right_data.data)
        
        elif join_type == "THETA_JOIN":
            # theta join: join berdasarkan kondisi tertentu (=, <, >, <=, >=, !=)
            result = self._theta_join(left_data.data, right_data.data, condition)
        
        return Rows.from_list(result)
    
    # natural join berdasarkan kolom dengan nilai yang sama
    def _natural_join(self, left_rows: list, right_rows: list) -> list:
        result = []
        
        if not left_rows or not right_rows:
            return result
        
        left_first = left_rows[0]
        right_first = right_rows[0]
        
        if not isinstance(left_first, dict) or not isinstance(right_first, dict):
            return result
        
        common_cols = set(left_first.keys()) & set(right_first.keys())
        
        # join rows berdasarkan common columns
        for left_row in left_rows:
            for right_row in right_rows:
                # cek apakah semua common columns memiliki nilai yang sama
                match = all(left_row.get(col) == right_row.get(col) for col in common_cols)
                
                if match:
                    # combine rows, common columns dari left_row
                    combined = {**left_row}
                    for key, val in right_row.items():
                        if key not in common_cols:
                            combined[key] = val
                    result.append(combined)
        
        return result
    
    # theta join berdasarkan kondisi
    def _theta_join(self, left_rows: list, right_rows: list, condition: str) -> list:
        result = []
        
        if not condition:
            # jika tidak ada condition, return cartesian product
            return self._cartesian_join(left_rows, right_rows)
        
        # parse condition: format "left_col op right_col" atau "left_col op value"
        operators = [">=", "<=", "!=", "=", ">", "<"]
        operator = None
        left_col = None
        right_col_or_value = None
        
        for op in operators:
            if op in condition:
                parts = condition.split(op)
                left_col = parts[0].strip()
                right_col_or_value = parts[1].strip().strip("'\"")
                operator = op
                break
        
        if not operator or not left_col:
            return self._cartesian_join(left_rows, right_rows)
        
        # join rows berdasarkan kondisi
        for left_row in left_rows:
            for right_row in right_rows:
                if isinstance(left_row, dict) and isinstance(right_row, dict):
                    if left_col in left_row:
                        left_val = left_row[left_col]
                        
                        # cek apakah right_col_or_value adalah kolom di right_row
                        if right_col_or_value in right_row:
                            right_val = right_row[right_col_or_value]
                        else:
                            right_val = right_col_or_value
                        
                        # evaluasi condition
                        if self._evaluate_condition(left_val, operator, right_val):
                            combined = {**left_row, **right_row}
                            result.append(combined)
        
        return result
    
    # cartesian product untuk join
    def _cartesian_join(self, left_rows: list, right_rows: list) -> list:
        result = []
        for left_row in left_rows:
            for right_row in right_rows:
                if isinstance(left_row, dict) and isinstance(right_row, dict):
                    combined = {**left_row, **right_row}
                    result.append(combined)
        return result
    
    # evaluasi kondisi untuk join
    def _evaluate_condition(self, left_val, operator: str, right_val) -> bool:
        try:
            # coba convert ke float untuk numeric comparison
            left_num = float(left_val)
            right_num = float(right_val)
            
            if operator == "=":
                return left_num == right_num
            elif operator == "!=":
                return left_num != right_num
            elif operator == ">":
                return left_num > right_num
            elif operator == "<":
                return left_num < right_num
            elif operator == ">=":
                return left_num >= right_num
            elif operator == "<=":
                return left_num <= right_num
        except (ValueError, TypeError):
            # string comparison jika tidak bisa convert ke number
            left_str = str(left_val)
            right_str = str(right_val)
            
            if operator == "=":
                return left_str == right_str
            elif operator == "!=":
                return left_str != right_str
            elif operator == ">":
                return left_str > right_str
            elif operator == "<":
                return left_str < right_str
            elif operator == ">=":
                return left_str >= right_str
            elif operator == "<=":
                return left_str <= right_str
        
        return False

    # apply CARTESIAN product
    def _apply_cartesian(self, left_data: Rows, right_data: Rows) -> Rows:
        result = []
        for left_row in left_data.data:
            for right_row in right_data.data:
                if isinstance(left_row, dict) and isinstance(right_row, dict):
                    combined = {**left_row, **right_row}
                    result.append(combined)
        
        return Rows.from_list(result)

    def _apply_sort(self, data: Rows, column: str) -> Rows:

        # column = list of OrderByItem
        for node in reversed(column):
            column_str = node.column.column
            ascending = node.direction.upper() == "ASC"
            data.data.sort(key=lambda datum: datum.get(column_str), reverse=not ascending)

        # NOTE : DONE yak bang -bri
        # NOTE : dah ku benerin yak bang - kiwz
    
        return data

    # apply LIMIT operation
    def _apply_limit(self, data: Rows, limit: str) -> Rows:
        try:
            limit_num = int(limit)
            limited_data = data.data[:limit_num]
            return Rows.from_list(limited_data)
        except ValueError:
            return data
    # apply GROUP BY operation
    def _apply_group(self, data: Rows, column: str) -> Rows:
        return Rows.from_list([{"info": f"GROUP BY {column} - basic implementation"}])

    # perform UPDATE operation via storage manager
    # returns number of rows updated
    def _perform_update(self, table_name: str, update_operations: list, conditions: list) -> int:
        updates = {}

        # NOTE: PENTING, dari SM cuma nerima new_value, ga bisa nilai "kolom * 0.4"
        
        first_condition = conditions[0] if conditions else None # dari spek cuma consider 1 condition aja
        if first_condition.__class__.__name__ != "ConditionNode":
            print("Error: UPDATE only supports one condition")
            return -1
        
        cond_objs = [self._condition_factory(column=first_condition.attr.column, operation=first_condition.op, operand=first_condition.value)] if first_condition else []

        try:
            total_updated = 0
            for update in update_operations[0]:
                data_write = self._data_write_factory(
                    table=repr(table_name),
                    column=update.column,
                    conditions=cond_objs,
                    new_value=update.value,
                )
                
                result = self.storage_manager.write_block(data_write)
                
                if isinstance(result, int):
                    total_updated = max(total_updated, result)
            
            return total_updated
            
        except Exception as e:
            print(f"Error calling Storage Manager write_block: {e}")
            return 0

    # parse condition string to Condition object
    def _parse_condition(self, condition_str: str) -> cond | None:
        operators = [">=", "<=", "!=", "=", ">", "<"]
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op)
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                storage_op = "<>" if op == "!=" else op
                return self._condition_factory(column=col, operation=storage_op, operand=val)
        
        return None

    # parse INSERT (dari optimizer) & panggil write_block dari storage manager
    def execute_insert(self, query: str) -> Union[Rows, int]:
        try:
            parsed = None
            try:
                parsed = self.optimization_engine.parse_query(query)
            except Exception:
                parsed = None

            table_name = None
            cols_list = []
            values_list = []

            if parsed and parsed.query_tree and getattr(parsed.query_tree, "type", "").upper() == "INSERT":
                val = parsed.query_tree.val
                
                # untuk handle InsertData 
                if hasattr(val, "table") and hasattr(val, "columns") and hasattr(val, "values"):
                    table_name = val.table
                    cols_list = list(val.columns) if val.columns else []
                    values_list = list(val.values) if val.values else []
                # Legacy string format: "table|columns|values"
                elif isinstance(val, str):
                    parts = val.split("|", 2)
                    if len(parts) >= 1:
                        table_name = parts[0].strip()
                    if len(parts) >= 2:
                        cols_list = [c.strip() for c in parts[1].split(",")]
                    if len(parts) == 3:
                        values_list = [v.strip().strip("'\"") for v in parts[2].split(",")]
            else:
                return Rows.from_list(["INSERT parsing failed - parsed error"])

            if not table_name:
                return Rows.from_list(["INSERT parsing failed - no table found"])

            # Build row dict
            row_to_insert = {}
            if cols_list and values_list and len(cols_list) == len(values_list):
                row_to_insert = dict(zip(cols_list, values_list))
            elif values_list:
                row_to_insert = {"values": values_list}

            try:
                data_write = self._data_write_factory(
                    table=table_name,
                    column=None,
                    conditions=[],
                    new_value=row_to_insert,
                )
                res = self.storage_manager.write_block(data_write)

                if isinstance(res, int):
                    return Rows.from_list([f"Inserted {res} rows"])
                elif res:
                    return Rows.from_list([f"Inserted rows via storage manager: {res}"])
            except Exception as e:
                print(f"Error calling StorageManager.write_block for insert: {e}")
                return -1

            return Rows.from_list(["INSERT executed - storage manager returned no status"])

        except Exception as e:
            print(f"Error executing INSERT: {e}")
            return -1

    def execute_delete(self, query: str) -> Union[Rows, int]:
        try:
            parsed = None
            try:
                parsed = self.optimization_engine.parse_query(query)
            except Exception:
                parsed = None

            table_name = None
            conditions = []

            if parsed and parsed.query_tree and getattr(parsed.query_tree, "type", "").upper() == "DELETE":
                current = parsed.query_tree
                while current:
                    t = getattr(current, "type", "").upper()
                    if t == "SIGMA":
                        if current.val:
                            conditions.append(current.val)
                    elif t == "TABLE":
                        table_name = current.val
                        break
                    current = current.childs[0] if current.childs else None
            else:
                return Rows.from_list([f"DELETE parsing failed - parsed error"])

            if not table_name:
                return Rows.from_list([f"DELETE parsing failed - no table found"])

            first_condition = conditions[0] if conditions else None # dari spek cuma consider 1 condition aja
            if first_condition.__class__.__name__ != "ConditionNode":
                print("Error: DELETE only supports one condition")
                return -1

            cond_objs = [self._condition_factory(column=first_condition.attr.column, operation=first_condition.op, operand=first_condition.value)] if first_condition else []

            try:
                data_deletion = self._data_write_factory(
                    table=repr(table_name),
                    column="*",
                    conditions=cond_objs,
                    new_value=None,
                )
                res = self.storage_manager.delete_block(data_deletion)
                # NOTE: If delete_block is implemented it should return an int or truthy result, kaya write_block
                if isinstance(res, int):
                    return Rows.from_list([f"Deleted {res} rows"])
                elif res:
                    return Rows.from_list([f"Deleted rows via storage manager: {res}"])
            except Exception as e:
                print(f"Error calling StorageManager.delete_block: {e}")

            return Rows.from_list(["DELETE executed - storage manager returned no status"])

        except Exception as e:
            print(f"Error executing DELETE: {e}")
            return -1

    def execute_create_table(self, query: str) -> Union[Rows, int]:
        """
        Executes a CREATE TABLE query.
        Format: CREATE TABLE table_name (col1 type, col2 type(size))
        """
        # pattern CREATE TABLE <name> (<columns>)
        match = re.search(r"(?i)CREATE\s+TABLE\s+(\w+)\s*\((.+)\)", query)
        if not match:
            return Rows.from_list(["Syntax Error: Invalid CREATE TABLE format."])

        table_name = match.group(1)
        columns_part = match.group(2)

        # existence
        if self.storage_manager.schema_manager.get_table_schema(table_name):
             return Rows.from_list([f"Error: Table '{table_name}' already exists."])

        new_schema = self._schema_factory()
        
        # NOTE: assumes no commas inside the column definition itself
        columns = [c.strip() for c in columns_part.split(',')]

        for col_def in columns:
            # expected formats: "id int" or "name varchar(50)"
            # aga PR si ini
            parts = col_def.split()
            if len(parts) < 2:
                continue # skip invalid definitions
                
            col_name = parts[0]
            raw_type = parts[1]
            
            col_type = raw_type.lower()
            col_size = 0

            # handle Varchar/Char with size (e.g., varchar(50))
            if '(' in raw_type and ')' in raw_type:
                type_match = re.match(r"(\w+)\((\d+)\)", raw_type)
                if type_match:
                    col_type = type_match.group(1).lower()
                    col_size = int(type_match.group(2))
            
            # handle integers (fixed size 4 bytes)
            elif col_type in ['int', 'integer']:
                col_size = 4
            
            # add to schema
            try:
                add_attribute = getattr(new_schema, "add_attribute")
                add_attribute(col_name, col_type, col_size)
            except ValueError as e:
                 return Rows.from_list([f"Error: {str(e)}"])

        # save to Schema Manager
        self.storage_manager.schema_manager.add_table_schema(table_name, new_schema)
        self.storage_manager.schema_manager.save_schemas()

        # physical data file 
        file_path = os.path.join(self.storage_manager.base_path, f"{table_name}.dat")
        try:
            with open(file_path, 'wb') as f:
                pass # Create empty file
        except IOError as e:
            return Rows.from_list([f"Error creating table file: {str(e)}"])

        return Rows.from_list([f"Table '{table_name}' created successfully."])

    def execute_drop_table(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["DROP TABLE - to be implemented (info cara drop tabel dari storagemngr)"])



    # placeholder BEGIN TRANSACTION
    def execute_begin_transaction(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["BEGIN TRANSACTION - to be implemented"])

    # placeholder COMMIT 
    def execute_commit(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["COMMIT - to be implemented"])

    # placeholder rollback
    def execute_rollback(self, query: str) -> Union[Rows, int]:
        # NOTE: di abort ada rollback otomatis?
        return Rows.from_list(["ROLLBACK - to be implemented"])


    # abort transaction - rollback all changes in current transaction
    def execute_abort(self, query: str) -> Union[Rows, int]:
        try:
            # abort the current transaction and rollback all changes
            # this will discard any modifications made within the transaction
            abort_result = self._rollback_transaction()
            
            if abort_result:
                return Rows.from_list(["ABORT completed successfully"])
            else:
                return Rows.from_list(["ABORT failed - no active transaction"])
                
        except Exception as e:
            print(f"Error executing ABORT: {e}")
            return -1
    
    # rollback all changes made in the current transaction
    def _rollback_transaction(self) -> bool:
        try:
            # check if transaction is active
            if not hasattr(self, '_transaction_active') or not self._transaction_active:
                return False
            
            # clear any pending changes
            if hasattr(self, '_transaction_changes'):
                self._transaction_changes.clear()
            
            # mark transaction as inactive
            self._transaction_active = False
            
            print("Transaction rolled back successfully")
            return True
            
        except Exception as e:
            print(f"Error rolling back transaction: {e}")
            return False




