from typing import Union

from query_processor.model.Rows import Rows
from storage_manager.StorageManager import StorageManager
from storage_manager.model.data_retrieval import DataRetrieval
from storage_manager.model.data_write import DataWrite
from storage_manager.model.condition import Condition

from query_optimizer.QueryOptimizer import OptimizationEngine
from query_optimizer.model.query_tree import QueryTree

class QueryExecutor:
    def __init__(self) -> None:
        self.storage_manager = StorageManager()
        self.optimization_engine = OptimizationEngine()

    # execute SELECT query:
    # 1. parse query menggunakan query optimizer
    # 2. optimize query tree
    # 3. execute query tree dan retrieve data dari storage manager
    def execute_select(self, query: str) -> Union[Rows, int]:
        try:
            parsed_query = self.optimization_engine.parse_query(query)
            optimized_query = self.optimization_engine.optimize_query(parsed_query)
            optimized_query = self.optimization_engine.optimize_query_non_join(optimized_query)
            result_data = self._execute_query_tree(optimized_query.query_tree)
            
            return result_data
            
        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            return -1

    # execute UPDATE query:
    # 1. parse query menggunakan query optimizer
    # 2. extract table, columns, and conditions dari query tree
    # 3. execute update via storage manager
    def execute_update(self, query: str) -> Union[Rows, int]:
        try:
            parsed_query = self.optimization_engine.parse_query(query)
            result = self._execute_update_tree(parsed_query.query_tree)
            
            return Rows.from_list([f"Updated {result} rows"])
            
        except Exception as e:
            print(f"Error executing UPDATE query: {e}")
            return -1

    # recursively execute query tree untuk SELECT operations
    # traverse dari root ke leaf (TABLE node) dan apply operations saat return
    def _execute_query_tree(self, node: QueryTree) -> Rows:
        if node is None:
            return Rows.from_list([])
        
        if node.type == "TABLE":
            return self._fetch_table_data(node.val)
        
        child_results = []
        for child in node.childs:
            child_result = self._execute_query_tree(child)
            child_results.append(child_result)
        
        if node.type == "PROJECT":
            return self._apply_projection(child_results[0], node.val)
        
        elif node.type == "SIGMA":
            return self._apply_selection(child_results[0], node.val)
        
        elif node.type == "JOIN" or node.type == "NATURAL_JOIN" or node.type == "THETA_JOIN":
            return self._apply_join(child_results[0], child_results[1], node.val, node.type)
        
        elif node.type == "CARTESIAN":
            return self._apply_cartesian(child_results[0], child_results[1])
        
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
    def _execute_update_tree(self, node: QueryTree) -> int:
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

    # fetch all data dari table menggunakan storage manager
    # will use dummy data if storage manager not implemented yet
    def _fetch_table_data(self, table_name: str) -> Rows:
        storage_ready = hasattr(self.storage_manager, 'read_block') and \
                       callable(getattr(self.storage_manager, 'read_block'))
        
        if storage_ready and self._is_storage_manager_read_implemented():
            try:
                data_retrieval = DataRetrieval(table=table_name, column="*", conditions=[])
                result = self.storage_manager.read_block(data_retrieval)
                
                if result is not None and isinstance(result, list):
                    print(f"✓ Data fetched from Storage Manager: {table_name}")
                    return Rows.from_list(result)
                else:
                    return self._dummy_table_data(table_name)
                    
            except Exception as e:
                print(f"Error calling Storage Manager read_block: {e}")
                return self._dummy_table_data(table_name)
        else:
            return self._dummy_table_data(table_name)
    
    # check if storage manager read_block sudah diimplementasikan
    def _is_storage_manager_read_implemented(self) -> bool:
        try:
            test_retrieval = DataRetrieval(table="test", column="*", conditions=[])
            result = self.storage_manager.read_block(test_retrieval)
            return result is not None
        except:
            return False
    
    # return dummy data untuk testing ketika storage manager belum ready
    def _dummy_table_data(self, table_name: str) -> Rows:
        print(f"Using dummy data (Storage Manager not ready): {table_name}")
        
        dummy_data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "Jakarta"},
            {"id": 2, "name": "Bob", "age": 30, "city": "Bandung"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Jakarta"},
            {"id": 4, "name": "David", "age": 28, "city": "Surabaya"},
        ]
        
        return Rows.from_list(dummy_data)

    # apply PROJECT operation - select specific columns
    def _apply_projection(self, data: Rows, columns: str) -> Rows:
        if columns == "*":
            return data
        
        col_list = [col.strip() for col in columns.split(",")]
        
        projected_data = []
        for row in data.data:
            if isinstance(row, dict):
                projected_row = {col: row.get(col) for col in col_list if col in row}
                projected_data.append(projected_row)
            else:
                projected_data.append(row)
        
        return Rows.from_list(projected_data)

    # apply SIGMA operation - filter rows based on WHERE condition
    def _apply_selection(self, data: Rows, condition: str) -> Rows:
        filtered_data = []
        
        operators = [">=", "<=", "!=", "=", ">", "<"]
        operator = None
        col_name = None
        value = None
        
        for op in operators:
            if op in condition:
                parts = condition.split(op)
                col_name = parts[0].strip()
                value = parts[1].strip().strip("'\"")
                operator = op
                break
        
        if not operator:
            return data
        
        for row in data.data:
            if isinstance(row, dict) and col_name in row:
                row_value = str(row[col_name])
                
                try:
                    row_value_num = float(row_value)
                    value_num = float(value)
                    
                    if operator == "=" and row_value_num == value_num:
                        filtered_data.append(row)
                    elif operator == "!=" and row_value_num != value_num:
                        filtered_data.append(row)
                    elif operator == ">" and row_value_num > value_num:
                        filtered_data.append(row)
                    elif operator == "<" and row_value_num < value_num:
                        filtered_data.append(row)
                    elif operator == ">=" and row_value_num >= value_num:
                        filtered_data.append(row)
                    elif operator == "<=" and row_value_num <= value_num:
                        filtered_data.append(row)
                except ValueError:
                    if operator == "=" and row_value == value:
                        filtered_data.append(row)
                    elif operator == "!=" and row_value != value:
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

    # placeholder untuk SORT operation (ORDER BY)
    def _apply_sort(self, data: Rows, column: str) -> Rows:
        return Rows.from_list([{"info": "SORT operation - to be implemented"}])

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
        for op in update_operations:
            if "=" in op:
                parts = op.split("=")
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                updates[col] = val
        
        storage_ready = hasattr(self.storage_manager, 'write_block') and \
                       callable(getattr(self.storage_manager, 'write_block'))
        
        if storage_ready and self._is_storage_manager_implemented():
            try:
                total_updated = 0
                for col, val in updates.items():
                    cond_objects = [self._parse_condition(c) for c in conditions] if conditions else []
                    
                    data_write = DataWrite(
                        table=table_name, 
                        column=col, 
                        conditions=cond_objects, 
                        new_value=val
                    )
                    
                    result = self.storage_manager.write_block(data_write)
                    
                    if isinstance(result, int):
                        total_updated = max(total_updated, result)
                
                print(f"✓ UPDATE executed via Storage Manager: {table_name} SET {updates} WHERE {conditions}")
                return total_updated
                
            except Exception as e:
                print(f"Error calling Storage Manager write_block: {e}")
                return self._dummy_update(table_name, updates, conditions)
        else:
            return self._dummy_update(table_name, updates, conditions)
    
    # check if storage manager sudah diimplementasikan atau masih stub
    def _is_storage_manager_implemented(self) -> bool:
        try:
            test_write = DataWrite(table="test", column="test", conditions=[], new_value="test")
            result = self.storage_manager.write_block(test_write)
            return result is not None
        except:
            return False
    
    # dummy UPDATE implementation untuk testing ketika storage manager belum ready
    def _dummy_update(self, table_name: str, updates: dict, conditions: list) -> int:
        print(f"Using dummy UPDATE (Storage Manager not ready): {table_name} SET {updates} WHERE {conditions}")
        
        if conditions:
            return len(conditions)
        else:
            return 1

    # parse condition string to Condition object
    def _parse_condition(self, condition_str: str) -> Condition:
        operators = [">=", "<=", "!=", "=", ">", "<"]
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op)
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                storage_op = "<>" if op == "!=" else op
                return Condition(column=col, operation=storage_op, operand=val)
        
        return None

    # placeholder BEGIN TRANSACTION
    def execute_begin_transaction(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["BEGIN TRANSACTION - to be implemented"])

    # placeholder COMMIT 
    def execute_commit(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["COMMIT - to be implemented"])

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