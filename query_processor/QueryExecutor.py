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

    # placeholder untuk JOIN operation
    def _apply_join(self, left_data: Rows, right_data: Rows, condition: str, join_type: str) -> Rows:
        return Rows.from_list([{"info": "JOIN operation - to be implemented"}])

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

    # placeholder ABORT
    def execute_abort(self, query: str) -> Union[Rows, int]:
        return Rows.from_list(["ABORT - to be implemented"]) 

    def execute_select(self, query: str) -> Union[Rows, int]:
        """
        Execute SELECT query:
        1. Parse query menggunakan Query Optimizer
        2. Optimize query tree
        3. Execute query tree dan retrieve data dari Storage Manager
        """
        try:
            # Step 1: Parse query to get query tree
            parsed_query = self.optimization_engine.parse_query(query)
            
            # Step 2: Optimize query (both join and non-join optimizations)
            optimized_query = self.optimization_engine.optimize_query(parsed_query)
            optimized_query = self.optimization_engine.optimize_query_non_join(optimized_query)
            
            # Step 3: Execute the optimized query tree
            result_data = self._execute_query_tree(optimized_query.query_tree)
            
            return result_data
            
        except Exception as e:
            print(f"Error executing SELECT query: {e}")
            return -1

    def execute_update(self, query: str) -> Union[Rows, int]:
        """
        Execute UPDATE query:
        1. Parse query menggunakan Query Optimizer
        2. Extract table, columns, and conditions dari query tree
        3. Execute update via Storage Manager
        """
        try:
            # Step 1: Parse query to get query tree
            parsed_query = self.optimization_engine.parse_query(query)
            
            # Step 2: Execute UPDATE operation
            result = self._execute_update_tree(parsed_query.query_tree)
            
            # Return jumlah rows yang ter-update
            return Rows.from_list([f"Updated {result} rows"])
            
        except Exception as e:
            print(f"Error executing UPDATE query: {e}")
            return -1

    def _execute_query_tree(self, node: QueryTree) -> Rows:
        """
        Recursively execute query tree untuk SELECT operations
        Traverse dari root ke leaf (TABLE node) dan apply operations saat return
        """
        if node is None:
            return Rows.from_list([])
        
        # Base case: TABLE node - retrieve data dari storage
        if node.type == "TABLE":
            return self._fetch_table_data(node.val)
        
        # Recursive case: Process children first
        child_results = []
        for child in node.childs:
            child_result = self._execute_query_tree(child)
            child_results.append(child_result)
        
        # Apply operation based on node type
        if node.type == "PROJECT":
            # Apply projection (select specific columns)
            return self._apply_projection(child_results[0], node.val)
        
        elif node.type == "SIGMA":
            # Apply selection (WHERE condition)
            return self._apply_selection(child_results[0], node.val)
        
        elif node.type == "JOIN" or node.type == "NATURAL_JOIN" or node.type == "THETA_JOIN":
            # Apply join operation
            return self._apply_join(child_results[0], child_results[1], node.val, node.type)
        
        elif node.type == "CARTESIAN":
            # Apply cartesian product
            return self._apply_cartesian(child_results[0], child_results[1])
        
        elif node.type == "SORT":
            # Apply ORDER BY
            return self._apply_sort(child_results[0], node.val)
        
        elif node.type == "LIMIT":
            # Apply LIMIT
            return self._apply_limit(child_results[0], node.val)
        
        elif node.type == "GROUP":
            # Apply GROUP BY
            return self._apply_group(child_results[0], node.val)
        
        elif node.type == "OR":
            # Handle OR conditions - combine results from multiple SIGMA nodes
            combined_data = []
            for result in child_results:
                combined_data.extend(result.data)
            # Remove duplicates
            unique_data = list(set(combined_data))
            return Rows.from_list(unique_data)
        
        else:
            # Default: return first child result
            return child_results[0] if child_results else Rows.from_list([])

    def _execute_update_tree(self, node: QueryTree) -> int:
        """
        Execute UPDATE query tree
        Returns jumlah rows yang ter-update
        """
        if node is None:
            return 0
        
        # Collect UPDATE operations
        update_operations = []
        conditions = []
        table_name = None
        
        # Traverse tree to collect information
        current = node
        while current:
            if current.type == "UPDATE":
                update_operations.append(current.val)
            elif current.type == "SIGMA":
                conditions.append(current.val)
            elif current.type == "TABLE":
                table_name = current.val
                break
            
            # Move to first child
            current = current.childs[0] if current.childs else None
        
        # Execute UPDATE via Storage Manager
        if table_name and update_operations:
            return self._perform_update(table_name, update_operations, conditions)
        
        return 0

    def _fetch_table_data(self, table_name: str) -> Rows:
        """
        Fetch all data dari table menggunakan Storage Manager
        Will use dummy data if Storage Manager not implemented yet
        """
        # Check if Storage Manager is ready
        storage_ready = hasattr(self.storage_manager, 'read_block') and \
                       callable(getattr(self.storage_manager, 'read_block'))
        
        if storage_ready and self._is_storage_manager_read_implemented():
            # Full integration dengan Storage Manager
            try:
                # Fetch all columns from table
                data_retrieval = DataRetrieval(table=table_name, column="*", conditions=[])
                result = self.storage_manager.read_block(data_retrieval)
                
                # Convert result to Rows format
                if result is not None and isinstance(result, list):
                    print(f"✓ Data fetched from Storage Manager: {table_name}")
                    return Rows.from_list(result)
                else:
                    # Fallback to dummy data
                    return self._dummy_table_data(table_name)
                    
            except Exception as e:
                print(f"Error calling Storage Manager read_block: {e}")
                return self._dummy_table_data(table_name)
        else:
            # Storage Manager belum diimplementasikan - use dummy data
            return self._dummy_table_data(table_name)
    
    def _is_storage_manager_read_implemented(self) -> bool:
        """
        Check if Storage Manager read_block sudah diimplementasikan
        """
        try:
            # Try to call read_block with a test retrieval
            test_retrieval = DataRetrieval(table="test", column="*", conditions=[])
            result = self.storage_manager.read_block(test_retrieval)
            # If it returns None, it's probably just a stub
            return result is not None
        except:
            return False
    
    def _dummy_table_data(self, table_name: str) -> Rows:
        """
        Return dummy data untuk testing ketika Storage Manager belum ready
        """
        print(f"Using dummy data (Storage Manager not ready): {table_name}")
        
        # Dummy data untuk testing (13 Nov milestone)
        dummy_data = [
            {"id": 1, "name": "Alice", "age": 25, "city": "Jakarta"},
            {"id": 2, "name": "Bob", "age": 30, "city": "Bandung"},
            {"id": 3, "name": "Charlie", "age": 35, "city": "Jakarta"},
            {"id": 4, "name": "David", "age": 28, "city": "Surabaya"},
        ]
        
        return Rows.from_list(dummy_data)

    def _apply_projection(self, data: Rows, columns: str) -> Rows:
        """
        Apply PROJECT operation - select specific columns
        """
        if columns == "*":
            return data
        
        # Parse column names
        col_list = [col.strip() for col in columns.split(",")]
        
        # Project only selected columns
        projected_data = []
        for row in data.data:
            if isinstance(row, dict):
                projected_row = {col: row.get(col) for col in col_list if col in row}
                projected_data.append(projected_row)
            else:
                projected_data.append(row)
        
        return Rows.from_list(projected_data)

    def _apply_selection(self, data: Rows, condition: str) -> Rows:
        """
        Apply SIGMA operation - filter rows based on WHERE condition
        Simple implementation for basic conditions like: column = value, column > value, etc.
        """
        filtered_data = []
        
        # Parse condition (simple parser for now)
        # Support: =, !=, <, >, <=, >=
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
        
        # Filter data
        for row in data.data:
            if isinstance(row, dict) and col_name in row:
                row_value = str(row[col_name])
                
                # Try to convert to number for comparison
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
                    # String comparison
                    if operator == "=" and row_value == value:
                        filtered_data.append(row)
                    elif operator == "!=" and row_value != value:
                        filtered_data.append(row)
        
        return Rows.from_list(filtered_data)

    def _apply_join(self, left_data: Rows, right_data: Rows, condition: str, join_type: str) -> Rows:
        """
        Apply JOIN operation
        """
        # Placeholder implementation
        return Rows.from_list([{"info": "JOIN operation - to be implemented"}])

    def _apply_cartesian(self, left_data: Rows, right_data: Rows) -> Rows:
        """
        Apply CARTESIAN product
        """
        result = []
        for left_row in left_data.data:
            for right_row in right_data.data:
                if isinstance(left_row, dict) and isinstance(right_row, dict):
                    combined = {**left_row, **right_row}
                    result.append(combined)
        
        return Rows.from_list(result)

    def _apply_sort(self, data: Rows, column: str) -> Rows:
        """
        Apply SORT operation (ORDER BY)
        """
        # Placeholder implementation
        return Rows.from_list([{"info": "SORT operation"}])

    def _apply_limit(self, data: Rows, limit: str) -> Rows:
        """
        Apply LIMIT operation
        """
        try:
            limit_num = int(limit)
            limited_data = data.data[:limit_num]
            return Rows.from_list(limited_data)
        except ValueError:
            return data

    def _apply_group(self, data: Rows, column: str) -> Rows:
        """
        Apply GROUP BY operation
        Simple implementation
        """
        # Placeholder implementation
        return Rows.from_list([{"info": f"GROUP BY {column} - basic implementation"}])

    def _perform_update(self, table_name: str, update_operations: list, conditions: list) -> int:
        """
        Perform UPDATE operation via Storage Manager
        Returns number of rows updated
        """
        # Parse update operations (format: "column = value")
        updates = {}
        for op in update_operations:
            if "=" in op:
                parts = op.split("=")
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                updates[col] = val
        
        # check if Storage Manager is ready
        storage_ready = hasattr(self.storage_manager, 'write_block') and \
                       callable(getattr(self.storage_manager, 'write_block'))
        
        if storage_ready and self._is_storage_manager_implemented():
            # full integration dengan Storage Manager
            try:
                total_updated = 0
                for col, val in updates.items():
                    # parse conditions menjadi Condition objects
                    cond_objects = [self._parse_condition(c) for c in conditions] if conditions else []
                    
                    # create DataWrite object
                    data_write = DataWrite(
                        table=table_name, 
                        column=col, 
                        conditions=cond_objects, 
                        new_value=val
                    )
                    
                    # execute write via Storage Manager
                    result = self.storage_manager.write_block(data_write)
                    
                    # assuming write_block returns number of affected rows
                    if isinstance(result, int):
                        total_updated = max(total_updated, result)
                
                print(f"✓ UPDATE executed via Storage Manager: {table_name} SET {updates} WHERE {conditions}")
                return total_updated
                
            except Exception as e:
                print(f"Error calling Storage Manager write_block: {e}")
                # Fallback to dummy implementation
                return self._dummy_update(table_name, updates, conditions)
        else:
            # Storage Manager belum diimplementasikan - use dummy data
            return self._dummy_update(table_name, updates, conditions)
    
    def _is_storage_manager_implemented(self) -> bool:
        """
        Check if Storage Manager sudah diimplementasikan atau masih stub
        """
        try:
            # Try to call a simple method to see if it's implemented
            # If write_block just has 'pass', it will return None
            test_write = DataWrite(table="test", column="test", conditions=[], new_value="test")
            result = self.storage_manager.write_block(test_write)
            # If it returns None, it's probably just a stub
            return result is not None
        except:
            return False
    
    def _dummy_update(self, table_name: str, updates: dict, conditions: list) -> int:
        """
        Dummy UPDATE implementation untuk testing ketika Storage Manager belum ready
        """
        print(f"Using dummy UPDATE (Storage Manager not ready): {table_name} SET {updates} WHERE {conditions}")
        
        # Simulate counting affected rows berdasarkan conditions
        if conditions:
            # Estimate based on number of conditions
            return len(conditions)
        else:
            # No WHERE clause - would update all rows
            # Return a dummy count
            return 1

    def _parse_condition(self, condition_str: str) -> Condition:
        """
        Parse condition string to Condition object
        Format: "column operator value"
        """
        # Simple parser untuk condition
        # Map != to <> for storage manager
        operators = [">=", "<=", "!=", "=", ">", "<"]
        for op in operators:
            if op in condition_str:
                parts = condition_str.split(op)
                col = parts[0].strip()
                val = parts[1].strip().strip("'\"")
                # Convert != to <> untuk storage manager
                storage_op = "<>" if op == "!=" else op
                return Condition(column=col, operation=storage_op, operand=val)
        
        return None

    def execute_begin_transaction(self, query: str) -> Union[Rows, int]:
        """
        Execute BEGIN TRANSACTION
        """
        return Rows.from_list(["BEGIN TRANSACTION - to be implemented"])

    def execute_commit(self, query: str) -> Union[Rows, int]:
        """
        Execute COMMIT
        """
        return Rows.from_list(["COMMIT - to be implemented"])

    def execute_abort(self, query: str) -> Union[Rows, int]:
        """
        Execute ABORT
        """
        return Rows.from_list(["ABORT - to be implemented"])
