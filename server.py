from __future__ import annotations

import json
import socket
import threading
import sys
from datetime import datetime
from typing import Any, Dict

from bootstrap import load_dependencies
from MiniDBMS import MiniDBMS


class DBMSServer:
    def __init__(self, host: str = "127.0.0.1", port: int = 13523):
        self.host = host
        self.port = port
        self.server_socket = None
        self.dbms = None
        self.deps = None
        self.running = False
        self.client_count = 0
        self.lock = threading.Lock()
        
    def initialize_dbms(self):
        print("[Server] Initializing MiniDBMS...")
        self.deps = load_dependencies()
        self.dbms = MiniDBMS(self.deps)
        print("[Server] MiniDBMS initialized successfully.")
        
    def start(self):
        try:
            self.initialize_dbms()
            
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            
            self.running = True
            print(f"[Server] MiniDBMS Server started on {self.host}:{self.port}")
            print("[Server] Waiting for client connections...")
            
            while self.running:
                try:
                    client_socket, client_address = self.server_socket.accept()
                    with self.lock:
                        self.client_count += 1
                        client_id = self.client_count
                    
                    print(f"[Server] New connection from {client_address} (Client #{client_id})")
                    
                    client_thread = threading.Thread(
                        target=self.handle_client,
                        args=(client_socket, client_address, client_id),
                        daemon=True
                    )
                    client_thread.start()
                    
                except KeyboardInterrupt:
                    print("\n[Server] Shutting down...")
                    break
                except Exception as e:
                    if self.running:
                        print(f"[Server Error] {e}")
                    
        except Exception as e:
            print(f"[Server Fatal Error] {e}")
        finally:
            self.stop()
    
    def handle_client(self, client_socket: socket.socket, client_address: tuple, client_id: int):
        print(f"[Client #{client_id}] Handler started for {client_address}")
        
        try:
            welcome_msg = {
                "type": "welcome",
                "message": f"Connected to MiniDBMS Server (Client #{client_id})",
                "server_time": datetime.now().isoformat()
            }
            self._send_response(client_socket, welcome_msg)
            
            while self.running:
                data = client_socket.recv(4096).decode('utf-8') # ini dulu di jarkom dibanned njir
                
                if not data:
                    print(f"[Client #{client_id}] Disconnected")
                    break
                
                try:
                    request = json.loads(data)
                    print(f"[Client #{client_id}] Received: {request.get('query', request.get('command', 'unknown'))}")
                    
                    response = self.process_request(request, client_id)
                    self._send_response(client_socket, response)
                    
                except json.JSONDecodeError:
                    error_response = {
                        "type": "error",
                        "message": "Invalid JSON format"
                    }
                    self._send_response(client_socket, error_response)
                except Exception as e:
                    error_response = {
                        "type": "error",
                        "message": f"Server error: {str(e)}"
                    }
                    self._send_response(client_socket, error_response)
                    
        except Exception as e:
            print(f"[Client #{client_id} Error] {e}")
        finally:
            client_socket.close()
            print(f"[Client #{client_id}] Connection closed")
    
    def _tree_to_string(self, node, indent="", is_last=True, lines=None):
        """Convert query tree to string representation for display."""
        if lines is None:
            lines = []
        
        if not node:
            return "(empty)"
        
        # Print current node
        prefix = indent + ("└── " if is_last else "├── ")
        node_type = node.type if hasattr(node, 'type') else "UNKNOWN"
        node_val = ""
        
        if hasattr(node, 'val') and node.val is not None:
            val = node.val
            if isinstance(val, list):
                if len(val) <= 3:
                    node_val = f" {val}"
                else:
                    node_val = f" [{len(val)} items]"
            elif isinstance(val, str) and len(val) > 50:
                node_val = f" {val[:47]}..."
            else:
                node_val = f" {val}"
        
        lines.append(f"{prefix}{node_type}{node_val}")
        
        children = None
        if hasattr(node, 'childs') and node.childs:
            children = node.childs
        elif hasattr(node, 'children') and node.children:
            children = node.children
        
        if children:
            for i, child in enumerate(children):
                child_indent = indent + ("    " if is_last else "│   ")
                self._tree_to_string(child, child_indent, i == len(children) - 1, lines)
        
        return "\n".join(lines) if indent == "" else None
    
    def process_request(self, request: Dict[str, Any], client_id: int) -> Dict[str, Any]:
        request_type = request.get("type", "query")
        
        if request_type == "query":
            query = request.get("query", "").strip()
            
            if not query:
                return {
                    "type": "error",
                    "message": "Empty query"
                }
            
            try:
                result = self.dbms.execute(query)
                
                response = self._format_execution_result(result)
                return response
                
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"Query execution failed: {str(e)}"
                }
        
        elif request_type == "checkpoint":
            try:
                success = self.dbms.checkpoint()
                return {
                    "type": "checkpoint",
                    "success": success,
                    "message": "Checkpoint completed" if success else "Checkpoint failed"
                }
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"Checkpoint failed: {str(e)}"
                }
        
        elif request_type == "list_tables":
            try:
                storage_manager = getattr(self.dbms.query_processor, 'storage_manager', None)
                if storage_manager and hasattr(storage_manager, 'schema_manager'):
                    tables = storage_manager.schema_manager.list_tables()
                    return {
                        "type": "list_tables",
                        "tables": sorted(list(tables)) if tables else []
                    }
                else:
                    return {
                        "type": "error",
                        "message": "Storage manager not available"
                    }
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"Failed to list tables: {str(e)}"
                }
        
        elif request_type == "describe_table":
            try:
                table_name = request.get("table_name", "")
                if not table_name:
                    return {
                        "type": "error",
                        "message": "Table name required"
                    }
                
                storage_manager = getattr(self.dbms.query_processor, 'storage_manager', None)
                if storage_manager and hasattr(storage_manager, 'schema_manager'):
                    schema = storage_manager.schema_manager.get_table_schema(table_name)
                    if schema:
                        attributes = []
                        if hasattr(schema, 'attributes'):
                            for attr in schema.attributes:
                                attributes.append({
                                    "name": attr.get('name', 'unknown'),
                                    "type": attr.get('type', 'unknown')
                                })
                        
                        stats = storage_manager.get_stats(table_name)
                        stats_dict = {}
                        if stats:
                            stats_dict = {
                                "n_r": getattr(stats, 'n_r', None),
                                "b_r": getattr(stats, 'b_r', None),
                                "f_r": getattr(stats, 'f_r', None)
                            }
                        
                        return {
                            "type": "describe_table",
                            "table_name": table_name,
                            "attributes": attributes,
                            "stats": stats_dict
                        }
                    else:
                        return {
                            "type": "error",
                            "message": f"Table '{table_name}' not found"
                        }
                else:
                    return {
                        "type": "error",
                        "message": "Storage manager not available"
                    }
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"Failed to describe table: {str(e)}"
                }
        
        elif request_type == "list_transactions":
            try:
                transactions = []
                if hasattr(self.dbms, '_active_transactions') and self.dbms._active_transactions:
                    if hasattr(self.dbms, 'concurrency_manager') and hasattr(self.dbms.concurrency_manager, 'transaction_manager'):
                        tx_manager = self.dbms.concurrency_manager.transaction_manager
                        for tx_id in sorted(self.dbms._active_transactions):
                            tx_info = {"transaction_id": tx_id, "status": "ACTIVE", "start_time": None}
                            if hasattr(tx_manager, 'get_transaction'):
                                tx = tx_manager.get_transaction(tx_id)
                                if tx:
                                    tx_info["status"] = str(tx.status.name if hasattr(tx.status, 'name') else tx.status)
                                    if hasattr(tx, 'start_time'):
                                        tx_info["start_time"] = tx.start_time.isoformat()
                            transactions.append(tx_info)
                    else:
                        for tx_id in sorted(self.dbms._active_transactions):
                            transactions.append({"transaction_id": tx_id, "status": "ACTIVE", "start_time": None})
                
                return {
                    "type": "list_transactions",
                    "transactions": transactions,
                    "count": len(transactions)
                }
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"Failed to list transactions: {str(e)}"
                }
        
        elif request_type == "explain":
            try:
                query = request.get("query", "").strip()
                if not query:
                    return {
                        "type": "error",
                        "message": "Query required for EXPLAIN"
                    }
                
                if not query.endswith(";"):
                    query = query + ";"
                
                if not query.upper().startswith("SELECT"):
                    return {
                        "type": "error",
                        "message": "EXPLAIN only supports SELECT queries"
                    }
                
                optimizer = getattr(self.dbms.query_processor, 'optimization_engine', None)
                if not optimizer:
                    return {
                        "type": "error",
                        "message": "Query optimizer not available"
                    }
                
                parsed = optimizer.parse_query(query)
                cost_before = optimizer.get_cost(parsed) if parsed else 0
                
                # Generate original tree structure
                original_tree = self._tree_to_string(parsed.query_tree if parsed else None)
                
                optimized = optimizer.optimize_query(parsed)
                cost_after = optimizer.get_cost(optimized) if optimized else 0
                
                # Generate optimized tree structure
                optimized_tree = self._tree_to_string(optimized.query_tree if optimized else None)
                
                improvement = 0
                if cost_before > 0 and cost_after > 0:
                    improvement = ((cost_before - cost_after) / cost_before) * 100
                
                optimization_info = {}
                if hasattr(optimizer, 'last_optimization_info') and optimizer.last_optimization_info:
                    # Convert optimization info to JSON-serializable format
                    raw_info = optimizer.last_optimization_info
                    optimization_info = {}
                    for key, value in raw_info.items():
                        if key == 'tables' and isinstance(value, list):
                            # Convert TableReference objects to strings
                            optimization_info[key] = [str(t) if hasattr(t, '__str__') else repr(t) for t in value]
                        elif hasattr(value, '__dict__'):
                            # Convert objects to string representation
                            optimization_info[key] = str(value)
                        else:
                            optimization_info[key] = value
                
                return {
                    "type": "explain",
                    "query": query.rstrip(";"),
                    "cost_before": cost_before,
                    "cost_after": cost_after,
                    "improvement_percent": improvement,
                    "optimization_info": optimization_info,
                    "original_tree": original_tree,
                    "optimized_tree": optimized_tree
                }
            except Exception as e:
                return {
                    "type": "error",
                    "message": f"EXPLAIN failed: {str(e)}"
                }
        
        elif request_type == "ping":
            return {
                "type": "pong",
                "server_time": datetime.now().isoformat()
            }
        
        elif request_type == "disconnect":
            return {
                "type": "disconnect",
                "message": "Goodbye!"
            }
        
        else:
            return {
                "type": "error",
                "message": f"Unknown request type: {request_type}"
            }
    
    def _format_execution_result(self, result: Any) -> Dict[str, Any]:
        response = {
            "type": "result",
            "transaction_id": getattr(result, "transaction_id", -1),
            "message": getattr(result, "message", ""),
            "timestamp": getattr(result, "timestamp", datetime.now()).isoformat(),
            "query": getattr(result, "query", ""),
        }
        
        # Handle data attribute
        data = getattr(result, "data", None)
        
        if data is None:
            response["data"] = None
        elif data == -1:
            response["data"] = -1
            response["error"] = True
        elif isinstance(data, self.deps.rows_cls):
            rows_list = []
            columns = []
            
            if hasattr(data, "columns") and data.columns:
                columns = list(data.columns)
            
            if hasattr(data, "data"):
                for row in data.data:
                    if isinstance(row, dict):
                        rows_list.append(row)
                    elif isinstance(row, (list, tuple)):
                        if columns:
                            rows_list.append(dict(zip(columns, row)))
                        else:
                            rows_list.append({"value": row})
                    else:
                        rows_list.append({"value": str(row)})
            
            response["data"] = {
                "rows": rows_list,
                "columns": columns,
                "row_count": len(rows_list)
            }
        else:
            try:
                json.dumps(data)
                response["data"] = data
            except (TypeError, ValueError):
                response["data"] = str(data)
        
        return response
    
    def _send_response(self, client_socket: socket.socket, response: Dict[str, Any]):
        try:
            response_json = json.dumps(response) + "\n"
            client_socket.sendall(response_json.encode('utf-8'))
        except Exception as e:
            print(f"[Server] Failed to send response: {e}")
    
    def stop(self):
        self.running = False
        if self.server_socket:
            self.server_socket.close()
        print("[Server] Server stopped")


def main():
    host = "127.0.0.1"
    port = 13523

    if len(sys.argv) > 1:
        port = int(sys.argv[1])
    if len(sys.argv) > 2:
        host = sys.argv[2]

    server = DBMSServer(host=host, port=port)

    try:
        server.start()
    except KeyboardInterrupt:
        print("\n[Server] Shutting down...")
    finally:
        server.stop()
if __name__ == "__main__":
    main()