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
