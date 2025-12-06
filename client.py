from __future__ import annotations

import json
import socket
import sys
from typing import Any, Dict, Optional


class DBMSClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 13523):
        self.host = host
        self.port = port
        self.socket: Optional[socket.socket] = None
        self.connected = False
        
    def connect(self) -> bool:
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.connected = True
            
            response = self._receive_response()
            if response and response.get("type") == "welcome":
                print(f"[Client] {response.get('message', 'Connected to server')}")
                return True
            else:
                print("[Client] Connected but no welcome message received")
                return True
                
        except ConnectionRefusedError:
            print(f"[Client Error] Could not connect to server at {self.host}:{self.port}")
            print("[Client Error] Make sure the server is running")
            return False
        except Exception as e:
            print(f"[Client Error] Connection failed: {e}")
            return False
    
    def disconnect(self):
        if self.connected and self.socket:
            try:
                request = {"type": "disconnect"}
                self._send_request(request)
            except:
                pass
            finally:
                self.socket.close()
                self.connected = False
                print("[Client] Disconnected from server")
    
    def execute_query(self, query: str) -> Optional[Dict[str, Any]]:
        if not self.connected:
            print("[Client Error] Not connected to server")
            return None
        
        try:
            request = {
                "type": "query",
                "query": query
            }
            
            self._send_request(request)
            response = self._receive_response()
            
            return response
            
        except Exception as e:
            print(f"[Client Error] Query execution failed: {e}")
            return None
    
    def checkpoint(self) -> Optional[Dict[str, Any]]:
        if not self.connected:
            print("[Client Error] Not connected to server")
            return None
        
        try:
            request = {"type": "checkpoint"}
            self._send_request(request)
            response = self._receive_response()
            return response
        except Exception as e:
            print(f"[Client Error] Checkpoint failed: {e}")
            return None
    
    def ping(self) -> bool:
        if not self.connected:
            return False
        
        try:
            request = {"type": "ping"}
            self._send_request(request)
            response = self._receive_response()
            return response is not None and response.get("type") == "pong"
        except:
            return False
    
    def _send_request(self, request: Dict[str, Any]):
        if not self.socket:
            raise Exception("Not connected")
        
        request_json = json.dumps(request) + "\n"
        self.socket.sendall(request_json.encode('utf-8'))
    
    def _receive_response(self) -> Optional[Dict[str, Any]]:
        if not self.socket:
            return None
        
        try:
            buffer = ""
            while True:
                chunk = self.socket.recv(4096).decode('utf-8')
                if not chunk:
                    break
                buffer += chunk
                if '\n' in buffer:
                    break
            
            if buffer:
                return json.loads(buffer.strip())
            return None
            
        except json.JSONDecodeError as e:
            print(f"[Client Error] Invalid JSON response: {e}")
            return None
        except Exception as e:
            print(f"[Client Error] Failed to receive response: {e}")
            return None


def print_result(response: Optional[Dict[str, Any]]):
    if not response:
        print("  [No response from server]")
        return
    
    response_type = response.get("type", "unknown")
    
    if response_type == "error":
        print(f"  [Error] {response.get('message', 'Unknown error')}")
        return
    
    if response_type == "checkpoint":
        success = response.get("success", False)
        message = response.get("message", "")
        print(f"  [Checkpoint] {message} - {'Success' if success else 'Failed'}")
        return
    
    if response_type == "result":
        transaction_id = response.get("transaction_id", -1)
        message = response.get("message", "")
        data = response.get("data")
        
        if response.get("error") or data == -1:
            print(f"  [Transaction {transaction_id}] Error: {message}")
            return
        
        print(f"  [Transaction {transaction_id}] {message}")
        
        # Print data
        if data is None:
            print("  Data: None")
        elif isinstance(data, dict) and "rows" in data:
            rows = data.get("rows", [])
            columns = data.get("columns", [])
            
            if not rows:
                print("  Rows: []")
            else:
                # Print as table
                if columns:
                    headers = columns
                elif rows:
                    headers = list(rows[0].keys())
                else:
                    headers = ["value"]
                
                widths = []
                for header in headers:
                    col_values = [str(row.get(header, "")) for row in rows]
                    width = max(len(str(header)), max((len(v) for v in col_values), default=0))
                    widths.append(width)
                
                # Print table
                header_fmt = "  | " + " | ".join(f"{{:{w}}}" for w in widths) + " |"
                sep = "  +-" + "-+-".join("-" * w for w in widths) + "-+"
                
                print(sep)
                print(header_fmt.format(*headers))
                print(sep)
                for row in rows:
                    values = [str(row.get(h, "")) for h in headers]
                    print(header_fmt.format(*values))
                print(sep)
                print(f"  ({len(rows)} row{'s' if len(rows) != 1 else ''})")
        else:
            print(f"  Data: {data}")


def run_interactive(client: DBMSClient):
    print("\n=== MiniDBMS Client Interactive Shell ===")
    print("Commands:")
    print("  - Type SQL queries ending with semicolon (;)")
    print("  - \\checkpoint  - Force checkpoint on server")
    print("  - \\ping        - Check server connection")
    print("  - exit or quit - Disconnect and exit")
    print()
    
    buffer = []
    
    while True:
        prompt = "SQL> " if not buffer else "...  "
        
        try:
            line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print()
            if buffer:
                buffer.clear()
                print("[Info] Cleared pending statement")
            continue
        
        stripped = line.strip()
        
        # Empty line
        if not buffer and not stripped:
            continue
        
        # Exit command
        if not buffer and stripped.lower() in {"exit", "quit"}:
            break
        
        # Special commands
        if not buffer and stripped == "\\checkpoint":
            response = client.checkpoint()
            print_result(response)
            continue
        
        if not buffer and stripped == "\\ping":
            if client.ping():
                print("  [Ping] Server is responding")
            else:
                print("  [Ping] Server is not responding")
            continue
        
        # Build query buffer
        buffer.append(line)
        statement = "\n".join(buffer).strip()
        
        # Execute when semicolon is found
        if statement.endswith(";"):
            buffer.clear()
            response = client.execute_query(statement)
            print_result(response)


def run_batch(client: DBMSClient, queries: list[str]):
    print("\n=== MiniDBMS Client Batch Mode ===")
    
    for i, query in enumerate(queries, 1):
        print(f"\n[Query {i}] {query}")
        response = client.execute_query(query)
        print_result(response)


def main():
    host = "127.0.0.1"
    port = 13523
    
    args = sys.argv[1:]
    
    if "--help" in args or "-h" in args:
        print("format: python client.py [OPTIONS]")
        print("\nOptions:")
        print("  --host HOST       Server host (default: 127.0.0.1)")
        print("  --port PORT       Server port (default: 13523)")
        print("  --interactive     default, mode interaktif")
        print("  --batch QUERY     Run satu query")
        print("  --help, -h        ya pesan ini")
        return
    
    # host & port
    for i, arg in enumerate(args):
        if arg == "--host" and i + 1 < len(args):
            host = args[i + 1]
        elif arg == "--port" and i + 1 < len(args):
            port = int(args[i + 1])
    
    # Create client and connect
    client = DBMSClient(host=host, port=port)
    
    if not client.connect():
        return
    
    try:
        # mode
        if "--batch" in args:
            batch_idx = args.index("--batch")
            if batch_idx + 1 < len(args):
                query = args[batch_idx + 1]
                if not query.endswith(";"):
                    query += ";"
                run_batch(client, [query])
        elif "--interactive" in args or len(args) == 0 or all(arg.startswith("--") for arg in args):
            run_interactive(client)
        else:
            # kweri demo
            demo_queries = [
                "BEGIN TRANSACTION;",
                "SELECT * FROM Student;",
                "UPDATE Student SET GPA = 3.9 WHERE StudentID = 1;",
                "SELECT StudentID, FullName, GPA FROM Student WHERE GPA > 3.0;",
                "COMMIT;",
            ]
            run_batch(client, demo_queries)
            
    except KeyboardInterrupt:
        print("\n[Client] Interrupted by user")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
