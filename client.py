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

