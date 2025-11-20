# server.py - main Query Processor Server
# orchestrates query routing ke komponen yang tepat

import socket
import threading, json
from query_processor.QueryProcessor import QueryProcessor
from query_processor.model.ExecutionResult import ExecutionResult
from storage_manager.storagemanager_helper.data_encoder import DataEncoder

class QueryProcessorServer:
    
    def __init__(self):
        """Initialize server dengan QueryProcessor sebagai model utama"""
        self.query_processor = QueryProcessor()
    
    def execute_query(self, query: str) -> ExecutionResult:
        """
        Main entry point untuk execute query.
        Delegate ke komponen yang sesuai berdasarkan query type.
        """
        query_stripped = query.strip()
        query_upper = query_stripped.upper()
        
        try:
            # delegate all queries (data + transaction control) to QueryProcessor
            return self.query_processor.execute_query(query_stripped)
        
        except Exception as e:
            print(f"[Server Error] {str(e)}")
            return ExecutionResult(
                transaction_id=None,
                timestamp=None,
                message=f"Error: {str(e)}",
                data=-1,
                query=query_stripped
            )
    
    # transaction lifecycle is handled by QueryProcessor / QueryExecutor
    
    def get_current_transaction(self):
        """Server does not track current transaction; query processor/executor manage transactions."""
        return None
    
    def shutdown(self):
        """Graceful shutdown - server cleanup"""
        print("[Shutdown] Server shutdown complete")


class CLIInterface:
    """
    CLI interface untuk interact dengan QueryProcessorServer.
    TODO: Implement by team member responsible for UI/CLI
    
    Harus provide:
    - display_banner() - tampilkan welcome message
    - display_result() - tampilkan hasil query
    - display_status() - tampilkan status server
    - get_prompt() - return formatted prompt
    - run() - main CLI loop
    """
    
    def __init__(self, server: QueryProcessorServer):
        """Initialize CLI dengan server instance"""
        self.server = server
        raise NotImplementedError("CLI Interface belum diimplementasikan - assign ke team member")
    
    def display_banner(self):
        """Display server banner dan available commands"""
        raise NotImplementedError("To be implemented")
    
    def display_result(self, result: ExecutionResult):
        """Display query result in formatted way"""
        raise NotImplementedError("To be implemented")
    
    def display_status(self):
        """Display current server status"""
        raise NotImplementedError("To be implemented")
    
    def get_prompt(self) -> str:
        """Get formatted input prompt"""
        raise NotImplementedError("To be implemented")
    
    def handle_client(self, client_socket : socket.socket, client_address):
        """
        The function executed by the server upon a client connection.
        It handles sending and receiving data for that specific client.
        """
        print(f"Connection accepted from: {client_address}")

        try:
           while True:
                query_bytes = client_socket.recv(4096) 
                if not query_bytes:
                    break

                query = query_bytes.decode('utf-8').strip()
                print(f"[{client_address}] Query: {query}")
                
                if query.upper() in ["QUIT", "EXIT"]:
                    break 

                # exec query
                execResult: ExecutionResult = self.server.execute_query(query)
                
                # Convert the ExecutionResult object to a JSON dictionary
                result_dict = execResult.to_json_dict()
                
                # Convert the dictionary to a JSON string, then encode it to bytes
                json_string = json.dumps(result_dict) + "\n"
                data_to_send = json_string.encode('utf-8')
                
                # Send the byte data
                client_socket.sendall(data_to_send)

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")

        finally:
            # 3. Always close the connection
            client_socket.close()
            print(f"🚪 Connection with {client_address} closed.")
    
    def run(self):
        HOST = '0.0.0.0'
        PORT = 2345

        # Create a TCP/IP socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Allows the socket to be immediately reusable (useful for debugging)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            # Bind the socket to the address and port
            server_socket.bind((HOST, PORT))
            
            # Enable the server to accept connections (up to 5 queued connections)
            server_socket.listen(5)
            print(f"Server listening on {HOST}:{PORT}")

            while True:
                # Wait for a connection
                # client_socket is the new socket used for communication
                # client_address is the (host, port) of the client
                client_socket, client_address = server_socket.accept()
                
                # Start a new thread to handle the client communication
                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address)
                )
                client_handler.start()

        except Exception as e:
            print(f"Server error: {e}")
        
        finally:
            # This part may not be reached in a typical server loop, 
            # but is good practice for graceful shutdown
            server_socket.close()
        raise NotImplementedError("To be implemented")


def main():
    """Main entry point untuk run server"""
    server = QueryProcessorServer()
    cli = CLIInterface(server)
    cli.run()


if __name__ == "__main__":
    main()
