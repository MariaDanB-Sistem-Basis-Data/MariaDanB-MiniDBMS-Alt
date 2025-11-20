import socket
import json
from typing import Dict, Any

# Define the server details
HOST = '127.0.0.1'  # Server is running locally
PORT = 2345

def receive_full_response(sock: socket.socket) -> bytes:
    """
    Receives data from the socket until a newline delimiter is found.
    This is necessary because TCP streams can fragment messages.
    """
    buffer = b""
    while True:
        # Receive data in chunks
        chunk = sock.recv(4096)
        if not chunk:
            # Server closed the connection
            return buffer
        buffer += chunk
        
        # Check for the message delimiter (the '\n' added by the server)
        if b'\n' in buffer:
            return buffer
        
def start_client():
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    try:
        print(f"Attempting to connect to {HOST}:{PORT}...")
        client_socket.connect((HOST, PORT))
        print("Connection successful.")

        # 1. Receive initial greeting message from server
        greeting_bytes = client_socket.recv(1024)
        print(f"Server Greeting: {greeting_bytes.decode('utf-8')}")

        while True:
            # Get user input for the query
            query = input("MariaDanB> ")
            
            if query.upper() in ["QUIT", "EXIT"]:
                print("Closing connection...")
                client_socket.sendall(query.encode('utf-8'))
                break 

            # 2. Send the query to the server
            client_socket.sendall(query.encode('utf-8'))

            # 3. Receive the full serialized ExecutionResult bytes
            response_bytes_with_newline = receive_full_response(client_socket)

            # Separate the JSON string from the newline delimiter
            json_string = response_bytes_with_newline.decode('utf-8').strip()
            
            if not json_string:
                print("Server closed connection or sent empty response.")
                break

            # deserialize into json
            try:
                result_dict: Dict[str, Any] = json.loads(json_string)
                
                # display result
                print("-" * 30)
                print(f"Query: {result_dict.get('query')}")
                print(f"Status Message: {result_dict.get('message')}")
                print(f"Transaction ID: {result_dict.get('transaction_id')}")
                
                data = result_dict.get('data')
                if isinstance(data, dict) and data.get('type') == 'Rows':
                    print(f"Data Received: {data['rows_count']} rows")
                    # Display the first row as an example
                    if data['data']:
                        print(f"  First Row: {data['data'][0]}")
                elif isinstance(data, int):
                    print(f"Data Received (Affected Rows/Result Code): {data}")
                
            except json.JSONDecodeError:
                print(f"Error decoding JSON response: {json_string[:50]}...")
                
    except ConnectionRefusedError:
        print("Connection refused. Make sure the server is running on the correct host and port.")
    except Exception as e:
        print(f"Client error: {e}")
    finally:
        print(f"Closing connection with {HOST} on {PORT}")
        client_socket.close()

if __name__ == '__main__':
    start_client()