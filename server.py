import socket
import threading

def activityClient(client_socket, addr):
    while True:
        data = client_socket.recv(1024).decode('utf-8')
        if not data:
            break
        print(f"Received from {addr}: {data}")

        if data == "ping":
            response = "OK"
            client_socket.send(response.encode('utf-8'))
        else:
            client_socket.send("Unknown command".encode('utf-8'))
    
    client_socket.close()

def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    host = socket.gethostname()
    
    port = 2108
    
    server_socket.bind((host, port))
    
    server_socket.listen(5)
    
    print(f"Server listening on {host}:{port}")
    
    while True:
        client_socket, addr = server_socket.accept()
        
        print(f"Got a connection from {addr}")
        client_thread = threading.Thread(target=activityClient, args=(client_socket, addr))
        client_thread.start()

if __name__ == "__main__":
    start_server()
