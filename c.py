import socket
import threading
import logging
import csv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class Client:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.connect_to_server()
        self.client_thread = threading.Thread(target=self.receive)
        self.client_thread.daemon = True  # Ensure the thread exits when the main program exits
        self.client_thread.start()

    def connect_to_server(self):
        try:
            self.client.connect((self.host, self.port))
            logging.info(f"Connected to server at {self.host}:{self.port}")
        except socket.error as e:
            logging.error(f"Unable to connect to server: {e}")
            exit(1)

    def receive(self):
        buffer = ""
        while True:
            try:
                data_received = self.client.recv(4096).decode()
                if data_received:
                    if data_received == "END_OF_DATA":
                        self.save_to_csv(buffer)
                        buffer = ""
                    else:
                        buffer += data_received
                else:
                    logging.info("Server closed the connection")
                    break
            except Exception as e:
                logging.error(f"Error receiving data: {e}")
                break
        self.client.close()

    def send(self, data):
        try:
            self.client.send(data.encode())
        except Exception as e:
            logging.error(f"Error sending data: {e}")

    def close(self):
        logging.info("Closing connection")
        self.client.close()

    def save_to_csv(self, data):
        try:
            rows = data.strip().split('\n')
            reader = csv.reader(rows, delimiter=',')
            with open('sensor_data.csv', 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['id', 'temperature', 'humidity', 'pm1', 'pm25', 'pm10', 'co_value', 'max_value', 'rate'])
                for row in reader:
                    writer.writerow(row)
            logging.info("Sensor data saved to sensor_data.csv")
        except Exception as e:
            logging.error(f"Error saving data to CSV: {e}")

if __name__ == "__main__":
    client = Client("localhost", 12345)
    try:
        while True: # Keep the main thread alive    
            data = input()
            client.send(data)
            if data.lower() == "exit":
                break
            elif data.lower() == "get_sensor_data":
                client.send("get_sensor_data,")
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    finally:
        client.close()
