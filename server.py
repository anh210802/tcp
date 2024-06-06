import socket
import threading
import logging
import mysql.connector
from mysql.connector import Error
from concurrent.futures import ThreadPoolExecutor

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connect_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='data_iot',
            user='root',
            password='123456'
        )
        if connection.is_connected():
            logging.info("Connected to MySQL database")
            return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def readDataGateWay(client_socket, addr, db_connection):
    try:
        cursor = db_connection.cursor()
        while True:
            data = client_socket.recv(1024).decode('utf-8')
            if not data:
                break

            split_data = data.split('_')
            if len(split_data) >= 7:
                temp = float(split_data[1])
                humi = float(split_data[2])
                pm1 = float(split_data[3])
                pm25 = float(split_data[4])
                pm10 = float(split_data[5])
                CO_value = float(split_data[6])
                max_value = max(pm1, pm25, pm10, CO_value)
                if max_value <= 35:
                    rate = 1
                elif max_value > 35 and max_value <= 55:
                    rate = 2
                elif max_value > 55:
                    rate = 3
                logging.info(f"Received from {addr}: {temp}, {humi}, {pm1}, {pm25}, {pm10}, {CO_value}, {max_value}, {rate}")
                cursor.execute(
                    "INSERT INTO sensor_data (temperature, humidity, pm1, pm25, pm10, co_value, max_value, rate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                    (temp, humi, pm1, pm25, pm10, CO_value, max_value, rate)
                )
                db_connection.commit()
                logging.info(f"Data inserted into database")
            else:
                logging.warning(f"Incomplete data received from {addr}: {split_data}")

    except Exception as e:
        logging.error(f"Error handling data from {addr}: {e}")
    finally:
        client_socket.close()
        logging.info(f"Connection from {addr} closed")

def start_server(host='177.30.34.49', port=3131, max_workers=5):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    db_connection = connect_db()
    if not db_connection:
        logging.error("Database connection failed. Exiting...")
        return

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        logging.info(f"Server listening on {host}:{port}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            while True:
                client_socket, addr = server_socket.accept()
                logging.info(f"Got a connection from {addr}")
                executor.submit(readDataGateWay, client_socket, addr, db_connection)
    
    except Exception as e:
        logging.error(f"Server error: {e}")
    
    finally:
        server_socket.close()
        db_connection.close()
        logging.info("Server socket and database connection closed")

if __name__ == "__main__":
    start_server()
