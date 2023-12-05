import serial
import sys
import multiprocessing
import threading
from pymongo import MongoClient
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime


# Firebase Admin SDK with credentials file
cred = credentials.Certificate("connected-coops-credentials.json")
firebase_admin.initialize_app(cred)

# Initialize Firestore client
db = firestore.client()

# Connect to the MongoDB container
mongo_client = MongoClient("mongodb://admin:123456@172.18.0.2:27017/?authSource=admin")

# Access the database
mongo_db = mongo_client["cocodb_local"]

# Create a collection named "readings" in MongoDB
mongo_collection = mongo_db["readings"]

users = [
    {
        "UID": "KVwb8RaoiTbxZp2lna5SD9fONZR2"
    }
]

# Define a function to read data from MongoDB and send it to Firestore
def process_and_send_data():
    while True:
        try:
            # Query MongoDB for data with onCloud=False
            data_to_send = list(mongo_collection.find({"onCloud": False}))
            
            for data in data_to_send:
                # Process the data and send it to Firestore
                if data["data"].startswith("S"):
                    parts = data["data"].split("S")
                    if len(parts) == 2:
                        sensor_id, reading = parts[1].split("R")
                        sensor_id = int(sensor_id)
                        reading = float(reading) if "null" not in reading else None
                        
                        # Find the corresponding sensor in Firestore by sensor_id
                        sensor_ref = db.collection("users").document(users[0]["UID"]).collection("sensors").document(str(sensor_id))
                        sensor = sensor_ref.get()
                        
                        if sensor.exists:
                            # Create a new sensor reading
                            new_sensor_reading = {
                                "created_at":firestore.SERVER_TIMESTAMP,
                                "reading": reading,
                                "sensor_info": sensor.to_dict(),
                                "timestamp": data["created_at"]
                            }
                            
                            # Set the new sensor reading in the user's "last_sensor_readings" collection with the sensor's ID
                            user_last_sensor_reading_ref = db.collection("users").document(users[0]["UID"]).collection("last_sensor_readings").document(str(sensor_id))
                            user_last_sensor_reading_ref.set(new_sensor_reading)
                            
                            # Add the sensor reading to the user's "sensor_readings" collection
                            user_sensor_readings_ref = db.collection("users").document(users[0]["UID"]).collection("sensor_readings")
                            user_sensor_readings_ref.add(new_sensor_reading)
                            print(f"Updated sensor reading for sensor with ID: {sensor_id} in Firestore")
                    
                # Check for RSSI data
                elif data["data"].startswith("N"):
                    parts = data["data"].split("N")
                    if len(parts) == 2:
                        if "R" in parts[1]:
                            node_id, rssi = parts[1].split("R")
                            node_id = int(node_id)
                            rssi = int(rssi)
                            
                            # Find the corresponding node in Firestore by node_id
                            node_ref = db.collection("users").document(users[0]["UID"]).collection("nodes").document(str(node_id))
                            node = node_ref.get()
                            
                            if node.exists:
                                # Update the RSSI field of the node
                                node_ref.update({"rssi": rssi, "last_timestamp": data["created_at"]})
                                print(f"Updated RSSI for node with ID: {node_id} in Firestore")
                            
                        # Check for battery data
                        elif "B" in parts[1]:
                            node_id, battery_level = parts[1].split("B")
                            node_id = int(node_id)
                            battery_level = float(battery_level) if "null" not in battery_level else None
                            
                            # Find the corresponding node in Firestore by node_id
                            node_ref = db.collection("users").document(users[0]["UID"]).collection("nodes").document(str(node_id))
                            node = node_ref.get()
                            
                            if node.exists:
                                # Update the battery_level field of the node
                                node_ref.update({"battery_level": battery_level, "last_timestamp": data["created_at"]})
                                print(f"Updated battery level for node with ID: {node_id} in Firestore")
                    
                
                # Mark the data as sent to Firestore by updating the onCloud flag
                mongo_collection.update_one({"_id": data["_id"]}, {"$set": {"onCloud": True}})
                
        except Exception as e:
            print(f"Error in the data processing and sending process: {str(e)}")

# Create a separate process to continuously process and send data
data_process = multiprocessing.Process(target=process_and_send_data)
data_process.start()


portnames = ['/dev/ttyUSB0']  # List of USB serial port names

def open_serial_port(portname):
    try:
        ser = serial.Serial(portname, 115200, timeout=1)
        ser.flush()
        print(f"Serial port {portname} opened successfully.")
        return ser
    except Exception as e:
        print(f"Failed to open serial port {portname}: {e}")
        return None

def read_and_save_data(ser):
    while True:
        try:
            data = ser.readline().decode('utf-8').rstrip()
            if data:
                print(f"Received data from {ser.port}: {data}")

                # Save the received data to the local MongoDB collection
                mongo_document = {
                    "created_at": datetime.now(),
                    "data": str(data),
                    "onCloud": False
                }
                mongo_collection.insert_one(mongo_document)
                print(f"Saved data from {ser.port} to local MongoDB collection.")

        except KeyboardInterrupt:
            print(f"Closing serial port {ser.port}.")
            ser.close()
            break

serial_ports = [open_serial_port(port) for port in portnames]

if all(ser is None for ser in serial_ports):
    print("No serial ports were successfully opened. Exiting.")
    sys.exit(0)

threads = []

for ser in serial_ports:
    if ser:
        thread = threading.Thread(target=read_and_save_data, args=(ser,))
        threads.append(thread)
        thread.start()

try:
    for thread in threads:
        thread.join()

except KeyboardInterrupt:
    for ser in serial_ports:
        if ser:
            print(f"Closing serial port {ser.port}.")
            ser.close()

    mongo_client.close()
    print("MongoDB connection closed.")
    print("Exiting the script.")
