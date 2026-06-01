import json
import random
import time
from kafka import KafkaProducer
import datetime

# Configuration
KAFKA_TOPIC = "iot-sensor-data"
KAFKA_BROKER = "localhost:9092"

# Create producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Vehicle IDs
vehicle_ids = [f"vehicle_{i}" for i in range(1, 101)]

def generate_sensor_data():
    return {
        "vehicle_id": random.choice(vehicle_ids),
        "timestamp": datetime.datetime.utcnow().isoformat(),
        "temperature": round(random.uniform(70, 120), 2),     # Celsius
        "vibration": round(random.uniform(0.1, 5.0), 3),      # mm/s
        "rpm": random.randint(500, 6000),
        "location": {
            "lat": round(random.uniform(40.0, 41.0), 6),
            "lon": round(random.uniform(-74.0, -73.0), 6)
        }
    }

if __name__ == "__main__":
    print("Starting Kafka producer... Press Ctrl+C to stop.")
    while True:
        data = generate_sensor_data()
        producer.send(KAFKA_TOPIC, value=data)
        print(f"Sent: {data['vehicle_id']} at {data['timestamp']}")
        time.sleep(1)   # 1 message per second (simulate 100 vehicles rotating)
