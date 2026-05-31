import json
import random
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuration
KAFKA_TOPIC = "shipment-events"
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

locations = ["Berlin", "Munich", "Hamburg", "Cologne", "Frankfurt"]
carriers = ["DHL", "UPS", "FedEx", "DB Schenker"]
risk_factors = ["weather", "strike", "port_congestion", "none"]

while True:
    event = {
        "shipment_id": f"SHP-{random.randint(10000,99999)}",
        "timestamp": datetime.utcnow().isoformat(),
        "origin": random.choice(locations),
        "destination": random.choice(locations),
        "carrier": random.choice(carriers),
        "estimated_delivery_days": random.randint(1, 10),
        "actual_delay_minutes": random.randint(0, 300),   # target for regression
        "risk_factor": random.choice(risk_factors),
        "temperature_anomaly": random.choice([True, False])  # binary anomaly
    }
    producer.send(KAFKA_TOPIC, event)
    time.sleep(0.5)   # 2 events per second
