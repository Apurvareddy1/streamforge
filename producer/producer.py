from confluent_kafka import Producer
import json
import time
import random
from datetime import datetime

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

topic = "truck-telemetry"

print("Sending IoT truck data...")

while True:
    data = {
        "truck_id": f"TRUCK-{random.randint(1, 100)}",
        "temperature": round(random.uniform(-5, 50), 2),
        "timestamp": datetime.now().isoformat()
    }

    producer.produce(
        topic,
        value=json.dumps(data)
    )

    producer.flush()

    print("Sent:", data)

    time.sleep(1)