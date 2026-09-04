from confluent_kafka import Consumer
import json

print("Starting Stream Processor...")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-week2",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["truck_telemetry"])

print("Connected to Kafka. Waiting for messages...")

while True:
    message = consumer.poll(1.0)

    if message is None:
        continue

    if message.error():
        print("Kafka Error:", message.error())
        continue

    data = json.loads(message.value().decode("utf-8"))

    # FILTER
    if data["temperature"] <= 0:
        print("Invalid temperature skipped:", data)
        continue

    # MAP
    processed_data = {
        "truck_id": data["truck_id"],
        "temperature": data["temperature"],
        "timestamp": data["timestamp"]
    }

    print("Processed Data:", processed_data)