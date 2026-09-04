from confluent_kafka import Consumer
import json
from datetime import datetime

print("Starting Stream Processor...")

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-week2",
    "auto.offset.reset": "latest"
})

consumer.subscribe(["truck-telemetry"])

# Store temperature data
truck_data = {}

# For testing: 30 seconds
WINDOW_SECONDS = 30
window_start = datetime.now()

print("Waiting for messages...")

try:
    while True:
        message = consumer.poll(1.0)

        if message is not None and not message.error():

            data = json.loads(message.value().decode("utf-8"))

            # FILTER
            if data["temperature"] > 0:

                truck_id = data["truck_id"]
                temperature = data["temperature"]

                # Store temperature for each truck
                if truck_id not in truck_data:
                    truck_data[truck_id] = []

                truck_data[truck_id].append(temperature)

                print("Processed:", data)

            else:
                print("Invalid data skipped:", data)

        # Check window
        current_time = datetime.now()

        if (current_time - window_start).total_seconds() >= WINDOW_SECONDS:

            print("\n----- WINDOW RESULT -----")

            for truck_id, temperatures in truck_data.items():
                average = sum(temperatures) / len(temperatures)

                print(
                    f"{truck_id} | "
                    f"Average Temperature: {average:.2f}°C"
                )

            print("-------------------------\n")

            # Start a new window
            truck_data = {}
            window_start = datetime.now()

except KeyboardInterrupt:
    print("\nStream Processor Stopped.")

finally:
    consumer.close()