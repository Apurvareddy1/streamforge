from state_store import StateStore
from recovery import ChangelogManager
from confluent_kafka import Consumer
import json
from datetime import datetime


print("Starting Stream Processor - Week 3...")


# Kafka Consumer
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-week3",
    "auto.offset.reset": "latest"
})

consumer.subscribe(["truck-telemetry"])


# RocksDB + Kafka Changelog
state_store = StateStore()

# Use the SAME RocksDB instance
changelog = ChangelogManager(state_store)


# Truck data
truck_data = {}

# 5-minute window
WINDOW_SECONDS = 300

window_start = datetime.now()

print("Waiting for messages...")


try:

    while True:

        message = consumer.poll(1.0)

        if message is not None and not message.error():

            data = json.loads(
                message.value().decode("utf-8")
            )

            # Filter temperature > 0
            if data["temperature"] > 0:

                truck_id = data["truck_id"]
                temperature = data["temperature"]

                if truck_id not in truck_data:
                    truck_data[truck_id] = []

                truck_data[truck_id].append(temperature)

                print("Processed:", data)

            else:

                print("Invalid data skipped:", data)


        # Check window
        current_time = datetime.now()

        if (
            current_time - window_start
        ).total_seconds() >= WINDOW_SECONDS:

            print("\n----- 5-MINUTE WINDOW RESULT -----")

            for truck_id, temperatures in truck_data.items():

                total = sum(temperatures)
                count = len(temperatures)

                if count == 0:
                    continue

                average = total / count

                state = {
                    "temperature_sum": total,
                    "count": count,
                    "average": average
                }

                print(
                    f"{truck_id} | "
                    f"Average Temperature: "
                    f"{average:.2f}°C"
                )

                # Save state to RocksDB
                state_store.save_state(
                    truck_id,
                    state
                )

                print(
                    f"RocksDB state saved: {truck_id}"
                )

                # Save state to Kafka changelog
                changelog.save_to_changelog(
                    truck_id,
                    state
                )

            print("-------------------------\n")

            # Start new window
            truck_data = {}

            window_start = datetime.now()


except KeyboardInterrupt:

    print("\nStream Processor Stopped.")


finally:

    consumer.close()
    state_store.close()

    print("Resources closed.")