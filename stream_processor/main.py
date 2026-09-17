from state_store import StateStore
from recovery import ChangelogManager
from confluent_kafka import Consumer
import json
from datetime import datetime
from metrics import start_metrics_server, record_event, set_worker_status

print("Starting Stream Processor - Week 3...")

# Start Prometheus metrics server
start_metrics_server(8000)
set_worker_status(1)

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-week3",
    "auto.offset.reset": "latest"
})

consumer.subscribe(["truck-telemetry"])

state_store = StateStore()
changelog = ChangelogManager(state_store)

truck_data = {}
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

            if data["temperature"] > 0:

                truck_id = data["truck_id"]
                temperature = data["temperature"]

                if truck_id not in truck_data:
                    truck_data[truck_id] = []

                truck_data[truck_id].append(temperature)

                print("Processed:", data)

                # Calculate real processing lag
                event_ts = datetime.fromisoformat(
                    data["timestamp"]
                ).timestamp()

                record_event(event_ts)

            else:
                print("Invalid data skipped:", data)

        current_time = datetime.now()

        # Check 5-minute window
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

            truck_data = {}
            window_start = datetime.now()

except KeyboardInterrupt:

    print("\nStream Processor Stopped.")

finally:

    set_worker_status(0)

    consumer.close()
    state_store.close()

    print("Resources closed.")