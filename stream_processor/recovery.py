import json
from confluent_kafka import Producer, Consumer
from state_store import StateStore

KAFKA_BOOTSTRAP = "localhost:9092"
CHANGELOG_TOPIC = "streamforge-changelog"


class ChangelogManager:

    def __init__(self, state_store=None):

        # Use existing StateStore if provided
        self.state_store = state_store if state_store else StateStore()

        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP
        })


    def save_to_changelog(self, truck_id, state):

        message = {
            "truck_id": truck_id,
            "state": state
        }

        self.producer.produce(
            CHANGELOG_TOPIC,
            key=truck_id,
            value=json.dumps(message)
        )

        self.producer.flush()

        print(f"Changelog saved: {truck_id}")


    def recover_state(self):

        consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": "streamforge-recovery",
            "auto.offset.reset": "earliest"
        })

        consumer.subscribe([CHANGELOG_TOPIC])

        print("Recovering state from Kafka changelog...")

        recovered = 0

        try:

            while True:

                msg = consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():

                    print("Kafka error:", msg.error())
                    continue


                data = json.loads(
                    msg.value().decode("utf-8")
                )

                truck_id = data["truck_id"]
                state = data["state"]


                # Restore state into RocksDB
                self.state_store.save_state(
                    truck_id,
                    state
                )

                recovered += 1

                print(
                    f"Recovered: {truck_id} -> {state}"
                )


        finally:

            consumer.close()


        print(
            f"Recovery completed. "
            f"Recovered {recovered} states."
        )
