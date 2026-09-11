from state_store import StateStore

print("Testing RocksDB...")

store = StateStore()

state = {
    "temperature_sum": 150,
    "count": 3,
    "average": 50
}

store.save_state("TRUCK001", state)

result = store.get_state("TRUCK001")

print("Saved state:")
print(state)

print("\nRecovered state:")
print(result)

store.close()

print("\nRocksDB test completed successfully!")