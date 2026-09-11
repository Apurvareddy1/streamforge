from recovery import ChangelogManager


manager = ChangelogManager()

state = {
    "temperature_sum": 150,
    "count": 3,
    "average": 50
}

manager.save_to_changelog(
    "TRUCK001",
    state
)

print("Changelog test completed!")