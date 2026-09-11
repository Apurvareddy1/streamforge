from recovery import ChangelogManager

manager = ChangelogManager()

print("Starting state recovery...")

manager.recover_state()

print("State recovery test completed!")