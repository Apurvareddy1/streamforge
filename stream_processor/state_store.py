from rocksdict import Rdict
import json


class StateStore:

    def __init__(self, path="rocksdb_test"):
        self.db = Rdict(path)

    def save_state(self, truck_id, state):
        self.db[truck_id] = json.dumps(state)

    def get_state(self, truck_id):
        value = self.db.get(truck_id)

        if value is None:
            return None

        return json.loads(value)

    def close(self):
        self.db.close()
