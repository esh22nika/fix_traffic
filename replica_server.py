# replica_server.py
from xmlrpc.server import SimpleXMLRPCServer
import sys
import os


class ReplicaServer:
    def __init__(self, replica_db_path):
        try:
            # Add current directory to path for imports
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            from zookeeper import DatabaseManager
            self.db = DatabaseManager(replica_db_path)
            print(f"Successfully initialized database: {replica_db_path}")
        except Exception as e:
            print(f"ERROR: Failed to initialize database: {e}")
            sys.exit(1)

    def get_signal_status(self):
        try:
            return self.db.get_signal_status()
        except Exception as e:
            print(f"ERROR in get_signal_status: {e}")
            return {}

    def get_system_stats(self):
        try:
            return self.db.get_system_stats()
        except Exception as e:
            print(f"ERROR in get_system_stats: {e}")
            return {}

    def ping(self):
        return "OK"

    def update_signal_status(self, signal_status_dict):
        try:
            self.db.update_signal_status(signal_status_dict)
            return "OK"
        except Exception as e:
            print(f"ERROR in update_signal_status: {e}")
            return "FAIL"

    def update_controller_status(self, controller_name, **kwargs):
        try:
            self.db.update_controller_status(controller_name, **kwargs)
            return "OK"
        except Exception as e:
            print(f"ERROR in update_controller_status: {e}")
            return "FAIL"


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 7001
    replica_id = int(sys.argv[2]) if len(sys.argv) > 2 else 1

    print(f"Starting replica server {replica_id} on port {port}...")

    try:
        replica_server = ReplicaServer(f"replica_{replica_id}.db")

        server = SimpleXMLRPCServer(("0.0.0.0", port), allow_none=True)
        server.register_function(replica_server.get_signal_status, "get_signal_status")
        server.register_function(replica_server.get_system_stats, "get_system_stats")
        server.register_function(replica_server.ping, "ping")
        server.register_function(replica_server.update_signal_status, "update_signal_status")
        server.register_function(replica_server.update_controller_status, "update_controller_status")

        print(f"Replica server {replica_id} ready on port {port}")
        server.serve_forever()
    except Exception as e:
        print(f"Failed to start replica server: {e}")
        sys.exit(1)
