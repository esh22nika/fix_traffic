# replica_server.py
from xmlrpc.server import SimpleXMLRPCServer
import sys
import os
import sqlite3
import time


class ReplicaServer:
    def __init__(self, replica_db_path):
        try:
            # Add current directory to path for imports
            sys.path.append(os.path.dirname(os.path.abspath(__file__)))
            self.db_path = replica_db_path
            self.init_database()
            print(f"Successfully initialized database: {replica_db_path}")
        except Exception as e:
            print(f"ERROR: Failed to initialize database: {e}")
            sys.exit(1)

    def init_database(self):
        """Initialize the replica database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS signal_status (
                    id INTEGER PRIMARY KEY,
                    signal_id TEXT UNIQUE,
                    status TEXT,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS controller_status (
                    id INTEGER PRIMARY KEY,
                    controller_name TEXT UNIQUE,
                    url TEXT,
                    is_available BOOLEAN,
                    active_requests INTEGER,
                    buffer_size INTEGER,
                    last_heartbeat TIMESTAMP,
                    total_processed INTEGER DEFAULT 0
                )
            ''')
            conn.execute('''
                CREATE TABLE IF NOT EXISTS request_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    request_id TEXT,
                    request_type TEXT,
                    target_pair TEXT,
                    controller_assigned TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    response_time REAL,
                    status TEXT
                )
            ''')

            # Initialize default signal status
            default_signals = {
                '1': 'RED', '2': 'RED', '3': 'GREEN', '4': 'GREEN',
                'P1': 'GREEN', 'P2': 'GREEN', 'P3': 'RED', 'P4': 'RED'
            }
            for signal_id, status in default_signals.items():
                conn.execute(
                    'INSERT OR REPLACE INTO signal_status (signal_id, status) VALUES (?, ?)',
                    (signal_id, status)
                )
            conn.commit()
            print(f"Database initialized at {self.db_path}")

    def update_signal_status(self, signal_status_dict):
        """Update signal status in database"""
        with sqlite3.connect(self.db_path) as conn:
            for signal_id, status in signal_status_dict.items():
                signal_id_str = str(signal_id)
                conn.execute(
                    'INSERT OR REPLACE INTO signal_status (signal_id, status, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)',
                    (signal_id_str, status)
                )
            conn.commit()
        return "OK"

    def get_signal_status(self):
        """Get current signal status"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT signal_id, status FROM signal_status')
            return {row[0]: row[1] for row in cursor.fetchall()}

    def get_system_stats(self):
        """Get comprehensive system statistics"""
        with sqlite3.connect(self.db_path) as conn:
            controllers = conn.execute('''
                SELECT controller_name, url, is_available, active_requests, 
                       total_processed, last_heartbeat
                FROM controller_status
            ''').fetchall()

            signals = self.get_signal_status()

            return {
                'controllers': [dict(zip(['name', 'url', 'available', 'active', 'processed', 'heartbeat'], c))
                                for c in controllers],
                'signal_status': signals,
                'timestamp': time.time(),
                'replica_name': os.path.basename(self.db_path).replace('.db', ''),
                'data_integrity': 'OK'
            }

    def update_controller_status(self, controller_name, is_available, active_requests, total_processed):
        """Update controller status in database"""
        try:
            # Convert input types
            is_available_bool = is_available.lower() == 'true' if isinstance(is_available, str) else bool(is_available)
            active_requests_int = int(active_requests)
            total_processed_int = int(total_processed)

            with sqlite3.connect(self.db_path) as conn:
                # Check if controller exists
                cursor = conn.execute(
                    'SELECT id FROM controller_status WHERE controller_name = ?',
                    (controller_name,)
                )
                exists = cursor.fetchone() is not None

                if exists:
                    # Update existing controller
                    conn.execute(
                        '''UPDATE controller_status 
                           SET is_available = ?, active_requests = ?, total_processed = ?, last_heartbeat = CURRENT_TIMESTAMP
                           WHERE controller_name = ?''',
                        (is_available_bool, active_requests_int, total_processed_int, controller_name)
                    )
                else:
                    # Insert new controller
                    conn.execute(
                        '''INSERT INTO controller_status 
                           (controller_name, is_available, active_requests, total_processed, last_heartbeat)
                           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)''',
                        (controller_name, is_available_bool, active_requests_int, total_processed_int)
                    )
                conn.commit()
            return "OK"
        except Exception as e:
            print(f"ERROR in update_controller_status: {e}")
            return "FAIL"

    def ping(self):
        return "OK"


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
