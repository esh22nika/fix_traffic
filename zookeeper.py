# zookeeper.py
# Enhanced Load Balancer with Dynamic Scaling, Database, and Performance Optimization
from xmlrpc import server
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid
import json
import sqlite3
from collections import deque
import subprocess
import sys
import os
import hashlib

import lb

# Configuration
ZOOKEEPER_PORT = 6000
BASE_CONTROLLERS = {
    "controller": "http://localhost:8000",
    "controller_clone": "http://localhost:8001"
}
BUFFER_SIZE = 5  # Increased buffer size
MAX_DYNAMIC_CLONES = 3  # Maximum additional dynamic clones
RESPONSE_TIMEOUT = 15  # Reduced timeout for faster failure detection
DB_PATH = "traffic_system.db"


def signal_request(self, client_id, target_pair, request_type="normal"):
    """Entry point for t_signal requests"""
    print(f"[ZOOKEEPER] Signal request from {client_id}: {target_pair} ({request_type})")

    # Choose available controller
    controller = self.get_available_controller()
    if not controller:
        return {"success": False, "reason": "No controllers available"}

    # Forward to controller
    try:
        proxy = ServerProxy(controller.url, allow_none=True,
                            transport=TimeoutTransport(RESPONSE_TIMEOUT))
        result = proxy.signal_controller(target_pair)

        print(f"[ZOOKEEPER] Request handled by {controller.name}")
        return {"success": True, "controller": controller.name, "result": result}

    except Exception as e:
        print(f"[ZOOKEEPER] Request failed on {controller.name}: {e}")
        return {"success": False, "reason": str(e)}
class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout


    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_database()

    def init_database(self):
        """Initialize the traffic system database"""
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
                    total_processed INTEGER DEFAULT 0,
                    is_dynamic BOOLEAN DEFAULT 0
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
            conn.execute('''
                CREATE TABLE IF NOT EXISTS vip_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    vehicle_id TEXT,
                    priority INTEGER,
                    target_pair TEXT,
                    arrival_time TIMESTAMP,
                    served_by TEXT,
                    service_time REAL
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
            print(f"[DATABASE] Database initialized at {self.db_path}")

    def update_signal_status(self, signal_status_dict):
        """Update signal status in database - Convert all keys to strings"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for signal_id, status in signal_status_dict.items():
                    # Ensure signal_id is always a string
                    signal_id_str = str(signal_id)
                    conn.execute(
                        'INSERT OR REPLACE INTO signal_status (signal_id, status, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)',
                        (signal_id_str, status)
                    )
                conn.commit()
                print(f"[DATABASE] Updated signal status for {len(signal_status_dict)} signals")

    def get_signal_status(self):
        """Get current signal status"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('SELECT signal_id, status, last_updated FROM signal_status')
                return {row[0]: {'status': row[1], 'last_updated': row[2]} for row in cursor.fetchall()}

    def update_controller_status(self, controller_name, **kwargs):
        """Update controller status in database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Check if controller exists
                cursor = conn.execute('SELECT id FROM controller_status WHERE controller_name = ?', (controller_name,))
                if cursor.fetchone():
                    # Update existing
                    set_clauses = []
                    values = []
                    for key, value in kwargs.items():
                        if key in ['url', 'is_available', 'active_requests', 'buffer_size', 'total_processed',
                                   'is_dynamic']:
                            set_clauses.append(f'{key} = ?')
                            values.append(value)
                    if set_clauses:
                        set_clauses.append('last_heartbeat = CURRENT_TIMESTAMP')
                        values.append(controller_name)
                        query = f'UPDATE controller_status SET {", ".join(set_clauses)} WHERE controller_name = ?'
                        conn.execute(query, values)
                else:
                    # Insert new
                    conn.execute('''
                        INSERT INTO controller_status 
                        (controller_name, url, is_available, active_requests, buffer_size, 
                         last_heartbeat, total_processed, is_dynamic)
                        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
                    ''', (
                        controller_name,
                        kwargs.get('url', ''),
                        kwargs.get('is_available', True),
                        kwargs.get('active_requests', 0),
                        kwargs.get('buffer_size', BUFFER_SIZE),
                        kwargs.get('total_processed', 0),
                        kwargs.get('is_dynamic', False)
                    ))
                conn.commit()

    def log_request(self, request_id, request_type, target_pair, controller_assigned,
                    start_time, end_time=None, status="processing"):
        """Log request to database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                response_time = (end_time - start_time) if end_time else None
                conn.execute('''
                    INSERT OR REPLACE INTO request_log 
                    (request_id, request_type, target_pair, controller_assigned, start_time, 
                     end_time, response_time, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (request_id, request_type, str(target_pair), controller_assigned,
                      start_time, end_time, response_time, status))
                conn.commit()

    def get_system_stats(self):
        """Get comprehensive system statistics from database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Controller stats
                controllers = conn.execute('''
                    SELECT controller_name, url, is_available, active_requests, buffer_size,
                           total_processed, is_dynamic, last_heartbeat
                    FROM controller_status
                ''').fetchall()

                # Recent requests
                recent_requests = conn.execute('''
                    SELECT request_type, controller_assigned, response_time, status
                    FROM request_log
                    ORDER BY start_time DESC LIMIT 10
                ''').fetchall()

                # Signal status
                signals = self.get_signal_status()

                return {
                    'controllers': [dict(
                        zip(['name', 'url', 'available', 'active', 'buffer_size', 'processed', 'dynamic', 'heartbeat'],
                            c)) for c in controllers],
                    'recent_requests': [dict(zip(['type', 'controller', 'response_time', 'status'], r)) for r in
                                        recent_requests],
                    'signal_status': signals,
                    'timestamp': time.time()
                }


class ControllerState:
    def __init__(self, name: str, url: str, is_dynamic: bool = False):
        self.name = name
        self.url = url
        self.active_requests = deque(maxlen=BUFFER_SIZE)
        self.is_available = True
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()
        self.total_processed = 0
        self.is_dynamic = is_dynamic

    def is_free(self) -> bool:
        with self.lock:
            return len(self.active_requests) < BUFFER_SIZE and self.is_available

    def add_request(self, request_id: str):
        with self.lock:
            self.active_requests.append(request_id)
            print(f"[ZOOKEEPER] Buffer {self.name}: {len(self.active_requests)}/{BUFFER_SIZE}")

    def complete_request(self, request_id: str):
        with self.lock:
            try:
                self.active_requests.remove(request_id)
                self.total_processed += 1
                print(f"[ZOOKEEPER] Completed {self.name} {request_id}, buffer: {len(self.active_requests)}/{BUFFER_SIZE}")
            except ValueError:
                pass


class DynamicCloneManager:
    def __init__(self, base_port=8002):
        self.base_port = base_port
        self.dynamic_clones = {}
        self.clone_counter = 0

    def create_dynamic_clone(self) -> tuple:
        """Create a new dynamic controller clone"""
        if len(self.dynamic_clones) >= MAX_DYNAMIC_CLONES:
            print(f"[CLONE-MANAGER] Maximum dynamic clones ({MAX_DYNAMIC_CLONES}) reached")
            return None, None

        self.clone_counter += 1
        clone_name = f"dynamic_clone_{self.clone_counter}"
        clone_port = self.base_port + self.clone_counter
        clone_url = f"http://localhost:{clone_port}"

        try:
            # Create a modified version of controller_clone.py with different port
            self._create_clone_script(clone_name, clone_port)

            # Start the clone process
            clone_process = subprocess.Popen([
                sys.executable, f"{clone_name}.py"
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # Wait a moment for it to start
            time.sleep(2)

            # Test if it's responding
            test_proxy = ServerProxy(clone_url, allow_none=True, transport=TimeoutTransport(2))
            response = test_proxy.ping()
            if response == "OK":
                self.dynamic_clones[clone_name] = {
                    'process': clone_process,
                    'port': clone_port,
                    'url': clone_url
                }
                print(f"[CLONE-MANAGER] Dynamic clone {clone_name} created successfully on port {clone_port}")
                return clone_name, clone_url
            else:
                clone_process.terminate()
                return None, None

        except Exception as e:
            print(f"[CLONE-MANAGER] Failed to create dynamic clone: {e}")
            return None, None

    def _create_clone_script(self, clone_name, port):
        """Create a dynamic clone script file"""
        template_content = f'''
# {clone_name}.py - Dynamically created controller clone
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# Configuration for dynamic clone
CLIENTS = {{
    "t_signal": "http://localhost:7000",
    "p_signal": "http://localhost:9000",
}}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 3
VIP_CROSSING_TIME = 0.1  # Minimal processing time
CONTROLLER_PORT = {port}
CONTROLLER_NAME = "{clone_name.upper()}"

server_skew = 0.0
state_lock = threading.Lock()
vip_queues = {{"12": [], "34": []}}

# Simplified signal status - all keys as strings
signal_status = {{
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}}

def ping():
    return "OK"

def signal_controller(target_pair):
    print(f"[{{CONTROLLER_NAME}}] Processing signal request for {{target_pair}}")
    time.sleep(0.1)
    return True

def vip_arrival(target_pair, priority=1, vehicle_id=None):
    print(f"[{{CONTROLLER_NAME}}] Processing VIP request for {{target_pair}}")
    time.sleep(0.1)
    return True

if __name__ == "__main__":
    print(f"[{{CONTROLLER_NAME}}] Dynamic clone starting on port {{CONTROLLER_PORT}}")
    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(ping, "ping")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\\n[{{CONTROLLER_NAME}}] Shutting down...")
'''
        with open(f"{clone_name}.py", 'w') as f:
            f.write(template_content)


class ZooKeeperLoadBalancer:
    def __init__(self):
        self.db = DatabaseManager(DB_PATH)
        self.controllers = {}
        self.clone_manager = DynamicCloneManager()
        self.round_robin_index = 0
        self.lock = threading.Lock()
        self.replica_manager = DataReplicaManager()
        self.lamport_clock = 0
        self.clock_lock = threading.Lock()
        self.ra_state = {}  # Ricart-Agrawala state management
        self.pending_requests = {}  # For deferred RA messages

        # Client registry for Berkeley sync
        self.clients = {
            "t_signal": "http://localhost:7000",
            "p_signal": "http://localhost:9000"
        }
        # Initialize base controllers
        for name, url in BASE_CONTROLLERS.items():
            self.controllers[name] = ControllerState(name, url)
            self.db.update_controller_status(
                name, url=url, is_available=True, active_requests=0,
                buffer_size=BUFFER_SIZE, total_processed=0, is_dynamic=False
            )

    def increment_lamport_clock(self):
        """Increment Lamport clock for RA algorithm"""
        with self.clock_lock:
            self.lamport_clock += 1
            return self.lamport_clock

    def request_ped_ack(self, controller_name, target_pair, timestamp, request_type, requester_info=None):
        """Mediate pedestrian acknowledgment requests"""
        print(f"[ZOOKEEPER] Forwarding pedestrian ack request from {controller_name}")
        try:
            proxy = ServerProxy(self.clients["p_signal"], allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            # Enhanced p_signal call with RA parameters
            response = proxy.p_signal_ra(target_pair, timestamp, controller_name, request_type)
            return response
        except Exception as e:
            print(f"[ZOOKEEPER] Failed to get pedestrian ack: {e}")
            return "DENY"

    def forward_ra_request(self, from_controller, to_controller, timestamp, target_pair, request_type):
        """Forward Ricart-Agrawala requests between controllers"""
        request_id = f"RA_{from_controller}_{timestamp}"
        print(f"[ZOOKEEPER] Forwarding RA request {request_id}: {from_controller} -> {to_controller}")

        try:
            if to_controller in self.controllers:
                controller = self.controllers[to_controller]
                proxy = ServerProxy(controller.url, allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))
                response = proxy.receive_ra_request(from_controller, timestamp, target_pair, request_type)
                return response
            return "DEFER"
        except Exception as e:
            print(f"[ZOOKEEPER] RA forwarding failed: {e}")
            return "DEFER"

    def forward_ra_response(self, from_controller, to_controller, response_type, timestamp, target_pair):
        """Forward RA responses (OK/DEFER)"""
        try:
            if to_controller in self.controllers:
                controller = self.controllers[to_controller]
                proxy = ServerProxy(controller.url, allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))
                proxy.receive_ra_response(from_controller, response_type, timestamp, target_pair)
                return "OK"
        except Exception as e:
            print(f"[ZOOKEEPER] RA response forwarding failed: {e}")
            return "FAIL"

    def initiate_berkeley_sync(self, controller_name):
        """Coordinate Berkeley clock synchronization"""
        print(f"[ZOOKEEPER] Initiating Berkeley sync for {controller_name}")

        if controller_name not in self.controllers:
            return "CONTROLLER_NOT_FOUND"

        controller = self.controllers[controller_name]
        try:
            proxy = ServerProxy(controller.url, allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            proxy.berkeley_cycle_once()
            return "OK"
        except Exception as e:
            print(f"[ZOOKEEPER] Berkeley sync initiation failed: {e}")
            return "FAIL"

    def get_client_list(self):
        """Provide client list for Berkeley sync"""
        return self.clients
    def debug_system_status(self):
        """Debug method to check system status"""
        print("[DEBUG] System Status:")
        print(f"[DEBUG] Controllers: {list(self.controllers.keys())}")
        for name, controller in self.controllers.items():
            print(f"[DEBUG] {name}: available={controller.is_available}, buffer={len(controller.active_requests)}")
        print(f"[DEBUG] Replica manager initialized: {hasattr(self, 'replica_manager')}")
        return "Debug complete"

    # Add these new RPC methods
    def request_data_access(self, client_id, data_type, operation_type):
        """Handle RTO officer data access requests"""
        print(f"[ZOOKEEPER] Data access request from {client_id}: {data_type} ({operation_type})")

        if operation_type == "read":
            # Get available server for reading
            server_name = self.replica_manager.get_available_read_server(data_type)
            if server_name:
                if self.replica_manager.acquire_read_lock(server_name):
                    chunks = self.replica_manager.get_chunk_info(data_type)
                    replica = self.replica_manager.replicas[server_name]
                    return {
                        'access_granted': True,
                        'server_name': server_name,
                        'server_url': f"http://localhost:7001",  # Replica server port
                        'chunks': chunks,
                        'data_type': data_type
                    }
            return {'access_granted': False, 'reason': 'No servers available for reading'}

        elif operation_type == "write":
            # Try to acquire write lock on all replicas
            locked_servers = self.replica_manager.acquire_write_lock(data_type)
            if locked_servers:
                return {
                    'access_granted': True,
                    'locked_servers': locked_servers,
                    'data_type': data_type
                }
            return {'access_granted': False, 'reason': 'Write lock unavailable - readers active'}

    def get_chunk_metadata(self, data_type):
        """Get chunk metadata using hashmap"""
        return self.replica_manager.get_chunk_metadata(data_type)

    def get_hash_ring_status(self):
        """Get hash ring status for debugging"""
        return {
            'server_load': self.replica_manager.hash_manager.get_server_load(),
            'ring_size': len(self.replica_manager.hash_manager.ring),
            'virtual_nodes_per_server': self.replica_manager.hash_manager.virtual_nodes
        }
    def release_data_access(self, client_id, server_name, operation_type):
        """Release data access locks"""
        if operation_type == "read":
            self.replica_manager.release_read_lock(server_name)
        elif operation_type == "write":
            self.replica_manager.release_write_lock([server_name])

        return "OK"

    def write_data(self, client_id, locked_servers, operation, *args, **kwargs):
        """Perform write operation on all replicas"""
        results = self.replica_manager.replicate_write(locked_servers, operation, *args, **kwargs)
        self.replica_manager.release_write_lock(locked_servers)
        return results
    def log_separator(self, title="", char="=", width=70):
        if title:
            padding = (width - len(title) - 2) // 2
            print(f"\n{char * padding} {title} {char * padding}")
        else:
            print(f"\n{char * width}")

    def get_available_controller(self) -> ControllerState:
        """Enhanced controller selection with dynamic scaling"""
        with self.lock:
            # First: Find completely free controllers
            free_controllers = [c for c in self.controllers.values()
                                if c.is_free() and len(c.active_requests) == 0]
            if free_controllers:
                controller = min(free_controllers, key=lambda c: c.total_processed)
                print(f"[ZOOKEEPER] Selected {controller.name} (completely free)")
                return controller

            # Second: Find controllers with buffer space
            available_controllers = [c for c in self.controllers.values() if c.is_free()]
            if available_controllers:
                controller = min(available_controllers, key=lambda c: len(c.active_requests))
                print(f"[ZOOKEEPER] Selected {controller.name} (buffer: {len(controller.active_requests)}/{BUFFER_SIZE})")
                return controller

            # Third: Try to create dynamic clone
            print(f"[ZOOKEEPER] All controllers busy! Attempting dynamic scaling...")
            clone_name, clone_url = self.clone_manager.create_dynamic_clone()
            if clone_name and clone_url:
                new_controller = ControllerState(clone_name, clone_url, is_dynamic=True)
                self.controllers[clone_name] = new_controller
                self.db.update_controller_status(
                    clone_name, url=clone_url, is_available=True, active_requests=0,
                    buffer_size=BUFFER_SIZE, total_processed=0, is_dynamic=True
                )
                print(f"[ZOOKEEPER] Dynamic scaling successful! Created {clone_name}")
                return new_controller

            # Fallback: Use least busy controller
            controller = min(self.controllers.values(), key=lambda c: len(c.active_requests))
            print(f"[ZOOKEEPER] Using overloaded controller {controller.name}")
            return controller

    def forward_request(self, method_name: str, *args, **kwargs):
        """Enhanced request forwarding with database logging"""
        request_id = str(uuid.uuid4())[:8]
        start_time = time.time()

        self.log_separator(f"LOAD BALANCER: {method_name.upper()}")
        print(f"[ZOOKEEPER] Request {request_id}: {method_name}{args}")

        controller = self.get_available_controller()
        controller.add_request(request_id)

        # Log request start
        self.db.log_request(request_id, method_name, args[0] if args else "",
                            controller.name, start_time)

        try:
            proxy = ServerProxy(controller.url, allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            method = getattr(proxy, method_name)
            result = method(*args, **kwargs)

            end_time = time.time()
            response_time = end_time - start_time

            print(f"[ZOOKEEPER] {controller.name} completed {request_id} in {response_time:.2f}s")

            # Update database
            controller.complete_request(request_id)
            self.db.log_request(request_id, method_name, args[0] if args else "",
                                controller.name, start_time, end_time, "completed")
            self.db.update_controller_status(controller.name,
                                             active_requests=len(controller.active_requests),
                                             total_processed=controller.total_processed)

            return result

        except Exception as e:
            end_time = time.time()
            print(f"[ZOOKEEPER] Error with {controller.name}: {e}")
            controller.complete_request(request_id)
            controller.is_available = True
            self.db.log_request(request_id, method_name, args[0] if args else "",
                                controller.name, start_time, end_time, "failed")

            # Retry with another controller
            return self.forward_request(method_name, *args, **kwargs)

    # RPC Methods
    def signal_controller(self, target_pair):
        return self.forward_request("signal_controller", target_pair)

    def vip_arrival(self, target_pair, priority=1, vehicle_id=None):
        return self.forward_request("vip_arrival", target_pair, priority, vehicle_id)

    def ping(self):
        return "ZooKeeper OK"

    def get_system_status(self):
        """RPC method for external clients to read system status"""
        return self.db.get_system_stats()

    def update_signal_status(self, signal_status):
        """RPC method for controllers to update signal status"""
        self.db.update_signal_status(signal_status)
        return "OK"

    def get_signal_status(self):
        """RPC method to get current signal status"""
        return self.db.get_signal_status()



    def request_data_access(self, client_id, data_type, operation_type):
        """Handle RTO officer data access requests with Reader-Writer logic"""
        self.log_separator(f"DATA ACCESS REQUEST: {client_id}")
        print(f"[ZOOKEEPER] Request from {client_id}: {data_type} ({operation_type})")

        if operation_type == "read":
            # Get available server for reading
            server_name = self.replica_manager.get_available_read_server(data_type)
            if server_name:
                if self.replica_manager.acquire_read_lock(server_name):
                    chunks = self.replica_manager.get_chunk_info(data_type)
                    # Map server names to actual ports
                    port_mapping = {'server_1': 7001, 'server_2': 7002, 'server_3': 7003}
                    server_port = port_mapping.get(server_name, 7001)

                    print(f"[ZOOKEEPER] READ access granted to {client_id} on {server_name}")
                    return {
                        'access_granted': True,
                        'server_name': server_name,
                        'server_url': f"http://localhost:{server_port}",
                        'chunks': chunks,
                        'data_type': data_type,
                        'client_id': client_id
                    }

            print(f"[ZOOKEEPER] READ access denied to {client_id} - No servers available")
            return {'access_granted': False, 'reason': 'No servers available for reading'}

        elif operation_type == "write":
            # Try to acquire write lock on all replicas
            locked_servers = self.replica_manager.acquire_write_lock(data_type)
            if locked_servers:
                print(f"[ZOOKEEPER] WRITE access granted to {client_id} on {locked_servers}")
                return {
                    'access_granted': True,
                    'locked_servers': locked_servers,
                    'data_type': data_type,
                    'client_id': client_id
                }

            print(f"[ZOOKEEPER] WRITE access denied to {client_id} - Writer active or readers present")
            return {'access_granted': False, 'reason': 'Write lock unavailable - readers active or writer present'}

    def release_data_access(self, client_id, server_name, operation_type):
        """Release data access locks"""
        print(f"[ZOOKEEPER] {client_id} releasing {operation_type} lock on {server_name}")

        if operation_type == "read":
            self.replica_manager.release_read_lock(server_name)
        elif operation_type == "write":
            self.replica_manager.release_write_lock([server_name])

        return "OK"

    def write_data(self, client_id, locked_servers, operation, *args, **kwargs):
        """Perform write operation on all replicas"""
        print(f"[ZOOKEEPER] {client_id} performing write operation: {operation}")

        results = self.replica_manager.replicate_write(locked_servers, operation, *args, **kwargs)
        self.replica_manager.release_write_lock(locked_servers)

        print(f"[ZOOKEEPER] Write operation completed on {len(locked_servers)} replicas")
        return results

class DataReplicaManager:
    def __init__(self, db_path_prefix="replica"):
        self.replicas = {}
        self.metadata_lock = threading.Lock()
        self.chunk_size = 5  # 5 records per chunk
        self.replica_count = 3  # 3 replicas for each data type

        # Initialize hash manager
        replica_names = [f"server_{i + 1}" for i in range(self.replica_count)]
        self.hash_manager = ConsistentHashManager(replica_names)

        # Initialize 3 replica databases
        for i in range(self.replica_count):
            replica_name = f"server_{i + 1}"
            replica_path = f"{db_path_prefix}_{i + 1}.db"
            self.replicas[replica_name] = {
                'db_path': replica_path,
                'db_manager': DatabaseManager(replica_path),
                'readers_count': 0,
                'writer_active': False,
                'lock': threading.Lock(),
                'chunks': {}  # chunk_id -> data
            }

        print(f"[REPLICA-MANAGER] Initialized {self.replica_count} replicas with consistent hashing")

    def get_chunk_info(self, data_type):
        """Get chunk information using hashmap"""
        # Get data from first replica to calculate chunks
        replica = self.replicas['server_1']
        if data_type == "signal_status":
            data = replica['db_manager'].get_signal_status()
        elif data_type == "controller_status":
            stats = replica['db_manager'].get_system_stats()
            data = {f"controller_{i}": ctrl for i, ctrl in enumerate(stats.get('controllers', []))}
        else:
            data = {}

        # Use hash manager to create chunks
        chunks = self.hash_manager.map_data_to_chunks(data_type, data)

        return {
            'total_chunks': len(chunks),
            'chunk_size': self.chunk_size,
            'chunk_distribution': {chunk_id: info['servers'] for chunk_id, info in chunks.items()}
        }

    def get_available_read_server(self, data_type):
        """Get an available server for reading (Reader-Writer algorithm)"""
        available_servers = []

        for server_name, replica in self.replicas.items():
            with replica['lock']:
                if not replica['writer_active']:  # No writer active
                    available_servers.append(server_name)

        return available_servers[0] if available_servers else None

    def get_available_read_server_for_chunk(self, data_type, chunk_id=None):
        """Get available server for reading specific chunk using hashmap"""
        if chunk_id:
            # Get servers responsible for this specific chunk
            chunk_key = f"{data_type}_chunk_{chunk_id}"
            responsible_servers = self.hash_manager.get_chunk_locations(chunk_key)
            if not responsible_servers:
                responsible_servers = list(self.replicas.keys())
        else:
            # Any server can handle full data requests
            responsible_servers = list(self.replicas.keys())

        # Find available server from responsible ones
        for server_name in responsible_servers:
            replica = self.replicas[server_name]
            with replica['lock']:
                if not replica['writer_active']:  # No writer active
                    return server_name

        return None

    def get_chunk_metadata(self, data_type):
        """Get detailed chunk metadata using hashmap"""
        chunks_info = self.get_chunk_info(data_type)
        server_load = self.hash_manager.get_server_load()

        return {
            'data_type': data_type,
            'total_chunks': chunks_info['total_chunks'],
            'chunk_size': self.chunk_size,
            'chunk_distribution': chunks_info['chunk_distribution'],
            'server_load': server_load,
            'hash_ring_info': {
                'virtual_nodes_per_server': self.hash_manager.virtual_nodes,
                'total_virtual_nodes': len(self.hash_manager.ring)
            }
        }
    def acquire_read_lock(self, server_name):
        """Acquire read lock on a server"""
        replica = self.replicas[server_name]
        with replica['lock']:
            if not replica['writer_active']:
                replica['readers_count'] += 1
                return True
        return False

    def release_read_lock(self, server_name):
        """Release read lock on a server"""
        replica = self.replicas[server_name]
        with replica['lock']:
            replica['readers_count'] = max(0, replica['readers_count'] - 1)

    def acquire_write_lock(self, data_type):
        """Acquire write lock on all replicas for a data type"""
        locked_servers = []

        for server_name, replica in self.replicas.items():
            with replica['lock']:
                if replica['readers_count'] == 0 and not replica['writer_active']:
                    replica['writer_active'] = True
                    locked_servers.append(server_name)
                else:
                    # Rollback locks if can't get all
                    for locked_server in locked_servers:
                        self.replicas[locked_server]['writer_active'] = False
                    return None

        return locked_servers

    def release_write_lock(self, server_names):
        """Release write lock on all servers"""
        for server_name in server_names:
            replica = self.replicas[server_name]
            with replica['lock']:
                replica['writer_active'] = False

    def replicate_write(self, server_names, operation, *args, **kwargs):
        """Perform write operation on all replicas"""
        results = []
        from xmlrpc.client import ServerProxy
        for server_name in server_names:
            port_mapping = {'server_1': 7001, 'server_2': 7002, 'server_3': 7003}
            proxy = ServerProxy(f"http://localhost:{port_mapping[server_name]}", allow_none=True)

            if operation == "update_signal_status":
                proxy.update_signal_status(*args, **kwargs)
            elif operation == "update_controller_status":
                proxy.update_controller_status(*args, **kwargs)
            # Add more operations as needed

            results.append(f"{server_name}: success")

        return results


class ConsistentHashManager:
    def __init__(self, replica_servers, virtual_nodes=3):
        self.replica_servers = replica_servers  # ['server_1', 'server_2', 'server_3']
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # Hash ring
        self.chunk_map = {}  # chunk_id -> [server1, server2, server3]
        self.server_chunks = {}  # server -> [chunk_ids]
        self._build_ring()

    def _hash(self, key):
        """Generate hash for a key"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self):
        """Build the consistent hash ring"""
        self.ring = {}
        for server in self.replica_servers:
            for i in range(self.virtual_nodes):
                virtual_key = f"{server}:{i}"
                hash_value = self._hash(virtual_key)
                self.ring[hash_value] = server

        # Sort the ring
        self.sorted_hashes = sorted(self.ring.keys())
        print(f"[HASH-MANAGER] Built hash ring with {len(self.ring)} virtual nodes")

    def get_servers_for_chunk(self, chunk_id, replication_factor=3):
        """Get servers responsible for a chunk using consistent hashing"""
        chunk_hash = self._hash(str(chunk_id))

        # Find the position in the ring
        servers = []
        for hash_value in self.sorted_hashes:
            if hash_value >= chunk_hash:
                server = self.ring[hash_value]
                if server not in servers:
                    servers.append(server)
                if len(servers) >= replication_factor:
                    break

        # Wrap around if needed
        if len(servers) < replication_factor:
            for hash_value in self.sorted_hashes:
                server = self.ring[hash_value]
                if server not in servers:
                    servers.append(server)
                if len(servers) >= replication_factor:
                    break

        return servers[:replication_factor]

    def map_data_to_chunks(self, data_type, data_items):
        """Map data items to chunks and assign servers"""
        chunk_size = 5  # items per chunk
        chunks = {}
        chunk_id = 0

        items_list = list(data_items.items()) if isinstance(data_items, dict) else list(data_items)

        # Group items into chunks
        for i in range(0, len(items_list), chunk_size):
            chunk_data = items_list[i:i + chunk_size]
            chunk_key = f"{data_type}_chunk_{chunk_id}"

            # Get servers for this chunk
            responsible_servers = self.get_servers_for_chunk(chunk_key)

            chunks[chunk_key] = {
                'chunk_id': chunk_id,
                'data': chunk_data,
                'servers': responsible_servers,
                'data_type': data_type
            }

            # Update chunk mapping
            self.chunk_map[chunk_key] = responsible_servers

            # Update server chunks mapping
            for server in responsible_servers:
                if server not in self.server_chunks:
                    self.server_chunks[server] = []
                self.server_chunks[server].append(chunk_key)

            chunk_id += 1

        print(f"[HASH-MANAGER] Mapped {len(items_list)} {data_type} items to {len(chunks)} chunks")
        return chunks

    def get_chunk_locations(self, chunk_id):
        """Get all server locations for a chunk"""
        chunk_key = f"chunk_{chunk_id}" if not chunk_id.startswith("chunk_") else chunk_id
        return self.chunk_map.get(chunk_key, [])

    def get_server_load(self):
        """Get load distribution across servers"""
        load_info = {}
        for server in self.replica_servers:
            chunk_count = len(self.server_chunks.get(server, []))
            load_info[server] = {
                'chunk_count': chunk_count,
                'chunks': self.server_chunks.get(server, [])
            }
        return load_info
if __name__ == "__main__":
    print("=" * 35)
    print("ENHANCED ZOOKEEPER LOAD BALANCER")
    print("=" * 35)
    print(f"Buffer size: {BUFFER_SIZE} | Max dynamic clones: {MAX_DYNAMIC_CLONES}")
    print(f"Database: {DB_PATH}")

    lb = ZooKeeperLoadBalancer()

    # Start health check
    #health_thread = threading.Thread(target=lb.health_check_loop, daemon=True)
    #health_thread.start()

    # Start RPC server
    server = SimpleXMLRPCServer(("0.0.0.0", ZOOKEEPER_PORT), allow_none=True)
    server.register_function(lb.signal_controller, "signal_controller")
    server.register_function(lb.vip_arrival, "vip_arrival")
    server.register_function(lb.ping, "ping")
    server.register_function(lb.get_system_status, "get_system_status")
    server.register_function(lb.update_signal_status, "update_signal_status")
    server.register_function(lb.get_signal_status, "get_signal_status")
    # In the main section, add these registrations:
    server.register_function(lb.request_data_access, "request_data_access")
    server.register_function(lb.release_data_access, "release_data_access")
    server.register_function(lb.write_data, "write_data")
    # Add to server registration in trial_zookeeper.py main section:
    server.register_function(lb.get_chunk_metadata, "get_chunk_metadata")
    server.register_function(lb.get_hash_ring_status, "get_hash_ring_status")
    # Register it:
    server.register_function(lb.debug_system_status, "debug_system_status")
    print(f"Enhanced ZooKeeper ready on port {ZOOKEEPER_PORT}")
    print("Features: Dynamic Scaling | Database | Performance Optimized")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nZooKeeper shutting down...")
