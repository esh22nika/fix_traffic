# zookeeper.py - Final, corrected version with ready flag
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List, Optional, Tuple
import uuid
import sqlite3
from collections import deque, defaultdict
import hashlib
import logging
import random
import psutil
import os

# ====== CONFIGURATION ======
ZOOKEEPER_PORT = 6000
RESPONSE_TIMEOUT = 20
DB_PATH = "traffic_system.db"

# Controllers configuration
BASE_CONTROLLERS = {
    "controller": "http://127.0.0.1:8000",
    "controller_clone": "http://127.0.0.1:8001"
}

# Client configuration for Berkeley sync
BERKELEY_CLIENTS = {
    "t_signal": "http://127.0.0.1:7000",
    "p_signal": "http://127.0.0.1:9000"
}

# Replica servers configuration (GFS-style) - COMMENTED OUT: moved to master_server
'''
REPLICA_SERVERS = {
    "server_1": {"url": "http://127.0.0.1:7001", "port": 7001},
    "server_2": {"url": "http://127.0.0.1:7002", "port": 7002},
    "server_3": {"url": "http://127.0.0.1:7003", "port": 7003}
}
'''

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def log_memory_usage():
    process = psutil.Process(os.getpid())
    memory_mb = process.memory_info().rss / 1024 / 1024
    print(f"ZooKeeper memory usage: {memory_mb:.2f} MB")


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
            conn.execute('''
                        CREATE TABLE IF NOT EXISTS signal_change_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            signal_id TEXT,
                            old_status TEXT,
                            new_status TEXT,
                            change_source TEXT,
                            change_reason TEXT,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
            try:
                conn.execute('ALTER TABLE signal_status ADD COLUMN last_source TEXT')
                conn.execute('ALTER TABLE signal_status ADD COLUMN last_reason TEXT')
            except sqlite3.OperationalError:
                pass  # Columns already exist
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
            logger.info(f"Database initialized at {self.db_path}")

    def update_signal_status(self, signal_status_dict):
        """Update signal status in database"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for signal_id, status in signal_status_dict.items():
                    signal_id_str = str(signal_id)
                    conn.execute(
                        'INSERT OR REPLACE INTO signal_status (signal_id, status, last_updated) VALUES (?, ?, CURRENT_TIMESTAMP)',
                        (signal_id_str, status)
                    )
                conn.commit()

    def get_signal_status(self):
        """Get current signal status"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute('SELECT signal_id, status FROM signal_status')
                return {row[0]: row[1] for row in cursor.fetchall()}

    def get_system_stats(self):
        """Get comprehensive system statistics"""
        with self.lock:
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
                    'timestamp': time.time()
                }

    def update_signal_status_with_reason(self, signal_status_dict, source, reason):
        """Update signal status with source attribution"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                for signal_id, status in signal_status_dict.items():
                    signal_id_str = str(signal_id)

                    # Get current status for history
                    cursor = conn.execute(
                        'SELECT status FROM signal_status WHERE signal_id = ?',
                        (signal_id_str,)
                    )
                    result = cursor.fetchone()
                    old_status = result[0] if result else "UNKNOWN"

                    # Update current status with source
                    conn.execute(
                        '''INSERT OR REPLACE INTO signal_status 
                           (signal_id, status, last_updated, last_source, last_reason) 
                           VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?)''',
                        (signal_id_str, status, source, reason)
                    )

                    # Log to history table
                    if old_status != status:
                        conn.execute(
                            '''INSERT INTO signal_change_history 
                               (signal_id, old_status, new_status, change_source, change_reason)
                               VALUES (?, ?, ?, ?, ?)''',
                            (signal_id_str, old_status, status, source, reason)
                        )

                conn.commit()
        return "OK"

    def get_signal_status_with_history(self, limit=50):
        """Get current signal status with recent change history"""
        with self.lock:
            with sqlite3.connect(self.db_path) as conn:
                # Current status
                cursor = conn.execute('''
                    SELECT signal_id, status, last_source, last_reason, last_updated 
                    FROM signal_status 
                    ORDER BY signal_id
                ''')
                current_status = {}
                for row in cursor.fetchall():
                    current_status[row[0]] = {
                        'status': row[1],
                        'source': row[2] or 'unknown',
                        'reason': row[3] or 'unknown',
                        'last_updated': row[4]
                    }

                # Recent changes
                cursor = conn.execute('''
                    SELECT signal_id, old_status, new_status, change_source, change_reason, timestamp
                    FROM signal_change_history 
                    ORDER BY timestamp DESC 
                    LIMIT ?
                ''', (limit,))

                change_history = []
                for row in cursor.fetchall():
                    change_history.append({
                        'signal_id': row[0],
                        'old_status': row[1],
                        'new_status': row[2],
                        'source': row[3],
                        'reason': row[4],
                        'timestamp': row[5]
                    })

                return {
                    'current_status': current_status,
                    'recent_changes': change_history
                }

# COMMENTED OUT: GFS functionality moved to master_server
'''
class ConsistentHashManager:
    """Hash ring for chunk distribution across replica servers"""

    def __init__(self, servers, virtual_nodes=150):  # Increased virtual nodes for better distribution
        self.servers = servers
        self.virtual_nodes = virtual_nodes
        self.ring = {}
        self.sorted_hashes = []
        self._build_ring()

    def _hash(self, key):
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def _build_ring(self):
        for server in self.servers:
            for i in range(self.virtual_nodes):
                virtual_key = f"{server}:{i}"
                hash_value = self._hash(virtual_key)
                self.ring[hash_value] = server

        self.sorted_hashes = sorted(self.ring.keys())
        logger.info(f"Built consistent hash ring with {len(self.ring)} virtual nodes")

    def get_servers_for_key(self, key, count=3):
        """Get servers responsible for a key"""
        key_hash = self._hash(str(key))
        servers = []

        start_idx = 0
        for i, hash_val in enumerate(self.sorted_hashes):
            if hash_val >= key_hash:
                start_idx = i
                break

        # Get servers starting from the hash position
        checked = set()
        for i in range(len(self.sorted_hashes)):
            idx = (start_idx + i) % len(self.sorted_hashes)
            server = self.ring[self.sorted_hashes[idx]]
            if server not in checked:
                servers.append(server)
                checked.add(server)
                if len(servers) >= count:
                    break

        return servers[:count]

class ReaderWriterLock:
    """Reader-Writer lock with starvation prevention using FIFO queue"""

    def __init__(self):
        self.lock = threading.Lock()
        self.readers = 0
        self.writers_waiting = 0
        self.writer_active = False
        self.read_condition = threading.Condition(self.lock)
        self.write_condition = threading.Condition(self.lock)
        self.request_queue = deque()  # FIFO queue to prevent starvation
        self.queue_condition = threading.Condition(self.lock)

    def acquire_read(self, client_id="unknown"):
        request_id = f"READ_{client_id}_{time.time()}"
        with self.lock:
            self.request_queue.append(('read', request_id, threading.current_thread()))
            logger.info(f"[RW-LOCK] {client_id} queued for READ access (queue size: {len(self.request_queue)})")

            while True:
                # Check if this read request is at the front and can proceed
                if (self.request_queue and
                        self.request_queue[0][1] == request_id and
                        not self.writer_active and
                        (self.writers_waiting == 0 or self.readers > 0)):
                    self.readers += 1
                    self.request_queue.popleft()  # Remove from queue
                    logger.info(f"[RW-LOCK] {client_id} acquired READ lock (readers: {self.readers})")
                    return

                self.queue_condition.wait()

    def release_read(self, client_id="unknown"):
        with self.lock:
            self.readers -= 1
            logger.info(f"[RW-LOCK] {client_id} released READ lock (readers: {self.readers})")
            if self.readers == 0:
                self.queue_condition.notify_all()  # Wake up waiting writers

    def acquire_write(self, client_id="unknown"):
        request_id = f"WRITE_{client_id}_{time.time()}"
        with self.lock:
            self.writers_waiting += 1
            self.request_queue.append(('write', request_id, threading.current_thread()))
            logger.info(f"[RW-LOCK] {client_id} queed for WRITE access (queue size: {len(self.request_queue)})")

            while True:
                # Check if this write request is at the front and can proceed
                if (self.request_queue and
                        self.request_queue[0][1] == request_id and
                        self.readers == 0 and
                        not self.writer_active):
                    self.writers_waiting -= 1
                    self.writer_active = True
                    self.request_queue.popleft()  # Remove from queue
                    logger.info(f"[RW-LOCK] {client_id} acquired WRITE lock")
                    return

                self.queue_condition.wait()

    def release_write(self, client_id="unknown"):
        with self.lock:
            self.writer_active = False
            logger.info(f"[RW-LOCK] {client_id} released WRITE lock")
            self.queue_condition.notify_all()  # Wake up all waiting requests

class ReplicaManager:
    """Google File System style replica management with proper chunking"""

    def __init__(self):
        self.hash_manager = ConsistentHashManager(list(REPLICA_SERVERS.keys()))
        self.locks = {server: ReaderWriterLock() for server in REPLICA_SERVERS.keys()}
        self.chunk_size = 3  # Smaller chunks for better distribution
        self.replication_factor = 3
        self.active_reads = defaultdict(int)
        self.active_writes = defaultdict(int)

    def _create_chunks(self, data_items, data_type):
        """Create chunks from data items"""
        if isinstance(data_items, dict):
            items = list(data_items.items())
        else:
            items = list(data_items) if data_items else []

        if not items:
            # Create at least one empty chunk
            items = [("placeholder", "empty")]

        chunks = {}
        for i in range(0, len(items), self.chunk_size):
            chunk_data = items[i:i + self.chunk_size]
            chunk_id = f"{data_type}_chunk_{i // self.chunk_size}"

            # Get servers for this chunk using consistent hashing
            responsible_servers = self.hash_manager.get_servers_for_key(chunk_id, self.replication_factor)

            chunks[chunk_id] = {
                'chunk_id': i // self.chunk_size,
                'data': chunk_data,
                'servers': responsible_servers,
                'size': len(chunk_data)
            }

        return chunks

    def get_chunk_metadata(self, data_type):
        """Get detailed chunk metadata"""
        try:
            # Get data from first available replica using XML-RPC
            sample_data = None
            for server_name, server_info in REPLICA_SERVERS.items():
                try:
                    proxy = ServerProxy(server_info['url'], allow_none=True,
                                        transport=TimeoutTransport(5))
                    if data_type == "signal_status":
                        sample_data = proxy.get_signal_status()
                    elif data_type == "system_status":
                        sample_data = proxy.get_system_stats()
                    if sample_data:
                        break  # Success, break out of loop
                except Exception as e:
                    logger.warning(f"Failed to get sample data from {server_name}: {e}")
                    continue

            if sample_data is None:
                # Return default metadata if no data available
                sample_data = {"placeholder": "empty"}

            chunks = self._create_chunks(sample_data, data_type)

            metadata = {
                'total_chunks': len(chunks),
                'chunk_size': self.chunk_size,
                'replication_factor': self.replication_factor,
                'data_type': data_type,
                'chunk_distribution': {chunk_id: chunk_info['servers']
                                       for chunk_id, chunk_info in chunks.items()},
                'chunk_details': {chunk_id: {
                    'size': chunk_info['size'],
                    'servers': chunk_info['servers'],
                    'chunk_number': chunk_info['chunk_id']
                } for chunk_id, chunk_info in chunks.items()}
            }

            logger.info(
                f"[GFS] Generated metadata for {data_type}: {len(chunks)} chunks across {len(REPLICA_SERVERS)} servers")
            return metadata

        except Exception as e:
            logger.error(f"[GFS] Failed to generate metadata for {data_type}: {e}")
            # Return minimal metadata to prevent empty returns
            return {
                'total_chunks': 1,
                'chunk_size': self.chunk_size,
                'replication_factor': self.replication_factor,
                'data_type': data_type,
                'chunk_distribution': {f"{data_type}_chunk_0": list(REPLICA_SERVERS.keys())},
                'chunk_details': {f"{data_type}_chunk_0": {
                    'size': 1,
                    'servers': list(REPLICA_SERVERS.keys()),
                    'chunk_number': 0
                }}
            }

    def acquire_read_access(self, data_type, client_id):
        """Acquire read access using load balancing"""
        chunk_key = f"{data_type}_read_{client_id}"
        candidate_servers = self.hash_manager.get_servers_for_key(chunk_key, 1)

        for server in candidate_servers:
            try:
                self.locks[server].acquire_read(client_id)
                self.active_reads[server] += 1
                logger.info(f"[GFS] {client_id} acquired READ access on {server}")
                return server
            except Exception as e:
                logger.warning(f"[GFS] Failed to acquire read on {server}: {e}")
                continue

        # Fallback: try any server
        for server in REPLICA_SERVERS.keys():
            try:
                self.locks[server].acquire_read(client_id)
                self.active_reads[server] += 1
                logger.info(f"[GFS] {client_id} acquired READ access on {server} (fallback)")
                return server
            except Exception as e:
                continue

        logger.error(f"[GFS] Failed to acquire READ access for {client_id}")
        return None

    def release_read_access(self, server, client_id):
        """Release read access"""
        try:
            self.locks[server].release_read(client_id)
            self.active_reads[server] = max(0, self.active_reads[server] - 1)
            logger.info(f"[GFS] {client_id} released READ access on {server}")
        except Exception as e:
            logger.error(f"[GFS] Failed to release read access for {client_id}: {e}")

    def acquire_write_access(self, data_type, client_id):
        """Acquire write access to all replicas with timeout"""
        servers = list(REPLICA_SERVERS.keys())
        acquired = []

        logger.info(f"[GFS] {client_id} attempting to acquire WRITE access on {len(servers)} servers")

        try:
            # Try to acquire locks on all servers with timeout
            for server in servers:
                try:
                    # Use a timeout mechanism
                    lock_acquired = False
                    start_time = time.time()

                    def acquire_with_timeout():
                        nonlocal lock_acquired
                        self.locks[server].acquire_write(client_id)
                        lock_acquired = True

                    thread = threading.Thread(target=acquire_with_timeout)
                    thread.daemon = True
                    thread.start()
                    thread.join(timeout=5.0)  # 5 second timeout per server

                    if lock_acquired:
                        acquired.append(server)
                        self.active_writes[server] += 1
                    else:
                        raise TimeoutError(f"Timeout acquiring write lock on {server}")

                except Exception as e:
                    logger.warning(f"[GFS] Failed to acquire write lock on {server}: {e}")
                    # Rollback acquired locks
                    for acq_server in acquired:
                        self.locks[acq_server].release_write(client_id)
                        self.active_writes[acq_server] -= 1
                    return None

            logger.info(f"[GFS] {client_id} acquired WRITE access on all {len(acquired)} servers")
            return acquired

        except Exception as e:
            logger.error(f"[GFS] Write access acquisition failed for {client_id}: {e}")
            return None

    def release_write_access(self, servers, client_id):
        """Release write access on all servers"""
        for server in servers:
            try:
                self.locks[server].release_write(client_id)
                self.active_writes[server] = max(0, self.active_writes[server] - 1)
            except Exception as e:
                logger.error(f"[GFS] Failed to release write access on {server}: {e}")

        logger.info(f"[GFS] {client_id} released WRITE access on {len(servers)} servers")

    def replicate_write(self, servers, operation, client_id, *args, **kwargs):
        """Perform write on all replicas with verification"""
        results = {}
        successful_writes = 0

        logger.info(f"[GFS] {client_id} performing {operation} on {len(servers)} replicas")

        for server in servers:
            try:
                proxy = ServerProxy(REPLICA_SERVERS[server]['url'], allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))

                if operation == "update_signal_status":
                    result = proxy.update_signal_status(*args)
                elif operation == "update_controller_status":
                    result = proxy.update_controller_status(*args, **kwargs)
                else:
                    result = "UNKNOWN_OPERATION"

                results[server] = result
                if result == "OK":
                    successful_writes += 1

            except Exception as e:
                results[server] = f"ERROR: {e}"
                logger.error(f"[GFS] Write failed on {server}: {e}")

        logger.info(f"[GFS] Write operation completed: {successful_writes}/{len(servers)} successful")
        return results
'''


class ControllerState:
    """State management for controllers with RA support"""

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.active_requests = 0
        self.total_processed = 0
        self.is_available = True
        self.last_heartbeat = time.time()
        self.lock = threading.Lock()

        # Ricart-Agrawala state
        self.requesting_cs = False
        self.in_cs = False
        self.deferred_replies = []
        self.lamport_clock = 0

    def add_request(self):
        with self.lock:
            self.active_requests += 1

    def complete_request(self):
        with self.lock:
            self.active_requests = max(0, self.active_requests - 1)
            self.total_processed += 1

    def get_load(self):
        with self.lock:
            return self.active_requests


class ZooKeeperLoadBalancer:
    def __init__(self):
        self.db = DatabaseManager(DB_PATH)
        # COMMENTED OUT: Replica manager moved to master_server
        # self.replica_manager = ReplicaManager()
        self.controllers = {}
        self.lock = threading.Lock()
        self.is_ready = threading.Event()

        # Ricart-Agrawala state
        self.lamport_clock = 0
        self.clock_lock = threading.Lock()
        self.pending_vip_requests = {"12": [], "34": []}
        self.ra_votes = {}  # Track RA voting

        # Initialize controllers
        for name, url in BASE_CONTROLLERS.items():
            self.controllers[name] = ControllerState(name, url)

        logger.info("ZooKeeper Load Balancer initialized with RA support")
        self.wait_for_controllers_ready()

    def wait_for_controllers_ready(self):
        """Wait for all controllers to be ready"""
        for name, controller in self.controllers.items():
            try:
                proxy = ServerProxy(controller.url, allow_none=True,
                                    transport=TimeoutTransport(15))
                for i in range(10):  # Retry 10 times
                    try:
                        if proxy.is_ready():
                            logger.info(f"Controller {name} is ready")
                            break
                        time.sleep(1)
                    except:
                        time.sleep(1)
            except Exception as e:
                logger.warning(f"Controller {name} not ready: {e}")

    def increment_lamport_clock(self):
        """Increment Lamport clock for RA"""
        with self.clock_lock:
            self.lamport_clock += 1
            return self.lamport_clock

    def update_lamport_clock(self, received_timestamp):
        """Update Lamport clock on message receipt"""
        with self.clock_lock:
            self.lamport_clock = max(self.lamport_clock, received_timestamp) + 1

    # ====== RICART-AGRAWALA IMPLEMENTATION ======
    def forward_ra_request(self, from_controller, to_controller, timestamp, target_pair, request_type,
                           requester_info=""):
        """Forward RA request between controllers AND p_signal"""
        logger.info(
            f"[RA] Forwarding RA request: {from_controller} -> {to_controller} (ts={timestamp}, type={request_type})")

        self.update_lamport_clock(timestamp)

        try:
            if to_controller == "p_signal":
                proxy = ServerProxy(BERKELEY_CLIENTS["p_signal"], allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))
                response = proxy.p_signal_ra(target_pair, timestamp, from_controller, request_type)
                logger.info(f"[RA] p_signal RA vote: {response}")
                return response

            elif to_controller in self.controllers:
                controller = self.controllers[to_controller]
                proxy = ServerProxy(controller.url, allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))
                response = proxy.receive_ra_request(from_controller, timestamp, target_pair, request_type)
                logger.info(f"[RA] {to_controller} RA response: {response}")
                return response

            return "DEFER"
        except Exception as e:
            logger.error(f"[RA] Failed to forward request: {e}")
            return "DEFER"

    def forward_ra_response(self, from_controller, to_controller, response_type, timestamp, target_pair):
        """Forward RA response (OK/DEFER) back to requesting controller"""
        logger.info(f"[RA] Forwarding RA response: {from_controller} -> {to_controller} ({response_type})")

        try:
            if to_controller in self.controllers:
                controller = self.controllers[to_controller]
                proxy = ServerProxy(controller.url, allow_none=True,
                                    transport=TimeoutTransport(RESPONSE_TIMEOUT))
                proxy.receive_ra_response(from_controller, response_type, timestamp, target_pair)
                return "OK"
        except Exception as e:
            logger.error(f"[RA] Failed to forward response: {e}")
            return "FAIL"

    def request_ped_ack(self, controller_name, target_pair, timestamp, request_type, requester_info=""):
        """SECOND p_signal OK: Get pedestrian acknowledgment (separate from RA voting)"""
        logger.info(f"[PEDESTRIAN] {controller_name} requesting pedestrian clearance for {target_pair}")

        try:
            proxy = ServerProxy(BERKELEY_CLIENTS["p_signal"], allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            response = proxy.p_signal(target_pair)  # Non-RA pedestrian safety check
            logger.info(f"[PEDESTRIAN] Safety check response: {response}")
            return response
        except Exception as e:
            logger.error(f"[PEDESTRIAN] Failed to get safety clearance: {e}")
            return "DENY"

    # ====== VIP DEADLOCK RESOLUTION ======
    def handle_vip_deadlock(self, vip1_info, vip2_info):
        """Handle VIP deadlock: let already-green junction go first"""
        vip1_pair = vip1_info['target_pair']
        vip2_pair = vip2_info['target_pair']

        current_green = self.get_current_green_pair()

        logger.warning(f"[VIP-DEADLOCK] DETECTED: VIP at {vip1_pair} and VIP at {vip2_pair}")
        logger.warning(f"[VIP-DEADLOCK] Current green signals: {current_green}")

        if current_green == vip1_pair:
            first, second = vip1_info, vip2_info
            logger.info(f"[VIP-DEADLOCK] RESOLVED: {vip1_pair} (already green) goes first, then {vip2_pair}")
        elif current_green == vip2_pair:
            first, second = vip2_info, vip1_info
            logger.info(f"[VIP-DEADLOCK] RESOLVED: {vip2_pair} (already green) goes first, then {vip1_pair}")
        else:
            # Neither is green, use timestamp
            if vip1_info['timestamp'] < vip2_info['timestamp']:
                first, second = vip1_info, vip2_info
            else:
                first, second = vip2_info, vip1_info
            logger.info(f"[VIP-DEADLOCK] RESOLVED: Using timestamp priority - {first['target_pair']} first")

        return first, second

    # ====== BERKELEY CLOCK SYNC (7 STEPS) ======
    def coordinate_berkeley_sync(self, time_server_controller):
        """7-Step Berkeley Clock Synchronization"""
        logger.info(f"[BERKELEY-STEP1] Starting 7-step sync with {time_server_controller} as time server")

        try:
            # Step 1: Time server gets its current time
            server_time = time.time()
            logger.info(f"[BERKELEY-STEP1] Time server time: {server_time}")

            # Step 2-3: Broadcast time and collect client responses
            client_offsets = {}
            all_clients = list(BERKELEY_CLIENTS.items()) + [(name, ctrl.url) for name, ctrl in self.controllers.items()]

            logger.info(f"[BERKELEY-STEP2-3] Broadcasting time to {len(all_clients)} clients")
            for client_name, client_url in all_clients:
                if client_name == time_server_controller:
                    client_offsets[client_name] = 0.0
                    logger.info(f"[BERKELEY-STEP3] {client_name} (time server) offset: 0.00s")
                    continue

                try:
                    proxy = ServerProxy(client_url, allow_none=True,
                                        transport=TimeoutTransport(RESPONSE_TIMEOUT))
                    client_offset = proxy.get_clock_value(server_time)
                    client_offsets[client_name] = float(client_offset)
                    logger.info(f"[BERKELEY-STEP3] {client_name} offset: {client_offset:+.2f}s")
                except Exception as e:
                    logger.warning(f"[BERKELEY-STEP3] Failed to get offset from {client_name}: {e}")
                    client_offsets[client_name] = 0.0

            # Step 4: Calculate average offset
            avg_offset = sum(client_offsets.values()) / len(client_offsets)
            new_time = server_time + avg_offset
            logger.info(f"[BERKELEY-STEP4] Average offset: {avg_offset:+.2f}s, New synchronized time: {new_time}")

            # Step 5-7: Set new time on all clients
            logger.info(f"[BERKELEY-STEP5-7] Setting synchronized time on all clients")
            for client_name, client_url in all_clients:
                try:
                    proxy = ServerProxy(client_url, allow_none=True,
                                        transport=TimeoutTransport(RESPONSE_TIMEOUT))
                    proxy.set_time(new_time)
                    logger.info(f"[BERKELEY-STEP7] {client_name} synchronized successfully")
                except Exception as e:
                    logger.warning(f"[BERKELEY-STEP7] Failed to sync {client_name}: {e}")

            logger.info(f"[BERKELEY] 7-step synchronization COMPLETED")
            return "SYNC_COMPLETE"

        except Exception as e:
            logger.error(f"[BERKELEY] Synchronization FAILED: {e}")
            return "SYNC_FAILED"

    # ====== LOAD BALANCING ======
    def get_least_loaded_controller(self):
        """Get controller with lowest load (proper dynamic calculation)"""
        with self.lock:
            if not self.controllers:
                return None

            # Filter out unavailable controllers
            available_controllers = {name: controller for name, controller in self.controllers.items() if
                                     controller.is_available}

            if not available_controllers:
                return None

            # Calculate detailed load for each controller
            controller_loads = []
            for name, controller in available_controllers.items():
                current_load = controller.get_load()
                total_processed = controller.total_processed

                # Combined score: lower is better
                combined_score = (current_load * 2) + (total_processed * 0.1)
                controller_loads.append((combined_score, current_load, total_processed, name, controller))

                logger.debug(
                    f"[LOAD-BALANCE] {name}: load={current_load}, processed={total_processed}, score={combined_score:.2f}")

            # Sort by combined score
            controller_loads.sort(key=lambda x: x[0])

            selected = controller_loads[0][4]
            logger.info(
                f"[LOAD-BALANCE] Selected {selected.name} (active: {selected.get_load()}, total: {selected.total_processed})")
            return selected

    # ====== GFS REPLICA MANAGEMENT - COMMENTED OUT: MOVED TO MASTER_SERVER ======
    '''
    def request_data_access(self, client_id, data_type, operation_type, retry_count=0):
        """Handle RTO data access with complete chunk metadata and limited retries"""
        # ... GFS implementation moved to master_server
        pass

    def release_data_access(self, client_id, resource_identifier, operation_type):
        """Release data access - handles both server names and server lists"""
        # ... GFS implementation moved to master_server
        pass

    def write_data(self, client_id, locked_servers, operation, *args, **kwargs):
        """Perform replicated write operation with verification"""
        # ... GFS implementation moved to master_server
        pass

    def get_chunk_metadata(self, data_type):
        """Get complete chunk metadata for RTO clients"""
        # ... GFS implementation moved to master_server
        pass
    '''

    # ====== MAIN RPC METHODS ======
    def signal_request(self, client_id, target_pair, request_type="normal"):
        """Handle signal requests from t_signal"""
        self.is_ready.wait()
        logger.info(f"[TRAFFIC-SIGNAL] Request from {client_id}: {target_pair} ({request_type})")

        controller = self.get_least_loaded_controller()
        if not controller:
            return {"success": False, "reason": "No controllers available"}

        controller.add_request()
        request_id = str(uuid.uuid4())[:8]

        try:
            proxy = ServerProxy(controller.url, allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            result = proxy.signal_controller(target_pair)

            controller.complete_request()
            logger.info(f"[TRAFFIC-SIGNAL] Request {request_id} completed by {controller.name}")

            return {"success": True, "controller": controller.name, "result": result, "request_id": request_id}

        except Exception as e:
            controller.complete_request()
            logger.error(f"[TRAFFIC-SIGNAL] Request {request_id} failed on {controller.name}: {e}")
            return {"success": False, "reason": str(e), "request_id": request_id}

    def vip_arrival(self, client_id, target_pair, priority=1, vehicle_id=None):
        """Handle VIP requests with proper deadlock resolution"""
        self.is_ready.wait()
        vehicle_id = vehicle_id or f"VIP_{uuid.uuid4().hex[:6]}"
        logger.info(f"[VIP-ARRIVAL] {client_id}: VIP {vehicle_id} (P{priority}) requesting {target_pair}")

        # Determine which direction group this VIP belongs to
        pair_key = "12" if target_pair in [[1, 2], [2, 1]] else "34"
        other_key = "34" if pair_key == "12" else "12"

        vip_info = {
            'vehicle_id': vehicle_id,
            'priority': priority,
            'target_pair': target_pair,
            'timestamp': time.time(),
            'client_id': client_id
        }

        # Add to pending queue
        self.pending_vip_requests[pair_key].append(vip_info)

        # Check for deadlock scenario
        deadlock_resolved = False
        if self.pending_vip_requests["12"] and self.pending_vip_requests["34"]:
            logger.warning(f"[VIP-DEADLOCK] Simultaneous VIP requests detected!")

            vip_12 = self.pending_vip_requests["12"][0]
            vip_34 = self.pending_vip_requests["34"][0]

            first_vip, second_vip = self.handle_vip_deadlock(vip_12, vip_34)
            deadlock_resolved = True

            # Process in order
            if first_vip == vip_info:
                logger.info(f"[VIP-DEADLOCK] {vehicle_id} selected to go FIRST")
            else:
                logger.info(f"[VIP-DEADLOCK] {vehicle_id} will go SECOND, waiting...")
                time.sleep(3)  # Wait for first VIP to complete

        # Assign to least loaded controller
        controller = self.get_least_loaded_controller()
        controller.add_request()
        request_id = str(uuid.uuid4())[:8]

        try:
            proxy = ServerProxy(controller.url, allow_none=True,
                                transport=TimeoutTransport(RESPONSE_TIMEOUT))
            result = proxy.vip_arrival(target_pair, priority, vehicle_id)

            controller.complete_request()

            # Remove from pending queue
            if self.pending_vip_requests[pair_key] and self.pending_vip_requests[pair_key][0][
                'vehicle_id'] == vehicle_id:
                self.pending_vip_requests[pair_key].pop(0)

            logger.info(f"[VIP-ARRIVAL] VIP {vehicle_id} request completed by {controller.name}")

            return {
                "success": True,
                "controller": controller.name,
                "result": result,
                "vehicle_id": vehicle_id,
                "deadlock_resolved": deadlock_resolved,
                "request_id": request_id
            }

        except Exception as e:
            controller.complete_request()
            logger.error(f"[VIP-ARRIVAL] VIP {vehicle_id} failed on {controller.name}: {e}")

            # Remove from queue on failure
            if self.pending_vip_requests[pair_key] and self.pending_vip_requests[pair_key][0][
                'vehicle_id'] == vehicle_id:
                self.pending_vip_requests[pair_key].pop(0)

            return {"success": False, "reason": str(e), "vehicle_id": vehicle_id, "request_id": request_id}

    def get_current_green_pair(self):
        """Get currently green signal pair from database"""
        signals = self.db.get_signal_status()
        if signals.get('3') == 'GREEN' and signals.get('4') == 'GREEN':
            return [3, 4]
        elif signals.get('1') == 'GREEN' and signals.get('2') == 'GREEN':
            return [1, 2]
        return [3, 4]  # Default

    def update_signal_status_with_reason(self, signal_status, source, reason):
        """RPC method to update signals with attribution"""
        self.db.update_signal_status_with_reason(signal_status, source, reason)
        logger.info(f"[DB-UPDATE] Signal status updated by {source}: {reason}")
        return "OK"

    def get_signal_status_with_history(self, limit=50):
        """RPC method to get status with history"""
        return self.db.get_signal_status_with_history(limit)
    # ====== UTILITY METHODS ======
    def ping(self):
        active_controllers = sum(1 for c in self.controllers.values() if c.is_available)
        return f"ZooKeeper OK - {active_controllers} controllers active"

    # COMMENTED OUT: GFS system status moved to master_server
    '''
    def get_system_status(self):
        """Enhanced system status with GFS and RA info"""
        base_stats = self.db.get_system_stats()

        # Add GFS status
        gfs_status = {
            'active_reads': dict(self.replica_manager.active_reads),
            'active_writes': dict(self.replica_manager.active_writes),
            'replica_servers': list(REPLICA_SERVERS.keys()),
            'total_replicas': len(REPLICA_SERVERS)
        }

        # Add controller load info
        controller_status = {}
        for name, controller in self.controllers.items():
            controller_status[name] = {
                'active_requests': controller.get_load(),
                'total_processed': controller.total_processed,
                'is_available': controller.is_available
            }

        base_stats.update({
            'gfs_status': gfs_status,
            'controller_loads': controller_status,
            'pending_vips': {k: len(v) for k, v in self.pending_vip_requests.items()},
            'lamport_clock': self.lamport_clock
        })

        return base_stats
    '''

    def update_signal_status(self, signal_status):
        """Update signal status in database"""
        self.db.update_signal_status(signal_status)
        logger.info(f"[DB-UPDATE] Signal status updated: {signal_status}")
        return "OK"

    def get_signal_status(self):
        """Get current signal status from database"""
        return self.db.get_signal_status()

    def get_client_list(self):
        """For Berkeley sync coordination"""
        return BERKELEY_CLIENTS


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("COMPLETE ENHANCED ZOOKEEPER LOAD BALANCER")
    logger.info("Features: Ricart-Agrawala | Berkeley 7-Step | VIP Deadlock")
    logger.info("GFS Features: MOVED TO MASTER_SERVER")
    logger.info("=" * 80)

    lb = ZooKeeperLoadBalancer()

    from socketserver import ThreadingMixIn


    class ThreadedXMLRPCServer(ThreadingMixIn, SimpleXMLRPCServer):
        pass


    server = ThreadedXMLRPCServer(("0.0.0.0", ZOOKEEPER_PORT), allow_none=True)

    # Traffic control
    server.register_function(lb.signal_request, "signal_request")
    server.register_function(lb.vip_arrival, "vip_arrival")

    # Ricart-Agrawala
    server.register_function(lb.forward_ra_request, "forward_ra_request")
    server.register_function(lb.forward_ra_response, "forward_ra_response")
    server.register_function(lb.request_ped_ack, "request_ped_ack")

    # Berkeley sync
    server.register_function(lb.coordinate_berkeley_sync, "coordinate_berkeley_sync")
    server.register_function(lb.get_client_list, "get_client_list")

    # COMMENTED OUT: GFS replica management moved to master_server
    '''
    server.register_function(lb.request_data_access, "request_data_access")
    server.register_function(lb.release_data_access, "release_data_access")
    server.register_function(lb.write_data, "write_data")
    server.register_function(lb.get_chunk_metadata, "get_chunk_metadata")
    '''

    # Utility
    server.register_function(lb.ping, "ping")
    # server.register_function(lb.get_system_status, "get_system_status")  # COMMENTED OUT: moved to master_server
    server.register_function(lb.update_signal_status, "update_signal_status")
    server.register_function(lb.get_signal_status, "get_signal_status")
    # Add these after the existing register_function calls
    server.register_function(lb.update_signal_status_with_reason, "update_signal_status_with_reason")
    server.register_function(lb.get_signal_status_with_history, "get_signal_status_with_history")
    log_memory_usage()

    # Set the ready flag once all initialization is complete
    lb.is_ready.set()

    logger.info(f"ZooKeeper ready on port {ZOOKEEPER_PORT}")

    try:
        print(f"ZooKeeper ready on port {ZOOKEEPER_PORT}")
        server.serve_forever()
    except Exception as e:
        print(f"ZooKeeper crashed: {e}")
        import traceback

        traceback.print_exc()