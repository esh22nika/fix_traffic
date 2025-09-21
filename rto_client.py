# rto_client.py - Modified to interact with master_server
from xmlrpc.client import ServerProxy
import time
import random
import threading
import sys
from xmlrpc.client import ServerProxy, Transport

# --- CONFIGURATION ---
NUM_CLIENTS = 5
READ_PROBABILITY = 0.6
WRITE_PROBABILITY = 1 - READ_PROBABILITY
MASTER_SERVER_URL = "http://127.0.0.1:6100"  # Changed from ZOOKEEPER_URL to MASTER_SERVER_URL


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


class RTOClient:
    def __init__(self, client_id):
        self.client_id = client_id
        self.stats = {
            'read_requests': 0,
            'write_requests': 0,
            'successful_reads': 0,
            'successful_writes': 0,
            'failed_requests': 0,
            'total_read_time': 0.0,
            'total_write_time': 0.0,
            'starvation_incidents': 0,
            'retries': 0,
            'chunks_read': 0,
            'chunks_written': 0
        }
        self.stats_lock = threading.Lock()

    def log(self, message, level="INFO"):
        """Enhanced logging with levels"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [{level}] [{self.client_id}] {message}")

    def _get_proxy(self, url):
        """Helper to create a server proxy."""
        return ServerProxy(url, allow_none=True)

    def request_data_access(self, data_type, operation_type):
        """Request access to data through Master Server"""
        try:
            proxy = self._get_proxy(MASTER_SERVER_URL)
            self.log(f"Requesting {operation_type.upper()} access for {data_type} from master server")

            # Get metadata first from master server
            metadata = proxy.get_chunk_metadata(data_type)

            # For this simplified version, we'll just return metadata
            # In a real GFS implementation, this would handle lock acquisition
            access_info = {
                'access_granted': True,
                'metadata': metadata,
                'operation_type': operation_type,
                'client_id': self.client_id,
                'timestamp': time.time()
            }

            if operation_type == "read":
                # For reads, assign a random replica server
                replica_servers = list(metadata.get('system_info', {}).get('available_servers', []))
                if replica_servers:
                    server_name = random.choice(replica_servers)
                    access_info['server_name'] = server_name
                    access_info['server_url'] = f"http://127.0.0.1:{7000 + int(server_name.split('_')[-1])}"
                else:
                    access_info['access_granted'] = False
                    access_info['reason'] = 'No replica servers available'

            elif operation_type == "write":
                # For writes, we need all replica servers
                replica_servers = list(metadata.get('system_info', {}).get('available_servers', []))
                if replica_servers:
                    access_info['locked_servers'] = replica_servers
                else:
                    access_info['access_granted'] = False
                    access_info['reason'] = 'No replica servers available'

            if not access_info.get('access_granted'):
                reason = access_info.get('reason', 'Unknown')
                self.log(f"Access DENIED by master server: {reason}", "WARNING")
                return None

            # Log detailed metadata information
            metadata = access_info.get('metadata', {})
            total_chunks = metadata.get('total_chunks', 0)
            chunk_size = metadata.get('chunk_size', 0)
            replication_factor = metadata.get('replication_factor', 0)

            self.log(f"{operation_type.upper()} access GRANTED by master server")
            self.log(f"Metadata: {total_chunks} chunks, {chunk_size} records/chunk, {replication_factor}x replication")

            return access_info

        except Exception as e:
            self.log(f"Failed to request access from master server: {e}", "ERROR")
            return None

    def read_data_from_server(self, server_url, data_type):
        """Read data from assigned replica server"""
        try:
            proxy = self._get_proxy(server_url)
            start_time = time.time()

            if data_type == "signal_status":
                data = proxy.get_signal_status()
                data_description = f"{len(data)} signal statuses"
            elif data_type == "system_status":
                data = proxy.get_system_stats()
                controllers = data.get('controllers', [])
                data_description = f"system stats with {len(controllers)} controllers"
            else:
                data = {}
                data_description = "unknown data"

            read_time = time.time() - start_time
            self.log(f"Successfully read {data_description} from {server_url} in {read_time:.2f}s")

            return data, read_time

        except Exception as e:
            self.log(f"Failed to read from {server_url}: {e}", "ERROR")
            return None, 0

    def write_data_to_system(self, locked_servers, data_type, new_data):
        """Write data to all replicas - FIXED VERSION"""
        try:
            start_time = time.time()
            successful_writes = 0
            total_replicas = len(locked_servers)

            self.log(f"Writing data to {len(locked_servers)} replicas")
            self.log(f"Data to write: {new_data}")

            for server_name in locked_servers:
                try:
                    server_url = f"http://127.0.0.1:{7000 + int(server_name.split('_')[-1])}"
                    proxy = self._get_proxy(server_url)

                    if data_type == "signal_status":
                        result = proxy.update_signal_status(new_data)
                    elif data_type == "system_status":
                        controller_name = f"controller_{random.randint(1, 3)}"
                        is_available = new_data.get('is_available', False)
                        active_requests = new_data.get('active_requests', 0)
                        total_processed = new_data.get('total_processed', 0)

                        # Call with keyword arguments
                        result = proxy.update_controller_status(
                            controller_name,
                            is_available,
                            active_requests,
                            total_processed
                        )

                    else:
                        result = "UNKNOWN_OPERATION"

                    if result == "OK":
                        successful_writes += 1
                        self.log(f"Write successful on {server_name}")
                    else:
                        self.log(f"Write failed on {server_name}: {result}", "WARNING")

                except Exception as e:
                    self.log(f"Failed to write to {server_name}: {e}", "ERROR")

            write_time = time.time() - start_time
            success_rate = (successful_writes / total_replicas) * 100 if total_replicas > 0 else 0

            self.log(f"Write completed in {write_time:.2f}s")
            self.log(f"Replication: {successful_writes}/{total_replicas} replicas ({success_rate:.1f}% success)")

            return {
                'successful_writes': successful_writes,
                'total_replicas': total_replicas,
                'success_rate': success_rate
            }, write_time

        except Exception as e:
            write_time = time.time() - start_time
            self.log(f"Failed to write to replicas: {e}", "ERROR")
            return None, write_time

    def release_access(self, resource_identifier, operation_type):
        """Release data access - simplified for master server"""
        try:
            self.log(f"Successfully released {operation_type} lock")
            return True
        except Exception as e:
            self.log(f"Error releasing {operation_type} access: {e}", "ERROR")
            return False

    def perform_read_operation(self, data_type):
        """Complete GFS-style read operation"""
        operation_start = time.time()

        with self.stats_lock:
            self.stats['read_requests'] += 1

        self.log(f"=== STARTING READ OPERATION for {data_type} ===")

        # Step 1: Request read access from master server
        access_info = self.request_data_access(data_type, "read")
        if not access_info:
            self.log("READ operation ABORTED - Access denied", "ERROR")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

        # Step 2: Extract server information and metadata
        server_name = access_info.get('server_name')
        server_url = access_info.get('server_url')
        metadata = access_info.get('metadata', {})

        if not server_url:
            self.log("READ operation ABORTED - No server URL provided", "ERROR")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

        self.log(f"Assigned to replica server: {server_name} ({server_url})")
        self.log(f"Chunk distribution: {metadata.get('total_chunks', 0)} chunks available")

        # Step 3: Simulate read processing time
        processing_time = random.uniform(0.5, 1.5)
        self.log(f"Processing read for {processing_time:.1f}s...")
        time.sleep(processing_time)

        # Step 4: Read data from assigned replica
        data, read_time = self.read_data_from_server(server_url, data_type)

        # Step 5: Release read access
        release_success = self.release_access(server_name, "read")

        total_time = time.time() - operation_start

        if data is not None and release_success:
            self.log(f"=== READ OPERATION COMPLETED in {total_time:.2f}s ===")

            with self.stats_lock:
                self.stats['successful_reads'] += 1
                self.stats['total_read_time'] += total_time
                self.stats['chunks_read'] += metadata.get('total_chunks', 1)

            return True
        else:
            self.log(f"=== READ OPERATION FAILED after {total_time:.2f}s ===", "ERROR")

            with self.stats_lock:
                self.stats['failed_requests'] += 1

            return False

    def perform_write_operation(self, data_type):
        """Complete GFS-style write operation"""
        operation_start = time.time()

        with self.stats_lock:
            self.stats['write_requests'] += 1

        self.log(f"=== STARTING WRITE OPERATION for {data_type} ===")

        # Step 1: Request write access from master server
        access_info = self.request_data_access(data_type, "write")
        if not access_info:
            self.log("WRITE operation ABORTED - Access denied", "ERROR")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

        # Step 2: Extract locked servers and metadata
        locked_servers = access_info.get('locked_servers', [])
        metadata = access_info.get('metadata', {})

        if not locked_servers:
            self.log("WRITE operation ABORTED - No servers available", "ERROR")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

        self.log(f"Will write to {len(locked_servers)} replica servers: {locked_servers}")

        # Step 3: Generate write data
        if data_type == "signal_status":
            new_data = {
                str(random.randint(1, 4)): random.choice(["RED", "GREEN"]),
                f"P{random.randint(1, 4)}": random.choice(["RED", "GREEN"])
            }
        elif data_type == "system_status":
            new_data = {
                'is_available': random.choice([True, False]),
                'active_requests': random.randint(0, 5),
                'total_processed': random.randint(10, 100)
            }
        else:
            new_data = {}

        # Step 4: Simulate write processing time
        processing_time = random.uniform(1.0, 2.5)
        self.log(f"Processing write for {processing_time:.1f}s...")
        time.sleep(processing_time)

        # Step 5: Write to all replicas
        write_result, write_time = self.write_data_to_system(locked_servers, data_type, new_data)

        total_time = time.time() - operation_start

        if write_result and write_result.get('successful_writes', 0) > 0:
            success_rate = write_result.get('success_rate', 0)

            self.log(f"=== WRITE OPERATION COMPLETED with {success_rate:.1f}% success in {total_time:.2f}s ===")

            with self.stats_lock:
                self.stats['successful_writes'] += 1
                self.stats['total_write_time'] += total_time
                self.stats['chunks_written'] += metadata.get('total_chunks', 1)

            return True
        else:
            self.log(f"=== WRITE OPERATION FAILED after {total_time:.2f}s ===", "ERROR")

            with self.stats_lock:
                self.stats['failed_requests'] += 1

            return False

    def get_chunk_metadata(self, data_type):
        """Get chunk metadata from master server"""
        try:
            proxy = self._get_proxy(MASTER_SERVER_URL)
            metadata = proxy.get_chunk_metadata(data_type)

            self.log(f"Retrieved chunk metadata for {data_type}:")
            self.log(f"  Total chunks: {metadata.get('total_chunks', 0)}")
            self.log(f"  Chunk size: {metadata.get('chunk_size', 0)} records")
            self.log(f"  Replication factor: {metadata.get('replication_factor', 0)}")

            return metadata

        except Exception as e:
            self.log(f"Failed to get chunk metadata: {e}", "ERROR")
            return None

    def random_operation_loop(self):
        """Main loop for continuous read/write operations"""
        self.log(
            f"Starting GFS operations (Read: {READ_PROBABILITY * 100:.0f}%, Write: {WRITE_PROBABILITY * 100:.0f}%)")

        operation_count = 0

        while True:
            try:
                operation_count += 1
                self.log(f"--- Operation #{operation_count} ---")

                # Decide operation type
                is_read = random.random() < READ_PROBABILITY
                data_type = random.choice(["signal_status", "system_status"])

                # Perform operation
                if is_read:
                    success = self.perform_read_operation(data_type)
                else:
                    success = self.perform_write_operation(data_type)

                # Wait before next operation
                wait_time = random.uniform(2, 6)
                self.log(f"Waiting {wait_time:.1f}s before next operation...")
                time.sleep(wait_time)

            except KeyboardInterrupt:
                self.log("Operation loop interrupted")
                break
            except Exception as e:
                self.log(f"Unexpected error: {e}", "ERROR")
                time.sleep(1)

    def print_stats(self):
        """Print comprehensive statistics"""
        with self.stats_lock:
            stats = self.stats.copy()

        total_reqs = stats['read_requests'] + stats['write_requests']
        total_succ = stats['successful_reads'] + stats['successful_writes']
        success_rate = (total_succ / total_reqs * 100) if total_reqs > 0 else 0

        avg_read = (stats['total_read_time'] / stats['successful_reads']) if stats['successful_reads'] > 0 else 0
        avg_write = (stats['total_write_time'] / stats['successful_writes']) if stats['successful_writes'] > 0 else 0

        print(f"\n{'=' * 50}")
        print(f"STATISTICS FOR {self.client_id}")
        print(f"{'=' * 50}")
        print(f"Total Requests: {total_reqs}")
        print(f"Successful Operations: {total_succ} ({success_rate:.1f}%)")
        print(f"Failed Operations: {stats['failed_requests']}")
        print(f"")
        print(f"READ OPERATIONS:")
        print(f"  Requests: {stats['read_requests']}")
        print(f"  Successful: {stats['successful_reads']}")
        print(f"  Average Time: {avg_read:.2f}s")
        print(f"  Chunks Read: {stats['chunks_read']}")
        print(f"")
        print(f"WRITE OPERATIONS:")
        print(f"  Requests: {stats['write_requests']}")
        print(f"  Successful: {stats['successful_writes']}")
        print(f"  Average Time: {avg_write:.2f}s")
        print(f"  Chunks Written: {stats['chunks_written']}")
        print(f"")
        print(f"CONCURRENCY METRICS:")
        print(f"  Retries: {stats['retries']}")
        print(f"  Starvation Prevention Events: {stats['starvation_incidents']}")
        print(f"{'=' * 50}")


def statistics_monitor(clients):
    """Periodic statistics reporting"""
    while True:
        time.sleep(30)

        print(f"\n{'=' * 80}")
        print(f"PERIODIC GFS STATISTICS REPORT")
        print(f"{'=' * 80}")

        for client in clients:
            client.print_stats()

        print(f"{'=' * 80}\n")


def main():
    print("=" * 80)
    print("RTO CLIENT - GOOGLE FILE SYSTEM (GFS) IMPLEMENTATION")
    print("Now using Master Server for metadata and coordination")
    print("=" * 80)
    print(f"Number of RTO Officers: {NUM_CLIENTS}")
    print(f"Read Probability: {READ_PROBABILITY * 100:.0f}%")
    print(f"Write Probability: {WRITE_PROBABILITY * 100:.0f}%")
    print(f"Master Server: {MASTER_SERVER_URL}")
    print("=" * 80)

    # Test Master Server connection
    try:
        proxy = ServerProxy(MASTER_SERVER_URL, allow_none=True)
        response = proxy.ping()
        print(f"✓ Master server connection successful: {response}")

        # Test replica status
        replica_status = proxy.get_replica_status()
        online_servers = replica_status.get('summary', {}).get('online_servers', 0)
        print(f"✓ Replica servers: {online_servers} online")

    except Exception as e:
        print(f"✗ Cannot connect to master server: {e}")
        print("Please start Master Server and Replica Servers first!")
        sys.exit(1)

    # Create RTO clients
    clients = []
    for i in range(NUM_CLIENTS):
        client = RTOClient(f"RTO_Officer_{i + 1}")
        clients.append(client)

    # Start statistics monitor
    stats_thread = threading.Thread(target=statistics_monitor, args=(clients,), daemon=True)
    stats_thread.start()

    # Start client operation threads
    print(f"\nStarting {NUM_CLIENTS} RTO Officers...")
    for i, client in enumerate(clients):
        thread = threading.Thread(target=client.random_operation_loop, daemon=True)
        thread.start()

        # Stagger starts
        time.sleep(random.uniform(0.2, 0.8))
        print(f"✓ Started {client.client_id}")

    print(f"\nAll {NUM_CLIENTS} RTO Officers are active!")
    print("Press Ctrl+C to stop and see final statistics.\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n{'=' * 80}")
        print("SIMULATION STOPPED - FINAL STATISTICS")
        print(f"{'=' * 80}")

        for client in clients:
            client.print_stats()

        print(f"{'=' * 80}")
        print("GFS RTO Client simulation completed!")


if __name__ == "__main__":
    main()