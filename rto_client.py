# rto_client.py - Fixed Google File System Implementation
# Multiple RTO officers accessing traffic data with proper Reader-Writer synchronization
from xmlrpc.client import ServerProxy
import time
import random
import threading
import sys

# --- CONFIGURATION ---
NUM_CLIENTS = 5
READ_PROBABILITY = 0.6
WRITE_PROBABILITY = 1 - READ_PROBABILITY
ZOOKEEPER_URL = "http://localhost:6000"  # Fixed: was using wrong IP


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
        """Request access to data through ZooKeeper - FIXED to not recurse"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            self.log(f"Requesting {operation_type.upper()} access for {data_type} from master server")

            # Direct call - no retry loop here (ZooKeeper handles retries)
            access_info = proxy.request_data_access(self.client_id, data_type, operation_type)

            if not access_info or not access_info.get('access_granted'):
                reason = access_info.get('reason', 'Unknown') if access_info else 'No response'
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
        """Write data to all replicas through ZooKeeper coordination"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            start_time = time.time()

            self.log(f"Writing data to {len(locked_servers)} replicas via master server")
            self.log(f"Data to write: {new_data}")

            if data_type == "signal_status":
                result = proxy.write_data(self.client_id, locked_servers, "update_signal_status", new_data)
            elif data_type == "system_status":
                # Fixed: proper controller data format
                controller_name = f"controller_{random.randint(1, 3)}"
                result = proxy.write_data(self.client_id, locked_servers, "update_controller_status",
                                          controller_name, **new_data)
            else:
                result = None

            write_time = time.time() - start_time

            if result:
                successful_writes = result.get('successful_writes', 0)
                total_replicas = result.get('total_replicas', len(locked_servers))
                success_rate = result.get('success_rate', 0)

                self.log(f"Write completed in {write_time:.2f}s")
                self.log(f"Replication: {successful_writes}/{total_replicas} replicas ({success_rate:.1f}% success)")

                return result, write_time
            else:
                self.log("Write operation failed - no result returned", "ERROR")
                return None, write_time

        except Exception as e:
            write_time = time.time() - start_time
            self.log(f"Failed to write via master server: {e}", "ERROR")
            return None, write_time

    def release_access(self, resource_identifier, operation_type):
        """Release data access locks via master server"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            result = proxy.release_data_access(self.client_id, resource_identifier, operation_type)

            if result == "OK":
                self.log(f"Successfully released {operation_type} lock")
                return True
            else:
                self.log(f"Failed to release {operation_type} lock: {result}", "WARNING")
                return False

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
        chunks = access_info.get('chunks', {})

        self.log(f"Assigned to replica server: {server_name} ({server_url})")
        self.log(f"Chunk distribution: {len(chunks)} chunk locations provided")

        # Step 3: Simulate read processing time
        processing_time = random.uniform(0.5, 1.5)
        self.log(f"Processing read for {processing_time:.1f}s...")
        time.sleep(processing_time)

        # Step 4: Read data from assigned replica
        data, read_time = self.read_data_from_server(server_url, data_type)

        # Step 5: Release read access - FIXED parameter type
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

        self.log(f"Acquired write locks on {len(locked_servers)} replica servers: {locked_servers}")

        # Step 3: Generate write data - FIXED data format
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

        # Note: ZooKeeper automatically releases write locks after operation

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
            proxy = self._get_proxy(ZOOKEEPER_URL)
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
    print("Reader-Writer Problem with Starvation Prevention")
    print("=" * 80)
    print(f"Number of RTO Officers: {NUM_CLIENTS}")
    print(f"Read Probability: {READ_PROBABILITY * 100:.0f}%")
    print(f"Write Probability: {WRITE_PROBABILITY * 100:.0f}%")
    print(f"Master Server (ZooKeeper): {ZOOKEEPER_URL}")
    print("=" * 80)

    # Test ZooKeeper connection
    try:
        proxy = ServerProxy(ZOOKEEPER_URL, allow_none=True)
        response = proxy.ping()
        print(f"✓ Master server connection successful: {response}")
    except Exception as e:
        print(f"✗ Cannot connect to master server: {e}")
        print("Please start ZooKeeper and Replica Servers first!")
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