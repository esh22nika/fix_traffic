# rto_client.py - Reader-Writer Problem Simulation
# Multiple RTO officers accessing traffic data with random read/write operations
from xmlrpc.client import ServerProxy
import time
import random
import threading
import sys

# --- CONFIGURATION ---
NUM_CLIENTS = 5  # Increased number of clients for more activity
READ_PROBABILITY = 0.6  # 60% reads, 40% writes for a better balance
WRITE_PROBABILITY = 1 - READ_PROBABILITY  # Explicitly defined for clarity
ZOOKEEPER_URL = "http://192.168.0.111:6000"


# --- END CONFIGURATION ---

class RTOClient:
    def __init__(self, client_id):
        self.client_id = client_id
        # Restored zookeeper_url attribute for compatibility with original functions
        self.zookeeper_url = ZOOKEEPER_URL
        self.stats = {
            'read_requests': 0, 'write_requests': 0,
            'successful_reads': 0, 'successful_writes': 0,
            'failed_requests': 0, 'total_read_time': 0.0, 'total_write_time': 0.0,
            'total_wait_time': 0.0  # From original for chunked read
        }
        self.stats_lock = threading.Lock()

    def log(self, message):
        """Centralized logging with client ID"""
        timestamp = time.strftime("%H:%M:%S")
        print(f"[{timestamp}] [{self.client_id}] {message}")

    def _get_proxy(self, url):
        """Helper to create a server proxy."""
        return ServerProxy(url, allow_none=True)

    def request_data_access(self, data_type, operation_type):
        """Request access to data through ZooKeeper"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            self.log(f"Requesting {operation_type.upper()} access for {data_type}... (will wait if queued)")
            return proxy.request_data_access(self.client_id, data_type, operation_type)
        except Exception as e:
            self.log(f"ZooKeeper access error: {e}")
            return None

    def read_data_from_server(self, server_url, data_type):
        """Read data from assigned replica server"""
        try:
            proxy = self._get_proxy(server_url)
            if data_type == "signal_status":
                data = proxy.get_signal_status()
                self.log(f"Read signal data: {len(data)} signals from {server_url}")
                return data
            elif data_type == "system_status":
                data = proxy.get_system_stats()
                self.log(f"Read system data: {len(data.get('controllers', []))} controllers from {server_url}")
                return data
            return None
        except Exception as e:
            self.log(f"Read error from {server_url}: {e}")
            return None

    def write_data_to_system(self, locked_servers, data_type, new_data):
        """Write data to all replicas through ZooKeeper"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            if data_type == "signal_status":
                return proxy.write_data(self.client_id, locked_servers, "update_signal_status", new_data)
            elif data_type == "system_status":
                return proxy.write_data(self.client_id, locked_servers, "update_controller_status", "controller",
                                        **new_data)
            return None
        except Exception as e:
            self.log(f"Write error: {e}")
            return None

    def release_access(self, resource_identifier, operation_type):
        """Release data access locks"""
        try:
            proxy = self._get_proxy(ZOOKEEPER_URL)
            return proxy.release_data_access(self.client_id, resource_identifier, operation_type) == "OK"
        except Exception as e:
            self.log(f"Release access error: {e}")
            return False

    def perform_read_operation(self, data_type):
        """Complete read operation workflow"""
        start_time = time.time()
        with self.stats_lock:
            self.stats['read_requests'] += 1

        access_info = self.request_data_access(data_type, "read")
        if not access_info or not access_info.get('access_granted'):
            self.log(f"READ access denied by ZK: {access_info.get('reason', 'No response')}")
            with self.stats_lock: self.stats['failed_requests'] += 1
            return

        server_name = access_info['server_name']
        server_url = access_info['server_url']
        chunks = access_info.get('chunks', {})

        # THIS IS THE RESTORED LOGGING FORMAT YOU REQUESTED
        self.log(f"READ access granted - Server: {server_name}, Chunks: {chunks}")

        time.sleep(random.uniform(0.5, 1.5))  # Simulate read processing time
        data = self.read_data_from_server(server_url, data_type)

        # Release the lock after reading
        self.release_access(data_type, "read")
        operation_time = time.time() - start_time

        if data is not None:
            self.log(f"READ completed and lock released in {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['successful_reads'] += 1
                self.stats['total_read_time'] += operation_time
        else:
            self.log(f"READ failed after {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['failed_requests'] += 1

    def perform_write_operation(self, data_type):
        """Complete write operation workflow"""
        start_time = time.time()
        with self.stats_lock:
            self.stats['write_requests'] += 1

        access_info = self.request_data_access(data_type, "write")
        if not access_info or not access_info.get('access_granted'):
            self.log(f"WRITE access denied by ZK: {access_info.get('reason', 'No response')}")
            with self.stats_lock: self.stats['failed_requests'] += 1
            return

        locked_servers = access_info['locked_servers']
        self.log(f"WRITE access granted - Acquired lock on {len(locked_servers)} servers: {locked_servers}")

        new_data = {
            str(random.randint(1, 4)): random.choice(["RED", "GREEN", "YELLOW"])} if data_type == "signal_status" else {
            'is_available': random.choice([True, False])}

        self.log(f"Preparing to write new data: {new_data}")
        time.sleep(random.uniform(1, 2.5))  # Simulate write processing
        result = self.write_data_to_system(locked_servers, data_type, new_data)

        # Release the lock after writing
        self.release_access(data_type, "write")
        operation_time = time.time() - start_time

        if result:
            self.log(f"WRITE completed, replicated, and lock released in {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['successful_writes'] += 1
                self.stats['total_write_time'] += operation_time
        else:
            self.log(f"WRITE failed after {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['failed_requests'] += 1

    # --- RESTORED ORIGINAL FUNCTION ---
    # This function was part of your original code and has been restored.
    def perform_chunked_read_operation(self, data_type, specific_chunk=None):
        """Read operation with chunk-specific access"""
        start_time = time.time()

        if specific_chunk:
            self.log(f"Requesting READ access for {data_type} chunk {specific_chunk}")
        else:
            self.log(f"Requesting READ access for all {data_type} chunks")

        with self.stats_lock:
            self.stats['read_requests'] += 1

        # Step 1: Request access from ZooKeeper
        access_info = self.request_data_access(data_type, "read")
        if not access_info or not access_info.get('access_granted'):
            reason = access_info.get('reason', 'Unknown') if access_info else 'No response'
            self.log(f"READ access denied: {reason}")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

        # Step 2: Get chunk metadata
        try:
            proxy = self._get_proxy(self.zookeeper_url)
            # This function might need to be added back to your zookeeper if it was removed
            chunk_metadata = proxy.get_chunk_metadata(data_type)
            total_chunks = chunk_metadata['total_chunks']
            chunk_distribution = chunk_metadata['chunk_distribution']
            self.log(f"Data distributed across {total_chunks} chunks")
            self.log(f"Chunk distribution: {chunk_distribution}")
        except Exception as e:
            self.log(f"Failed to get chunk metadata: {e}")
            return False

        # Step 3: Read data from assigned server
        server_name = access_info['server_name']
        server_url = access_info['server_url']
        self.log(f"READ access granted - Server: {server_name}")
        time.sleep(random.uniform(1, 3))
        data = self.read_data_from_server(server_url, data_type)

        # Step 4: Release the read lock
        self.release_access(server_name, "read")
        operation_time = time.time() - start_time

        if data is not None:
            self.log(f"CHUNKED READ completed in {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['successful_reads'] += 1
                self.stats['total_wait_time'] += operation_time
            return True
        else:
            self.log(f"CHUNKED READ failed after {operation_time:.2f}s")
            with self.stats_lock:
                self.stats['failed_requests'] += 1
            return False

    def random_operation_loop(self):
        """Main loop for continuous random read/write operations"""
        self.log(f"Starting continuous operations (Read prob: {READ_PROBABILITY * 100:.0f}%)")
        operation_count = 0
        while True:
            operation_count += 1
            self.log(f"--- Operation #{operation_count} ---")

            is_read = random.random() < READ_PROBABILITY
            data_type = random.choice(["signal_status", "system_status"])

            if is_read:
                self.perform_read_operation(data_type)
            else:
                self.perform_write_operation(data_type)

            wait_time = random.uniform(2, 6)
            self.log(f"Waiting {wait_time:.1f}s...")
            time.sleep(wait_time)

    def print_stats(self):
        with self.stats_lock: stats = self.stats.copy()
        total_reqs = stats['read_requests'] + stats['write_requests']
        total_succ = stats['successful_reads'] + stats['successful_writes']
        success_rate = (total_succ / total_reqs * 100) if total_reqs > 0 else 0
        avg_read = (stats['total_read_time'] / stats['successful_reads']) if stats['successful_reads'] > 0 else 0
        avg_write = (stats['total_write_time'] / stats['successful_writes']) if stats['successful_writes'] > 0 else 0
        print(
            f"\n--- STATS [{self.client_id}] ---\n  Total: {total_reqs} | Success: {total_succ} ({success_rate:.1f}%)\n  READS : Req: {stats['read_requests']}, OK: {stats['successful_reads']}, Avg Time: {avg_read:.2f}s\n  WRITES: Req: {stats['write_requests']}, OK: {stats['successful_writes']}, Avg Time: {avg_write:.2f}s\n--------------------------")


def statistics_monitor(clients):
    while True:
        time.sleep(30);
        print("\n\n" + "=" * 22 + " PERIODIC SUMMARY " + "=" * 22)
        for client in clients: client.print_stats()
        print("=" * 64)


def main():
    print("RTO CLIENT SIMULATION - Reader-Writer Problem (Continuous & Queued)")
    print(
        "=" * 60 + f"\nRTO Officers: {NUM_CLIENTS}\nRead Probability: {READ_PROBABILITY * 100:.0f}%\nWrite Probability: {WRITE_PROBABILITY * 100:.0f}%\n" + "=" * 60)
    try:
        ServerProxy(ZOOKEEPER_URL, allow_none=True).ping()
        print(f"ZooKeeper connection successful.")
    except Exception as e:
        print(
            f"Cannot connect to ZooKeeper at {ZOOKEEPER_URL}: {e}\nPlease start ZooKeeper and Replica Servers first!");
        sys.exit(1)

    clients = [RTOClient(f"RTO_Officer_{i + 1}") for i in range(NUM_CLIENTS)]
    for client in clients: threading.Thread(target=client.random_operation_loop, daemon=True).start(); time.sleep(
        random.uniform(0.2, 0.8))

    print(
        f"All {NUM_CLIENTS} RTO Officers are now active...\nFinal statistics will be printed when you stop the simulation (Ctrl+C).\n")

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nSimulation stopped by user.\n" + "=" * 25 + " FINAL SUMMARY " + "=" * 26)
        for client in clients: client.print_stats()
        print("=" * 64 + "\nGoodbye!")


if __name__ == "__main__":
    main()



