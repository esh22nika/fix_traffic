# t_signal.py (Performance Optimized)
# Traffic client with high-frequency requests to test dynamic scaling
import time
import random
import threading
import uuid
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

# -------------------------
# CONFIGURATION
# -------------------------
MY_PORT = 7000
MY_NAME = "t_signal"
ZOOKEEPER_IP = "http://localhost:6000"  # Fixed to localhost

# Initial clock skew: +30 minutes (in seconds)
local_skew = 30 * 60

# Simulation parameters
VIP_PROBABILITY = 0.35  # 35% chance of a VIP vehicle
REQUEST_INTERVAL_MIN = 2  # Minimum seconds between request bursts
REQUEST_INTERVAL_MAX = 5  # Maximum seconds between request bursts
REQUEST_BURST_SIZE = 2  # Number of requests to send in a quick burst

signal_pairs = {"1": [1, 2], "2": [1, 2], "3": [3, 4], "4": [3, 4]}

# Statistics tracking
request_stats = {
    "total_requests": 0,
    "vip_requests": 0,
    "normal_requests": 0,
    "successful_requests": 0,
    "failed_requests": 0,
    "total_response_time": 0.0,
    "burst_count": 0,
    "average_burst_response_time": 0.0
}
stats_lock = threading.Lock()

# Connection caching for better performance
zookeeper_proxy = None


# -------------------------
# CONNECTION MANAGEMENT
# -------------------------
def get_zookeeper_connection():
    """Get fresh ZooKeeper connection each time"""
    try:
        proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True)
        # Test connection immediately
        proxy.ping()
        return proxy
    except Exception as e:
        print(f"[{MY_NAME}] ZooKeeper connection failed: {e}")
        return None


def test_zookeeper_connection():
    """Initial connection test"""
    try:
        proxy = get_zookeeper_connection()
        if proxy:
            response = proxy.ping()
            print(f"[{MY_NAME}] Successfully connected to ZooKeeper: {response}")
            return True
        return False
    except Exception as e:
        print(f"[{MY_NAME}] FATAL: Could not connect to ZooKeeper at {ZOOKEEPER_IP}. Error: {e}")
        return False


# -------------------------
# TRAFFIC SIMULATION LOGIC
# -------------------------
def traffic_detection_loop():
    """High-frequency traffic simulation to test load balancing and scaling."""
    if not test_zookeeper_connection():
        print(f"[{MY_NAME}] Cannot start traffic simulation - ZooKeeper unreachable")
        return

    burst_counter = 0

    while True:
        burst_counter += 1
        burst_start_time = time.time()

        print(f"\n[{MY_NAME}] Starting burst #{burst_counter} with {REQUEST_BURST_SIZE} requests...")

        # Create threads for concurrent requests
        threads = []
        for i in range(REQUEST_BURST_SIZE):
            thread = threading.Thread(target=send_traffic_request, args=(i + 1, burst_counter))
            threads.append(thread)
            thread.start()
            # Stagger requests slightly to simulate real traffic detection
            time.sleep(random.uniform(0.05, 0.2))

        # Wait for all requests in burst to complete
        for thread in threads:
            thread.join()

        burst_end_time = time.time()
        burst_duration = burst_end_time - burst_start_time

        # Update burst statistics
        with stats_lock:
            request_stats["burst_count"] += 1
            if request_stats["burst_count"] > 0:
                total_burst_time = request_stats.get("total_burst_time", 0) + burst_duration
                request_stats["total_burst_time"] = total_burst_time
                request_stats["average_burst_response_time"] = total_burst_time / request_stats["burst_count"]

        print(f"[{MY_NAME}] Burst #{burst_counter} completed in {burst_duration:.2f}s")
        # After burst completion, add adaptive throttling
        with stats_lock:
            recent_failures = request_stats["failed_requests"]
            total_requests = request_stats["total_requests"]

        if total_requests > 0:
            failure_rate = recent_failures / total_requests
            if failure_rate > 0.3:  # If >30% failure rate
                adaptive_delay = sleep_time * 2  # Double the wait time
                print(f"[{MY_NAME}] High failure rate detected, adaptive throttling: {adaptive_delay}s")
                time.sleep(adaptive_delay - sleep_time)  # Additional delay
        # Random wait before next burst
        sleep_time = random.randint(REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX)
        print(f"[{MY_NAME}] Waiting {sleep_time}s before next burst...")
        time.sleep(sleep_time)


def send_traffic_request(request_index, burst_number):
    """Sends a single normal or VIP traffic request to the ZooKeeper."""
    # Randomly select which signal detected traffic
    sensed_signal = random.choice(list(signal_pairs.keys()))
    target_pair = signal_pairs[sensed_signal]
    start_time = time.time()
    is_vip = random.random() < VIP_PROBABILITY

    request_id = f"B{burst_number}R{request_index}"

    try:
        proxy = get_zookeeper_connection()
        if not proxy:
            raise Exception("No ZooKeeper connection available")

        if is_vip:
            # Generate VIP vehicle details
            vehicle_id = f"VIP-{uuid.uuid4().hex[:6]}"
            priority = random.randint(1, 4)  # 1=ambulance, 2=fire, 3=police, 4=vip
            vehicle_types = {1: "AMBULANCE", 2: "FIRE", 3: "POLICE", 4: "VIP"}
            vehicle_type = vehicle_types.get(priority, "UNKNOWN")

            print(
                f"[{MY_NAME}] Detected {vehicle_type} {vehicle_id} (P{priority}) at signal {sensed_signal} → {target_pair}")
            result = proxy.vip_arrival(MY_NAME, target_pair, priority, vehicle_id)

            with stats_lock:
                request_stats["vip_requests"] += 1
        else:
            print(f"[{MY_NAME}] Detected normal traffic at signal {sensed_signal} → {target_pair}")
            result = proxy.signal_request(MY_NAME, target_pair, "normal")

            with stats_lock:
                request_stats["normal_requests"] += 1

        end_time = time.time()
        response_time = end_time - start_time

        print(f"[{MY_NAME}] Request {request_id} successful in {response_time:.2f}s")

        with stats_lock:
            request_stats["successful_requests"] += 1
            request_stats["total_response_time"] += response_time

    except Exception as e:
        end_time = time.time()
        response_time = end_time - start_time
        print(f"[{MY_NAME}] Request {request_id} failed for {target_pair} after {response_time:.2f}s. Error: {e}")

        with stats_lock:
            request_stats["failed_requests"] += 1

    with stats_lock:
        request_stats["total_requests"] += 1


# -------------------------
# PERIODIC STATUS REPORTING
# -------------------------
def periodic_status_report():
    """Periodic status reporting for monitoring"""
    while True:
        time.sleep(30)  # Report every 30 seconds

        with stats_lock:
            total = request_stats["total_requests"]
            successful = request_stats["successful_requests"]
            failed = request_stats["failed_requests"]
            vip = request_stats["vip_requests"]
            normal = request_stats["normal_requests"]
            bursts = request_stats["burst_count"]

            if total > 0:
                success_rate = (successful / total) * 100
                avg_response = request_stats["total_response_time"] / successful if successful > 0 else 0
                avg_burst_time = request_stats.get("average_burst_response_time", 0)

                print(f"\n[{MY_NAME}] === TRAFFIC DETECTION REPORT ===")
                print(f"[{MY_NAME}] Total Requests: {total} | Success: {successful} | Failed: {failed}")
                print(
                    f"[{MY_NAME}] VIP: {vip} ({(vip / total) * 100:.1f}%) | Normal: {normal} ({(normal / total) * 100:.1f}%)")
                print(f"[{MY_NAME}] Success Rate: {success_rate:.1f}%")
                print(f"[{MY_NAME}] Avg Response Time: {avg_response:.2f}s")
                print(f"[{MY_NAME}] Bursts Completed: {bursts} | Avg Burst Time: {avg_burst_time:.2f}s")
                print(f"[{MY_NAME}] ==========================================")


# -------------------------
# OPTIMIZED BERKELEY CLIENT METHODS
# -------------------------
def get_clock_value(server_time):
    """Optimized Step 2 & 3: Return own_time - server_time faster"""
    own_time = time.time() + local_skew
    clock_value = own_time - server_time
    return clock_value


def set_time(new_time):
    """Optimized Step 6 & 7: Set local clock to new_time faster"""
    global local_skew
    current_actual_time = time.time()
    local_skew = new_time - current_actual_time
    return "OK"


# -------------------------
# UTILITY AND DEBUG METHODS
# -------------------------
def get_traffic_stats():
    """RPC method to get comprehensive traffic signal statistics"""
    with stats_lock:
        stats_copy = request_stats.copy()

    # Add computed fields
    stats_copy["local_time"] = format_time(time.time() + local_skew)
    stats_copy["local_skew"] = f"{local_skew:+.2f}s"

    if stats_copy["successful_requests"] > 0:
        stats_copy[
            "average_response_time"] = f"{stats_copy['total_response_time'] / stats_copy['successful_requests']:.2f}s"
        stats_copy["success_rate"] = f"{(stats_copy['successful_requests'] / stats_copy['total_requests']) * 100:.1f}%"
    else:
        stats_copy["average_response_time"] = "N/A"
        stats_copy["success_rate"] = "N/A"

    if stats_copy["total_requests"] > 0:
        stats_copy["vip_percentage"] = f"{(stats_copy['vip_requests'] / stats_copy['total_requests']) * 100:.1f}%"
        stats_copy["normal_percentage"] = f"{(stats_copy['normal_requests'] / stats_copy['total_requests']) * 100:.1f}%"
    else:
        stats_copy["vip_percentage"] = "N/A"
        stats_copy["normal_percentage"] = "N/A"

    return stats_copy


def get_simulation_config():
    """RPC method to get current simulation configuration"""
    return {
        "vip_probability": VIP_PROBABILITY,
        "request_interval_min": REQUEST_INTERVAL_MIN,
        "request_interval_max": REQUEST_INTERVAL_MAX,
        "request_burst_size": REQUEST_BURST_SIZE,
        "signal_pairs": signal_pairs,
        "zookeeper_ip": ZOOKEEPER_IP,
        "local_port": MY_PORT,
        "local_skew": f"{local_skew:+.2f}s"
    }


def update_simulation_config(vip_prob=None, interval_min=None, interval_max=None, burst_size=None):
    """RPC method to update simulation parameters during runtime"""
    global VIP_PROBABILITY, REQUEST_INTERVAL_MIN, REQUEST_INTERVAL_MAX, REQUEST_BURST_SIZE

    changes = []
    if vip_prob is not None and 0 <= vip_prob <= 1:
        VIP_PROBABILITY = vip_prob
        changes.append(f"VIP probability: {vip_prob}")

    if interval_min is not None and interval_min > 0:
        REQUEST_INTERVAL_MIN = interval_min
        changes.append(f"Min interval: {interval_min}s")

    if interval_max is not None and interval_max > REQUEST_INTERVAL_MIN:
        REQUEST_INTERVAL_MAX = interval_max
        changes.append(f"Max interval: {interval_max}s")

    if burst_size is not None and burst_size > 0:
        REQUEST_BURST_SIZE = burst_size
        changes.append(f"Burst size: {burst_size}")

    return f"Updated: {', '.join(changes)}" if changes else "No valid changes made"


def ping():
    """Health check method"""
    try:
        proxy = get_zookeeper_connection()
        zk_status = "Connected" if proxy else "Disconnected"
        return f"{MY_NAME} OK - ZooKeeper: {zk_status}"
    except:
        return f"{MY_NAME} OK - ZooKeeper: Disconnected"


def reset_stats():
    """RPC method to reset statistics"""
    global request_stats
    with stats_lock:
        request_stats = {
            "total_requests": 0,
            "vip_requests": 0,
            "normal_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "total_response_time": 0.0,
            "burst_count": 0,
            "average_burst_response_time": 0.0
        }
    return "Statistics reset successfully"


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


# -------------------------
# MAIN ENTRY POINT
# -------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"OPTIMIZED TRAFFIC SIGNAL CLIENT [{MY_NAME}]")
    print("=" * 70)
    print(f"[{MY_NAME}] Performance optimized: High-frequency request bursts")
    print(f"[{MY_NAME}] Connecting to ZooKeeper: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] Initial clock skew: {local_skew:+.2f}s")
    print(f"[{MY_NAME}] Burst size: {REQUEST_BURST_SIZE} requests")
    print(f"[{MY_NAME}] VIP probability: {VIP_PROBABILITY * 100:.1f}%")
    print("=" * 70)

    # Start background threads
    simulation_thread = threading.Thread(target=traffic_detection_loop, daemon=True)
    status_thread = threading.Thread(target=periodic_status_report, daemon=True)

    simulation_thread.start()
    status_thread.start()

    # Start the RPC server to listen for requests from controllers and monitoring
    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)

    # Berkeley clock sync methods
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")

    # Statistics and monitoring methods
    server.register_function(get_traffic_stats, "get_traffic_stats")
    server.register_function(get_simulation_config, "get_simulation_config")
    server.register_function(update_simulation_config, "update_simulation_config")
    server.register_function(reset_stats, "reset_stats")
    server.register_function(ping, "ping")

    print(f"[{MY_NAME}] Traffic client ready on port {MY_PORT}")
    print(f"[{MY_NAME}] RPC interface available for monitoring and control")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")

        # Print final statistics
        with stats_lock:
            if request_stats["total_requests"] > 0:
                print(f"[{MY_NAME}] === FINAL STATISTICS ===")
                print(f"[{MY_NAME}] Total requests processed: {request_stats['total_requests']}")
                print(
                    f"[{MY_NAME}] Success rate: {(request_stats['successful_requests'] / request_stats['total_requests']) * 100:.1f}%")
                print(
                    f"[{MY_NAME}] VIP requests: {request_stats['vip_requests']} ({(request_stats['vip_requests'] / request_stats['total_requests']) * 100:.1f}%)")
                print(f"[{MY_NAME}] Bursts completed: {request_stats['burst_count']}")

        print(f"[{MY_NAME}] Goodbye!")



