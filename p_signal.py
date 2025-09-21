# Pedestrian signal client with synchronization to traffic controllers
import time
import threading
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy
from xmlrpc.client import ServerProxy, Transport  # <-- Make sure Transport is imported

# Add this class definition after the imports

# -------------------------
# CONFIGURATION
# -------------------------
MY_PORT = 9000
MY_NAME = "p_signal"

# Initial clock skew: -45 minutes (in seconds)
local_skew = -45 * 60

# ZooKeeper IP (for RTO integration / monitoring)
ZOOKEEPER_IP = "http://127.0.0.1:6000"

# Shared signal status (kept in sync with controllers)
# This should be updated whenever controllers change states
current_signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

# Statistics tracking
request_stats = {
    "total_requests": 0,
    "granted_requests": 0,
    "denied_requests": 0,
    "last_request_time": 0
}
stats_lock = threading.Lock()

class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn
# -------------------------
# PEDESTRIAN VOTING LOGIC
# -------------------------
def p_signal(target_pair):
    """SECOND OK: Non-RA pedestrian safety check"""
    with stats_lock:
        request_stats["total_requests"] += 1
        request_stats["last_request_time"] = time.time()

    pedestrian_clear = check_pedestrian_crossing(target_pair)

    if pedestrian_clear:
        print(f"[{MY_NAME}] PEDESTRIAN SAFETY CHECK: CLEAR for {target_pair}")
        with stats_lock:
            request_stats["granted_requests"] += 1
        return "OK"
    else:
        print(f"[{MY_NAME}] PEDESTRIAN SAFETY CHECK: CROSSING DETECTED for {target_pair}")
        with stats_lock:
            request_stats["denied_requests"] += 1
        return "DENY"


def p_signal_ra(target_pair, timestamp, requester_controller, request_type):
    """Enhanced p_signal for Ricart-Agrawala voting with timestamp"""
    with stats_lock:
        request_stats["total_requests"] += 1
        request_stats["last_request_time"] = time.time()

    print(f"[{MY_NAME}] RA vote request from {requester_controller} (ts={timestamp})")
    print(f"[{MY_NAME}] Target: {target_pair}, Type: {request_type}")

    # According to requirements, p_signal always returns "OK" for RA voting
    # but also checks if pedestrians are safe
    pedestrian_clear = check_pedestrian_crossing(target_pair)

    if pedestrian_clear:
        print(f"[{MY_NAME}] PEDESTRIANS CLEAR - RA VOTE: OK")
        with stats_lock:
            request_stats["granted_requests"] += 1
        return "OK"
    else:
        # Even if pedestrians are crossing, per requirements p_signal should return OK
        # but log the safety concern
        print(f"[{MY_NAME}] WARNING: Pedestrians crossing but RA requires OK - VOTE: OK")
        with stats_lock:
            request_stats["granted_requests"] += 1
        return "OK"


# Register the new function in the server setup:
# server.register_function(p_signal_ra, "p_signal_ra")

def check_pedestrian_crossing(target_pair):
    """
    Pedestrians are clear if their pedestrian signals are RED.
    If pedestrian lights are GREEN, then they are crossing -> DENY.
    """
    mapping = {
        (1, 2): ["P1", "P2"],
        (3, 4): ["P3", "P4"]
    }

    pedestrian_lights = mapping.get(tuple(target_pair), [])
    for ped in pedestrian_lights:
        if current_signal_status.get(ped) == "GREEN":
            return False   # Pedestrians walking -> block traffic
    return True  # Pedestrians are RED -> allow traffic


# -------------------------
# BERKELEY CLOCK SYNC
# -------------------------
def get_clock_value(server_time):
    """Step 2 & 3: return own_time - server_time"""
    own_time = time.time() + local_skew
    clock_value = own_time - server_time
    print(f"[{MY_NAME}] get_clock_value -> diff={clock_value:+.2f}s")
    return clock_value


def set_time(new_time):
    """Step 6 & 7: set local clock"""
    global local_skew
    current_actual_time = time.time()
    local_skew = new_time - current_actual_time
    print(f"[{MY_NAME}] set_time -> new_skew={local_skew:+.2f}s")
    return "OK"


# -------------------------
# STATUS & MONITORING
# -------------------------
def get_pedestrian_stats():
    with stats_lock:
        stats_copy = request_stats.copy()

    stats_copy["local_time"] = format_time(time.time() + local_skew)
    stats_copy["local_skew"] = local_skew
    if stats_copy["total_requests"] > 0:
        stats_copy["success_rate"] = (stats_copy["granted_requests"] /
                                      request_stats["total_requests"]) * 100
    else:
        stats_copy["success_rate"] = 100
    return stats_copy


def ping():
    """Health check"""
    return f"{MY_NAME} OK"


def format_time(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def log_system_status():
    while True:
        time.sleep(30)
        with stats_lock:
            total = request_stats["total_requests"]
            granted = request_stats["granted_requests"]
            denied = request_stats["denied_requests"]
            last_req = request_stats["last_request_time"]

        if total > 0:
            time_since_last = time.time() - last_req if last_req > 0 else 0
            success_rate = (granted / total) * 100

            print(f"\n[{MY_NAME}] === PEDESTRIAN SYSTEM STATUS ===")
            print(f"[{MY_NAME}] Total Votes: {total} | Granted: {granted} | Denied: {denied}")
            print(f"[{MY_NAME}] Success Rate: {success_rate:.1f}%")
            print(f"[{MY_NAME}] Last vote request: {time_since_last:.1f}s ago")
            print(f"[{MY_NAME}] ==========================================\n")


# -------------------------
# RTO OFFICER SUPPORT
# -------------------------
def get_real_time_data():
    """RTO officers can get real-time pedestrian data via ZooKeeper"""
    try:
        # FIXED: Use correct Transport pattern with timeout
        transport = TimeoutTransport(10)  # 10 second timeout
        proxy = ServerProxy(ZOOKEEPER_IP, allow_none=True, transport=transport)
        system_status = proxy.get_system_status()

        with stats_lock:
            pedestrian_data = {
                "pedestrian_stats": request_stats.copy(),
                "system_status_from_zookeeper": system_status,
                "local_time": format_time(time.time() + local_skew),
                "service_availability": "ACTIVE - Connected to ZooKeeper"
            }

        return pedestrian_data

    except Exception as e:
        print(f"[{MY_NAME}] Could not fetch ZooKeeper data: {e}")
        with stats_lock:
            return {
                "pedestrian_stats": request_stats.copy(),
                "local_time": format_time(time.time() + local_skew),
                "service_availability": "LIMITED - ZooKeeper Unreachable"
            }


# -------------------------
# MAIN
# -------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"OPTIMIZED PEDESTRIAN SIGNAL CLIENT [{MY_NAME}]")
    print("=" * 70)
    print(f"[{MY_NAME}] Performance optimized: Real pedestrian rules (no randomness)")
    print(f"[{MY_NAME}] ZooKeeper integration: {ZOOKEEPER_IP}")
    print(f"[{MY_NAME}] Initial clock skew: {local_skew:+.2f}s")
    print(f"[{MY_NAME}] Enhanced RTO officer data access enabled")
    print("=" * 70)

    status_thread = threading.Thread(target=log_system_status, daemon=True)
    status_thread.start()

    server = SimpleXMLRPCServer(("0.0.0.0", MY_PORT), allow_none=True)
    server.register_function(p_signal, "p_signal")
    server.register_function(p_signal_ra, "p_signal_ra")
    server.register_function(get_clock_value, "get_clock_value")
    server.register_function(set_time, "set_time")
    server.register_function(get_pedestrian_stats, "get_pedestrian_stats")
    server.register_function(get_real_time_data, "get_real_time_data")
    server.register_function(ping, "ping")

    print(f"[{MY_NAME}] Pedestrian client ready on port {MY_PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{MY_NAME}] Shutting down...")


