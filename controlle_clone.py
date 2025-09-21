# controller.py - Full Ricart-Agrawala Implementation with Enhanced Logging
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid

# Configuration
CONTROLLER_PORT = 8001
CONTROLLER_NAME = "CONTROLLER_CLONE"
ZOOKEEPER_URL = "http://127.0.0.1:6000"
RESPONSE_TIMEOUT = 15

# Ricart-Agrawala State
lamport_clock = 0
clock_lock = threading.Lock()
ra_state = {
    'requesting_cs': False,
    'in_cs': False,
    'deferred_replies': [],
    'pending_request': None,
    'received_oks': 0,
    'expected_oks': 1  # Will be set based on active controllers
}
ra_lock = threading.Lock()

# Berkeley Clock
server_skew = 0.0

# Signal State
signal_status = {
    1: "RED", 2: "RED", 3: "GREEN", 4: "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}
state_lock = threading.Lock()

# VIP queues for deadlock detection
vip_queues = {"12": [], "34": []}
vip_lock = threading.Lock()


def is_ready():
    return True


def update_zookeeper_signals():
    """Push current signal status to ZooKeeper database"""
    try:
        zk_proxy = get_zookeeper_proxy()
        with state_lock:
            current_status = signal_status.copy()

        result = zk_proxy.update_signal_status(current_status)
        print(f"[{CONTROLLER_NAME}] Updated ZooKeeper database: {result}")
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Failed to update ZooKeeper database: {e}")

class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


# ====== LOGGING UTILITIES ======
def log_separator(message=""):
    """Print a separator line with optional message"""
    if message:
        print(f"\n{'=' * 80}\n{message}\n{'=' * 80}")
    else:
        print(f"\n{'=' * 80}")


def log_mutex_state():
    """Log current mutex state"""
    with ra_lock:
        state = f"[MUTEX-STATE] Requesting: {ra_state['requesting_cs']}, In-CS: {ra_state['in_cs']}, "
        state += f"OKs: {ra_state['received_oks']}/{ra_state['expected_oks']}, Deferred: {len(ra_state['deferred_replies'])}"
        print(state)


def log_vip_queues(context=""):
    """Log VIP queue status"""
    with vip_lock:
        vip12 = len(vip_queues["12"])
        vip34 = len(vip_queues["34"])
        if context:
            print(f"[VIP-QUEUES] {context}: 1-2: {vip12}, 3-4: {vip34}")
        else:
            print(f"[VIP-QUEUES] 1-2: {vip12}, 3-4: {vip34}")


def log_signal_status():
    """Log current signal status"""
    with state_lock:
        traffic = f"🚦 Traffic: 1:{signal_status[1]}, 2:{signal_status[2]}, 3:{signal_status[3]}, 4:{signal_status[4]}"
        pedestrian = f"🚶 Pedestrian: P1:{signal_status['P1']}, P2:{signal_status['P2']}, P3:{signal_status['P3']}, P4:{signal_status['P4']}"
        print(f"[SIGNAL-STATUS] {traffic}")
        print(f"[SIGNAL-STATUS] {pedestrian}")


# ====== LAMPORT CLOCK FUNCTIONS ======
def increment_lamport_clock():
    """Increment Lamport clock for RA"""
    global lamport_clock
    with clock_lock:
        lamport_clock += 1
        print(f"[LAMPORT] Clock incremented to: {lamport_clock}")
        return lamport_clock


def update_lamport_clock(received_timestamp):
    """Update Lamport clock on message receipt"""
    global lamport_clock
    with clock_lock:
        lamport_clock = max(lamport_clock, received_timestamp) + 1
        print(f"[LAMPORT] Clock updated to: {lamport_clock} (received: {received_timestamp})")


def get_zookeeper_proxy():
    """Get ZooKeeper connection"""
    return ServerProxy(ZOOKEEPER_URL, allow_none=True,
                       transport=TimeoutTransport(RESPONSE_TIMEOUT))


# ====== RICART-AGRAWALA IMPLEMENTATION ======
def request_critical_section(target_pair, request_type="normal", requester_info=""):
    """Request critical section using Ricart-Agrawala"""
    log_separator(f"RICART-AGRAWALA REQUEST [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] Requesting CS at {time.time()}")
    global ra_state

    print(f"[{CONTROLLER_NAME}] Requesting CS for {target_pair} ({request_type}) - {requester_info}")

    with ra_lock:
        if ra_state['in_cs']:
            print(f"[{CONTROLLER_NAME}] Already in CS!")
            return True

        ra_state['requesting_cs'] = True
        ra_state['received_oks'] = 0
        ra_state['expected_oks'] = 2  # controller_clone + p_signal
        timestamp = increment_lamport_clock()
        ra_state['pending_request'] = {
            'timestamp': timestamp,
            'target_pair': target_pair,
            'request_type': request_type,
            'requester_info': requester_info
        }

    # Send RA request to other nodes via ZooKeeper
    try:
        zk_proxy = get_zookeeper_proxy()
        other_nodes = ["controller", "p_signal"]

        responses = {}
        for node in other_nodes:
            try:
                response = zk_proxy.forward_ra_request(
                    CONTROLLER_NAME, node, timestamp, target_pair, request_type, requester_info
                )
                responses[node] = response
                print(f"[{CONTROLLER_NAME}] RA request to {node}: {response}")
            except Exception as e:
                print(f"[{CONTROLLER_NAME}] RA request to {node} failed: {e}")
                responses[node] = "ERROR"

        # If we got at least one OK, consider it a partial success
        if "OK" in responses.values():
            print(f"[{CONTROLLER_NAME}] Partial RA success, proceeding with available responses")
            with ra_lock:
                ra_state['in_cs'] = True
                ra_state['requesting_cs'] = False
            return True

    except Exception as e:
        print(f"[{CONTROLLER_NAME}] RA request failed: {e}")
        with ra_lock:
            ra_state['requesting_cs'] = False
        return False

    # Wait for all OKs (2 total: controller_clone + p_signal)
    max_wait_time = 12  # Maximum time to wait for OKs
    start_time = time.time()

    while time.time() - start_time < max_wait_time:
        with ra_lock:
            if ra_state['received_oks'] >= ra_state['expected_oks']:
                ra_state['in_cs'] = True
                ra_state['requesting_cs'] = False
                print(f"[{CONTROLLER_NAME}] 🎉 ENTERED CRITICAL SECTION")
                log_mutex_state()
                return True
        time.sleep(0.1)

    print(f"[{CONTROLLER_NAME}] ⚠️ Timeout waiting for RA responses")
    with ra_lock:
        ra_state['requesting_cs'] = False
    return False


def release_critical_section():
    """Release critical section and send deferred replies"""
    global ra_state

    with ra_lock:
        if not ra_state['in_cs']:
            return

        ra_state['in_cs'] = False
        deferred = ra_state['deferred_replies'].copy()
        ra_state['deferred_replies'] = []

    print(f"[{CONTROLLER_NAME}] ✅ EXITED CRITICAL SECTION")
    log_mutex_state()

    # Send deferred OK replies via ZooKeeper
    try:
        zk_proxy = get_zookeeper_proxy()
        for deferred_request in deferred:
            zk_proxy.forward_ra_response(
                CONTROLLER_NAME, deferred_request['from_controller'],
                "OK", deferred_request['timestamp'], deferred_request['target_pair']
            )
            print(f"[{CONTROLLER_NAME}] Sent deferred OK to {deferred_request['from_controller']}")
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Failed to send deferred replies: {e}")


def receive_ra_request(from_controller, timestamp, target_pair, request_type):
    """Receive RA request from another controller"""
    print(f"[{CONTROLLER_NAME}] Received RA request at {time.time()}")
    update_lamport_clock(timestamp)

    print(f"[{CONTROLLER_NAME}] RA request from {from_controller} (ts={timestamp})")
    print(f"[{CONTROLLER_NAME}] Target: {target_pair}, Type: {request_type}")
    print(f"[{CONTROLLER_NAME}] Current state: requesting_cs={ra_state['requesting_cs']}, in_cs={ra_state['in_cs']}")

    with ra_lock:
        if (not ra_state['requesting_cs'] and not ra_state['in_cs']) or \
                (ra_state['requesting_cs'] and
                 (timestamp < ra_state['pending_request']['timestamp'] or
                  (timestamp == ra_state['pending_request']['timestamp'] and
                   from_controller < CONTROLLER_NAME))):
            # Send OK immediately
            print(f"[{CONTROLLER_NAME}] ✅ Sending IMMEDIATE OK to {from_controller}")
            try:
                zk_proxy = get_zookeeper_proxy()
                zk_proxy.forward_ra_response(CONTROLLER_NAME, from_controller, "OK", timestamp, target_pair)
            except Exception as e:
                print(f"[{CONTROLLER_NAME}] Failed to send OK: {e}")
            return "OK"
        else:
            # Defer the reply
            print(f"[{CONTROLLER_NAME}] ⏳ DEFERRING reply to {from_controller}")
            ra_state['deferred_replies'].append({
                'from_controller': from_controller,
                'timestamp': timestamp,
                'target_pair': target_pair,
                'request_type': request_type
            })
            return "DEFER"


def print_ra_state():
    """Debug function to print RA state"""
    with ra_lock:
        print(f"[{CONTROLLER_NAME}] RA State:")
        print(f"  requesting_cs: {ra_state['requesting_cs']}")
        print(f"  in_cs: {ra_state['in_cs']}")
        print(f"  received_oks: {ra_state['received_oks']}/{ra_state['expected_oks']}")
        print(f"  deferred_replies: {len(ra_state['deferred_replies'])}")
        if ra_state['pending_request']:
            print(f"  pending_request: {ra_state['pending_request']}")


def receive_ra_response(from_controller, response_type, timestamp, target_pair):
    print(f"[{CONTROLLER_NAME}] RA response from {from_controller}: {response_type}")

    if response_type == "OK":
        with ra_lock:
            ra_state['received_oks'] += 1
            print(f"[{CONTROLLER_NAME}] ✅ Received OK {ra_state['received_oks']}/{ra_state['expected_oks']}")
    else:
        print(f"[{CONTROLLER_NAME}] Received {response_type} response")


# ====== BERKELEY CLOCK SYNC ======
def berkeley_cycle_once():
    """Berkeley clock synchronization - Controller acts as time server"""
    global server_skew

    log_separator("BERKELEY CLOCK SYNCHRONIZATION")
    print(f"[{CONTROLLER_NAME}] Starting Berkeley sync cycle")
    server_time = time.time() + server_skew

    try:
        zk_proxy = get_zookeeper_proxy()
        clients = zk_proxy.get_client_list()

        clock_values = {CONTROLLER_NAME: 0.0}

        # Step 1-3: Collect clock values via ZooKeeper mediation
        for client_name, client_url in clients.items():
            try:
                transport = TimeoutTransport(RESPONSE_TIMEOUT)
                client_proxy = ServerProxy(client_url, allow_none=True, transport=transport)
                clock_value = float(client_proxy.get_clock_value(server_time))
                clock_values[client_name] = clock_value
                print(f"[{CONTROLLER_NAME}] {client_name} clock_value: {clock_value:+.2f}s")
            except Exception as e:
                print(f"[{CONTROLLER_NAME}] Failed to get clock from {client_name}: {e}")

        # Step 4: Calculate average
        if len(clock_values) > 1:
            avg_offset = sum(clock_values.values()) / len(clock_values)
            new_epoch = server_time + avg_offset

            # Step 5-7: Set new time on all clients
            for client_name, client_url in clients.items():
                try:
                    transport = TimeoutTransport(RESPONSE_TIMEOUT)
                    client_proxy = ServerProxy(client_url, allow_none=True, transport=transport)
                    client_proxy.set_time(new_epoch)
                    print(f"[{CONTROLLER_NAME}] Set time on {client_name}")
                except Exception as e:
                    print(f"[{CONTROLLER_NAME}] Failed to set time on {client_name}: {e}")

            # Adjust own clock
            adjustment = new_epoch - server_time
            server_skew += adjustment
            print(f"[{CONTROLLER_NAME}] Clock adjusted by {adjustment:+.2f}s")

    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Berkeley sync failed: {e}")


# ====== SIGNAL CONTROL FUNCTIONS ======
def _get_pedestrian_ack(target_pair, request_type, requester_info):
    """Get pedestrian acknowledgment via ZooKeeper"""
    try:
        zk_proxy = get_zookeeper_proxy()
        timestamp = increment_lamport_clock()
        ped_response = zk_proxy.request_ped_ack(
            CONTROLLER_NAME, target_pair, timestamp, request_type, requester_info
        )

        if ped_response != "OK":
            print(f"[{CONTROLLER_NAME}] ❌ Pedestrian denied permission: {ped_response}")
            return False
        print(f"[{CONTROLLER_NAME}] ✅ Pedestrian clearance granted")
        return True
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] Failed to get pedestrian acknowledgment: {e}")
        return False


def _process_vip_requests():
    """Process any pending VIP requests"""
    with vip_lock:
        for direction in ["12", "34"]:
            if vip_queues[direction]:
                vip_info = vip_queues[direction][0]
                print(f"[VIP-PROCESSING] Handling VIP {vip_info['vehicle_id']} for direction {direction}")
                # In a real implementation, this would trigger VIP processing
                time.sleep(1)  # Simulate VIP processing time
                vip_queues[direction].pop(0)


def handle_pedestrian_signals(target_pair):
    """Handle pedestrian signal transitions with blinking effects"""
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    print(f"[{CONTROLLER_NAME}] Pedestrian {[f'P{x}' for x in red_group]} → BLINKING RED (5s)")

    # Blinking effect for red group
    for i in range(10):  # 5 seconds of blinking (10 half-second intervals)
        with state_lock:
            for sig in red_group:
                signal_status[f"P{sig}"] = "BLINKING RED" if i % 2 == 0 else "OFF"
        time.sleep(0.5)
        if i % 2 == 0:
            print(f"[{CONTROLLER_NAME}] 🔴 Pedestrian {red_group} BLINKING...")

    # Set final red state
    with state_lock:
        for sig in red_group:
            signal_status[f"P{sig}"] = "RED"
    print(f"[{CONTROLLER_NAME}] 🔴 Pedestrian {red_group} → SOLID RED")

    # Yellow transition for green group
    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "YELLOW"
    print(f"[{CONTROLLER_NAME}] 🟡 Pedestrian {green_group} → YELLOW")
    time.sleep(2)  # Yellow light duration

    # Set green state
    with state_lock:
        for sig in green_group:
            signal_status[f"P{sig}"] = "GREEN"
    print(f"[{CONTROLLER_NAME}] 🟢 Pedestrian {green_group} → GREEN")


def handle_traffic_signals(target_pair):
    """Handle traffic signal transitions"""
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    # Yellow transition for red group
    with state_lock:
        for sig in red_group:
            signal_status[sig] = "YELLOW"
    print(f"[{CONTROLLER_NAME}] 🟡 Traffic {red_group} → YELLOW")
    time.sleep(3)  # Yellow light duration

    # Set red state
    with state_lock:
        for sig in red_group:
            signal_status[sig] = "RED"
    print(f"[{CONTROLLER_NAME}] 🔴 Traffic {red_group} → RED")

    # Short pause before turning green
    time.sleep(1)

    # Set green state for target pair
    with state_lock:
        for sig in target_pair:
            signal_status[sig] = "GREEN"
    print(f"[{CONTROLLER_NAME}] 🟢 Traffic {target_pair} → GREEN")


def _switch_to(target_pair):
    """Perform full pedestrian + traffic signal transition"""
    handle_pedestrian_signals(target_pair)
    handle_traffic_signals(target_pair)


# ====== MAIN TRAFFIC CONTROL LOGIC ======
def signal_controller(target_pair):
    """Main signal control with RA mutual exclusion"""
    log_separator(f"NORMAL TRAFFIC REQUEST: {target_pair} [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] Request for intersection {target_pair}")

    # Initiate Berkeley sync
    berkeley_cycle_once()

    # Check if any VIPs are pending - they get priority
    with vip_lock:
        vip_count = len(vip_queues["12"]) + len(vip_queues["34"])

    if vip_count > 0:
        print(f"[RICART-AGRAWALA] {vip_count} VIPs have higher priority - deferring normal traffic")
        log_vip_queues("VIPs blocking normal traffic")
        _process_vip_requests()
        print(f"[RICART-AGRAWALA] VIPs cleared - processing normal traffic")

    # Request critical section using Ricart-Agrawala
    if not request_critical_section(target_pair, "normal", "Normal Traffic"):
        print(f"[{CONTROLLER_NAME}] ❌ Failed to acquire critical section")
        return False

    # Get pedestrian acknowledgment
    if not _get_pedestrian_ack(target_pair, "normal", "Normal Traffic"):
        release_critical_section()
        return False

    try:
        print(f"[CRITICAL-SECTION] Normal traffic ACQUIRED intersection mutex for {target_pair} [{CONTROLLER_NAME}]")

        # Perform signal changes
        _switch_to(target_pair)

        print(f"[CRITICAL-SECTION] Normal traffic RELEASED intersection mutex [{CONTROLLER_NAME}]")
        print(f"[{CONTROLLER_NAME}] ✅ Completed normal signal cycle.")
        log_signal_status()
        return True

    finally:
        # Always release critical section
        release_critical_section()
        log_mutex_state()
        log_separator()


def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """VIP request with RA mutual exclusion"""
    vehicle_id = vehicle_id or str(uuid.uuid4())[:8]
    requester_info = f"VIP_{priority}_{vehicle_id}"

    log_separator(f"VIP ARRIVAL: {requester_info} [{CONTROLLER_NAME}]")
    print(f"[{CONTROLLER_NAME}] VIP arrival: {requester_info} for {target_pair}")

    # Add to VIP queue for deadlock detection
    pair_key = "12" if target_pair in [[1, 2], [2, 1]] else "34"
    with vip_lock:
        vip_queues[pair_key].append({
            'vehicle_id': vehicle_id,
            'priority': priority,
            'target_pair': target_pair,
            'timestamp': time.time()
        })
        log_vip_queues(f"VIP {vehicle_id} added to queue")

    # Check for deadlock scenario
    with vip_lock:
        if vip_queues["12"] and vip_queues["34"]:
            print(f"🚨 VIP DEADLOCK DETECTED! Both directions have VIPs waiting")
            vip12 = vip_queues["12"][0]
            vip34 = vip_queues["34"][0]

            # Resolve deadlock - higher priority or earlier timestamp wins
            if vip12['priority'] > vip34['priority'] or \
                    (vip12['priority'] == vip34['priority'] and vip12['timestamp'] < vip34['timestamp']):
                print(f"⚖️ Deadlock resolved: VIP {vip12['vehicle_id']} goes first (higher priority/earlier)")
            else:
                print(f"⚖️ Deadlock resolved: VIP {vip34['vehicle_id']} goes first (higher priority/earlier)")

    if not request_critical_section(target_pair, "VIP", requester_info):
        return False

    try:
        # Get pedestrian acknowledgment for VIP
        if not _get_pedestrian_ack(target_pair, "VIP", requester_info):
            return False

        # Perform signal changes
        _switch_to(target_pair)

        # VIP crossing time
        print(f"[{CONTROLLER_NAME}] VIP {vehicle_id} crossing...")
        time.sleep(2)

        print(f"[{CONTROLLER_NAME}] ✅ VIP {vehicle_id} completed crossing")
        log_signal_status()

        # Remove from VIP queue
        with vip_lock:
            if vip_queues[pair_key] and vip_queues[pair_key][0]['vehicle_id'] == vehicle_id:
                vip_queues[pair_key].pop(0)
                log_vip_queues(f"VIP {vehicle_id} removed from queue")

        return True

    finally:
        release_critical_section()
        log_mutex_state()
        log_separator()


# ====== RPC METHODS ======
def ping():
    return "OK"


def get_signal_status():
    with state_lock:
        return signal_status.copy()


if __name__ == "__main__":
    print(f"[{CONTROLLER_NAME}] Starting Ricart-Agrawala Traffic Controller")
    print(f"[{CONTROLLER_NAME}] Port: {CONTROLLER_PORT}")
    print(f"[{CONTROLLER_NAME}] ZooKeeper: {ZOOKEEPER_URL}")
    print_ra_state()
    log_signal_status()

    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(is_ready, "is_ready")
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(receive_ra_request, "receive_ra_request")
    server.register_function(receive_ra_response, "receive_ra_response")
    server.register_function(berkeley_cycle_once, "berkeley_cycle_once")
    server.register_function(ping, "ping")
    server.register_function(get_signal_status, "get_signal_status")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{CONTROLLER_NAME}] Shutting down...")


