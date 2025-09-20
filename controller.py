# controller.py - Full Ricart-Agrawala Implementation
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import uuid

# Configuration
CONTROLLER_PORT = 8000
CONTROLLER_NAME = "CONTROLLER"
ZOOKEEPER_URL = "http://localhost:6000"
RESPONSE_TIMEOUT = 10

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


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def increment_lamport_clock():
    """Increment Lamport clock for RA"""
    global lamport_clock
    with clock_lock:
        lamport_clock += 1
        return lamport_clock


def update_lamport_clock(received_timestamp):
    """Update Lamport clock on message receipt"""
    global lamport_clock
    with clock_lock:
        lamport_clock = max(lamport_clock, received_timestamp) + 1


def get_zookeeper_proxy():
    """Get ZooKeeper connection"""
    return ServerProxy(ZOOKEEPER_URL, allow_none=True,
                       transport=TimeoutTransport(RESPONSE_TIMEOUT))


# Ricart-Agrawala Implementation
def request_critical_section(target_pair, request_type="normal", requester_info=""):
    """Request critical section using Ricart-Agrawala"""
    global ra_state

    print(f"[{CONTROLLER_NAME}] Requesting CS for {target_pair} ({request_type})")

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
        other_nodes = ["controller_clone", "p_signal"]  # Both nodes for RA

        for node in other_nodes:
            response = zk_proxy.forward_ra_request(
                CONTROLLER_NAME, node, timestamp, target_pair, request_type
            )
            print(f"[{CONTROLLER_NAME}] RA request to {node}: {response}")
    except Exception as e:
        print(f"[{CONTROLLER_NAME}] RA request failed: {e}")
        with ra_lock:
            ra_state['requesting_cs'] = False
        return False

    # Wait for all OKs (2 total: controller_clone + p_signal)
    while True:
        with ra_lock:
            if ra_state['received_oks'] >= ra_state['expected_oks']:
                ra_state['in_cs'] = True
                ra_state['requesting_cs'] = False
                print(f"[{CONTROLLER_NAME}] ENTERED CRITICAL SECTION")
                return True
        time.sleep(0.1)


def release_critical_section():
    """Release critical section and send deferred replies"""
    global ra_state

    with ra_lock:
        if not ra_state['in_cs']:
            return

        ra_state['in_cs'] = False
        deferred = ra_state['deferred_replies'].copy()
        ra_state['deferred_replies'] = []

    print(f"[{CONTROLLER_NAME}] EXITED CRITICAL SECTION")

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
    update_lamport_clock(timestamp)

    print(f"[{CONTROLLER_NAME}] RA request from {from_controller} (ts={timestamp})")

    with ra_lock:
        if (not ra_state['requesting_cs'] and not ra_state['in_cs']) or \
                (ra_state['requesting_cs'] and
                 (timestamp < ra_state['pending_request']['timestamp'] or
                  (timestamp == ra_state['pending_request']['timestamp'] and
                   from_controller < CONTROLLER_NAME))):
            # Send OK immediately
            print(f"[{CONTROLLER_NAME}] Sending OK to {from_controller}")
            try:
                zk_proxy = get_zookeeper_proxy()
                zk_proxy.forward_ra_response(CONTROLLER_NAME, from_controller, "OK", timestamp, target_pair)
            except Exception as e:
                print(f"[{CONTROLLER_NAME}] Failed to send OK: {e}")
            return "OK"
        else:
            # Defer the reply
            ra_state['deferred_replies'].append({
                'from_controller': from_controller,
                'timestamp': timestamp,
                'target_pair': target_pair,
                'request_type': request_type
            })
            print(f"[{CONTROLLER_NAME}] Deferring reply to {from_controller}")
            return "DEFER"


def receive_ra_response(from_controller, response_type, timestamp, target_pair):
    """Receive RA response from another controller"""
    print(f"[{CONTROLLER_NAME}] RA response from {from_controller}: {response_type}")

    if response_type == "OK":
        with ra_lock:
            ra_state['received_oks'] += 1
            print(f"[{CONTROLLER_NAME}] Received OK {ra_state['received_oks']}/{ra_state['expected_oks']}")


# Berkeley Clock Sync
def berkeley_cycle_once():
    """Berkeley clock synchronization - Controller acts as time server"""
    global server_skew

    print(f"[{CONTROLLER_NAME}] Starting Berkeley sync cycle")
    server_time = time.time() + server_skew

    try:
        zk_proxy = get_zookeeper_proxy()
        clients = zk_proxy.get_client_list()

        clock_values = {CONTROLLER_NAME: 0.0}

        # Step 1-3: Collect clock values via ZooKeeper mediation
        for client_name, client_url in clients.items():
            try:
                client_proxy = ServerProxy(client_url, allow_none=True,
                                           transport=TimeoutTransport(RESPONSE_TIMEOUT))
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
                    client_proxy = ServerProxy(client_url, allow_none=True,
                                               transport=TimeoutTransport(RESPONSE_TIMEOUT))
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


# Main Traffic Control Logic
def signal_controller(target_pair):
    """Main signal control with RA mutual exclusion"""
    print(f"[{CONTROLLER_NAME}] Processing signal request for {target_pair}")

    # Request critical section using Ricart-Agrawala
    if not request_critical_section(target_pair, "normal", "Normal Traffic"):
        print(f"[{CONTROLLER_NAME}] Failed to acquire critical section")
        return False

    try:
        # Initiate Berkeley sync while in CS
        zk_proxy = get_zookeeper_proxy()
        zk_proxy.coordinate_berkeley_sync(CONTROLLER_NAME)

        # Get pedestrian acknowledgment via ZooKeeper
        zk_proxy = get_zookeeper_proxy()
        timestamp = increment_lamport_clock()
        ped_response = zk_proxy.request_ped_ack(
            CONTROLLER_NAME, target_pair, timestamp, "normal", "Normal Traffic"
        )

        if ped_response != "OK":
            print(f"[{CONTROLLER_NAME}] Pedestrian denied permission: {ped_response}")
            return False

        # Perform signal changes
        handle_pedestrian_signals(target_pair)
        handle_traffic_signals(target_pair)

        print(f"[{CONTROLLER_NAME}] Signal change completed")
        return True

    finally:
        # Always release critical section
        release_critical_section()


def vip_arrival(target_pair, priority=1, vehicle_id=None):
    """VIP request with RA mutual exclusion"""
    vehicle_id = vehicle_id or str(uuid.uuid4())[:8]
    requester_info = f"VIP_{priority}_{vehicle_id}"

    print(f"[{CONTROLLER_NAME}] VIP arrival: {requester_info} for {target_pair}")

    if not request_critical_section(target_pair, "VIP", requester_info):
        return False

    try:


        # Get pedestrian acknowledgment for VIP
        zk_proxy = get_zookeeper_proxy()
        zk_proxy.coordinate_berkeley_sync(CONTROLLER_NAME)
        timestamp = increment_lamport_clock()
        ped_response = zk_proxy.request_ped_ack(
            CONTROLLER_NAME, target_pair, timestamp, "VIP", requester_info
        )

        if ped_response != "OK":
            print(f"[{CONTROLLER_NAME}] Pedestrian denied VIP: {ped_response}")
            return False

        handle_pedestrian_signals(target_pair)
        handle_traffic_signals(target_pair)

        # VIP crossing time
        time.sleep(2)

        print(f"[{CONTROLLER_NAME}] VIP {vehicle_id} completed crossing")
        return True

    finally:
        release_critical_section()


# Signal Control Functions (same as before but with proper locking)
def handle_pedestrian_signals(target_pair):
    red_group = target_pair
    green_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    with state_lock:
        for sig in red_group:
            signal_status[f"P{sig}"] = "RED"
        for sig in green_group:
            signal_status[f"P{sig}"] = "GREEN"

    print(f"[{CONTROLLER_NAME}] Pedestrian signals updated")


def handle_traffic_signals(target_pair):
    red_group = [3, 4] if target_pair == [1, 2] else [1, 2]

    with state_lock:
        for sig in red_group:
            signal_status[sig] = "RED"
        for sig in target_pair:
            signal_status[sig] = "GREEN"

    print(f"[{CONTROLLER_NAME}] Traffic signals: {target_pair} -> GREEN")


# RPC Methods
def ping():
    return "OK"


def get_signal_status():
    with state_lock:
        return signal_status.copy()


if __name__ == "__main__":
    print(f"[{CONTROLLER_NAME}] Starting Ricart-Agrawala Traffic Controller")
    print(f"[{CONTROLLER_NAME}] Port: {CONTROLLER_PORT}")
    print(f"[{CONTROLLER_NAME}] ZooKeeper: {ZOOKEEPER_URL}")

    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
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


