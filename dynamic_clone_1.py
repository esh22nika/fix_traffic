
# dynamic_clone_1.py - Dynamically created controller clone
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from dataclasses import dataclass
from typing import List
import uuid

# Configuration for dynamic clone
CLIENTS = {
    "t_signal": "http://192.168.0.165:7000",
    "p_signal": "http://192.168.0.176:9000",
}
PEDESTRIAN_IP = CLIENTS["p_signal"]
RESPONSE_TIMEOUT = 3
VIP_CROSSING_TIME = 0.1  # Minimal processing time
CONTROLLER_PORT = 8003
CONTROLLER_NAME = "DYNAMIC_CLONE_1"

server_skew = 0.0
state_lock = threading.Lock()
vip_queues = {"12": [], "34": []}

# Simplified signal status - all keys as strings
signal_status = {
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

def ping():
    return "OK"

def signal_controller(target_pair):
    print(f"[{CONTROLLER_NAME}] Processing signal request for {target_pair}")
    time.sleep(0.1)
    return True

def vip_arrival(target_pair, priority=1, vehicle_id=None):
    print(f"[{CONTROLLER_NAME}] Processing VIP request for {target_pair}")
    time.sleep(0.1)
    return True

if __name__ == "__main__":
    print(f"[{CONTROLLER_NAME}] Dynamic clone starting on port {CONTROLLER_PORT}")
    server = SimpleXMLRPCServer(("0.0.0.0", CONTROLLER_PORT), allow_none=True)
    server.register_function(signal_controller, "signal_controller")
    server.register_function(vip_arrival, "vip_arrival")
    server.register_function(ping, "ping")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print(f"\n[{CONTROLLER_NAME}] Shutting down...")

