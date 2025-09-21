# app.py - Fixed Flask server for the frontend
from flask import Flask, render_template, jsonify
import requests
import threading
import time
from xmlrpc.client import ServerProxy
import logging
import http.client

# Configuration
FLASK_PORT = 5000
ZOOKEEPER_URL = "http://127.0.0.1:6000"
UPDATE_INTERVAL = 2000  # Update every 2 seconds

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Global state variables
signal_status = {
    "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
    "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
}

active_vips = {}
deadlock_active = False
connection_status = False


class TimeoutTransport:
    def __init__(self, timeout=10):
        self.timeout = timeout

    def request(self, host, handler, request_body, verbose=0):
        """Make an XML-RPC request with timeout"""
        try:
            # Parse host to get hostname and port
            import urllib.parse
            if "://" in host:
                parsed = urllib.parse.urlparse(host)
                hostname = parsed.hostname
                port = parsed.port or 80
            else:
                parts = host.split(":")
                hostname = parts[0]
                port = int(parts[1]) if len(parts) > 1 else 80

            # Create connection with timeout
            conn = http.client.HTTPConnection(hostname, port, timeout=self.timeout)

            # Ensure request_body is bytes
            if isinstance(request_body, str):
                request_body = request_body.encode('utf-8')
            elif isinstance(request_body, bytes):
                pass  # Already bytes, no need to encode

            # Send request
            conn.putrequest("POST", handler)
            conn.putheader("Content-Type", "text/xml")
            conn.putheader("Content-Length", str(len(request_body)))
            conn.putheader("User-Agent", "Python-xmlrpc/3.x")
            conn.endheaders()
            conn.send(request_body)

            # Get response
            response = conn.getresponse()
            data = response.read()
            conn.close()

            # Parse XML-RPC response
            import xmlrpc.client
            parser, unmarshaller = xmlrpc.client.getparser()
            parser.feed(data)
            parser.close()
            return unmarshaller.close()

        except Exception as e:
            logger.error(f"XML-RPC request failed: {e}")
            raise


def get_zookeeper_proxy():
    """Get ZooKeeper connection with proper timeout handling"""
    try:
        transport = TimeoutTransport(timeout=10)
        return ServerProxy(ZOOKEEPER_URL, allow_none=True, transport=transport)
    except Exception as e:
        logger.error(f"Failed to create ZooKeeper proxy: {e}")
        return None


def fetch_status():
    """Fetch status from ZooKeeper"""
    global signal_status, connection_status

    try:
        proxy = get_zookeeper_proxy()
        if proxy is None:
            raise Exception("Failed to create ZooKeeper proxy")

        # Test connection first
        ping_response = proxy.ping()
        connection_status = True
        logger.info(f"Connected to ZooKeeper: {ping_response}")

        # Get signal status
        status = proxy.get_signal_status()
        if status and isinstance(status, dict):
            # Ensure all keys are strings for JSON serialization
            normalized_status = {}
            for key, value in status.items():
                normalized_status[str(key)] = str(value)
            signal_status = normalized_status

    except Exception as e:
        logger.error(f"Failed to connect to ZooKeeper: {e}")
        connection_status = False
        # Use default status when connection fails
        signal_status = {
            "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
            "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
        }


def status_updater():
    """Background thread to update status periodically"""
    while True:
        try:
            fetch_status()
            time.sleep(UPDATE_INTERVAL / 1000)  # Convert ms to seconds
        except Exception as e:
            logger.error(f"Error in status updater: {e}")
            time.sleep(5)


# Start the status updater thread
status_thread = threading.Thread(target=status_updater, daemon=True)
status_thread.start()


@app.route('/')
def index():
    """Serve the main page"""
    return render_template('frontend.html')


@app.route('/api/status')
def get_status():
    """API endpoint to get current status"""
    try:
        return jsonify({
            'signal_status': signal_status,
            'connection_status': connection_status,
            'active_vips': active_vips,
            'deadlock_active': deadlock_active,
            'last_update': time.time()
        })
    except Exception as e:
        logger.error(f"Error in get_status: {e}")
        return jsonify({
            'error': str(e),
            'signal_status': {
                "1": "RED", "2": "RED", "3": "GREEN", "4": "GREEN",
                "P1": "GREEN", "P2": "GREEN", "P3": "RED", "P4": "RED"
            },
            'connection_status': False,
            'active_vips': {},
            'deadlock_active': False,
            'last_update': time.time()
        }), 500


@app.route('/api/simulate/traffic', methods=['POST'])
def simulate_traffic():
    """Simulate traffic detection"""
    try:
        proxy = get_zookeeper_proxy()
        if proxy is None:
            return jsonify({'success': False, 'error': 'ZooKeeper connection failed'})

        # Randomly choose a signal pair
        pairs = [[1, 2], [3, 4]]
        target_pair = pairs[0] if time.time() % 2 == 0 else pairs[1]

        result = proxy.signal_request("web_client", target_pair, "normal")

        return jsonify({
            'success': True,
            'target_pair': target_pair,
            'result': str(result)  # Ensure result is JSON serializable
        })

    except Exception as e:
        logger.error(f"Traffic simulation error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/simulate/vip', methods=['POST'])
def simulate_vip():
    """Simulate VIP arrival"""
    try:
        proxy = get_zookeeper_proxy()
        if proxy is None:
            return jsonify({'success': False, 'error': 'ZooKeeper connection failed'})

        # Randomly choose a signal pair and priority
        pairs = [[1, 2], [3, 4]]
        target_pair = pairs[0] if time.time() % 2 == 0 else pairs[1]
        priority = int(time.time() % 4) + 1  # Priority 1-4
        vehicle_id = f"VIP_{int(time.time())}"

        result = proxy.vip_arrival("web_client", target_pair, priority, vehicle_id)

        # Track this VIP
        direction = "12" if target_pair in [[1, 2], [2, 1]] else "34"
        active_vips[direction] = {
            'vehicle_id': vehicle_id,
            'priority': priority,
            'target_pair': target_pair
        }

        return jsonify({
            'success': True,
            'vehicle_id': vehicle_id,
            'priority': priority,
            'target_pair': target_pair,
            'result': str(result)  # Ensure result is JSON serializable
        })

    except Exception as e:
        logger.error(f"VIP simulation error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/simulate/deadlock', methods=['POST'])
def simulate_deadlock():
    """Simulate deadlock scenario"""
    global deadlock_active, active_vips

    try:
        # Create VIPs in both directions
        vip1 = {
            'vehicle_id': f"VIP_1_{int(time.time())}",
            'priority': 3,
            'target_pair': [1, 2]
        }

        vip2 = {
            'vehicle_id': f"VIP_2_{int(time.time())}",
            'priority': 2,
            'target_pair': [3, 4]
        }

        active_vips["12"] = vip1
        active_vips["34"] = vip2
        deadlock_active = True

        # Auto-resolve after some time
        def resolve_deadlock():
            time.sleep(8)  # Increased time for better visibility
            global deadlock_active
            deadlock_active = False

            # Higher priority goes first
            if vip1['priority'] > vip2['priority']:
                if "12" in active_vips:
                    del active_vips["12"]
            else:
                if "34" in active_vips:
                    del active_vips["34"]

        threading.Thread(target=resolve_deadlock, daemon=True).start()

        return jsonify({
            'success': True,
            'vip1': vip1,
            'vip2': vip2,
            'message': 'Deadlock scenario created, will auto-resolve in 8 seconds'
        })

    except Exception as e:
        logger.error(f"Deadlock simulation error: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/system/info')
def get_system_info():
    """Get system information"""
    try:
        proxy = get_zookeeper_proxy()
        system_info = {
            'zookeeper_connected': connection_status,
            'active_vips_count': len(active_vips),
            'deadlock_active': deadlock_active,
            'signal_pairs': {
                'green': [],
                'red': []
            }
        }

        # Categorize signals
        for sig_id, status in signal_status.items():
            if sig_id.startswith('P'):  # Skip pedestrian signals for this view
                continue
            if status == 'GREEN':
                system_info['signal_pairs']['green'].append(int(sig_id))
            else:
                system_info['signal_pairs']['red'].append(int(sig_id))

        if proxy and connection_status:
            try:
                # Try to get additional system info
                zk_info = proxy.ping()
                system_info['zookeeper_info'] = str(zk_info)
            except:
                system_info['zookeeper_info'] = 'Connected but limited info'

        return jsonify(system_info)

    except Exception as e:
        logger.error(f"System info error: {e}")
        return jsonify({
            'zookeeper_connected': False,
            'error': str(e),
            'active_vips_count': 0,
            'deadlock_active': False
        })


@app.errorhandler(500)
def internal_error(error):
    """Handle internal server errors"""
    logger.error(f"Internal server error: {error}")
    return jsonify({
        'error': 'Internal server error',
        'message': 'Please check the server logs for details'
    }), 500


@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'error': 'Not found',
        'message': 'The requested resource was not found'
    }), 404


if __name__ == '__main__':
    logger.info(f"Starting Flask frontend on port {FLASK_PORT}")
    logger.info(f"ZooKeeper URL: {ZOOKEEPER_URL}")

    # Test initial connection
    try:
        fetch_status()
        if connection_status:
            logger.info("Initial ZooKeeper connection successful")
        else:
            logger.warning("Initial ZooKeeper connection failed - will keep retrying")
    except Exception as e:
        logger.error(f"Initial connection test failed: {e}")

    app.run(host='0.0.0.0', port=FLASK_PORT, debug=True, threaded=True)