# master_server.py
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.client import ServerProxy, Transport
import time
import threading
from typing import Dict, List
import hashlib
import logging
from collections import deque

# Configuration
MASTER_PORT = 6100
REPLICA_SERVERS = {
    "server_1": "http://127.0.0.1:7001",
    "server_2": "http://127.0.0.1:7002",
    "server_3": "http://127.0.0.1:7003"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [MASTER] %(message)s')
logger = logging.getLogger(__name__)


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


class ConsistentHashManager:
    """Hash ring for chunk distribution"""

    def __init__(self, servers, virtual_nodes=150):
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
        key_hash = self._hash(str(key))
        servers = []
        start_idx = 0

        for i, hash_val in enumerate(self.sorted_hashes):
            if hash_val >= key_hash:
                start_idx = i
                break

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


class MasterServer:
    def __init__(self):
        self.hash_manager = ConsistentHashManager(list(REPLICA_SERVERS.keys()))
        self.chunk_size = 3
        self.replication_factor = 3

    def get_chunk_metadata(self, data_type):
        """Get detailed chunk metadata for RTO clients with comprehensive logging"""
        try:
            logger.info(f"Generating metadata for {data_type}...")

            # Get sample data from first available replica
            sample_data = None
            replica_used = None
            for server_name, server_url in REPLICA_SERVERS.items():
                try:
                    transport = TimeoutTransport(5)
                    proxy = ServerProxy(server_url, allow_none=True, transport=transport)
                    if data_type == "signal_status":
                        sample_data = proxy.get_signal_status()
                    elif data_type == "system_status":
                        sample_data = proxy.get_system_stats()
                    if sample_data:
                        replica_used = server_name
                        logger.info(f"Sample data retrieved from {server_name}")
                        break
                except Exception as e:
                    logger.warning(f"Failed to get data from {server_name}: {e}")
                    continue

            if sample_data is None:
                sample_data = {"placeholder": "empty"}
                logger.warning("No sample data available, using placeholder")

            # Create chunks
            if isinstance(sample_data, dict):
                items = list(sample_data.items())
                total_items = len(sample_data)
            else:
                items = list(sample_data) if sample_data else []
                total_items = len(sample_data) if sample_data else 0

            if not items:
                items = [("placeholder", "empty")]
                total_items = 1

            chunks = {}
            for i in range(0, len(items), self.chunk_size):
                chunk_data = items[i:i + self.chunk_size]
                chunk_id = f"{data_type}_chunk_{i // self.chunk_size}"

                responsible_servers = self.hash_manager.get_servers_for_key(
                    chunk_id, self.replication_factor
                )

                chunks[chunk_id] = {
                    'chunk_id': i // self.chunk_size,
                    'data': chunk_data,
                    'servers': responsible_servers,
                    'size': len(chunk_data),
                    'start_index': i,
                    'end_index': min(i + self.chunk_size - 1, len(items) - 1)
                }

            # Calculate statistics
            total_chunks = len(chunks)
            total_data_items = total_items
            avg_chunk_size = total_data_items / total_chunks if total_chunks > 0 else 0

            # Server distribution analysis
            server_distribution = {}
            for chunk_info in chunks.values():
                for server in chunk_info['servers']:
                    server_distribution[server] = server_distribution.get(server, 0) + 1

            # Build detailed metadata response
            metadata = {
                'total_chunks': total_chunks,
                'chunk_size': self.chunk_size,
                'replication_factor': self.replication_factor,
                'data_type': data_type,
                'total_data_items': total_data_items,
                'average_chunk_size': round(avg_chunk_size, 2),
                'sample_source': replica_used or "none",

                'chunk_distribution': {chunk_id: chunk_info['servers']
                                       for chunk_id, chunk_info in chunks.items()},

                'server_distribution': server_distribution,

                'chunk_details': {chunk_id: {
                    'chunk_number': chunk_info['chunk_id'],
                    'size': chunk_info['size'],
                    'servers': chunk_info['servers'],
                    'data_range': f"items {chunk_info['start_index']}-{chunk_info['end_index']}",
                    'server_count': len(chunk_info['servers'])
                } for chunk_id, chunk_info in chunks.items()},

                'system_info': {
                    'available_servers': list(REPLICA_SERVERS.keys()),
                    'total_servers': len(REPLICA_SERVERS),
                    'generation_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                    'data_integrity': "complete" if replica_used else "degraded"
                }
            }

            # Log detailed metadata information
            self._log_metadata_details(metadata)

            return metadata

        except Exception as e:
            logger.error(f"Failed to generate metadata for {data_type}: {e}")
            # Return minimal but detailed error metadata
            return self._get_error_metadata(data_type, str(e))

    def _log_metadata_details(self, metadata):
        """Log detailed metadata information"""
        logger.info("=" * 80)
        logger.info("METADATA GENERATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Data Type: {metadata['data_type']}")
        logger.info(f"Total Chunks: {metadata['total_chunks']}")
        logger.info(f"Total Data Items: {metadata['total_data_items']}")
        logger.info(f"Chunk Size: {metadata['chunk_size']} items")
        logger.info(f"Replication Factor: {metadata['replication_factor']}")
        logger.info(f"Average Chunk Size: {metadata['average_chunk_size']:.2f} items")
        logger.info(f"Sample Source: {metadata['sample_source']}")

        logger.info("\nSERVER DISTRIBUTION:")
        for server, chunk_count in metadata['server_distribution'].items():
            logger.info(f"  {server}: {chunk_count} chunks")

        logger.info("\nCHUNK DETAILS:")
        for chunk_id, details in metadata['chunk_details'].items():
            logger.info(f"  {chunk_id}:")
            logger.info(f"    Size: {details['size']} items")
            logger.info(f"    Servers: {', '.join(details['servers'])}")
            logger.info(f"    Data Range: {details['data_range']}")

        logger.info("\nSYSTEM INFO:")
        logger.info(f"  Available Servers: {', '.join(metadata['system_info']['available_servers'])}")
        logger.info(f"  Generation Time: {metadata['system_info']['generation_time']}")
        logger.info(f"  Data Integrity: {metadata['system_info']['data_integrity']}")
        logger.info("=" * 80)

    def _get_error_metadata(self, data_type, error_message):
        """Generate error metadata when things go wrong"""
        return {
            'total_chunks': 1,
            'chunk_size': self.chunk_size,
            'replication_factor': self.replication_factor,
            'data_type': data_type,
            'total_data_items': 1,
            'average_chunk_size': 1.0,
            'sample_source': 'error',
            'error': error_message,

            'chunk_distribution': {f"{data_type}_chunk_0": list(REPLICA_SERVERS.keys())},

            'server_distribution': {server: 1 for server in REPLICA_SERVERS.keys()},

            'chunk_details': {f"{data_type}_chunk_0": {
                'chunk_number': 0,
                'size': 1,
                'servers': list(REPLICA_SERVERS.keys()),
                'data_range': "items 0-0",
                'server_count': len(REPLICA_SERVERS)
            }},

            'system_info': {
                'available_servers': list(REPLICA_SERVERS.keys()),
                'total_servers': len(REPLICA_SERVERS),
                'generation_time': time.strftime("%Y-%m-%d %H:%M:%S"),
                'data_integrity': 'error',
                'error_message': error_message
            }
        }

    def ping(self):
        return "Master Server OK"

    def get_replica_status(self):
        """Check status of all replica servers with detailed reporting"""
        status = {}
        online_servers = 0

        logger.info("Checking replica server status...")

        for server_name, server_url in REPLICA_SERVERS.items():
            try:
                transport = TimeoutTransport(3)
                proxy = ServerProxy(server_url, allow_none=True, transport=transport)
                response = proxy.ping()

                # Try to get some basic info from the replica
                try:
                    signal_status = proxy.get_signal_status()
                    signal_count = len(signal_status) if isinstance(signal_status, dict) else 0
                except:
                    signal_count = 0

                status[server_name] = {
                    "status": "online",
                    "response": response,
                    "signal_count": signal_count,
                    "url": server_url,
                    "response_time": time.strftime("%H:%M:%S")
                }
                online_servers += 1
                logger.info(f"✓ {server_name}: ONLINE ({signal_count} signals)")

            except Exception as e:
                status[server_name] = {
                    "status": "offline",
                    "error": str(e),
                    "url": server_url,
                    "response_time": time.strftime("%H:%M:%S")
                }
                logger.warning(f"✗ {server_name}: OFFLINE - {e}")

        # Add summary information
        status['summary'] = {
            'total_servers': len(REPLICA_SERVERS),
            'online_servers': online_servers,
            'offline_servers': len(REPLICA_SERVERS) - online_servers,
            'health_percentage': (online_servers / len(REPLICA_SERVERS)) * 100 if REPLICA_SERVERS else 0,
            'check_time': time.strftime("%Y-%m-%d %H:%M:%S")
        }

        logger.info(f"Replica Status: {online_servers}/{len(REPLICA_SERVERS)} servers online")
        logger.info(f"Health: {status['summary']['health_percentage']:.1f}%")

        return status


if __name__ == "__main__":
    logger.info("=" * 80)
    logger.info("STARTING GFS MASTER SERVER")
    logger.info("=" * 80)
    logger.info(f"Port: {MASTER_PORT}")
    logger.info(f"Replica Servers: {list(REPLICA_SERVERS.keys())}")
    logger.info(f"Chunk Size: 3")
    logger.info(f"Replication Factor: 3")
    logger.info("=" * 80)

    master = MasterServer()
    server = SimpleXMLRPCServer(("0.0.0.0", MASTER_PORT), allow_none=True)

    server.register_function(master.get_chunk_metadata, "get_chunk_metadata")
    server.register_function(master.ping, "ping")
    server.register_function(master.get_replica_status, "get_replica_status")

    logger.info(f"Master Server ready on port {MASTER_PORT}")
    logger.info("Available methods: get_chunk_metadata, ping, get_replica_status")
    logger.info("=" * 80)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Master Server shutting down...")
    except Exception as e:
        logger.error(f"Master Server crashed: {e}")