from xmlrpc.client import ServerProxy, Transport
import socket


class TimeoutTransport(Transport):
    def __init__(self, timeout):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        conn = super().make_connection(host)
        conn.timeout = self.timeout
        return conn


def test_connection():
    print("Testing replica server connection...")

    # Test if port is actually open
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('127.0.0.1', 7001))
    sock.close()

    if result == 0:
        print("✓ Port 7001 is open and accessible")

        # Test XML-RPC connection with proper timeout transport
        try:
            transport = TimeoutTransport(5)
            proxy = ServerProxy("http://127.0.0.1:7001", allow_none=True, transport=transport)
            response = proxy.ping()
            print(f"✓ XML-RPC connection successful: {response}")
            return True
        except Exception as e:
            print(f"✗ XML-RPC failed: {e}")
            return False
    else:
        print(f"✗ Port 7001 is closed (error code: {result})")
        return False


if __name__ == "__main__":
    test_connection()