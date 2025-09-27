# Simple test script
from xmlrpc.client import ServerProxy
controller1 = ServerProxy("http://127.0.0.1:8000")
controller2 = ServerProxy("http://127.0.0.1:8001")
print(controller1.ping())
print(controller2.ping())