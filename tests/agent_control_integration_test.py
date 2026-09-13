"""Real local HTTP + C++ bridge client + fake Pi, no model or live IM services."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cannbot/scripts"))
from pi_gateway import GatewayState, PiRpcClient, make_handler

with tempfile.TemporaryDirectory(prefix="agent-control-test-") as temporary:
    root = Path(temporary)
    client = PiRpcClient([sys.executable, str(ROOT / "tests/fixtures/fake_pi_rpc.py")],
                         root, os.environ.copy(), root / "sessions")
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(GatewayState(client, "local-test-only", "pi-agent")))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        result = subprocess.run([sys.argv[1], f"http://127.0.0.1:{server.server_port}/v1"], timeout=30)
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
        client.stop()
    sys.exit(result.returncode)
