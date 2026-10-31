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
from agent_events import agent_event

with tempfile.TemporaryDirectory(prefix="agent-control-test-") as temporary:
    root = Path(temporary)
    client = PiRpcClient([sys.executable, str(ROOT / "tests/fixtures/fake_pi_rpc.py")],
                         root, os.environ.copy(), root / "sessions")
    def receipt(request):
        # Transport contract only. Real Router/Native ledger checks have their
        # own tests and the public Compose model E2E.
        assert request['collaboration']=={'kind':'room_agent_v1','room_id':1,'agent_id':'public-hermes'}
        assert request['request_id']=='receipt-fixture' and request['run_id']=='run-fixture'
        return agent_event('assistant_final',{'text':'receipt fixture accepted'})
    client.confirm_room_reply=receipt
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
