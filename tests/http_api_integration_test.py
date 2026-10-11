import http.client
import json
import socket
import subprocess
import sys
import time

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
process = subprocess.Popen([sys.argv[1], str(port)], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL)
try:
    deadline = time.monotonic() + 10
    while True:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                break
        except OSError:
            assert process.poll() is None, "HTTP fixture exited"
            assert time.monotonic() < deadline, "HTTP fixture not ready"
            time.sleep(0.03)
    def post(path, body, authenticated=True):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        headers = {"Content-Type": "application/json"}
        if authenticated:
            headers["Authorization"] = "Bearer probe"
        conn.request("POST", path, json.dumps(body), headers)
        response = conn.getresponse()
        result = json.loads(response.read())
        conn.close()
        return result["code"]
    readiness = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    readiness.request("GET", "/ready")
    assert readiness.getresponse().status == 503
    readiness.close()
    message = {"from_user_id": 7, "to_user_id": 8, "content": "hello"}
    assert post("/api/message/send", message, False) == 401
    assert post("/api/message/send", dict(message, from_user_id=8)) == 403
    for path, fields in {
        "/api/knowledge/document": ["doc_id", "chunk_id"],
        "/api/attachment/upload": ["session_id", "name", "data"],
        "/api/attachment/get": ["id"],
    }.items():
        for field in fields:
            for value in (123, None, [], {}):
                assert post(path, {field: value}) == 400, (path, field, value)
    # A partial header must not monopolize the loop or block unrelated requests.
    with socket.create_connection(("127.0.0.1", port), timeout=2) as partial:
        partial.sendall(b"GET /metrics HTTP/1.1\r\nHost: partial")
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("GET", "/metrics")
        assert conn.getresponse().status == 200
        conn.close()
    assert process.poll() is None
finally:
    process.terminate()
    try:
        process.wait(timeout=3)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
