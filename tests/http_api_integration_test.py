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
    assert post("/api/users/lookup", {"account":"peer'quoted"}, False) == 401
    assert post("/api/users/lookup", {"account":"peer'quoted", "actor_user_id":8}) == 400
    assert post("/api/users/lookup", {"account":"peer"}) == 404
    for value in (123, None, [], {}):
        assert post("/api/users/lookup", {"account":value}) == 400
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    conn.request("POST", "/api/users/lookup", json.dumps({"account":"peer'quoted"}),
                 {"Authorization":"Bearer probe", "Content-Type":"application/json"})
    contact = json.loads(conn.getresponse().read())
    conn.close()
    assert contact["code"] == 0 and contact["data"] == {
        "user_id":8, "account":"peer'quoted", "name":'Peer "quoted"'}
    assert post("/api/spaces/discover", {}, False) == 401
    assert post("/api/spaces/discover", {"limit":[]}) == 400
    for path, fields in {
        "/api/knowledge/document": ["doc_id", "chunk_id"],
        "/api/attachment/upload": ["session_id", "name", "data"],
        "/api/attachment/get": ["id"],
    }.items():
        for field in fields:
            for value in (123, None, [], {}):
                assert post(path, {field: value}) == 400, (path, field, value)
    assert post("/api/agent/connections", {"action":"list"}, False) == 401
    assert post("/api/agent/connections", {"action":"list", "actor_user_id":8}) == 400
    assert post("/api/agent/connections", {"action":"save", "binary":"/bin/sh"}) == 400
    assert post("/api/agent/connections", {"action":"save", "endpoint":None}) == 400
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    conn.request("POST", "/api/agent/connections", json.dumps({"action":"list"}),
                 {"Authorization":"Bearer probe", "Content-Type":"application/json"})
    result = json.loads(conn.getresponse().read())
    assert result["code"] == 0 and result["data"]["actor_user_id"] == 7
    conn.close()
    for spelling in ("authorization", "aUtHoRiZaTiOn"):
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
        conn.request("POST", "/api/agent/connections", json.dumps({"action":"list"}),
                     {spelling:"Bearer probe", "Content-Type":"application/json"})
        assert json.loads(conn.getresponse().read())["code"] == 0, spelling
        conn.close()
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
    conn.request("POST", "/api/agent/connections", json.dumps({"action":"list"}),
                 {"Authorization":"Bearer probe","authorization":"Bearer conflict","Content-Type":"application/json"})
    assert json.loads(conn.getresponse().read())["code"] == 401
    conn.close()
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
