import os
import socket
import struct
import subprocess
import sys
import time

def frame(opcode, data=b""):
    mask = b"abcd"
    assert len(data) < 126
    return bytes([0x80 | opcode, 0x80 | len(data)]) + mask + bytes(c ^ mask[i % 4] for i, c in enumerate(data))

def read_frame(connection):
    def exact(n):
        output = b""
        while len(output) < n:
            chunk = connection.recv(n - len(output))
            assert chunk, "socket closed before frame"
            output += chunk
        return output
    first, second = exact(2)
    size = second & 127
    if size == 126:
        size = struct.unpack("!H", exact(2))[0]
    elif size == 127:
        size = struct.unpack("!Q", exact(8))[0]
    return first & 15, exact(size)

with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
process = subprocess.Popen([sys.argv[1], str(port), *sys.argv[2:]], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
connections = []
try:
    deadline = time.monotonic() + 10
    while True:
        try:
            connection = socket.create_connection(("127.0.0.1", port), timeout=0.1)
            connection.close()
            break
        except OSError:
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(0.03)
    def begin(token="normal", coalesced=b""):
        connection = socket.create_connection(("127.0.0.1", port), timeout=2)
        connection.settimeout(2)
        connections.append(connection)
        request = (f"GET /?token={token} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
                   "Connection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
                   "Sec-WebSocket-Version: 13\r\n\r\n").encode()
        connection.sendall(request + coalesced)
        return connection
    def upgraded(connection):
        # Read exactly through CRLFCRLF so coalesced frame bytes remain available.
        output = b""
        while not output.endswith(b"\r\n\r\n"):
            output += connection.recv(1)
            assert len(output) < 8192
        assert output.startswith(b"HTTP/1.1 101")
    live = begin(coalesced=frame(9, b"first"))
    upgraded(live)
    assert read_frame(live) == (10, b"first")
    for payload in (b"", b"heartbeat", os.urandom(125)):
        live.sendall(frame(9, payload))
        assert read_frame(live) == (10, payload)
    import json
    live.sendall(frame(1, b'{"type":"sync","session_id":"s_7_8","after_seq":0}'))
    opcode, payload = read_frame(live)
    recovered = json.loads(payload)
    assert opcode == 1 and recovered["msg_seq"] == 1 and recovered["msg_id"] == "s_7_8-1"
    assert recovered["content"]["text"] == "legacy text"
    opcode, payload = read_frame(live)
    ended = json.loads(payload)
    assert opcode == 1 and ended["type"] == "sync_end" and ended["safe_seq"] == 1
    if len(sys.argv) > 2:
        import json
        started = time.monotonic()
        live.sendall(frame(1, b'{"type":"single_chat","to_user_id":8,"client_msg_id":"timeout","content":{"text":"test"}}'))
        opcode, payload = read_frame(live)
        assert opcode == 1 and json.loads(payload)["client_msg_id"] == "timeout"
        assert json.loads(payload)["type"] == "error"
        assert time.monotonic() - started < 2, "ACK watchdog must run independently of stream Write"
    slow = begin("slow")
    time.sleep(0.1)
    started = time.monotonic()
    live.sendall(frame(9, b"independent"))
    assert read_frame(live) == (10, b"independent")
    assert time.monotonic() - started < 0.5, "slow authentication blocked the single IO loop"
    upgraded(slow)
    slow.sendall(frame(8))
    assert read_frame(slow) == (8, b"")
    # Repeated abrupt disconnects exercise queued callbacks during resource cleanup.
    for _ in range(30):
        connection = begin()
        connection.close()
    idle = socket.create_connection(("127.0.0.1", port), timeout=1)
    connections.append(idle)
    idle.settimeout(7)
    idle.sendall(b"GET /?token=normal HTTP/1.1\r\nHost: partial")
    assert idle.recv(1) == b"", "incomplete handshake must expire"
    started = time.monotonic()
    process.terminate()
    assert process.wait(timeout=6) == 0, "Comet failed controlled SIGTERM shutdown"
    assert time.monotonic() - started < 6
finally:
    for connection in connections:
        connection.close()
    if process.poll() is None:
        process.kill()
        process.wait()
