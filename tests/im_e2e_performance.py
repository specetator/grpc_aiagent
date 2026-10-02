#!/usr/bin/env python3
"""Isolated live IM acceptance and recipient-observed latency benchmark.

The manifest (kept outside Git) supplies two variant build/config paths and
ports. Credentials are generated in memory and never copied into results.
Only processes started by this harness are signalled. Each result records the
observed behavior; missing delivery/ACK is a failure, never a latency sample.
"""
import argparse
import base64
import concurrent.futures
import csv
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import struct
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request


def http_json(port, path, data):
    req = urllib.request.Request(f"http://127.0.0.1:{port}{path}",
        data=json.dumps(data).encode(), headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as response:
            result = json.load(response)
    except urllib.error.HTTPError as error:
        result = json.loads(error.read())
    if result.get("code", 0):
        raise RuntimeError(f"HTTP {path} application code {result.get('code')}")
    return result.get("data", result)


class RawWebSocket:
    def __init__(self, port, token, device="", read=True, callback=None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=8)
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        if not read: self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        self.sock.settimeout(.5)
        query = {"token": token}
        if device:
            query.update(device_id=device, receive_ack="1")
        key = base64.b64encode(os.urandom(16)).decode()
        request = (f"GET /ws?{urllib.parse.urlencode(query)} HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{port}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
            f"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: {key}\r\n\r\n")
        self.sock.sendall(request.encode())
        self.buffer = bytearray()
        deadline = time.monotonic() + 8
        while b"\r\n\r\n" not in self.buffer:
            if time.monotonic() > deadline:
                raise RuntimeError("WebSocket handshake timeout")
            try:
                chunk = self.sock.recv(8192)
            except socket.timeout:
                continue
            if not chunk:
                raise RuntimeError("WebSocket handshake closed")
            self.buffer.extend(chunk)
        headers, trailing = bytes(self.buffer).split(b"\r\n\r\n", 1)
        if not headers.startswith(b"HTTP/1.1 101"):
            raise RuntimeError("WebSocket upgrade refused")
        accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest())
        if accept not in headers:
            raise RuntimeError("WebSocket accept mismatch")
        self.buffer = bytearray(trailing)
        self.send_lock = threading.Lock()
        self.condition = threading.Condition()
        self.events = []
        self.callback = callback
        self.error = None
        self.closed = False
        self.thread = None
        if read:
            self.thread = threading.Thread(target=self._reader, daemon=True)
            self.thread.start()

    def send(self, value):
        self.send_bytes(json.dumps(value, separators=(",", ":")).encode())

    def send_bytes(self, payload, opcode=1):
        mask = os.urandom(4)
        length = len(payload)
        header = bytes([0x80 | opcode])
        if length < 126:
            header += bytes([0x80 | length])
        elif length < 65536:
            header += bytes([0x80 | 126]) + struct.pack("!H", length)
        else:
            header += bytes([0x80 | 127]) + struct.pack("!Q", length)
        masked = bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload))
        with self.send_lock:
            self.sock.sendall(header + mask + masked)

    def _fill(self, needed):
        while len(self.buffer) < needed:
            try:
                chunk = self.sock.recv(65536)
            except socket.timeout:
                return False
            if not chunk:
                raise EOFError("WebSocket closed")
            self.buffer.extend(chunk)
        return True

    def _frame(self):
        if not self._fill(2):
            return None
        opcode = self.buffer[0] & 15
        length = self.buffer[1] & 127
        header = 2
        if length == 126:
            if not self._fill(4): return None
            length = struct.unpack("!H", self.buffer[2:4])[0]
            header = 4
        elif length == 127:
            if not self._fill(10): return None
            length = struct.unpack("!Q", self.buffer[2:10])[0]
            header = 10
        if length > 8 * 1024 * 1024: raise RuntimeError("Oversize server frame")
        if self.buffer[1] & 128: raise RuntimeError("Masked server frame")
        if not self._fill(header + length): return None
        payload = bytes(self.buffer[header:header + length])
        del self.buffer[:header + length]
        return opcode, payload

    def _reader(self):
        try:
            while not self.closed:
                frame = self._frame()
                if frame is None: continue
                opcode, payload = frame
                if opcode == 8: raise EOFError("WebSocket closed")
                if opcode == 9:
                    self.send_bytes(payload, 10)
                    continue
                if opcode != 1: continue
                value = json.loads(payload)
                value["_observed_ns"] = time.perf_counter_ns()
                if self.callback: self.callback(value)
                with self.condition:
                    if len(self.events) >= 100000: raise RuntimeError("Test event bound reached")
                    self.events.append(value)
                    self.condition.notify_all()
        except (OSError, ValueError, EOFError, RuntimeError) as error:
            if not self.closed:
                with self.condition:
                    self.error = type(error).__name__
                    self.condition.notify_all()

    def wait(self, predicate, timeout=15, after=0):
        deadline = time.monotonic() + timeout
        with self.condition:
            index = after
            while True:
                for value in self.events[index:]:
                    if predicate(value): return value
                index = len(self.events)
                if self.error: raise RuntimeError(f"WebSocket reader {self.error}")
                remaining = deadline - time.monotonic()
                if remaining <= 0: raise TimeoutError("Expected WebSocket event absent")
                self.condition.wait(remaining)

    def close(self):
        self.closed = True
        try: self.sock.shutdown(socket.SHUT_RDWR)
        except OSError: pass
        self.sock.close()
        if self.thread: self.thread.join(timeout=2)


class Stack:
    def __init__(self, config, log_dir, manifest=None):
        self.config, self.log_dir = config, Path(log_dir)
        self.manifest = manifest or {}
        self.processes, self.logs = {}, {}
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def start_component(self, component):
        build = Path(self.config["build"])
        binary = build / component / f"{component}_server"
        log = open(self.log_dir / f"{component}.log", "ab")
        self.logs[component] = log
        config = Path(self.config[f"{component}_config"])
        content = config.read_text()
        # The supplied variant configurations own separate databases/topics.
        if component == "job":
            override = getattr(self, "job_comet_target_override", None)
            if override:
                content = "\n".join("comet_targets=" + override if line.startswith("comet_targets=") else line
                    for line in content.splitlines()) + "\n"
        generated = self.log_dir / f"{component}.conf"
        generated.write_text(content)
        self.processes[component] = subprocess.Popen(
            [str(binary), "--config", str(generated)],
            cwd=str(build.parent), stdout=log, stderr=subprocess.STDOUT)
        port = self.config.get({"logic": "http_port", "comet": "ws_port", "job": "job_port"}[component], 19202)
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            if self.processes[component].poll() is not None:
                raise RuntimeError(f"{component} exited {self.processes[component].returncode}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.2): return
            except OSError: time.sleep(.1)
        raise RuntimeError(f"{component} listen timeout")

    def stop_component(self, component):
        process = self.processes.pop(component, None)
        if process:
            process.terminate()
            try: process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                process.kill(); process.wait(timeout=3)
        log = self.logs.pop(component, None)
        if log: log.close()

    def start(self):
        for component in ("logic", "comet", "job"): self.start_component(component)
        time.sleep(1)

    def close(self):
        for component in ("job", "comet", "logic"): self.stop_component(component)

    def register(self, label):
        return http_json(self.config["http_port"], "/api/register", {
            "account": "lab_" + label + "_" + secrets.token_hex(5),
            "password": secrets.token_urlsafe(18), "name": "Local test"})

    def connect(self, identity, device="", **kwargs):
        return RawWebSocket(self.config["ws_port"], identity["token"], device, **kwargs)

    def redis(self, *arguments):
        config = self.manifest.get("redis", {})
        process = subprocess.run(["redis-cli", "-p", str(config.get("port", 6379)),
            "-n", str(config.get("database", 14)), "--raw", *map(str, arguments)],
            check=True, capture_output=True, text=True)
        return process.stdout.strip()

    def mysql(self, sql):
        config = self.manifest["mysql"]
        database = config["database"]
        for line in Path(self.config["logic_config"]).read_text().splitlines():
            if line.startswith("mysql_db="): database = line.split("=", 1)[1].strip()
        if not database.startswith("spark_im_"): raise RuntimeError("Refusing non-lab database")
        env = os.environ.copy(); env["MYSQL_PWD"] = config.get("password", "")
        result = subprocess.run(["mysql", "--host=" + config.get("host", "127.0.0.1"),
            "--port=" + str(config.get("port", 3306)), "--user=" + config["user"],
            "--batch", "--skip-column-names", database, "--execute", sql], env=env,
            capture_output=True, text=True)
        if result.returncode: raise RuntimeError("Lab SQL command failed")
        return result.stdout.strip()

    def metrics(self, component="comet"):
        port = self.config.get(component + "_metrics_port", 19203 if component == "comet" else 19202)
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=3) as response:
            lines = response.read().decode().splitlines()
        return {line.split()[0]: float(line.split()[1]) for line in lines if line and not line.startswith("#") and len(line.split()) == 2}


def percentile(samples, p):
    if not samples: return None
    ordered = sorted(samples)
    return round(ordered[min(len(ordered) - 1, int((len(ordered) - 1) * p))], 3)


def benchmark(stack, variant, connections, per_connection, output, serial_ack=False, timeout=120):
    setup = time.monotonic()
    receiver_id = stack.register("receiver")
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        identities = list(executor.map(lambda i: stack.register(f"sender{i}"), range(connections)))
    condition = threading.Condition()
    send_times, accepted, received, errors = {}, {}, {}, []
    run = "perf-" + secrets.token_hex(8)
    def collect(value, recipient=False):
        client_id = value.get("client_msg_id", "")
        if not client_id.startswith(run): return
        with condition:
            if value.get("type") == "accepted_ack": accepted.setdefault(client_id, value["_observed_ns"])
            elif recipient and value.get("msg_seq", 0) > 0 and value.get("type") not in ("delivered_ack", "error"):
                received.setdefault(client_id, value["_observed_ns"])
            elif value.get("type") == "error": errors.append({"code": value.get("code"), "message": value.get("message")})
            condition.notify_all()
    receiver = stack.connect(receiver_id, callback=lambda value: collect(value, True))
    senders = [stack.connect(identity, callback=collect) for identity in identities]
    warmup_id = "warmup-" + secrets.token_hex(8)
    send_message(senders[0], receiver_id["user_id"], "consumer readiness warmup", warmup_id)
    receiver.wait(lambda value: value.get("client_msg_id") == warmup_id and value.get("msg_seq", 0) > 0, timeout=25)
    setup_seconds = time.monotonic() - setup
    expected = connections * per_connection
    start_ns = time.perf_counter_ns()
    def send(index):
        for sequence in range(per_connection):
            client_id = f"{run}-{index}-{sequence}"
            with condition: send_times[client_id] = time.perf_counter_ns()
            senders[index].send({"type": "single_chat", "to_user_id": receiver_id["user_id"],
                "client_msg_id": client_id, "content": "local latency benchmark"})
            if serial_ack:
                senders[index].wait(lambda value: value.get("type") == "accepted_ack" and value.get("client_msg_id") == client_id, timeout=timeout)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=connections) as executor:
            list(executor.map(send, range(connections)))
        deadline = time.monotonic() + timeout
        with condition:
            while (len(received) < expected or len(accepted) < expected) and time.monotonic() < deadline:
                condition.wait(min(1, deadline - time.monotonic()))
        duration = ((max(received.values()) if received else time.perf_counter_ns()) - start_ns) / 1e9
        samples = [(received[cid] - sent) / 1e6 for cid, sent in send_times.items() if cid in received]
        ack_samples = [(accepted[cid] - sent) / 1e6 for cid, sent in send_times.items() if cid in accepted]
        result = {"variant": variant, "connections": connections, "messages_per_connection": per_connection,
            "expected": expected, "sent": len(send_times), "accepted": len(accepted), "recipient_received_unique": len(received),
            "errors": len(errors), "pass": len(received) == expected and len(accepted) == expected and not errors,
            "elapsed_seconds": round(duration, 6), "recipient_messages_per_second": round(len(received) / max(duration, .000001), 3),
            "recipient_p50_ms": percentile(samples, .5), "recipient_p95_ms": percentile(samples, .95), "recipient_p99_ms": percentile(samples, .99),
            "accepted_p50_ms": percentile(ack_samples, .5), "accepted_p95_ms": percentile(ack_samples, .95),
            "setup_seconds_excluded": round(setup_seconds, 3), "receiver_connections": 1, "mode": "legacy_wire_comparison",
            "warmup": "one recipient-confirmed message before timing",
            "sender_inflight_window": 1 if serial_ack else per_connection,
            "completion_timeout_seconds": timeout,
            "sender_accounts": connections, "latency_boundary": "before sender socket send to receiver frame parsed",
            "updated_extra_behavior": "persisted messages also fan out to the sender's other devices" if variant == "updated" else "baseline"}
        path = Path(output) / f"{variant}-{connections}-latency.csv"
        with path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["sample", "sender_index", "accepted_ms", "recipient_ms"])
            for cid, sent in send_times.items():
                writer.writerow([cid.rsplit("-", 1)[-1], cid.rsplit("-", 2)[-2],
                    round((accepted[cid] - sent) / 1e6, 6) if cid in accepted else "",
                    round((received[cid] - sent) / 1e6, 6) if cid in received else ""])
        return result
    finally:
        receiver.close()
        for sender in senders: sender.close()


def send_message(peer, target, content="test", client_id=None):
    client_id = client_id or "functional-" + secrets.token_hex(8)
    start = len(peer.events)
    peer.send({"type": "single_chat", "to_user_id": target, "client_msg_id": client_id, "content": content})
    ack = peer.wait(lambda value: value.get("type") == "accepted_ack" and value.get("client_msg_id") == client_id, after=start)
    return client_id, ack


def receipt(peer, message, prefix=0, extra=None):
    seqs = extra if extra is not None else [message["msg_seq"]]
    start = len(peer.events)
    peer.send({"type": "received_ack", "session_id": message["session_id"], "msg_seq": prefix, "received_seqs": seqs})
    return peer.wait(lambda value: value.get("type") in ("received_ack_ok", "received_ack_error") and
        value.get("session_id") == message["session_id"], after=start)


def grpc_clients(stack):
    import grpc
    from grpc_tools import protoc
    generated = Path(stack.log_dir).parent.parent / "generated-python"
    generated.mkdir(parents=True, exist_ok=True)
    source = Path(__file__).resolve().parents[1] / "proto"
    status = protoc.main(["grpc_tools.protoc", "-I" + str(source), "--python_out=" + str(generated),
        "--grpc_python_out=" + str(generated), str(source / "spark_push.proto")])
    if status: raise RuntimeError("Test RPC generation failed")
    sys.path.insert(0, str(generated))
    import spark_push_pb2 as pb
    import spark_push_pb2_grpc as rpc
    channel = grpc.insecure_channel(f"127.0.0.1:{stack.config['grpc_port']}")
    return grpc, pb, rpc, channel


def functional_tests(stack, on_result=None, ai_live=False):
    results = []
    connections = []
    def connect(identity, device=""):
        peer = stack.connect(identity, device); connections.append(peer); return peer
    def record(name, action):
        started = time.monotonic()
        try:
            detail = action()
            results.append({"test": name, "pass": True, "seconds": round(time.monotonic() - started, 3), "observed": detail})
        except Exception as error:
            results.append({"test": name, "pass": False, "seconds": round(time.monotonic() - started, 3),
                "error": type(error).__name__ + ": " + str(error)})
        print(json.dumps(results[-1]), flush=True)
        if on_result: on_result(results[-1])
    sender_identity, receiver_identity = stack.register("functional_sender"), stack.register("functional_receiver")
    sender = connect(sender_identity, "sender-main")
    device_a = connect(receiver_identity, "device-a")
    device_b = connect(receiver_identity, "device-b")
    state = {}
    def independent():
        cid, ack = send_message(sender, receiver_identity["user_id"])
        first = device_a.wait(lambda value: value.get("client_msg_id") == cid and value.get("msg_seq", 0) > 0)
        second = device_b.wait(lambda value: value.get("client_msg_id") == cid and value.get("msg_seq", 0) > 0)
        assert first["msg_seq"] == second["msg_seq"] == ack["msg_seq"]
        assert receipt(device_a, first)["type"] == "received_ack_ok"
        state.update(first=first, cid=cid)
        device_a.close(); device_b.close()
        recovered_a, recovered_b = connect(receiver_identity, "device-a"), connect(receiver_identity, "device-b")
        recovered_b.wait(lambda value: value.get("client_msg_id") == cid, timeout=8)
        time.sleep(3.5)
        assert not any(value.get("client_msg_id") == cid for value in recovered_a.events), "device A receipt was not retained"
        state.update(a=recovered_a, b=recovered_b)
        return "A persisted sparse receipt suppresses replay; unacknowledged B receives replay"
    record("independent_device_receipts", independent)
    def new_device():
        peer = connect(receiver_identity, "new-device")
        msg = peer.wait(lambda value: value.get("client_msg_id") == state["cid"], timeout=8)
        assert receipt(peer, msg)["type"] == "received_ack_ok"
        return "A new device receives history despite another device's receipt"
    record("new_device_history_recovery", new_device)
    def sparse_gap():
        source, target = stack.register("gap_source"), stack.register("gap_target")
        src, dst = connect(source), connect(target, "sparse-device")
        first_id, first_ack = send_message(src, target["user_id"], "lower message intentionally uncommitted")
        first = dst.wait(lambda value: value.get("client_msg_id") == first_id)
        session = first_ack["session_id"]
        assert all(c.isdigit() or c in "s_" for c in session)
        # Simulate an allocated sequence whose persistence event did not commit.
        # This affects only the freshly registered pair's isolated test session.
        stack.mysql("UPDATE session_sequence_reservation SET highest_seq=highest_seq+1 WHERE session_id='" + session + "'")
        later_id, later_ack = send_message(src, target["user_id"], "higher message committed first")
        later = dst.wait(lambda value: value.get("client_msg_id") == later_id)
        assert later_ack["msg_seq"] == first_ack["msg_seq"] + 2
        assert receipt(dst, later, prefix=0)["type"] == "received_ack_ok"
        dst.close()
        restored = connect(target, "sparse-device")
        restored.wait(lambda value: value.get("client_msg_id") == first_id, timeout=8)
        time.sleep(3.5)
        assert not any(value.get("client_msg_id") == later_id for value in restored.events)
        assert receipt(restored, first, prefix=0)["type"] == "received_ack_ok"
        return "Allocation gap retained; sparse ACK of higher seq does not suppress uncommitted lower message"
    record("sparse_receipt_allocation_gap_no_loss", sparse_gap)
    def paginated_recovery():
        source, target = stack.register("page_source"), stack.register("page_target")
        src = connect(source)
        sent = [send_message(src, target["user_id"], "offline recovery page")[0] for _ in range(205)]
        session = src.wait(lambda value: value.get("type") == "accepted_ack" and value.get("client_msg_id") == sent[0])["session_id"]
        deadline = time.monotonic() + 30
        while int(stack.mysql("SELECT COUNT(*) FROM message WHERE session_id='" + session + "'")) < 205:
            if time.monotonic() > deadline: raise TimeoutError("Offline messages did not finish persistence")
            time.sleep(.1)
        dst = connect(target, "paginated-device")
        dst.wait(lambda value: value.get("client_msg_id") == sent[199], timeout=10)
        page = [value for value in dst.events if value.get("client_msg_id") in set(sent) and value.get("msg_seq", 0) > 0]
        assert len({value["msg_seq"] for value in page}) >= 200
        assert receipt(dst, page[0], extra=sorted({value["msg_seq"] for value in page}))["type"] == "received_ack_ok"
        dst.wait(lambda value: value.get("client_msg_id") == sent[-1], timeout=6)
        return "205 offline messages recover across the 200-message page boundary after client durable receipt"
    record("device_recovery_multiple_pages", paginated_recovery)
    def idempotency():
        cid, first = send_message(sender, receiver_identity["user_id"], "original")
        _, duplicate = send_message(sender, receiver_identity["user_id"], "changed", cid)
        assert (first["msg_id"], first["msg_seq"], first["session_id"]) == (duplicate["msg_id"], duplicate["msg_seq"], duplicate["session_id"])
        peer = connect(receiver_identity, "idempotency-reader")
        stored = peer.wait(lambda value: value.get("client_msg_id") == cid, timeout=10)
        assert stored.get("content") == "original", "Duplicate changed original durable payload"
        assert stored["msg_seq"] == first["msg_seq"]
        return "Stable client ID returns the original message identity and durable original payload"
    record("idempotent_different_payload", idempotency)
    def group_membership():
        owner, member, outsider = stack.register("group_owner"), stack.register("group_member"), stack.register("group_outsider")
        own, mem, out = connect(owner), connect(member, "group-member-device"), connect(outsider)
        group = int(stack.mysql("INSERT INTO im_group(name,owner_id,group_type) VALUES('local reliability test'," + str(owner["user_id"]) + ",1); SELECT LAST_INSERT_ID();"))
        stack.mysql("INSERT INTO group_member(group_id,user_id,role) VALUES(" + str(group) + "," + str(owner["user_id"]) + ",2),(" + str(group) + "," + str(member["user_id"]) + ",0)")
        cid = "group-" + secrets.token_hex(8)
        own.send({"type": "chatroom", "group_id": group, "client_msg_id": cid, "content": "durable group fanout"})
        own.wait(lambda value: value.get("type") == "accepted_ack" and value.get("client_msg_id") == cid)
        durable = mem.wait(lambda value: value.get("client_msg_id") == cid and value.get("msg_seq", 0) > 0)
        assert durable["session_id"] == "r_" + str(group)
        assert receipt(mem, durable)["type"] == "received_ack_ok"
        grpc, pb, rpc, channel = grpc_clients(stack)
        denied = rpc.LogicServiceStub(channel).SendUpstreamMessage(pb.UpstreamMessageRequest(
            from_user_id=outsider["user_id"], scene="chatroom", group_id=group,
            client_msg_id="nonmember-" + secrets.token_hex(8), content_json='{"type":"chatroom","content":"denied"}'), timeout=4)
        assert denied.error.code == 403
        channel.close()
        assert not any(value.get("client_msg_id") == cid for value in out.events)
        return "Persisted group message fans out to members; authenticated nonmember RPC returns 403 and outsider receives no group message"
    record("group_fanout_membership_authorization", group_membership)
    def job_restart():
        stack.stop_component("job")
        cid, _ = send_message(sender, receiver_identity["user_id"], "accepted while Job stopped")
        peer = connect(receiver_identity, "job-recovery")
        start = len(peer.events)
        try:
            peer.wait(lambda value: value.get("client_msg_id") == cid, timeout=1, after=start)
            raise AssertionError("Message delivered despite persistence Job being stopped")
        except TimeoutError:
            pass
        stack.start_component("job")
        stored = peer.wait(lambda value: value.get("client_msg_id") == cid, timeout=25)
        assert receipt(peer, stored)["type"] == "received_ack_ok"
        return "Kafka accepted event survives Job outage and reaches recipient after restart"
    record("job_outage_accepted_event_recovery", job_restart)
    def redis_identity_loss():
        source, target = stack.register("redis_loss_source"), stack.register("redis_loss_target")
        src, dst = connect(source), connect(target, "redis-loss-device")
        stack.stop_component("job")
        cid, original = send_message(src, target["user_id"], "immutable original before Redis loss")
        session = original["session_id"]
        keys = ["session:msg_seq:" + session, "session:last_seq:" + session]
        for pattern in ("message:original:" + session + ":*", "message:dedup:" + session + ":*"):
            keys.extend(stack.redis("KEYS", pattern).splitlines())
        stack.redis("DEL", *keys)
        _, retried = send_message(src, target["user_id"], "changed retry after Redis loss", cid)
        assert (original["msg_id"], original["msg_seq"]) == (retried["msg_id"], retried["msg_seq"])
        stack.start_component("job")
        stored = dst.wait(lambda value: value.get("client_msg_id") == cid, timeout=25)
        assert stored.get("content") == "immutable original before Redis loss"
        assert receipt(dst, stored)["type"] == "received_ack_ok"
        assert stack.mysql("SELECT COUNT(*) FROM message WHERE session_id='" + session + "' AND client_msg_id='" + cid + "'") == "1"
        return "Accepted identity survives deleting only this test session's Redis allocator/cache keys while Job is stopped"
    record("accepted_identity_survives_redis_cache_loss", redis_identity_loss)
    def explicit_fencing():
        grpc, pb, rpc, channel = grpc_clients(stack)
        identity = stack.register("rpc_fencing")
        stub = rpc.LogicServiceStub(channel)
        comet = "rpc-test-fencing"
        boot = secrets.token_hex(8)
        def verify(epoch):
            result = stub.VerifyToken(pb.VerifyTokenRequest(token=identity["token"], comet_id=comet,
                route_generation=f"{boot}:{epoch}"), timeout=3)
            assert result.error.code == 0
        verify(2)
        stub.UserOffline(pb.UserOfflineRequest(user_id=identity["user_id"], comet_id=comet, route_generation=boot + ":1"), timeout=3)
        verify(1)
        key = "route:epochs:" + str(identity["user_id"])
        assert stack.redis("HGET", key, comet) == boot + ":2"
        stack.redis("ZREM", "route:leased:" + str(identity["user_id"]), comet)
        stub.RefreshRoutes(pb.RefreshRoutesRequest(comet_id=comet,
            leases=[pb.RouteLease(user_id=identity["user_id"], generation=boot + ":2")]), timeout=3)
        assert float(stack.redis("ZSCORE", "route:leased:" + str(identity["user_id"]), comet)) > time.time() * 1000
        stub.UserOffline(pb.UserOfflineRequest(user_id=identity["user_id"], comet_id=comet, route_generation=boot + ":2"), timeout=3)
        channel.close()
        return "Stale VerifyToken and UserOffline cannot replace/remove epoch 2; matching heartbeat restores expired lease"
    record("rpc_generation_fencing_and_lease_recovery", explicit_fencing)
    def negative_comet_retry():
        grpc, pb, rpc, channel = grpc_clients(stack)
        channel.close()
        actual = grpc.insecure_channel(f"127.0.0.1:{stack.config.get('comet_grpc_port', 19105)}")
        actual_stub = rpc.CometServiceStub(actual)
        deny = threading.Event(); deny.set()
        class FaultComet(rpc.CometServiceServicer):
            def PushToComet(self, request, context):
                if deny.is_set():
                    return pb.PushToCometReply(request_id=request.request_id,
                        error=pb.ErrorInfo(code=503, message="injected local retryable rejection"))
                return actual_stub.PushToComet(request, timeout=4)
            def PushStream(self, requests, context):
                for request in requests: yield self.PushToComet(request, context)
            def PushDeliveryAck(self, request, context):
                return actual_stub.PushDeliveryAck(request, timeout=4)
        proxy = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=8))
        rpc.add_CometServiceServicer_to_server(FaultComet(), proxy)
        port = proxy.add_insecure_port("127.0.0.1:0"); proxy.start()
        source, target = stack.register("reject_source"), stack.register("reject_target")
        src, dst = connect(source), connect(target)
        stack.stop_component("job")
        stack.job_comet_target_override = f"comet-lab=127.0.0.1:{port}"
        try:
            stack.start_component("job")
            cid, _ = send_message(src, target["user_id"], "retry after explicit Comet 503")
            deadline = time.monotonic() + 15
            metric = "spark_push_delivery_outbox_retries_total"
            while stack.metrics("job").get(metric, 0) < 1 and time.monotonic() < deadline: time.sleep(.1)
            assert stack.metrics("job").get(metric, 0) >= 1, "No durable outbox retry observed after Comet 503"
            assert not any(value.get("client_msg_id") == cid for value in dst.events)
            deny.clear()
            dst.wait(lambda value: value.get("client_msg_id") == cid, timeout=15)
            return "Explicit 503 leaves a durable retry task, then delivers to an online legacy recipient without reconnect/sync"
        finally:
            stack.stop_component("job")
            stack.job_comet_target_override = None
            proxy.stop(0).wait(); actual.close()
            stack.start_component("job")
    record("comet_503_durable_automatic_retry", negative_comet_retry)
    def generation_race():
        source, target = stack.register("race_source"), stack.register("race_target")
        src = connect(source)
        for _ in range(20):
            old = connect(target)
            old.close()
        newest = connect(target)
        cid, _ = send_message(src, target["user_id"], "latest connection route")
        newest.wait(lambda value: value.get("client_msg_id") == cid, timeout=2)
        return "20 rapid last-close/reconnect cycles retain the latest route; legacy peer receives without device polling"
    record("last_close_generation_race", generation_race)
    def slow_reader():
        source, target = stack.register("slow_source"), stack.register("slow_target")
        src = connect(source)
        slow = stack.connect(target, read=False); connections.append(slow)
        before = stack.metrics()
        payload = "x" * (64 * 1024)
        for _ in range(200): send_message(src, target["user_id"], payload)
        deadline = time.monotonic() + 35
        key = "spark_push_comet_backpressure_total"
        highwater = "spark_push_comet_slow_disconnect_total"
        while time.monotonic() < deadline:
            after = stack.metrics()
            if after.get(key, 0) > before.get(key, 0) or after.get(highwater, 0) > before.get(highwater, 0):
                return {"payload_bytes_each": len(payload), "accepted_messages": 200,
                    "backpressure_rejections": after.get(key, 0) - before.get(key, 0),
                    "highwater_disconnects": after.get(highwater, 0) - before.get(highwater, 0),
                    "behavior": "Slow reader is disconnected at bounded queue/output budget"}
            time.sleep(.2)
        raise AssertionError("No bounded-queue rejection/disconnect observed for the stalled receiver")
    record("slow_reader_backpressure", slow_reader)
    def comet_restart():
        cid, _ = send_message(sender, receiver_identity["user_id"], "unacknowledged before Comet restart")
        state["b"].wait(lambda value: value.get("client_msg_id") == cid, timeout=15)
        stack.stop_component("comet"); stack.start_component("comet")
        peer = connect(receiver_identity, "device-b")
        peer.wait(lambda value: value.get("client_msg_id") == cid, timeout=15)
        return "Unacknowledged device message survives Comet process restart"
    record("comet_restart_device_recovery", comet_restart)
    if ai_live:
      def ai_delta_final():
        from confluent_kafka import Producer
        cfg = dict(line.split("=", 1) for line in Path(stack.config["logic_config"]).read_text().splitlines()
            if "=" in line and not line.startswith("#"))
        assert cfg.get("hermes_enabled") == "true"
        identity = stack.register("ai_receiver")
        peer = connect(identity, "ai-device")
        bot = int(cfg.get("hermes_bot_user_id", 900000000001))
        session = "s_" + str(min(bot, identity["user_id"])) + "_" + str(max(bot, identity["user_id"]))
        request_id = "synthetic-ai-" + secrets.token_hex(8)
        producer = Producer({"bootstrap.servers": cfg["kafka_brokers"], "acks": "all"})
        def publish(topic, value):
            errors = []
            producer.produce(topic, key=request_id, value=json.dumps(value),
                callback=lambda error, message: errors.append(str(error)) if error else None)
            assert producer.flush(8) == 0 and not errors, "Synthetic Kafka event publish failed"
        count_before = int(stack.mysql("SELECT COUNT(*) FROM message WHERE session_id='" + session + "'"))
        publish(cfg["kafka_ai_delta_topic"], {"request_id": request_id, "user_id": identity["user_id"],
            "bot_user_id": bot, "session_id": session, "delta": "ephemeral synthetic delta", "delta_index": 0})
        delta = peer.wait(lambda value: value.get("type") == "hermes_delta" and value.get("request_id") == request_id, timeout=20)
        assert delta.get("msg_seq", 0) == 0
        time.sleep(.2)
        assert int(stack.mysql("SELECT COUNT(*) FROM message WHERE session_id='" + session + "'")) == count_before
        assert stack.mysql("SELECT COUNT(*) FROM device_receipt WHERE user_id=" + str(identity["user_id"]) +
            " AND device_id='ai-device' AND session_id='" + session + "'") == "0"
        envelope = {"request_id": request_id, "user_id": identity["user_id"], "bot_user_id": bot,
            "session_id": session, "ok": True, "text": "durable synthetic final reply", "completed_at_ms": int(time.time() * 1000)}
        publish(cfg["kafka_ai_reply_topic"], envelope)
        final = peer.wait(lambda value: value.get("client_msg_id") == "hermes:" + request_id and value.get("msg_seq", 0) > 0, timeout=20)
        assert final["content"]["text"] == "durable synthetic final reply"
        assert receipt(peer, final)["type"] == "received_ack_ok"
        publish(cfg["kafka_ai_reply_topic"], envelope)
        time.sleep(1)
        assert stack.mysql("SELECT COUNT(*) FROM message WHERE session_id='" + session + "' AND client_msg_id='hermes:" + request_id + "'") == "1"
        restored = connect(identity, "ai-other-device")
        recovered = restored.wait(lambda value: value.get("client_msg_id") == "hermes:" + request_id, timeout=10)
        assert recovered["msg_seq"] == final["msg_seq"]
        return "Synthetic Kafka ai_delta reaches UI with seq 0 and no history/receipt; ai_reply persists once, acknowledges and recovers on another device"
      record("ai_delta_ephemeral_final_durable_idempotent", ai_delta_final)
    for peer in connections: peer.close()
    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--connections", default="8,32,128")
    parser.add_argument("--messages-per-connection", type=int, default=20)
    parser.add_argument("--variants", default="baseline,updated")
    parser.add_argument("--skip-functional", action="store_true")
    parser.add_argument("--functional-only", action="store_true")
    parser.add_argument("--ai-live", action="store_true", help="Synthetic Kafka AI delta/final; requires lab hermes_enabled=true")
    parser.add_argument("--serial-accepted-ack", action="store_true", help="One outstanding accepted ACK per sending connection")
    parser.add_argument("--completion-timeout", type=int, default=120)
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    output = Path(args.output); output.mkdir(parents=True, exist_ok=True)
    report = {"schema": 1, "benchmark": "Spark Push local recipient-observed E2E", "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "benchmarks": [], "functional": [], "binary_sha256": {},
        "environment": {"platform": os.uname().sysname, "release": os.uname().release,
            "cpu_count": os.cpu_count(), "python": os.sys.version.split()[0]},
        "notes": ["Local loopback measurements; no production-capacity claim.",
            "Account registration and connection setup excluded from latency and throughput.",
            "Both variants tested sequentially using identical client workload.",
            "Each sending connection uses a separate account; one common receiving connection.",
            "Tokens and passwords are omitted from reports and latency samples."]}
    def save_report():
        (output / "report.json").write_text(json.dumps(report, indent=2) + "\n")
        columns = ["variant", "connections", "messages_per_connection", "sender_inflight_window", "expected", "accepted",
            "recipient_received_unique", "errors", "pass", "elapsed_seconds", "recipient_messages_per_second",
            "recipient_p50_ms", "recipient_p95_ms", "recipient_p99_ms", "accepted_p50_ms", "accepted_p95_ms"]
        with (output / "summary.csv").open("w", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=columns, extrasaction="ignore")
            writer.writeheader(); writer.writerows(report["benchmarks"])
    failed = False
    for variant in args.variants.split(","):
        stack = Stack(manifest["variants"][variant], Path(args.manifest).parent / "logs" / variant, manifest)
        report["binary_sha256"][variant] = {}
        for component in ("logic", "comet", "job"):
            binary = Path(stack.config["build"]) / component / (component + "_server")
            with binary.open("rb") as stream:
                report["binary_sha256"][variant][component] = hashlib.file_digest(stream, "sha256").hexdigest()
        try:
            stack.start()
            for count in ([] if args.functional_only else map(int, args.connections.split(","))):
                result = benchmark(stack, variant, count, args.messages_per_connection, output,
                    args.serial_accepted_ack, args.completion_timeout)
                report["benchmarks"].append(result); failed |= not result["pass"]
                print(json.dumps(result), flush=True)
                save_report()
                time.sleep(2)
            if variant == "updated" and not args.skip_functional:
                def functional_progress(row):
                    report["functional"].append(row)
                    save_report()
                functional_tests(stack, functional_progress, args.ai_live)
                failed |= any(not test["pass"] for test in report["functional"])
        finally:
            stack.close()
            save_report()
    raise SystemExit(1 if failed else 0)


if __name__ == "__main__":
    main()
