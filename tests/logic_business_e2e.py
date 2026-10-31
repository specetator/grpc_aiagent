#!/usr/bin/env python3
"""Opt-in live Logic regression. Use ONLY a dedicated initialized test database.

Requires grpcio, grpcio-tools and websockets. Uses the existing private-config
benchmark helpers; never emits passwords or tokens. Not a throughput benchmark.
"""
import argparse
import asyncio
import base64
import importlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request

import grpc
from grpc_tools import protoc
import websockets

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "inference/benchmarks"))
from run_app_matrix import free_port, write_config


class Checks:
    def __init__(self):
        self.names = []

    def require(self, condition, name):
        if not condition:
            raise AssertionError(name)
        self.names.append(name)


def http(base, path, body, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    request = urllib.request.Request(base + path, json.dumps(body).encode(), headers)
    try:
        response = urllib.request.urlopen(request, timeout=10)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        return response.status, json.load(response)


async def receive(ws, predicate):
    async with asyncio.timeout(15):
        while True:
            event = json.loads(await ws.recv())
            if predicate(event):
                return event


async def exercise(base, ws_base, stub, pb, check):
    def call(path, body, user=None, code=0, status=None):
        http_status, result = http(base, path, body, user["token"] if user else None)
        check.require(result["code"] == code, path + " business code " + str(code))
        if status is not None:
            check.require(http_status == status, path + " HTTP status " + str(status))
        return result.get("data", {})

    users = []
    for label in ["Alice", "Bob", "Outsider"]:
        credentials = {"account": "layers_" + secrets.token_hex(10),
                       "password": secrets.token_hex(16), "name": label}
        user = call("/api/register", credentials, status=200)
        logged = call("/api/login", credentials, status=200)
        check.require(logged["user_id"] == user["user_id"], "register/login identity")
        user["credentials"] = credentials
        users.append(user)
    alice, bob, outsider = users
    aid, bid, oid = [u["user_id"] for u in users]
    session = f"s_{min(aid, bid)}_{max(aid, bid)}"
    call("/api/session/history", {"session_id": session}, code=401, status=401)
    admin_credentials = {"account": os.environ["SPARK_PUSH_ADMIN_ACCOUNT"],
                         "password": os.environ["SPARK_PUSH_ADMIN_PASSWORD"]}
    admin = call("/api/login", admin_credentials)
    call("/api/admin/user/list", {}, alice, code=403, status=400)

    async def wait_history(session_id, user, predicate):
        # accepted/delivered can precede Job's MySQL write, including creation
        # of the session row. A transient 404 here is part of the contract.
        for _ in range(100):
            status, result = http(base, "/api/session/history",
                                  {"session_id": session_id, "limit": 100}, user["token"])
            if result["code"] == 0 and predicate(result["data"]["messages"]):
                check.require(status == 200, "persisted history HTTP response")
                return result["data"]["messages"]
            if result["code"] not in (0, 404):
                raise AssertionError("unexpected history authorization/storage error")
            await asyncio.sleep(.1)
        raise AssertionError("Job did not persist history within test deadline")

    async def connect(user):
        return await websockets.connect(ws_base + "/ws?token=" + user["token"],
                                        open_timeout=10, max_size=2**20)

    async with await connect(alice) as a, await connect(bob) as b:
        client = "layers-" + secrets.token_hex(12)
        payload = {"type": "single_chat", "to_user_id": bid,
                   "client_msg_id": client, "content": {"text": "layered hello"}}
        await a.send(json.dumps(payload))
        ack = await receive(a, lambda e: e.get("type") == "accepted_ack" and e.get("client_msg_id") == client)
        received = await receive(b, lambda e: e.get("type") == "single_chat" and e.get("client_msg_id") == client)
        delivered = await receive(a, lambda e: e.get("type") == "delivered_ack" and e.get("client_msg_id") == client)
        check.require(ack["msg_seq"] == received["msg_seq"] == delivered["msg_seq"], "accepted/delivered share message sequence")
        first_seq = ack["msg_seq"]
        await a.send(json.dumps(payload))
        replay = await receive(a, lambda e: e.get("type") == "accepted_ack" and e.get("client_msg_id") == client)
        check.require(replay["msg_seq"] == first_seq and replay["msg_id"] == ack["msg_id"], "duplicate client ID keeps sequence and message ID")

        rows = await wait_history(session, alice, lambda rows: bool(rows))
        check.require(len(rows) == 1 and rows[0]["msg_seq"] == first_seq, "one persisted row after duplicate acceptance")
        call("/api/session/history", {"session_id": session}, outsider, code=403, status=403)
        sync = stub.SyncMessages(pb.SyncMessagesRequest(user_id=oid, session_id=session), timeout=10)
        check.require(sync.error.code == 403 and not sync.messages, "gRPC sync shares HTTP history permission")
        sync = stub.SyncMessages(pb.SyncMessagesRequest(user_id=bid, session_id=session, limit=10), timeout=10)
        check.require(sync.error.code == 0 and len(sync.messages) == 1 and sync.next_seq == first_seq, "gRPC cursor page")
        rejected = stub.MarkDelivered(pb.MarkDeliveredRequest(user_id=oid, cursors=[pb.DeliveredCursor(session_id=session, msg_seq=first_seq)]), timeout=10)
        check.require(rejected.error.code == 403, "gRPC delivered cursor rejects outsider")
        cursor = stub.MarkDelivered(pb.MarkDeliveredRequest(user_id=bid, cursors=[pb.DeliveredCursor(session_id=session, msg_seq=first_seq)]), timeout=10)
        check.require(cursor.error.code == 0, "gRPC delivered cursor persisted")
        call("/api/session/mark_read", {"user_id": bid, "session_id": session, "read_seq": first_seq}, alice, code=403, status=403)
        call("/api/session/mark_read", {"user_id": bid, "session_id": session, "read_seq": first_seq}, bob)
        unread = call("/api/session/unread", {"user_id": bid, "session_id": session}, bob)
        check.require(unread["unread"] == 0, "read state independent of delivered cursor")
        call("/api/session/list_single", {"user_id": aid}, alice)

        png = base64.b64encode(bytes.fromhex("89504e470d0a1a0a") + b"layer-test").decode()
        image = call("/api/attachment/upload", {"session_id": session, "name": "layers.png", "data": png}, alice)
        loaded = call("/api/attachment/get", {"id": image["id"]}, bob)
        check.require(loaded["data"] == png, "participant reads uploaded image bytes")
        call("/api/attachment/get", {"id": image["id"]}, outsider, code=403, status=403)
        call("/api/attachment/upload", {"session_id": session, "data": png}, outsider, code=403, status=403)
        bad = stub.SendUpstreamMessage(pb.UpstreamMessageRequest(from_user_id=bid, auth_token=bob["token"],
            to_user_id=aid, scene="single", client_msg_id="image-owner-" + secrets.token_hex(8),
            content_json=json.dumps({"content": {"attachments": [image]}})), timeout=10)
        check.require(bad.error.code == 400 and bad.ack_stage == pb.ACK_STAGE_UNSPECIFIED, "image sender ownership checked before accepted ACK")
        bad = stub.SendUpstreamMessage(pb.UpstreamMessageRequest(from_user_id=aid, auth_token=alice["token"],
            to_user_id=oid, scene="single", client_msg_id="image-session-" + secrets.token_hex(8),
            content_json=json.dumps({"content": {"attachments": [image]}})), timeout=10)
        check.require(bad.error.code == 400, "image cross-session reference rejected")

        room = call("/api/admin/chatroom/create", {"name": "Layered room", "owner_id": aid}, admin)["room_id"]
        room_body = {"room_id": room, "user_id": aid}
        call("/api/chatroom/join", room_body, alice)
        call("/api/chatroom/join", {"room_id": room, "user_id": bid}, alice, code=403, status=403)
        call("/api/chatroom/leave", room_body, alice)
        rooms = call("/api/chatroom/list", {"user_id": aid}, alice)["rooms"]
        check.require(any(r["room_id"] == room for r in rooms), "HTTP leave preserves durable subscription")
        report = pb.RoomReportRequest(user_id=aid, room_id=room, comet_id="layers-test")
        check.require(stub.ReportRoomJoin(report, timeout=10).error.code == 0, "gRPC online room join")
        count = call("/api/chatroom/online_count", {"room_id": room}, alice)["online_count"]
        check.require(count == 1, "online presence differs from subscription")
        check.require(stub.ReportRoomLeave(report, timeout=10).error.code == 0, "gRPC online room leave")
        # Formal spaces keep an owner; ordinary public members may still leave.
        call("/api/chatroom/unsubscribe", room_body, alice, code=409, status=200)
        call("/api/chatroom/join", {"room_id":room,"user_id":bid}, bob)
        call("/api/chatroom/unsubscribe", {"room_id":room,"user_id":bid}, bob)
        rooms = call("/api/chatroom/list", {"user_id": bid}, bob)["rooms"]
        check.require(all(r["room_id"] != room for r in rooms), "unsubscribe removes ordinary member while preserving owner")
        call("/api/admin/chatroom/list", {}, admin)
        call("/api/admin/broadcast", {"scope": "chatroom", "room_id": room, "text": "Layer broadcast"}, admin)
        call("/api/admin/broadcast", {"scope": "unknown", "text": "bad"}, admin, code=400, status=400)
        call("/api/danmaku/send", {"video_id": "layers", "timeline_ms": 0, "text": "test"}, code=401, status=400)
        call("/api/danmaku/send", {"token": alice["token"], "video_id": "layers", "timeline_ms": 0, "text": "test"})
        call("/api/danmaku/list", {"video_id": "layers"})
        call("/api/knowledge/document", {"doc_id": "../../etc/passwd"}, alice, code=400, status=400)

    # Both endpoints disconnected. Sending via unary RPC still acknowledges
    # Kafka persistence; the receiver recovers exactly that sequence on WS login.
    client = "offline-" + secrets.token_hex(12)
    offline = stub.SendUpstreamMessage(pb.UpstreamMessageRequest(from_user_id=aid, auth_token=alice["token"],
        to_user_id=bid, scene="single", client_msg_id=client,
        content_json=json.dumps({"content": {"text": "offline recovery"}})), timeout=10)
    check.require(offline.error.code == 0 and offline.ack_stage == pb.ACK_STAGE_ACCEPTED, "offline unary accepts durable event")
    rows = await wait_history(session, alice, lambda rows: any(r["client_msg_id"] == client for r in rows))
    check.require(sum(r["client_msg_id"] == client for r in rows) == 1, "offline message persisted exactly once in test")
    async with await connect(bob) as b:
        event = await receive(b, lambda e: e.get("client_msg_id") == client)
        check.require(event["msg_seq"] == offline.message.msg_seq, "WS reconnect restores offline sequence")

    call("/api/message/send", {"from_user_id": aid, "to_user_id": bid, "content": "legacy text"}, code=401, status=401)
    call("/api/message/send", {"from_user_id": aid, "to_user_id": bid, "content": "legacy text"}, alice)
    call("/api/admin/user/update", {"user_id": oid, "name": "Renamed outsider"}, admin)
    call("/api/admin/user/list", {}, admin)
    call("/api/admin/user/revoke_tokens", {"user_id": oid}, admin)
    call("/api/session/list_single", {"user_id": oid}, outsider, code=401, status=401)
    logged = call("/api/login", outsider["credentials"])
    outsider["token"] = logged["token"]
    call("/api/admin/user/disable", {"user_id": oid, "reason": "isolated regression"}, admin)
    call("/api/login", outsider["credentials"], code=403, status=400)
    verified = stub.VerifyToken(pb.VerifyTokenRequest(token=outsider["token"], comet_id="layers-test"), timeout=10)
    check.require(verified.error.code == 401 and verified.user_id == 0, "disabled token invalid on realtime entrance")
    call("/api/admin/user/restore", {"user_id": oid}, admin)
    call("/api/login", outsider["credentials"])
    audit = call("/api/admin/audit/list", {"target_user_id": oid}, admin)
    check.require(audit["total"] >= 4, "user administration audited")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True)
    parser.add_argument("--secret-file", required=True)
    parser.add_argument("--mysql-port", type=int, default=3308)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-db", default="spark_push")
    parser.add_argument("--redis-port", type=int, default=6381)
    parser.add_argument("--redis-db", type=int, default=11)
    parser.add_argument("--brokers", default="127.0.0.1:29092")
    args = parser.parse_args()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    check, processes, logs = Checks(), [], []
    result = {"schema": "sparkpush.logic_layers_regression.v1", "ok": False}
    start = time.monotonic()
    env = dict(os.environ, SPARK_PUSH_MYSQL_PASSWORD=json.loads(Path(args.secret_file).read_text())["mysql_password"],
               SPARK_PUSH_HERMES_ENABLED="false", SPARK_PUSH_ADMIN_ACCOUNT="layers_admin",
               SPARK_PUSH_ADMIN_PASSWORD=secrets.token_hex(24))
    # Test credentials are only propagated to local child processes.
    os.environ["SPARK_PUSH_ADMIN_ACCOUNT"] = env["SPARK_PUSH_ADMIN_ACCOUNT"]
    os.environ["SPARK_PUSH_ADMIN_PASSWORD"] = env["SPARK_PUSH_ADMIN_PASSWORD"]
    try:
        with tempfile.TemporaryDirectory(prefix="spark-logic-layers-") as temp:
            private = Path(temp)
            private.chmod(0o700)
            if protoc.main(["protoc", "-I" + str(ROOT / "proto"),
                            "--python_out=" + temp, "--grpc_python_out=" + temp,
                            str(ROOT / "proto/spark_push.proto")]) != 0:
                raise RuntimeError("protobuf test client generation failed")
            sys.path.insert(0, temp)
            pb = importlib.import_module("spark_push_pb2")
            rpc = importlib.import_module("spark_push_pb2_grpc")
            ports = {name: free_port() for name in ["logic", "http", "ws", "comet", "jobm", "cometm"]}
            prefix = "layers_" + secrets.token_hex(8) + "_"
            common = {"kafka_brokers": args.brokers, "kafka_consumer_group": prefix + "group",
                      "mysql_port": args.mysql_port, "mysql_user": args.mysql_user,
                      "mysql_db": args.mysql_db, "mysql_password": "${SPARK_PUSH_MYSQL_PASSWORD}"}
            common.update({"kafka_" + name + "_topic": prefix + name for name in ["single", "group", "broadcast", "persist"]})
            configs = {
                "logic": dict(common, listen_addr="127.0.0.1", listen_port=ports["logic"],
                              http_port=ports["http"], redis_port=args.redis_port, redis_db=args.redis_db,
                              hermes_enabled="false", agent_bot_users="", attachment_dir=str(private / "attachments")),
                "comet": {"listen_addr": "127.0.0.1", "listen_port": ports["ws"],
                          "comet_grpc_port": ports["comet"], "metrics_port": ports["cometm"],
                          "logic_grpc_target": f"127.0.0.1:{ports['logic']}"},
                "job": dict(common, comet_targets=f"comet-1=127.0.0.1:{ports['comet']}", metrics_port=ports["jobm"]),
            }
            try:
                for name, updates in configs.items():
                    cfg = write_config(private, name, updates)
                    log = open(out / f"{name}.log", "w")
                    logs.append(log)
                    processes.append(subprocess.Popen([str(ROOT / f"build-wsl/{name}/{name}_server"), "--config", str(cfg)],
                                                     cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT))
                    time.sleep(.5)
                base = f"http://127.0.0.1:{ports['http']}"
                for _ in range(100):
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError("application exited at startup; inspect service logs")
                    try:
                        urllib.request.urlopen(base + "/metrics", timeout=1).close()
                        break
                    except (urllib.error.URLError, TimeoutError):
                        time.sleep(.1)
                time.sleep(5)  # New Kafka groups need initial assignment.
                with grpc.insecure_channel(f"127.0.0.1:{ports['logic']}") as channel:
                    grpc.channel_ready_future(channel).result(timeout=10)
                    asyncio.run(exercise(base, f"ws://127.0.0.1:{ports['ws']}", rpc.LogicServiceStub(channel), pb, check))
                result["ok"] = True
            finally:
                # Stop children before private attachment files/configs disappear.
                for process in reversed(processes):
                    if process.poll() is None:
                        process.terminate()
                for process in reversed(processes):
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
    except Exception as error:
        # Do not serialize request bodies, tokens, credentials or exception URLs.
        result["error_type"] = type(error).__name__
        result["error"] = str(error) if isinstance(error, (AssertionError, RuntimeError)) else "integration execution failed; inspect private logs"
    finally:
        for log in logs:
            log.close()
        result.update(checks=check.names, checks_passed=len(check.names), duration_s=time.monotonic() - start,
                      isolated_dependencies={"mysql_port": args.mysql_port, "redis_port": args.redis_port,
                                             "redis_db": args.redis_db, "kafka": args.brokers})
        (out / "business-e2e.json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
        print(json.dumps({k: v for k, v in result.items() if k != "checks"}, ensure_ascii=False))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
