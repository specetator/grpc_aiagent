"""Opt-in: real HTTP + MySQL + Redis + local text worker, no mocked authority.

Lease expiry and lost replies are controlled faults. Server restarts are actual
process kills. This does not exercise Kafka, WebSocket, native models or browsers.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cannbot/scripts"))
import media_text_worker as worker


def sql(statement):
    result = subprocess.run(["docker", "exec", "-i", os.environ["SPARK_TEST_MYSQL_CONTAINER"], "sh", "-c",
                             'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names spark_review_test'],
                            input=statement, text=True, capture_output=True, check=True)
    return result.stdout.strip()


def main():
    if not all(os.environ.get(k) for k in ("SPARK_PUSH_RUN_MYSQL_TESTS", "SPARK_TEST_MYSQL_CONTAINER", "SPARK_TEST_REDIS_PORT")):
        return 77
    for raw in (b"a\r\nb\n", "中文\v内容\n末行".encode(), b"\xef\xbb\xbfhello\n", b"\n"):
        parts = worker.fragments(raw)
        assert "".join(p["text"] for p in parts).encode() == raw
    for raw, error in ((b"\xff", "invalid_utf8"), (b"a\0b", "invalid_utf8"),
                       (b"x" * (worker.PART_BYTES + 1), "line_too_long")):
        try:
            worker.fragments(raw)
            raise AssertionError("invalid input accepted")
        except worker.InvalidText as exc:
            assert str(exc) == error
    with tempfile.TemporaryDirectory(prefix="spark-media-http-") as directory:
        root = Path(directory)
        key = secrets.token_hex(32)
        key_path = root / "worker.key"
        key_path.write_text(key)
        key_path.chmod(0o600)
        mismatched = worker.State(root / "different-worker-build")
        mismatched.save({"claim_id": "1000000000-" + "d" * 32, "pipeline_sha": "0" * 64})
        try:
            worker.once(None, mismatched)
            raise AssertionError("changed worker executed an older assignment")
        except ValueError:
            assert mismatched.load() is not None
        finally:
            mismatched.close()
        env = dict(os.environ, SPARK_PUSH_MEDIA_WORKER_KEY=key, SPARK_PUSH_MEDIA_PIPELINE_SHA256=worker.fingerprint())
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        process = None

        def start():
            nonlocal process
            process = subprocess.Popen([sys.argv[1], str(port), str(root / "blobs")], env=env,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            deadline = time.monotonic() + 10
            while True:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        return
                except OSError:
                    assert process.poll() is None and time.monotonic() < deadline, "HTTP fixture failed"
                    time.sleep(0.03)

        def stop():
            process.kill()
            process.wait(timeout=10)

        def post(path, body, token=None, code=0):
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=15)
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = "Bearer " + token
            conn.request("POST", path, json.dumps(body, ensure_ascii=False).encode(), headers)
            response = conn.getresponse()
            result = json.loads(response.read())
            conn.close()
            assert result["code"] == code, (path, result.get("code"), result.get("message"))
            return result.get("data", {})

        def user(label):
            return post("/api/register", {"account": "media_" + label + secrets.token_hex(5),
                                         "password": secrets.token_hex(12), "name": label})

        def media(op, data, who, code=0):
            return post("/api/room/media/" + op, data, who["token"], code)

        def internal(op, data, code=0):
            return post("/internal/media/" + op, data, key, code)

        def claim_input():
            return {"claim_id": f"{int(time.time())}-{secrets.token_hex(16)}", "pipeline_sha": worker.fingerprint()}

        def lease_of(assignment):
            return {k: assignment[k] for k in ("id", "attempt", "revision", "lease_token")}

        def upload(raw, mime="text/markdown"):
            item = media("init", {"room_id": room, "client_upload_id": secrets.token_hex(16),
                                   "name": "设计 O'Reilly '; --.md", "mime": mime,
                                   "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}, a)
            for index, offset in enumerate(range(0, len(raw), 1024 * 1024)):
                media("chunk", {"id": item["id"], "index": index,
                                "data": base64.b64encode(raw[offset:offset+1024*1024]).decode()}, a)
            media("finish", {"id": item["id"]}, a)
            return item["id"]

        try:
            start()
            a, b, c = user("A"), user("B"), user("C")
            room = post("/api/spaces/create", {"name": "资料协作"}, a["token"])["room_id"]
            other = post("/api/spaces/create", {"name": "隔离空间"}, c["token"])["room_id"]
            post("/api/spaces/invite", {"room_id": room, "user_id": b["user_id"]}, a["token"])
            post("/api/spaces/accept", {"room_id": room}, b["token"])
            post("/api/room/media/search", {"room_id": room, "query": "设计"}, code=401)
            post("/internal/media/claim", claim_input(), a["token"], 401)
            rejected = claim_input(); rejected["pipeline_sha"] = "f" * 64
            internal("claim", rejected, 409)
            rejected = claim_input(); rejected["claim_id"] = "1000000000-" + "a" * 32
            internal("claim", rejected, 410)
            expired_state = worker.State(root / "expired-identity")
            expired_state.save(rejected)
            assert worker.once(worker.Client(f"http://127.0.0.1:{port}", key), expired_state) == {"state": "expired_claim"}
            assert expired_state.load() is None
            expired_state.close()

            # Actual HTTP chunk upload interrupted by an actual server process kill.
            raw = ("设计讨论：Kafka 确认与 SQL 持久化必须区分。\n" * 22000 + "最终决策：保留原始请求标识。\n").encode()
            assert len(raw) > 1024 * 1024
            init = {"room_id": room, "client_upload_id": secrets.token_hex(16), "name": "会议文字.md",
                    "mime": "text/markdown", "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
            for invalid in (-1, True, None, 1.5, "1", 2**64 - 1):
                for field in ("room_id", "bytes"):
                    media("init", dict(init, **{field: invalid}), a, 400)
                media("search", {"room_id": room, "query": "设计", "limit": invalid}, a, 400)
                internal("reclaim_candidates", {"limit": invalid}, 400)
            ident = media("init", init, a)["id"]
            media("chunk", {"id": ident, "index": 0, "data": base64.b64encode(raw[:1024*1024]).decode()}, a)
            media("status", {"id": ident}, b, 403)
            stop(); start()
            assert len(media("status", {"id": ident}, a)["chunks"]) == 1
            assert media("init", init, a)["id"] == ident
            media("chunk", {"id": ident, "index": 1, "data": base64.b64encode(raw[1024*1024:]).decode()}, a)
            media("finish", {"id": ident}, a)
            media("read", {"id": ident}, c, 403)
            media("fragments", {"id": ident}, b, 409)

            # Durable pending claim before a simulated lost claim response. A real
            # worker process and a restarted HTTP server recover that assignment.
            pending = claim_input()
            with ThreadPoolExecutor(max_workers=2) as pool:
                assigned = list(pool.map(lambda _: internal("claim", pending), range(2)))
            assert assigned[0]["revision"] == assigned[1]["revision"] and assigned[0]["attempt"] == 1
            assert internal("claim", claim_input()) == {"idle": True}
            state = worker.State(root / "worker-state")
            state.save(pending); state.close()
            stop(); start()
            # Run the real once() implementation in a child and kill that actual
            # process immediately after its first staged fragment was committed.
            crash_script = '''import os, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
import media_text_worker as w
client=w.Client(sys.argv[2],w.private_read(Path(sys.argv[3]),257).decode())
state=w.State(Path(sys.argv[4]))
call=client.call
def crash(op,payload):
    result=call(op,payload)
    if op == "stage": os.kill(os.getpid(),9)
    return result
client.call=crash
w.once(client,state)
'''
            crashed = subprocess.run([sys.executable, "-c", crash_script, str(ROOT / "cannbot/scripts"),
                                      f"http://127.0.0.1:{port}", str(key_path), str(root / "worker-state")],
                                     capture_output=True, timeout=30)
            assert crashed.returncode == -9
            assert sql(f"SELECT COUNT(*) FROM room_media_fragment WHERE revision_id='{assigned[0]['revision']}'") == "1"
            assert sql(f"SELECT COUNT(*) FROM room_media_publication WHERE asset_id='{ident}'") == "0"
            result = subprocess.run([sys.executable, str(ROOT / "cannbot/scripts/media_text_worker.py"),
                                     "--origin", f"http://127.0.0.1:{port}", "--key-file", str(key_path),
                                     "--state-dir", str(root / "worker-state"), "--once"],
                                    capture_output=True, text=True, timeout=75)
            assert result.returncode == 0, "real worker failed: " + result.stderr
            published = json.loads(result.stdout)
            assert published["state"] == "ready" and published["revision"] == assigned[0]["revision"]
            info = media("processing", {"id": ident}, b)
            assert info["state"] == "ready" and info["attempt"] == 1
            assert "lease_token" not in json.dumps(info)
            text, offset = [], 0
            while True:
                page = media("fragments", {"id": ident, "offset": offset, "revision": published["revision"]}, b)
                text += [part["content"]["text"] for part in page["items"]]
                if not page["has_more"]:
                    break
                offset = page["next_offset"]
            assert "".join(text).encode() == raw
            assert media("search", {"room_id": room, "query": "最终决策"}, b)["items"][0]["id"] == ident
            assert not media("search", {"room_id": other, "query": "最终决策"}, c)["items"]
            media("search", {"room_id": room, "query": "最终决策"}, c, 403)
            post("/api/attachment/get", {"id": ident}, a["token"], 409)
            media("fragments", {"id": ident, "revision": "f" * 64}, b, 409)
            old = lease_of(assigned[0])
            internal("complete", dict(old, fragment_count=published["fragment_count"], manifest_sha256=published["manifest_sha256"]))
            internal("complete", dict(old, fragment_count=published["fragment_count"], manifest_sha256="f"*64), 409)
            internal("source", old, 409)
            # An existing backdated identity can only recover its original
            # receipt. A missing old identity returned 410 above, never a job.
            ancient = dict(pending, claim_id="1000000000-" + "b" * 32)
            ancient_key = hashlib.sha256(ancient["claim_id"].encode()).hexdigest()
            sql(f"INSERT INTO room_media_claim(claim_key,revision_id,pipeline_sha) VALUES('{ancient_key}','{old['revision']}','{worker.fingerprint()}')")
            assert internal("claim", ancient)["revision"] == old["revision"]
            assert not internal("claim", ancient)["live"]

            # Never publish a partial index; expiry fences every operation and
            # takeover gets a distinct revision. Three losses exhaust the budget.
            partial_id = upload(b"first\nsecond\n")
            claims = [claim_input(), claim_input()]
            with ThreadPoolExecutor(max_workers=2) as pool:
                competing = list(pool.map(lambda body: internal("claim", body), claims))
            winners = [i for i, value in enumerate(competing) if not value.get("idle")]
            assert len(winners) == 1
            pending, assignment = claims[winners[0]], competing[winners[0]]
            lease = lease_of(assignment)
            fragment = {"kind": "text", "text": "first\n", "anchor": {"line_start": 1, "line_end": 1}}
            staged = internal("stage", dict(lease, index=0, fragment=fragment))
            assert internal("stage", dict(lease, index=0, fragment=fragment)) == staged
            altered = dict(fragment, text="wrong\n")
            internal("stage", dict(lease, index=0, fragment=altered), 409)
            internal("stage", dict(lease, index=1, fragment=dict(fragment, text="x" * 16385)), 400)
            internal("complete", dict(lease, fragment_count=2, manifest_sha256="a"*64), 409)
            internal("complete", dict(lease, fragment_count=1, manifest_sha256="a"*64), 422)
            internal("stage", dict(lease, index=1, fragment={"kind": "text", "text": "second\n",
                                                            "anchor": {"line_start": 2, "line_end": 2}}))
            internal("complete", dict(lease, fragment_count=2, manifest_sha256="a"*64), 409)
            assert not media("search", {"room_id": room, "query": "first"}, b)["items"]
            media("fragments", {"id": partial_id}, b, 409)
            sql(f"UPDATE room_media_job SET lease_until=TIMESTAMPADD(SECOND,-1,NOW(3)) WHERE asset_id='{partial_id}'")
            for operation, body in (("source", lease), ("check", lease), ("heartbeat", lease),
                                    ("stage", dict(lease, index=1, fragment=fragment)),
                                    ("complete", dict(lease, fragment_count=1, manifest_sha256="a"*64)),
                                    ("fail", dict(lease, error_code="processing_error"))):
                internal(operation, body, 409)
            for attempt in (2, 3):
                newer = internal("claim", claim_input())
                assert newer["id"] == partial_id and newer["attempt"] == attempt and newer["revision"] != lease["revision"]
                assert internal("claim", pending)["revision"] == lease["revision"]
                assert not internal("claim", pending)["live"]
                assert sql(f"SELECT COUNT(*) FROM room_media_fragment WHERE revision_id='{lease['revision']}'") == "0"
                sql(f"UPDATE room_media_job SET lease_until=TIMESTAMPADD(SECOND,-1,NOW(3)) WHERE asset_id='{partial_id}'")
            assert internal("claim", claim_input()) == {"idle": True}
            assert media("processing", {"id": partial_id}, b)["error_code"] == "attempts_exhausted"
            assert media("read", {"id": partial_id}, a)["data"] == base64.b64encode(b"first\nsecond\n").decode()

            # Heartbeat can extend a live lease, but never its one-hour deadline.
            deadline_id = upload(b"bounded processing time\n")
            deadline_lease = lease_of(internal("claim", claim_input()))
            sql(f"UPDATE room_media_job SET lease_until=TIMESTAMPADD(SECOND,5,NOW(3)) WHERE asset_id='{deadline_id}'")
            internal("heartbeat", deadline_lease)
            assert sql(f"SELECT lease_until>TIMESTAMPADD(SECOND,100,NOW(3)) FROM room_media_job WHERE asset_id='{deadline_id}'") == "1"
            sql(f"UPDATE room_media_revision SET created_at=TIMESTAMPADD(SECOND,-3550,NOW(3)) WHERE revision_id='{deadline_lease['revision']}'")
            internal("heartbeat", deadline_lease)
            assert sql(f"SELECT j.lease_until<=TIMESTAMPADD(SECOND,3600,r.created_at) FROM room_media_job j JOIN room_media_revision r ON r.asset_id=j.asset_id AND r.attempt=j.attempt WHERE j.asset_id='{deadline_id}'") == "1"
            sql(f"UPDATE room_media_revision SET created_at=TIMESTAMPADD(SECOND,-3601,NOW(3)) WHERE revision_id='{deadline_lease['revision']}'")
            internal("heartbeat", deadline_lease, 409)
            internal("source", deadline_lease, 409)
            internal("stage", dict(deadline_lease, index=0, fragment=fragment), 409)
            reclaimed = internal("claim", claim_input())
            assert reclaimed["id"] == deadline_id and reclaimed["attempt"] == 2
            internal("fail", dict(lease_of(reclaimed), error_code="processing_error"))

            # Independent stages cannot exceed a shared revision budget even
            # when each fragment would fit by itself.
            budget_id = upload(b"1234567890\n1234567890\n")
            budget_lease = lease_of(internal("claim", claim_input()))
            fragment16 = {"kind": "text", "text": "123456789012345\n", "anchor": {"line_start": 1, "line_end": 1}}
            raw_client = worker.Client(f"http://127.0.0.1:{port}", key)
            def insert_part(index):
                try:
                    raw_client.call("stage", dict(budget_lease, index=index, fragment=fragment16))
                    return 0
                except worker.BackendError as exc:
                    return exc.code
            with ThreadPoolExecutor(max_workers=2) as pool:
                assert sorted(pool.map(insert_part, (0, 1))) == [0, 413]
            internal("fail", dict(budget_lease, error_code="processing_error"))
            assert media("processing", {"id": budget_id}, a)["state"] == "failed"

            for wrong_index, wrong_end, expected in ((0, 2, 422), (1, 1, 409)):
                invalid_id = upload(b"one\n")
                invalid_lease = lease_of(internal("claim", claim_input()))
                invalid = internal("stage", dict(invalid_lease, index=wrong_index, fragment={
                    "kind": "text", "text": "one\n", "anchor": {"line_start": 1, "line_end": wrong_end}}))
                digest = hashlib.sha256(json.dumps([invalid["sha256"]], separators=(",", ":")).encode()).hexdigest()
                internal("complete", dict(invalid_lease, fragment_count=1, manifest_sha256=digest), expected)
                media("fragments", {"id": invalid_id}, a, 409)
                internal("fail", dict(invalid_lease, error_code="processing_error"))

            for raw_bad, expected_error in ((b"valid\n\xff", "invalid_utf8"), (b"x" * 16385, "line_too_long")):
                bad_id = upload(raw_bad)
                bad_result = subprocess.run([sys.executable, str(ROOT / "cannbot/scripts/media_text_worker.py"),
                                             "--origin", f"http://127.0.0.1:{port}", "--key-file", str(key_path),
                                             "--state-dir", str(root / ("bad-" + expected_error)), "--once"],
                                            capture_output=True, text=True, timeout=30)
                assert bad_result.returncode == 1
                assert json.loads(bad_result.stdout)["error_code"] == expected_error
                assert media("processing", {"id": bad_id}, a)["error_code"] == expected_error
                media("fragments", {"id": bad_id}, a, 409)

            # Completion committed but the client cannot observe its reply. Keep
            # the claim file, restart server, recover without rerunning the job.
            recovered_id = upload("引号 ' 与百分号 % 是普通内容。\n".encode())
            state = worker.State(root / "lost-reply-state")
            client = worker.Client(f"http://127.0.0.1:{port}", key)
            call = client.call
            def lose_completion(operation, payload):
                result = call(operation, payload)
                if operation == "complete":
                    raise worker.BackendError(503)
                return result
            client.call = lose_completion
            try:
                worker.once(client, state)
                raise AssertionError("completion reply was not lost")
            except worker.BackendError:
                assert state.load() is not None
            stop(); start()
            recovered = worker.once(worker.Client(f"http://127.0.0.1:{port}", key), state)
            state.close()
            assert recovered == {"state": "ready", "id": recovered_id}
            assert media("processing", {"id": recovered_id}, b)["attempt"] == 1
            assert media("search", {"room_id": room, "query": "'"}, b)["items"]
            assert not media("search", {"room_id": room, "query": "' OR 1=1 --"}, b)["items"]
            pdf = upload(b"%PDF-1.7\nnot-yet-parsed", "application/pdf")
            assert internal("claim", claim_input()) == {"idle": True}
            assert not media("processing", {"id": pdf}, b)["text_pipeline_supported"]
            assert media("processing", {"id": pdf}, b)["state"] == "queued"

            post("/api/spaces/invite", {"room_id": room, "user_id": c["user_id"]}, a["token"])
            post("/api/spaces/accept", {"room_id": room}, c["token"])
            assert all(media("search", {"room_id": room, "query": "最终决策"}, who)["items"] for who in (a, b, c))
            post("/api/spaces/remove", {"room_id": room, "user_id": b["user_id"]}, a["token"])
            for operation in ("processing", "fragments", "read", "status", "finish"):
                media(operation, {"id": ident}, b, 403)
            media("search", {"room_id": room, "query": "最终决策"}, b, 403)
            assert sql(f"SELECT COUNT(*) FROM room_media_publication WHERE asset_id='{ident}'") == "1"
            print("PASS real HTTP/MySQL/Redis: three accounts, >1MiB upload and process restart, real text worker, "
                  "claim/complete recovery, fenced expiry, bounded attempts, private published search, revoked access; "
                  "PDF/audio/video/native-Agent/browser E2E not exercised")
        finally:
            if process is not None and process.poll() is None:
                stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
