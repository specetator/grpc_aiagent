"""Real HTTP/MySQL/Redis context authority; seeded message/request fixtures.

Uploads and text parsing use real services. Optional image uses the production
Docker parser worker. No Kafka, native model, browser or tool-execution claim.
"""
import base64
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import selectors
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib

from media_processing_http_test import sql, worker as text_worker
from room_context import RoomContextClient, RoomContextError, hermes_result
from room_workspace import WorkspaceAuthority

ROOT = Path(__file__).resolve().parents[1]


def literal(value):
    raw = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return "CONVERT(0x" + raw.encode().hex() + " USING utf8mb4)"


def main():
    if not all(os.environ.get(k) for k in ("SPARK_PUSH_RUN_MYSQL_TESTS", "SPARK_TEST_MYSQL_CONTAINER", "SPARK_TEST_REDIS_PORT")):
        return 77
    report = {"scope": __doc__, "checks": [], "status": "running"}
    destination = Path(os.environ.get("SPARK_TEST_CONTEXT_REPORT", "/tmp/spark-media-context-http.json"))

    def passed(name):
        report["checks"].append(name)
        destination.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print("real room context passed:", name, flush=True)

    with tempfile.TemporaryDirectory(prefix="spark-context-http-") as temporary:
        root = Path(temporary)
        connector_key, worker_key = secrets.token_hex(32), secrets.token_hex(32)
        env = dict(os.environ, SPARK_PUSH_WORKSPACE_AUTH_KEY=connector_key,
                   SPARK_PUSH_MEDIA_WORKER_KEY=worker_key, SPARK_PUSH_MEDIA_PIPELINE_SHA256=text_worker.fingerprint())
        config = None
        if os.environ.get("SPARK_TEST_CONTEXT_IMAGE"):
            import media_worker
            image = subprocess.check_output(["docker", "image", "inspect", os.environ["SPARK_TEST_CONTEXT_IMAGE"],
                                             "--format", "{{.Id}}"], text=True).strip()
            config = media_worker.Configuration(image, os.environ["SPARK_TEST_MEDIA_MODEL"])
            env.update(SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256=config.pipeline, SPARK_PUSH_MEDIA_PARSER_SHA256=config.parser_sha)
            report["parser_configuration"] = config.description()
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0)); port = sock.getsockname()[1]
        process = None

        def start():
            nonlocal process
            process = subprocess.Popen([sys.argv[1], str(port), str(root / "blobs")], env=env,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            end = time.monotonic() + 10
            while time.monotonic() < end:
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):return
                except OSError:
                    assert process.poll() is None, "context HTTP probe exited"
                    time.sleep(0.03)
            raise AssertionError("context HTTP probe not ready")

        def stop():
            if process is not None and process.poll() is None:
                process.kill(); process.wait(timeout=10)

        def post(path, body, token="", code=0):
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
            try:
                connection.request("POST", path, json.dumps(body, ensure_ascii=False).encode(),
                                   {"Content-Type": "application/json", "Authorization": "Bearer " + token})
                result = json.loads(connection.getresponse().read(2*1024*1024))
                allowed=code if isinstance(code,tuple) else (code,)
                assert result["code"] in allowed, (path, code, result.get("code"), result.get("message"))
                if result["code"]:assert not result.get("data"), "protected data on failed request"
                return result.get("data", {})
            finally:connection.close()

        def context(operation, body, code=0, key=None):
            return post("/internal/room-agent/context/" + operation, dict(request_id=request, **body),
                        connector_key if key is None else key, code)

        def locked_calls(calls, mutation=""):
            """Observe each real HTTP request waiting at Record's room lock."""
            holder=subprocess.Popen(["docker","exec","-i",os.environ["SPARK_TEST_MYSQL_CONTAINER"],"sh","-c",
                'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names --unbuffered spark_review_test'],
                stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
            executor=ThreadPoolExecutor(max_workers=len(calls))
            try:
                assert holder.stdin and holder.stdout
                holder.stdin.write(f"START TRANSACTION; SELECT id FROM im_group WHERE id={room} FOR UPDATE;\n")
                holder.stdin.flush()
                with selectors.DefaultSelector() as selector:
                    selector.register(holder.stdout,selectors.EVENT_READ)
                    assert selector.select(10), "room lock holder did not start"
                    assert holder.stdout.readline().strip()==str(room)
                pending=[executor.submit(call) for call in calls]
                end=time.monotonic()+10
                while time.monotonic()<end:
                    waiting=sql("SELECT COUNT(DISTINCT w.REQUESTING_ENGINE_TRANSACTION_ID) FROM performance_schema.data_lock_waits w "
                        "JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID "
                        "WHERE l.OBJECT_SCHEMA='spark_review_test' AND l.OBJECT_NAME='im_group' "
                        f"AND l.LOCK_DATA='{room}';")
                    if int(waiting)>=len(calls):break
                    assert not any(p.done() for p in pending), "context request did not reach the room lock"
                    time.sleep(0.02)
                else:raise AssertionError("context room-lock waits not observed")
                holder.stdin.write(mutation+" COMMIT;\n");holder.stdin.flush();holder.stdin.close();holder.stdin=None
                holder.wait(timeout=10);assert holder.returncode==0
                return [p.result(timeout=10) for p in pending]
            finally:
                if holder.poll() is None:holder.kill();holder.wait(timeout=10)
                for stream in (holder.stdin,holder.stdout,holder.stderr):
                    if stream:stream.close()
                executor.shutdown(wait=True,cancel_futures=True)

        def upload(raw, name, mime="text/plain", target=None):
            target = room if target is None else target
            def media(op, body):return post("/api/room/media/" + op, body, a["token"])
            asset = media("init", {"room_id": target, "client_upload_id": secrets.token_hex(16), "name": name,
                "mime": mime, "bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()})["id"]
            media("chunk", {"id": asset, "index": 0, "data": base64.b64encode(raw).decode()})
            media("finish", {"id": asset})
            if mime == "text/plain":
                state = text_worker.State(root / ("text-" + asset))
                try:result = text_worker.once(text_worker.Client(f"http://127.0.0.1:{port}", worker_key), state)
                finally:state.close()
                assert result["state"] == "ready"
            return asset

        def seed_message(seq, text, sender=None, client=None, source=None, target=None):
            target = room if target is None else target
            content = {"content": {"text": text}, "msg_seq": seq, "msg_id": f"r_{target}-{seq}", "create_time": 1000+seq}
            if source:content.update(agent_id="public-pi", request_id=source)
            sql("INSERT INTO message(session_id,msg_seq,sender_id,content_json,timestamp_ms,client_msg_id) VALUES(" +
                f"'r_{target}',{seq},{sender or a['user_id']},{literal(content)},{1000+seq},{literal(client or 'human-'+str(seq))});")

        def seed_request(actor, seq=500, epoch=0, suffix=None):
            identity = "ra_" + (suffix or secrets.token_hex(32))
            scope = dict(kind="room_agent_v1", room_id=room, agent_id="public-pi", actor_user_id=actor,
                         owner_user_id=a["user_id"], bot_user_id=90001, generation=1, policy="read_only", runtime="pi",
                         source_msg_id=f"r_{room}-{seq}", request_id=identity, project_id="", workspace_generation=0,
                         context_generation=epoch)
            body = dict(request_id=identity, session_id=f"r_{room}", agent_id="public-pi", user_id=actor,
                        bot_user_id=90001, message_seq=seq, source_msg_id=f"r_{room}-{seq}", collaboration=scope)
            sql(f"INSERT INTO room_agent_request(request_id,room_id,message_seq,payload) VALUES('{identity}',{room},{seq},{literal(body)});")
            return identity

        def pi_client(scope, tool, arguments, **expected):
            node = os.environ.get('SPARK_TEST_CONTEXT_PI_NODE')
            if not node:
                report['pi_extension_check'] = 'not executed: SPARK_TEST_CONTEXT_PI_NODE required'
                return
            value = dict(scope=scope, origin=f'http://127.0.0.1:{port}', key=connector_key,
                         tool=tool, arguments=arguments, **expected)
            run = subprocess.run([node, str(ROOT/'tests/room_context_http_client.cjs')], input=json.dumps(value),
                                 text=True, capture_output=True, timeout=30)
            assert run.returncode == 0, 'Pi extension real HTTP client failed; no credential output retained'
            report['pi_extension_check'] = 'real Logic transport passed; controlled capability, no model'

        try:
            start()
            def user():return post("/api/register", {"account": "context_"+secrets.token_hex(8), "password": secrets.token_hex(12), "name": "context tester"})
            a,b,c = user(),user(),user()
            room = post("/api/spaces/create", {"name": "context room"}, a["token"])["room_id"]
            other = post("/api/spaces/create", {"name": "other room"}, a["token"])["room_id"]
            for target in (room,other):
                post("/api/spaces/invite", {"room_id": target,"user_id": b["user_id"]}, a["token"])
                post("/api/spaces/accept", {"room_id": target}, b["token"])
            sql(f"INSERT INTO room_agent_binding(room_id,agent_id,installed,generation,installed_by) VALUES({room},'public-pi',1,1,{a['user_id']});")
            request = seed_request(b["user_id"])
            seed_message(1,"Kafka 中文 O'Reilly decision")
            seed_message(2,"Kafka second human")
            seed_message(600,"Kafka future invisible")
            seed_message(1,"Kafka foreign private",target=other)
            stale = seed_request(a["user_id"], epoch=1)
            # More stale Agent replies than the page limit: eligibility must
            # happen before LIMIT, otherwise the human rows disappear.
            for seq in range(10,40):seed_message(seq,"Kafka stale derived",90001,"hermes:"+stale,stale)
            current = seed_request(a["user_id"])
            seed_message(3,"Kafka current derived",90001,"hermes:"+current,current)
            for seq,epoch in enumerate((None,False,True,0.0,"0",-1,9007199254740992),80):
                bad=seed_request(a["user_id"],epoch=epoch)
                seed_message(seq,"typed invalid origin",90001,"hermes:"+bad,bad)
            legacy=seed_request(a["user_id"])
            sql(f"UPDATE room_agent_request SET payload=JSON_REMOVE(payload,'$.collaboration.context_generation') WHERE request_id='{legacy}';")
            seed_message(79,"typed missing epoch origin",90001,"hermes:"+legacy,legacy)
            context("search", {"query":"Kafka"},401,key=b["token"])
            context("search", {"room_id": other},400)
            context("search", {"actor_user_id": a["user_id"]},400)
            context("search", {"query":"x"*5000},413)
            context("search", {"query":"x"*241},400)
            page = context("search", {"query":"Kafka","type":"messages","limit":2})
            assert [x["reference"]["msg_seq"] for x in page["items"]] == [3,2], page
            second = context("search", {"query":"Kafka","type":"messages","limit":2,"before_seq":page["messages_page"]["before_seq"]})
            assert [x["reference"]["msg_seq"] for x in second["items"]] == [1]
            assert not context("search", {"query":"' OR 1=1 --","type":"messages"})["items"]
            quoted=context("search", {"query":"O'Reilly","type":"messages"})
            assert len(quoted["items"])==1
            typed=context("search", {"query":"typed","type":"messages","limit":1})
            assert [x["reference"]["msg_seq"] for x in typed["items"]]==[79], typed
            passed("literal SQL search; pre-LIMIT epoch filter; canonical room and message ceiling")
            read=context("read", {"kind":"message","msg_seq":1,"offset":6,"limit":2})
            assert read["items"][0]["text"]=="中文"
            assert context("read", {"kind":"message","msg_seq":1,"offset":6,"limit":2})==read
            end=context("read", {"kind":"message","msg_seq":1,"offset":read["items"][0]["total_characters"],"limit":2})
            assert end["items"][0]["text"]=="" and end["items"][0]["reference"]["length"]==0 and not end["has_more"]
            context("read", {"kind":"message","msg_seq":10},404)
            context("read", {"kind":"message","msg_seq":600},400)
            context("read", {"kind":"message","msg_seq":1,"offset":999},416)
            for value in (True,-1,1.0,9007199254740992,"2"):
                context("read", {"kind":"message","msg_seq":1,"limit":value},400)
            source=read["items"][0]["source_id"]
            assert sql(f"SELECT COUNT(*) FROM room_context_source WHERE request_id='{request}' AND source_id='{source}';")=="1"
            source_body={"room_id":room,"request_id":request,"source_ids":[source]}
            def member_sources(body=source_body,token=None,code=0):
                return post('/api/chatroom/agents/sources',body,a['token'] if token is None else token,code)
            resolved=member_sources()
            assert resolved['items'][0]['available'] and resolved['items'][0]['text']=='中文'
            member_sources(token=c['token'],code=403)
            member_sources(dict(source_body,room_id=other),code=404)
            member_sources(dict(source_body,request_id=current),code=404)
            member_sources(dict(source_body,source_ids=['src_'+'f'*64]),code=404)
            member_sources(dict(source_body,source_ids=[source,source]),code=400)
            member_sources(dict(source_body,reference={'asset_id':'forged'}),code=400)
            first_sources=member_sources({'room_id':room,'request_id':request,'limit':1})
            assert len(first_sources['items'])==1 and first_sources['has_more']
            next_sources=member_sources({'room_id':room,'request_id':request,'limit':1,'after':first_sources['next_after']})
            assert next_sources['items'][0]['source_id']!=first_sources['items'][0]['source_id']
            source_task_input=dict(source_body,title='Review the cited discussion',steps=['Read the original excerpt'])
            source_task=post('/api/chatroom/tasks/create',source_task_input,a['token'])
            assert source_task['source_ids']==[source]
            post('/api/chatroom/tasks/create',dict(source_task_input,source_ids=['src_'+'e'*64]),a['token'],404)
            post('/api/chatroom/tasks/update',{'room_id':room,'task_id':source_task['task_id'],'revision':1,'source_ids':[]},a['token'],400)
            stop();start()
            assert context("read", {"kind":"message","msg_seq":1,"offset":6,"limit":2})==read
            saved=post('/api/chatroom/tasks/list',{'room_id':room},a['token'])['tasks'][0]
            assert saved['source_ids']==[source] and saved['history'][0]['source_ids']==[source]
            assert member_sources()==resolved
            passed('member source lookup, exact Unicode excerpt, pagination, forged/cross-request/foreign-room denial and task sources across real HTTP restart')
            passed("Unicode bounded read; deterministic durable receipts; actual HTTP process restart")
            regular=request;request=seed_request(a["user_id"])
            bounded=context("read", {"kind":"message","msg_seq":1,"offset":6,"limit":2})
            # Seed only the volume boundary; real reads still use the service
            # and verify current authority/content, including duplicate replay.
            sql("SET SESSION cte_max_recursion_depth=8192; INSERT INTO room_context_source(request_id,source_id,room_id,context_generation,reference) "
                "WITH RECURSIVE n AS (SELECT 1 AS v UNION ALL SELECT v+1 FROM n WHERE v<8190) "
                f"SELECT '{request}',CONCAT('padding_',LPAD(v,60,'0')),{room},0,JSON_OBJECT() FROM n;")
            assert bounded["items"][0]["source_id"]!=source
            outcomes=locked_calls([
                lambda:context("read", {"kind":"message","msg_seq":2,"offset":0,"limit":1},(0,429)),
                lambda:context("read", {"kind":"message","msg_seq":2,"offset":1,"limit":1},(0,429))])
            assert sum(bool(value) for value in outcomes)==1
            assert context("read", {"kind":"message","msg_seq":1,"offset":6,"limit":2})==bounded
            context("read", {"kind":"message","msg_seq":2,"offset":2,"limit":1},429)
            assert sql(f"SELECT COUNT(*) FROM room_context_source WHERE request_id='{request}';")=="8192"
            request=regular
            passed("two observed SQL lock waiters compete for last receipt slot; no overrun; duplicate at capacity; request-bound IDs")
            transcript=("会议决策：Kafka 保留原 ID。\n"+"background discussion without decisions\n"*1700+
                        "尾段唯一决策：保留原 request_id 并核对日志。\n")
            ident=upload(transcript.encode(),"meeting O'Reilly.txt")
            foreign=upload(b"private foreign media", "foreign.txt",target=other)
            catalog=context("search", {"type":"media","query":""})
            assert [x["reference"]["asset_id"] for x in catalog["items"]]==[ident]
            ref=catalog["items"][0]["reference"]
            media=context("read", {"kind":"fragment","asset_id":ident,"revision":ref["revision"]})
            assert "会议决策" in media["items"][0]["content"]["text"]
            tail=context("search", {"type":"fragments","query":"尾段唯一决策","limit":1})["items"][0]
            assert tail["reference"]["asset_id"]==ident and tail["reference"]["index"]>0
            assert tail["reference"]["anchor"]["line_end"]==1702 and "尾段唯一决策" in tail["content"]["text"]
            tail_read=context("read", {"kind":"fragment","asset_id":ident,"revision":ref["revision"],
                                       "offset":tail["reference"]["index"],"limit":1})["items"][0]
            assert tail_read["source_id"]==tail["source_id"] and tail_read["content"]==tail["content"]
            fragment_source_body=dict(source_body,source_ids=[tail['source_id']])
            fragment_source=member_sources(fragment_source_body)['items'][0]
            assert fragment_source['available'] and fragment_source['name']=="meeting O'Reilly.txt"
            assert fragment_source['reference']==tail['reference']
            authority = WorkspaceAuthority(f'http://127.0.0.1:{port}', connector_key)
            client = RoomContextClient(authority)
            scope = authority.authorize(request)
            arguments = dict(kind='fragment',asset_id=ident,revision=ref['revision'],offset=tail['reference']['index'],limit=1)
            assert client.call(scope,'room_read',arguments)['items'][0] == tail_read
            pi_client(scope,'room_read',arguments,source_id=tail_read['source_id'])
            context("search", {"type":"fragments","query":""},400)
            assert not context("search", {"type":"fragments","query":"' OR 1=1 --"})["items"]
            passed("literal fragment search locates long-text tail with real line anchor and stable read receipt")
            foreign_revision=post("/api/room/media/processing", {"id":foreign}, a["token"])["revision"]
            context("read", {"kind":"fragment","asset_id":foreign,"revision":foreign_revision},404)
            context("read", {"kind":"fragment","asset_id":ident,"revision":"f"*64},409)
            passed("real text upload/worker/publication; fixed revision; cross-room read denied even for dual member")
            locked_calls([lambda:context("read", {"kind":"fragment","asset_id":ident,"revision":ref["revision"]},409)],
                         f"UPDATE attachment SET status='failed' WHERE id='{ident}';")
            withdrawn=member_sources(fragment_source_body)['items'][0]
            assert not withdrawn['available'] and 'reference' not in withdrawn and 'name' not in withdrawn
            post('/api/chatroom/tasks/create',dict(source_task_input,source_ids=[tail['source_id']]),a['token'],409)
            sql(f"UPDATE attachment SET status='ready' WHERE id='{ident}';")
            original=sql(f"SELECT payload FROM room_media_fragment WHERE revision_id='{ref['revision']}' AND fragment_index=0;")
            try:
                sql(f"UPDATE room_media_fragment SET payload=JSON_SET(payload,'$.text','corrupted') WHERE revision_id='{ref['revision']}' AND fragment_index=0;")
                context("read", {"kind":"fragment","asset_id":ident,"revision":ref["revision"]},409)
            finally:
                sql(f"UPDATE room_media_fragment SET payload={literal(original)} WHERE revision_id='{ref['revision']}' AND fragment_index=0;")
            passed("source withdrawal while HTTP read waits; fragment payload/hash mismatch rejected")
            if config:
                # Real, text-free PNG. No OCR result is required for discovery.
                def chunk(kind,data):return struct.pack(">I",len(data))+kind+data+struct.pack(">I",zlib.crc32(kind+data)&0xffffffff)
                png=b"\x89PNG\r\n\x1a\n"+chunk(b"IHDR",struct.pack(">IIBBBBB",32,32,8,2,0,0,0))
                png+=chunk(b"IDAT",zlib.compress((b"\0"+b"\x11\x66\xcc"*32)*32))+chunk(b"IEND",b"")
                image_asset=upload(png,"no-text-image.png","image/png")
                keyfile=root/"worker.key";keyfile.write_text(worker_key);keyfile.chmod(0o600)
                run=subprocess.run([sys.executable,str(ROOT/"cannbot/scripts/media_worker.py"),"--image-id",config.image,
                    "--model-dir",str(config.model),"--origin",f"http://127.0.0.1:{port}","--key-file",str(keyfile),
                    "--state-dir",str(root/"image-worker"),"--once"],capture_output=True,text=True,timeout=100)
                assert run.returncode==0,(run.stdout,run.stderr)
                found=context("search", {"type":"media","query":"no-text-image","limit":1})["items"][0]
                assert found["reference"]["asset_id"]==image_asset and found["image_count"]==1
                revision=found["reference"]["revision"]
                metadata=context("read", {"kind":"image_metadata","asset_id":image_asset,"revision":revision})["items"][0]
                image=context("media", {"asset_id":image_asset,"revision":revision,"index":0})["items"][0]
                binary=base64.b64decode(image["data"],validate=True)
                assert binary.startswith(b"\xff\xd8") and hashlib.sha256(binary).hexdigest()==image["reference"]["sha256"]
                assert metadata["source_id"]!=image["source_id"] and image["reference"]["anchor"]=={"image":1}
                arguments = dict(asset_id=image_asset,revision=revision,index=0)
                actual = client.call(scope,'room_read_media',arguments)
                assert actual['items'][0] == image
                assert hermes_result(actual)['content'][1]['image_url']['url'].endswith(image['data'])
                pi_client(scope,'room_read_media',arguments,source_id=image['source_id'],sha256=image['reference']['sha256'])
                passed("real Docker image parsing and JPEG read; zero-text discovery; metadata versus image receipts")
            else:report["image_check"]="not executed: SPARK_TEST_CONTEXT_IMAGE and model directory required"
            # Hold the actual room lock until this HTTP request reaches Record.
            # Advancing the epoch under that lock is a controlled authoritative
            # mutation, not a production delete API or a mocked permission gate.
            locked_calls([lambda:context("read", {"kind":"message","msg_seq":2,"offset":1,"limit":1},409)],
                         f"INSERT INTO room_context(room_id,generation) VALUES({room},1);")
            passed("actual HTTP read blocked on room lock; epoch commit fences receipt and protected response")
            invalid=member_sources()['items'][0]
            assert not invalid['available'] and 'reference' not in invalid and 'text' not in invalid
            post('/api/chatroom/tasks/create',source_task_input,a['token'],409)
            updated=post('/api/chatroom/tasks/update',{'room_id':room,'task_id':source_task['task_id'],'revision':1,'claim':True,'status':'running'},a['token'])
            assert updated['source_ids']==[source] and updated['status']=='running'
            passed('context change hides old source payload and refuses new attachment; existing task remains editable with its original source IDs')
            # Continue member/binding checks with a new explicit request at epoch 1.
            request_before=request
            request=seed_request(b["user_id"],epoch=1)
            current_scope = authority.authorize(request)
            locked_calls([lambda:context("read", {"kind":"message","msg_seq":2,"offset":0,"limit":1},403)],
                         f"DELETE FROM group_member WHERE group_id={room} AND user_id={b['user_id']};")
            try:
                client.call(current_scope,'room_search',{})
                raise AssertionError('revoked Python room client returned protected content')
            except RoomContextError as error:
                assert error.code == 403
            pi_client(current_scope,'room_search',{},revoked=True)
            passed("public runtime tool clients read real Logic sources and reject authoritative member revocation; no model")
            passed("actual in-flight HTTP member revocation; final authorization withholds source data")
            member_sources(token=b['token'],code=403)
            post("/api/spaces/invite", {"room_id":room,"user_id":b["user_id"]}, a["token"])
            post("/api/spaces/accept", {"room_id":room}, b["token"])
            post("/api/spaces/remove", {"room_id":room,"user_id":b["user_id"]}, a["token"])
            context("search", {"type":"messages"},403)
            context("read", {"kind":"fragment","asset_id":ident,"revision":ref["revision"]},403)
            request=seed_request(c["user_id"]);context("search", {},403)
            request=seed_request(a["user_id"],epoch=1)
            sql(f"UPDATE room_agent_binding SET installed=0 WHERE room_id={room};")
            context("search", {},403)
            sql(f"UPDATE room_agent_binding SET installed=1 WHERE room_id={room};")
            sql(f"UPDATE room_context SET generation=2 WHERE room_id={room};")
            context("search", {},403)
            passed("member removal, never-member, uninstalled Agent and stale epoch denied")
            # Migration unavailability must not be treated as generation zero or
            # an invitation to return content without a durable source receipt.
            request=seed_request(a["user_id"],epoch=2)
            sql("RENAME TABLE room_context_source TO room_context_source_missing;")
            try:context("search", {"type":"messages","query":"Kafka"},503)
            finally:sql("RENAME TABLE room_context_source_missing TO room_context_source;")
            assert sql(f"SELECT COUNT(*) FROM room_context_source WHERE request_id='{request_before}' AND source_id='{source}';")=="1"
            passed("missing receipt migration fails closed; old receipts remain unchanged")
            report["status"]="passed"
            destination.write_text(json.dumps(report,ensure_ascii=False,indent=2)+"\n")
            return 0
        finally:stop()


if __name__ == "__main__":
    raise SystemExit(main())
