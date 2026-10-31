#!/usr/bin/env python3
"""Isolated real MySQL/Redis/Kafka + two Comets, Logic, Job and controlled @CANN.
Python >=3.11; pip install grpcio grpcio-tools websockets. No personal config or DB.
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
import threading
import time
from http.server import ThreadingHTTPServer
import grpc
from grpc_tools import protoc
import websockets
from run_current_benchmarks import free_port, write_config
from logic_business_e2e import Checks, http

ROOT=Path(__file__).resolve().parents[1]

async def receive(ws,predicate,timeout=15):
    async with asyncio.timeout(timeout):
        while True:
            event=json.loads(await ws.recv())
            ws._space_test_events=getattr(ws,'_space_test_events',[])[-100:]+[event]
            if predicate(event): return event

async def exercise(base,urls,stub,pb,check,job_stubs,job_key,sql):
    def call(path,body,user=None,code=0):
        _,data=http(base,path,body,user['token'] if user else None)
        check.require(data['code']==code,f'{path}: expected {code}, got {data.get("code")} {data.get("message","")}')
        return data.get('data',{})
    users=[call('/api/register',{'account':'spaces_'+secrets.token_hex(8),'password':secrets.token_hex(16),'name':label}) for label in ('Owner','Admin','Member','Outsider')]
    a,b,c,d=users
    name="O'Reilly \\ '; DROP TABLE im_group; -- 空间"
    room=call('/api/spaces/create',{'name':name,'description':'private description'},a)['room_id'];sid=f'r_{room}'
    spaces=call('/api/spaces/list',{},a)['spaces']
    check.require(any(s['room_id']==room and s['role']=='owner' and s['visibility']=='private' and s['name']==name for s in spaces),'ordinary user creates private owner space; SQL roundtrip')
    call('/api/chatroom/join',{'room_id':room,'user_id':d['user_id']},d,403)
    call('/api/spaces/accept',{'room_id':room},d,403)
    call('/api/spaces/invite',{'room_id':room,'user_id':b['user_id']},a)
    call('/api/session/history',{'session_id':sid},b,403)
    invitations=call('/api/spaces/invitations',{},b)['invitations'];check.require(any(i['room_id']==room for i in invitations),'invitation inbox')
    call('/api/spaces/accept',{'room_id':room},b)
    call('/api/spaces/role',{'room_id':room,'user_id':b['user_id'],'role':'admin'},a)
    call('/api/spaces/invite',{'room_id':room,'user_id':c['user_id']},b)
    call('/api/spaces/accept',{'room_id':room},c)
    call('/api/spaces/invite',{'room_id':room,'user_id':d['user_id']},c,403)
    call('/api/spaces/role',{'room_id':room,'user_id':c['user_id'],'role':'admin'},b,403)
    call('/api/spaces/remove',{'room_id':room,'user_id':a['user_id']},b,403)
    call('/api/spaces/leave',{'room_id':room},a,409)
    for path,body in [('/api/spaces/members',{'room_id':room}),('/api/chatroom/online_count',{'room_id':room}),('/api/session/unread',{'user_id':d['user_id'],'session_id':sid}),('/api/session/mark_read',{'user_id':d['user_id'],'session_id':sid,'read_seq':1}),('/api/chatroom/agent/task',{'room_id':room})]:
        call(path,body,d,403)
    image=base64.b64encode(bytes.fromhex('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082')).decode()
    upload={'session_id':sid,'name':'room.png','data':image}
    call('/api/attachment/upload',upload,d,403)
    attachment=call('/api/attachment/upload',upload,c)
    sockets=[]
    async def connect(user,index=0):
        ws=await websockets.connect(urls[index]+'/ws?token='+user['token'],proxy=None,max_size=2**20);sockets.append(ws);return ws
    async def join(ws):
        await ws.send(json.dumps({'type':'chatroom_join','group_id':room}))
        return await receive(ws,lambda e:e.get('op')=='chatroom_join')
    async def send(ws,text,cid=None):
        cid=cid or secrets.token_hex(12)
        body={'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':text}}
        await ws.send(json.dumps(body));ack=await receive(ws,lambda e:e.get('client_msg_id')==cid and e.get('type') in ('accepted_ack','error'))
        return body,ack
    async def denied_receive(ws):
        try:
            await receive(ws,lambda e:e.get('session_id')==sid and e.get('type') in ('chatroom','hermes_delta'),.6)
        except TimeoutError: return
        raise AssertionError('revoked member received room content')
    async def history_ready():
        for _ in range(100):
            data=call('/api/session/history',{'session_id':sid},a)
            if data.get('messages'): return data
            await asyncio.sleep(.05)
        raise AssertionError('history persistence timeout')
    try:
        aa=await connect(a);bb=await connect(b);cc=await connect(c);cc2=await connect(c,1);dd=await connect(d)
        for ws in (aa,bb,cc,cc2): check.require((await join(ws))['type']=='ack','member WS authorization')
        check.require((await join(dd)).get('code')==403,'outsider WS join denied')
        request=pb.UpstreamMessageRequest(from_user_id=d['user_id'],auth_token=d['token'],scene='chatroom',group_id=room,client_msg_id=secrets.token_hex(12),content_json='{"content":{"text":"forbidden"}}')
        check.require(stub.SendUpstreamMessage(request,timeout=10).error.code==403,'gRPC nonmember send rejected with Agent disabled')
        check.require(stub.SyncMessages(pb.SyncMessagesRequest(user_id=d['user_id'],session_id=sid),timeout=10).error.code==403,'gRPC nonmember sync rejected')
        check.require(not any(m.session_id==sid for m in stub.SyncOffline(pb.SyncOfflineRequest(user_id=d['user_id'],limit=100),timeout=10).messages),'offline excludes nonmembers')
        body,ack=await send(aa,'ordinary room message');check.require(ack['type']=='accepted_ack','Kafka acceptance')
        for ws in (bb,cc,cc2): await receive(ws,lambda e:e.get('type')=='chatroom' and e.get('client_msg_id')==body['client_msg_id'])
        await receive(aa,lambda e:e.get('type')=='delivered_ack' and e.get('client_msg_id')==body['client_msg_id'])
        duplicate=await send(aa,'ordinary room message',body['client_msg_id']);check.require(duplicate[1]['msg_seq']==ack['msg_seq'],'idempotent sequence preserved')
        await history_ready();await denied_receive(dd)
        # One tab leaving must not unsubscribe the other connection.
        cc3=await connect(c);await join(cc3);await cc3.send(json.dumps({'type':'chatroom_leave','group_id':room}));await cc3.close()
        check.require(call('/api/session/history',{'session_id':sid},c)['messages'],'tab leave keeps durable membership')
        await cc.close();await cc2.close()
        offline,offline_ack=await send(aa,'offline member recovery');check.require(offline_ack['type']=='accepted_ack','offline member message accepted')
        for _ in range(100):
            if len(call('/api/session/history',{'session_id':sid},a)['messages'])>=2:break
            await asyncio.sleep(.05)
        cc=await connect(c);await receive(cc,lambda e:e.get('client_msg_id')==offline['client_msg_id'] and e.get('type')=='chatroom')
        await join(cc);cc2=await connect(c,1);await join(cc2)
        check.require(True,'reconnect receives offline persisted room message')
        call('/api/spaces/remove',{'room_id':room,'user_id':c['user_id']},b)
        for path,body_ in [('/api/session/history',{'session_id':sid}),('/api/chatroom/agent/task',{'room_id':room}),('/api/attachment/get',{'id':attachment['id']})]:call(path,body_,c,403)
        call('/api/spaces/accept',{'room_id':room},c,403)
        _,denied=await send(cc,'removed sender');check.require(denied.get('code')==403,'removed WS sender rejected immediately')
        check.require(stub.SyncMessages(pb.SyncMessagesRequest(user_id=c['user_id'],session_id=sid),timeout=10).error.code==403,'removed cursor sync rejected')
        check.require(not any(m.session_id==sid for m in stub.SyncOffline(pb.SyncOfflineRequest(user_id=c['user_id'],limit=100),timeout=10).messages),'removed offline sync excluded')
        for ws in (cc,cc2):
            if not any(e.get('type')=='room_revoked' for e in getattr(ws,'_space_test_events',[])):
                await receive(ws,lambda e:e.get('type')=='room_revoked',5)
        check.require(True,'both Comets revoke existing subscriptions')
        # Simulate a Kafka target snapshot created before removal arriving after it.
        for js in job_stubs:
            message=pb.ChatMessage(msg_id='queued-old',session_id=sid,msg_seq=999,sender_id=a['user_id'],msg_type='text',content_json='{"type":"chatroom","content":{"text":"must not leak"}}')
            push=pb.PushToCometRequest(message=message,request_id=secrets.token_hex(12),targets=[pb.FanoutTarget(user_id=c['user_id'])])
            result=js.PushToComet(push,metadata=(('x-spark-rpc-key',job_key),),timeout=10)
            check.require(result.delivered_count==0,'queued pre-removal target rejected at delivery')
        await denied_receive(cc);await denied_receive(cc2)
        check.require(sql(f"SELECT COUNT(*) FROM message_delivery_receipt WHERE user_id={c['user_id']} AND session_id='{sid}' AND msg_seq=999")=="0",'rejected delivery does not advance receipts')
        call('/api/spaces/leave',{'room_id':room},b)
        call('/api/spaces/members',{'room_id':room},b,403)
        public=call('/api/spaces/create',{'name':'public compatibility','visibility':'public'},a)['room_id']
        call('/api/chatroom/join',{'room_id':public,'user_id':d['user_id']},d)
        call('/api/chatroom/leave',{'room_id':public,'user_id':d['user_id']},d)
        call('/api/session/history',{'session_id':f'r_{public}'},d)
        call('/api/spaces/leave',{'room_id':public},d)
        call('/api/chatroom/join',{'room_id':public,'user_id':d['user_id']},d)
        check.require(True,'public join, page leave, formal leave and rejoin remain compatible')
        return {'room':room,'owner':a}
    finally:
        await asyncio.gather(*(s.close() for s in sockets),return_exceptions=True)

class AuthStub:
    def __init__(self,stub,key):self.stub=stub;self.key=key
    def __getattr__(self,name):
        return lambda *a,**kw:getattr(self.stub,name)(*a,metadata=(('x-spark-rpc-key',self.key),),**kw)


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--build',default='build-wsl');parser.add_argument('--out',required=True);parser.add_argument('--generic-agents',action='store_true');parser.add_argument('--real-runtimes',action='store_true');parser.add_argument('--browser',action='store_true');parser.add_argument('--browser-only',action='store_true');args=parser.parse_args()
    if args.browser_only:args.generic_agents=True;args.browser=True
    out=Path(args.out).resolve();out.mkdir(parents=True,exist_ok=True);build=Path(args.build).resolve()
    names={x:'spaces-'+secrets.token_hex(6)+'-'+x for x in ('mysql','redis','kafka')}
    ports={x:free_port() for x in ('mysql','redis','kafka','logic','http','ws','ws2','comet','comet2','jobm','cometm','cometm2')}
    processes={};logs=[];owned=[];server=None;native_fixture=None;router=None;check=Checks();result={'ok':False,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'working_tree':True}
    def docker(*args,**kw):return subprocess.run(['docker',*args],check=True,timeout=120,**kw)
    def sql(query,database='spark_push'):
        try:
            return docker('exec','-i',names['mysql'],'sh','-c','MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --protocol=TCP -h127.0.0.1 --default-character-set=utf8mb4 --raw -uroot -N -B '+database,input=query,text=True,capture_output=True).stdout.strip()
        except subprocess.CalledProcessError as exc:
            raise RuntimeError('owned test MySQL: '+exc.stderr[-1200:]) from None
    def wait(fn,seconds=90):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            if any(p.poll() is not None for p in processes.values()):raise RuntimeError('service exited; inspect private logs')
            try:
                if fn():return
            except Exception:pass
            time.sleep(.2)
        raise RuntimeError('readiness timeout')
    with tempfile.TemporaryDirectory(prefix='spark-spaces-e2e-') as tmp:
        private=Path(tmp);private.chmod(0o700)
        env=dict(os.environ,SPARK_PUSH_MYSQL_PASSWORD=secrets.token_hex(32),SPARK_PUSH_REDIS_PASSWORD='',SPARK_PUSH_HERMES_ENABLED='false',SPARK_PUSH_ROOM_AGENT_ENABLED='false',SPARK_PUSH_ADMIN_ACCOUNT='space_test_admin',SPARK_PUSH_ADMIN_PASSWORD=secrets.token_hex(32),SPARK_PUSH_HERMES_API_KEY=secrets.token_hex(32),SPARK_LOG_CONSOLE='0')
        for role in ('COMET','JOB','ADMIN'):env['SPARK_RPC_'+role+'_KEY']=secrets.token_hex(32)
        for key in ('SPARK_RPC_TLS_CERT','SPARK_RPC_TLS_KEY','SPARK_RPC_TLS_CA'):env.pop(key,None)
        for key in ('SPARK_PUSH_ADMIN_ACCOUNT','SPARK_PUSH_ADMIN_PASSWORD'):os.environ[key]=env[key]
        def start(name,service,config):
            process_dir=private/name;process_dir.mkdir();cfg=write_config(process_dir,service,config);log=open(out/(name+'.log'),'w');logs.append(log)
            binary=build/service/(service if service=='hermes_bridge' else service+'_server')
            processes[name]=subprocess.Popen([str(binary),'--config',str(cfg)],cwd=process_dir,env=env,stdout=log,stderr=subprocess.STDOUT)
        try:
            mysql_env=private/'mysql.env';mysql_env.write_text('MYSQL_ROOT_PASSWORD='+env['SPARK_PUSH_MYSQL_PASSWORD']+'\nMYSQL_ROOT_HOST=%\n');mysql_env.chmod(0o600)
            kafka_env=private/'kafka.env';kafka_env.write_text('\n'.join(['CLUSTER_ID='+base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip('='),'KAFKA_NODE_ID=1','KAFKA_PROCESS_ROLES=broker,controller','KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093',f"KAFKA_ADVERTISED_LISTENERS=INTERNAL://127.0.0.1:29092,EXTERNAL://127.0.0.1:{ports['kafka']}",'KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT','KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL','KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER','KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093','KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1','KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0'])+'\n')
            for name,image,port,extra in [('mysql','mysql:8.0',3306,['--env-file',str(mysql_env)]),('redis','redis:7.2-alpine',6379,[]),('kafka','apache/kafka:3.7.0',9092,['--env-file',str(kafka_env)])]:
                owned.append(names[name]);docker('run','-d','--name',names[name],'--label','spark-spaces-test=true','-p',f'127.0.0.1:{ports[name]}:{port}',*extra,image,stdout=subprocess.DEVNULL)
            wait(lambda:sql('SELECT 1','mysql')=='1')
            schema=(ROOT/'sql/schema.sql').read_text()
            # Upgrade an old-style room while preserving existing Agent payloads.
            old='\n'.join(line for line in schema.splitlines() if '`description` VARCHAR(2048)' not in line and '`visibility` VARCHAR(8)' not in line)
            sql(old,'mysql')
            sql("INSERT INTO user(id,account,password_hash) VALUES(50,'legacy-owner','test'); INSERT INTO im_group(id,name,owner_id,group_type) VALUES(50,'legacy public',50,1); INSERT INTO room_agent_task VALUES(50,7,JSON_OBJECT('task_id','keep-workspace','revision',7));")
            migration=(ROOT/'sql/migrations/20261031_collaboration_spaces.sql').read_text();sql(migration);sql(migration)
            check.require(sql('SELECT visibility FROM im_group WHERE id=50')=='public','migration keeps old room public')
            check.require(sql('SELECT role FROM group_member WHERE group_id=50 AND user_id=50')=='2','migration backfills owner')
            check.require(sql("SELECT JSON_UNQUOTE(JSON_EXTRACT(payload,'$.task_id')) FROM room_agent_task WHERE room_id=50")=='keep-workspace','rerun migration preserves Agent workspace')
            kafka=['exec',names['kafka'],'/opt/kafka/bin/kafka-topics.sh','--bootstrap-server','127.0.0.1:29092']
            wait(lambda:docker(*kafka,'--list',stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0)
            topics={scene:'spaces_'+scene for scene in ('single','group','broadcast','persist','ai_request','ai_delta','ai_reply')}
            for topic in list(topics.values())+[topics['persist']+'.dlq']:docker(*kafka,'--create','--topic',topic,'--partitions','1','--replication-factor','1',stdout=subprocess.DEVNULL)
            common={'mysql_host':'127.0.0.1','mysql_port':ports['mysql'],'mysql_user':'root','mysql_db':'spark_push','mysql_password':'${SPARK_PUSH_MYSQL_PASSWORD}','kafka_brokers':f"127.0.0.1:{ports['kafka']}",'kafka_consumer_group':'spaces-test'}
            common.update({'kafka_'+k+'_topic':v for k,v in topics.items()})
            logic=dict(common,listen_addr='127.0.0.1',internal_grpc_listen_addr='127.0.0.1',listen_port=ports['logic'],http_port=ports['http'],redis_port=ports['redis'],redis_db=0,hermes_enabled='false',room_agent_enabled='false',agent_bot_users='',attachment_dir=str(private/'attachments'))
            start('logic','logic',logic)
            for suffix in ('','2'):
                start('comet'+suffix,'comet',dict(listen_addr='127.0.0.1',internal_grpc_listen_addr='127.0.0.1',listen_port=ports['ws'+suffix],comet_grpc_port=ports['comet'+suffix],metrics_port=ports['cometm'+suffix],comet_id='comet-'+(suffix or '1'),logic_grpc_target=f"127.0.0.1:{ports['logic']}"))
            start('job','job',dict(common,comet_targets=f"comet-1=127.0.0.1:{ports['comet']},comet-2=127.0.0.1:{ports['comet2']}",metrics_port=ports['jobm']))
            base=f"http://127.0.0.1:{ports['http']}";urls=[f"ws://127.0.0.1:{ports[k]}" for k in ('ws','ws2')]
            wait(lambda:http(base,'/api/login',{})[0]>0)
            if protoc.main(['protoc','-I'+str(ROOT/'proto'),'--python_out='+tmp,'--grpc_python_out='+tmp,str(ROOT/'proto/spark_push.proto')])!=0:raise RuntimeError('protoc failed')
            sys.path.insert(0,tmp);pb=importlib.import_module('spark_push_pb2');rpc=importlib.import_module('spark_push_pb2_grpc')
            channel=grpc.insecure_channel(f"127.0.0.1:{ports['logic']}");stub=AuthStub(rpc.LogicServiceStub(channel),env['SPARK_RPC_COMET_KEY'])
            job_channels=[grpc.insecure_channel(f"127.0.0.1:{ports[k]}") for k in ('comet','comet2')];job_stubs=[rpc.CometServiceStub(c) for c in job_channels]
            for connection in [channel,*job_channels]:grpc.channel_ready_future(connection).result(timeout=15)
            time.sleep(4)
            if not args.browser_only:
                from logic_business_e2e import exercise as business_exercise
                before_business=len(check.names);asyncio.run(business_exercise(base,urls[0],stub,pb,check))
                result['legacy_business_checks']=len(check.names)-before_business
                fixture=asyncio.run(exercise(base,urls,stub,pb,check,job_stubs,env['SPARK_RPC_JOB_KEY'],sql))
            # Enable existing controlled @CANN suite on the same preserved DB.
            from room_agent_e2e import ControlledFactory,exercise as agent_exercise
            from agent_router import AgentRouter,AgentStore
            from pi_gateway import GatewayState,make_handler
            (private/'workspace').mkdir();factory=ControlledFactory(private/'workspace')
            entries=[{'id':'pi','name':'CANN test','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000001}]
            adapters={'pi':factory};public_factory=None;generic_ids=[]
            if args.generic_agents:
                from test_generic_room_agent import Factory
                public_factory=Factory()
                entries.append({'id':'public-pi','name':'Pi','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000401,'room_enabled':True,'room_policy':'read_only'})
                adapters['public-pi']=public_factory;generic_ids=['public-pi']
                if args.real_runtimes:
                    from hermes_room_runtime_fixture import HermesRoomRuntime
                    from hermes_native_adapter import HermesNativeAdapter
                    from room_agent import PublicRoomPiFactory
                    from pi_gateway import PiRpcClient
                    native_fixture=HermesRoomRuntime();entry=native_fixture.start();entries.append(entry)
                    adapters[entry['id']]=HermesNativeAdapter(entry,AgentStore(private/'agent-state'));generic_ids.append(entry['id'])
                    os.environ['SPARK_PUSH_ROOM_HERMES_TEST']='true';env['SPARK_PUSH_ROOM_HERMES_TEST']='true'
                    os.environ['SPARK_PUSH_PUBLIC_PI_ENV_FILE']=str(Path.home()/'.pi-spark-agent/.env')
                    os.environ['SPARK_PUSH_PUBLIC_PI_MODELS_FILE']=str(Path.home()/'.pi-spark-agent/models.json')
                    prototype=PiRpcClient([str(Path.home()/'.local/node/bin/pi'),'--provider',os.environ.get('SPARK_TEST_PI_PROVIDER','deepseek'),'--model',os.environ.get('SPARK_TEST_PI_MODEL','deepseek-flash')],ROOT,
                        {'PATH':str(Path.home()/'.local/node/bin')+os.pathsep+os.environ.get('PATH',''),'PI_CODING_AGENT_DIR':str(Path.home()/'.pi-spark-agent')},Path.home()/'.pi-spark-agent/sessions')
                    public_factory=PublicRoomPiFactory(prototype,private/'public-pi')
                else:
                    entries.append({'id':'second-pi','name':'Pi test 2','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000403,'room_enabled':True,'room_policy':'read_only'})
                    adapters['second-pi']=public_factory;generic_ids.append('second-pi')
                catalog=private/'room-catalog.json';catalog.write_text(json.dumps({'schema':1,'agents':entries}));env['SPARK_PUSH_ROOM_AGENT_CATALOG']=str(catalog)
                sql((ROOT/'sql/migrations/20261031_generic_room_agents.sql').read_text());sql((ROOT/'sql/migrations/20261031_generic_room_agents.sql').read_text())
            execution_calls=[]
            if args.generic_agents:
                # Observe real execution boundaries; these wrappers do not replace
                # runtime calls, model responses or native tool execution.
                original_scope=public_factory.for_scope
                def tracked_scope(scope):
                    turn=original_scope(scope);original_chat=turn.chat
                    def chat(*a,**kw):execution_calls.append(('pi',scope['request_id']));return original_chat(*a,**kw)
                    turn.chat=chat;return turn
                public_factory.for_scope=tracked_scope
                for entry in entries:
                    if entry['runtime']=='hermes_native':
                        native=adapters[entry['id']];admit=native._admit
                        def tracked_admit(*a,**kw):execution_calls.append(('hermes-native-post',a[0]));return admit(*a,**kw)
                        native._admit=tracked_admit
            router=AgentRouter(entries,adapters,AgentStore(private/'agent-state'));router.room_adapter=factory;router.public_room_adapter=public_factory
            server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(GatewayState(router,env['SPARK_PUSH_HERMES_API_KEY'],'pi-agent')));threading.Thread(target=server.serve_forever,daemon=True).start()
            env['SPARK_PUSH_AGENT_GATEWAY_PORT']=str(server.server_port)
            async def retry_authorization_outage():
                async with websockets.connect(urls[0]+'/ws?token='+fixture['owner']['token'],proxy=None) as socket:
                    await socket.send(json.dumps({'type':'chatroom_join','group_id':fixture['room']}))
                    await receive(socket,lambda e:e.get('op')=='chatroom_join' and e.get('type')=='ack')
                    old_logic=processes.pop('logic');old_logic.terminate();old_logic.wait(timeout=10)
                    message=pb.ChatMessage(msg_id='authorization-retry',session_id='broadcast',msg_type='broadcast',content_json='{"type":"broadcast","scope":"group","content":{"text":"retry after authorization recovery"}}')
                    push=pb.PushToCometRequest(message=message,request_id=secrets.token_hex(12),access_room_id=fixture['room'],targets=[pb.FanoutTarget(user_id=fixture['owner']['user_id'])])
                    unavailable=job_stubs[0].PushToComet(push,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                    check.require(unavailable.error.code==503 and unavailable.delivered_count==0,'authorization outage is retryable, no delivery')
                    room_push=pb.PushToCometRequest(message=pb.ChatMessage(msg_id='room-snapshot-retry',session_id=f"r_{fixture['room']}",msg_type='text',content_json='{"type":"chatroom","content":{"text":"snapshot retry"}}'),request_id=secrets.token_hex(12))
                    unavailable_room=job_stubs[0].PushToComet(room_push,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                    check.require(unavailable_room.error.code==503,'room snapshot authorization outage is retryable')
                    env.update(SPARK_PUSH_HERMES_ENABLED='true',SPARK_PUSH_ROOM_AGENT_ENABLED='true');logic.update(hermes_enabled='true',room_agent_enabled='true');start('logic-agent','logic',logic)
                    wait(lambda:http(base,'/api/login',{})[0]>0)
                    resumed=None
                    for _ in range(20):
                        resumed=job_stubs[0].PushToComet(push,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                        if resumed.error.code!=503:break
                        await asyncio.sleep(.2)
                    check.require(resumed.error.code==0 and resumed.delivered_count>0,f'same request ID retries after authorization recovers: code={resumed.error.code}, delivered={resumed.delivered_count}')
                    await receive(socket,lambda e:e.get('type')=='broadcast')
                    resumed_room=job_stubs[0].PushToComet(room_push,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                    check.require(resumed_room.error.code==0 and resumed_room.delivered_count>0,'room snapshot original ID retries after outage')
                    await receive(socket,lambda e:e.get('type')=='chatroom')
                    locked=subprocess.Popen(['docker','exec','-i',names['mysql'],'sh','-c','MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql --unbuffered -uroot -N -B spark_push'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    try:
                        locked.stdin.write("LOCK TABLES group_member WRITE; SELECT 'locked';\n");locked.stdin.flush()
                        check.require(locked.stdout.readline().strip()=='locked','isolated membership lookup lock acquired')
                        pending=pb.PushToCometRequest();pending.CopyFrom(room_push);pending.request_id=secrets.token_hex(12)
                        first=asyncio.create_task(asyncio.to_thread(job_stubs[0].PushToComet,pending,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10))
                        await asyncio.sleep(.2)
                        duplicate=await asyncio.to_thread(job_stubs[0].PushToComet,pending,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                        check.require(duplicate.error.code==503,'concurrent fallback cannot acknowledge an unfinished push')
                        failed=await first;check.require(failed.error.code==503,'original authorization timeout remains retryable')
                    finally:
                        locked.stdin.write('UNLOCK TABLES;\n');locked.stdin.close();locked.wait(timeout=10)
                    retried=job_stubs[0].PushToComet(pending,metadata=(('x-spark-rpc-key',env['SPARK_RPC_JOB_KEY']),),timeout=10)
                    check.require(retried.error.code==0 and retried.delivered_count>0,'unfinished push retries after DB recovery')
                    await receive(socket,lambda e:e.get('type')=='chatroom')
            if args.browser_only:
                process=processes.pop('logic');process.terminate();process.wait(timeout=10)
                env.update(SPARK_PUSH_HERMES_ENABLED='true',SPARK_PUSH_ROOM_AGENT_ENABLED='true');logic.update(hermes_enabled='true',room_agent_enabled='true');start('logic-agent','logic',logic)
            else:asyncio.run(retry_authorization_outage())
            bridge_config=dict(kafka_brokers=common['kafka_brokers'],kafka_consumer_group='spaces-bridge',kafka_request_topic=topics['ai_request'],kafka_delta_topic=topics['ai_delta'],kafka_reply_topic=topics['ai_reply'],hermes_base_url=f'http://127.0.0.1:{server.server_port}/v1',hermes_api_key='${SPARK_PUSH_HERMES_API_KEY}',inference_backend='pi',hermes_streaming='true',processing_workers=2)
            start('bridge','hermes_bridge',bridge_config)
            wait(lambda:http(base,'/api/login',{})[0]>0);time.sleep(5)
            if not args.browser_only:result['agent']=asyncio.run(agent_exercise(base,urls[0],factory,check,stub,pb))
            if args.generic_agents and not args.browser_only:
                from generic_room_agent_e2e import exercise as generic_exercise
                async def restart_logic():
                    process=processes.pop('logic-agent');process.terminate();process.wait(timeout=10)
                    start('logic-agent-restart','logic',logic);wait(lambda:http(base,'/api/login',{})[0]>0)
                    await asyncio.sleep(4)
                async def broker_replay(request_ids):
                    # Lose Bridge's in-memory cache, then redeliver the exact
                    # authoritative requests through real Kafka.
                    before_executions=len(execution_calls)
                    old=processes.pop('bridge');old.terminate();old.wait(timeout=10)
                    values=[json.loads(sql("SELECT payload FROM room_agent_request WHERE request_id='"+rid+"'")) for rid in request_ids]
                    previous=[sql("SELECT result FROM room_agent_request WHERE request_id='"+rid+"'") for rid in request_ids]
                    content=''.join(v['session_id']+':'+v['agent_id']+'\t'+json.dumps(v)+'\n' for v in values)
                    await asyncio.to_thread(docker,'exec','-i',names['kafka'],'/opt/kafka/bin/kafka-console-producer.sh','--bootstrap-server','127.0.0.1:29092','--topic',topics['ai_request'],'--property','parse.key=true','--property','key.separator=\t',input=content,text=True,capture_output=True)
                    start('bridge-restarted','hermes_bridge',bridge_config)
                    deadline=time.monotonic()+45
                    while time.monotonic()<deadline:
                        state=await asyncio.to_thread(docker,'exec',names['kafka'],'/opt/kafka/bin/kafka-consumer-groups.sh','--bootstrap-server','127.0.0.1:29092','--group','spaces-bridge','--describe',text=True,capture_output=True)
                        rows=[line.split() for line in state.stdout.splitlines() if topics['ai_request'] in line]
                        if rows and all(len(r)>5 and r[3]==r[4] and r[5]=='0' for r in rows):break
                        await asyncio.sleep(.5)
                    else:raise AssertionError('Bridge replay did not commit Kafka offsets')
                    await asyncio.sleep(2)
                    check.require(previous==[sql("SELECT result FROM room_agent_request WHERE request_id='"+rid+"'") for rid in request_ids],'Bridge restart/redelivery preserves first durable result receipts')
                    check.require(len(execution_calls)==before_executions,'actual Pi chat / Native POST count unchanged after Kafka redelivery and Bridge restart')
                    return True
                result['generic']=asyncio.run(generic_exercise(base,urls,check,router,None if args.real_runtimes else public_factory,generic_ids,restart_logic,broker_replay,lambda:len(execution_calls),lambda rid:json.loads(sql("SELECT payload FROM room_agent_request WHERE request_id='"+rid+"'"))))
            if args.browser:
                browser_env=dict(os.environ,SPARK_TEST_HTTP=base,SPARK_TEST_WS=urls[0],SPARK_TEST_AGENTS=json.dumps(generic_ids))
                browser_log=subprocess.run(['node',str(ROOT/'tests/room_agents_live_browser_test.cjs')],env=browser_env,text=True,capture_output=True,timeout=240)
                (out/'browser.log').write_text(browser_log.stdout+browser_log.stderr)
                check.require(browser_log.returncode==0,'three independent browser contexts: actual auth, invitation, messages, Agents and tasks')
                result['browser']='real HTTP/WS; '+('real Pi/Hermes' if args.real_runtimes else 'controlled model')
            result['runtime_executions']={kind:sum(c[0]==kind for c in execution_calls) for kind in ('pi','hermes-native-post')}
            result['ok']=True
        except Exception as error:
            result['error']=f'{type(error).__name__}: {error}'
            raise
        finally:
            for process in reversed(list(processes.values())):
                if process.poll() is None:process.terminate()
            for process in processes.values():
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.wait()
            if server:server.shutdown();server.server_close()
            if router:router.stop()
            if native_fixture:native_fixture.close()
            for log in logs:log.close()
            for source in private.glob('*/logs/*.log*'):
                target=out/(source.parent.parent.name+'-'+source.name);target.write_bytes(source.read_bytes());target.chmod(0o600)
            for name in owned:subprocess.run(['docker','rm','-f',name],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            result['checks']=check.names;result['checks_passed']=len(check.names)
            (out/'spaces-e2e.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({k:v for k,v in result.items() if k!='checks'},ensure_ascii=False))
if __name__=='__main__':main()
