#!/usr/bin/env python3
"""Opt-in real Compose, public launchers, Pi and Windows Hermes model E2E.

Three independent HTTP/WS clients, not browser acceptance or OS sandbox proof.
Uses the user's explicitly authorized Windows default MODEL access token only.
Owns disposable Compose volumes and fresh runtime homes; never alters private
Agent configuration. Requires Python 3.11, websockets, PyYAML and local runtimes.
"""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
import websockets

from connector_deployment_e2e import free_port,http,ROOT
from public_runtime_fixture import PublicAgentGateway,PublicHermesRuntime
sys.path.insert(0,str(ROOT/'cannbot/scripts'))
from agent_router import AgentStore
from hermes_native_adapter import HermesNativeAdapter


async def until(fn,timeout=180):
    deadline=time.monotonic()+timeout
    while time.monotonic()<deadline:
        value=fn()
        if value:return value
        await asyncio.sleep(.1)
    raise AssertionError('public Runtime acceptance timed out')


def comparable_metadata(value):
    value=json.loads(json.dumps(value))
    envelope=value.get('agent_event',{}).get('envelope')
    if envelope:
        material=(envelope['request_id']+'\0'+str(envelope['sequence'])).encode()
        def fnv(seed):
            for byte in material:seed=((seed^byte)*1099511628211)&((1<<64)-1)
            return seed
        assert envelope['event_id']==f'evt_{fnv(1469598103934665603):016x}{fnv(1099511628211):016x}'
        for key in ('sequence','event_id','replayed','created_at_ms'):envelope.pop(key,None)
    return value


class Client:
    def __init__(self,base,user):self.base,self.user=base,user;self.events=[]
    async def connect(self,room):
        self.ws=await websockets.connect(self.base.replace('http','ws',1)+'/ws?token='+self.user['token'],proxy=None,max_size=2**22)
        async def read():
            try:
                async for raw in self.ws:self.events.append(json.loads(raw))
            except websockets.ConnectionClosed:pass
        self.reader=asyncio.create_task(read())
        await self.ws.send(json.dumps({'type':'chatroom_join','group_id':room}))
        await until(lambda:next((e for e in self.events if e.get('op')=='chatroom_join' and e.get('type')=='ack'),None),30)
        return self
    async def send(self,room,text,agent=None,cid=None):
        cid=cid or secrets.token_hex(16)
        value={'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':text}}
        if agent:value['agent_mentions']=[agent]
        start=len(self.events)
        await self.ws.send(json.dumps(value))
        event=await until(lambda:next((e for e in self.events[start:] if e.get('client_msg_id')==cid and e.get('type') in ('accepted_ack','error')),None),30)
        return value,event
    def final(self,rid):return next((e for e in self.events if e.get('type')=='chatroom' and e.get('request_id')==rid),None)
    async def close(self):
        await self.ws.close();await self.reader


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='spark-push:public-runtime')
    parser.add_argument('--out',required=True,type=Path)
    args=parser.parse_args();output=args.out.resolve()
    report={'ok':False,'mode':'real Compose + public launchers + actual Pi/Hermes models; three HTTP/WS clients, not browsers',
        'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'checks':[]}
    def check(label):report['checks'].append(label);print(label,flush=True)
    with tempfile.TemporaryDirectory(prefix='spark-public-e2e-',dir='/mnt/f') as temporary:
        root=Path(temporary);root.chmod(0o755)
        source=root/'source';(source/'logic').mkdir(parents=True)
        shutil.copyfile(ROOT/'logic/message_reservation.cpp',source/'logic/message_reservation.cpp')
        nonce=secrets.token_hex(12)
        (source/'probe.cpp').write_text('// Acceptance nonce: '+nonce+'\nint spark_workspace_probe() { return 17; }\n')
        config=root/'agents';config.mkdir(mode=0o755)
        projects=config/'projects.json'
        projects.write_text(json.dumps({'schema':1,'projects':[{'id':'spark-push','name':'Spark Push',
            'root':str(source),'paths':['probe.cpp','logic']}]}))
        registry=config/'agents.json'
        catalog=json.loads((ROOT/'deploy/agents/agents.json.example').read_text())
        keys={'SPARK_PUSH_HERMES_API_KEY':secrets.token_hex(32),'SPARK_PUSH_WORKSPACE_AUTH_KEY':secrets.token_hex(32)}
        keyfile=root/'connector-keys.json';keyfile.write_text(json.dumps(keys));keyfile.chmod(0o600)
        envfile=root/'.env';envfile.write_text('');envfile.chmod(0o600)
        port,authport,gatewayport=free_port(),free_port(),free_port()
        host=subprocess.check_output(['hostname','-I'],text=True).split()[0]
        authority=f'http://{host}:{authport}';base=f'http://127.0.0.1:{port}'
        environment={k:v for k,v in os.environ.items() if not k.startswith(('SPARK_','COMPOSE_','HERMES_','PI_','KERNEL_'))}
        environment.update(keys,SPARK_PUSH_IMAGE=args.image,SPARK_PUSH_PORT=str(port),SPARK_PUSH_BIND='127.0.0.1',
            SPARK_PUSH_AGENT_CONFIG_DIR=str(config),SPARK_PUSH_AGENT_AUTH_PORT=str(authport),SPARK_PUSH_AGENT_AUTH_BIND=host,
            SPARK_PUSH_AGENT_GATEWAY_ORIGIN=f'http://{host}:{gatewayport}')
        options=['--env-file',str(envfile),'-p','spark-public-'+secrets.token_hex(6),'-f',str(ROOT/'docker-compose.yml'),
                 '-f',str(ROOT/'docker-compose.agents.yml'),'--profile','agents']
        def compose(*arguments,input=None):
            result=subprocess.run(['docker','compose',*options,*arguments],cwd=ROOT,env=environment,input=input,
                                  stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=300)
            if arguments[0] in {'up','down','logs','restart'}:
                fd=os.open(output.with_suffix('.compose.private.log'),os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
                with os.fdopen(fd,'w') as log:log.write(result.stdout)
            if result.returncode:raise RuntimeError('owned Compose operation failed: '+arguments[0])
            return result.stdout
        def sql(query):
            # Read-only evidence from this disposable database. Runtime inputs
            # are still created exclusively through authenticated user APIs.
            return compose('exec','-T','mysql','sh','-c',
                'MYSQL_PWD="$(cat /run/spark-admin/mysql-root-password)" mysql -uroot --default-character-set=utf8mb4 --batch --raw --skip-column-names spark_push',input=query+';\n').strip()
        def call(path,body,user=None,expected=0):
            response=json.loads(http(base,path,body,user['token'] if user else None)[1])
            assert response.get('code')==expected,(path,response.get('code'),expected)
            return response.get('data',{})
        native=PublicHermesRuntime(workspace={'authority':authority,'key':keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'],
                                              'snapshots':str(root/'snapshots')})
        gateway=PublicAgentGateway(root/'gateway',registry,projects,keyfile,authority,root/'snapshots',gatewayport)
        adapter=None;started=False
        try:
            entry=native.start();catalog['agents'][1]=entry;registry.write_text(json.dumps(catalog))
            gateway.start()
            adapter=HermesNativeAdapter(entry,AgentStore(root/'native-probe'))
            caps=adapter._capabilities(15)
            assert caps['features']['spark_public_profile_v1']['agent_id']=='public-hermes'
            assert 'chat_completions' not in caps['features']
            probes=[(method,path,{} if method=='POST' else None,404) for method,path in [('POST','/v1/chat/completions'),('POST','/v1/responses'),('GET','/api/sessions'),
                                ('GET','/p/default/v1/capabilities'),('GET','/p/technical/v1/health'),
                                ('POST','/v1/browser-control/register'),('GET','/health/detailed')]]
            probes.append(('POST','/api/sessions',{'id':'private','toolsets':['terminal']},400))
            # Call Native directly: the normal relay also has a whitelist,
            # whose rejection would not prove Native's own HTTP policy.
            probe_code='''import sys,json,urllib.request,urllib.error
value=json.load(sys.stdin);opener=urllib.request.build_opener(urllib.request.ProxyHandler({}));codes=[]
for method,path,body,expected in value['probes']:
 request=urllib.request.Request(value['base']+path,None if body is None else json.dumps(body).encode(),{'Content-Type':'application/json','Authorization':'Bearer '+value['key']},method=method)
 try:
  with opener.open(request,timeout=15) as response:codes.append(response.status)
 except urllib.error.HTTPError as error:codes.append(error.code)
print(json.dumps(codes))
'''
            codes=json.loads(subprocess.check_output([entry['windows_python'],'-c',probe_code],text=True,
                input=json.dumps({'base':entry['base_url'].removesuffix('/v1'),'key':adapter.key,'probes':probes})))
            assert codes==[row[3] for row in probes],codes
            check('formal Native capabilities and HTTP whitelist deny chat/profile/tool/session bypasses')
            started=True;compose('up','-d','--wait','--wait-timeout','180','--pull','never')
            store=AgentStore(root/'gateway/service/router')
            def rid(source,agent):return 'ra_'+hashlib.sha256(json.dumps(['room-agent-v1',source,agent],separators=(',',':')).encode()).hexdigest()
            def history(user,room):return call('/api/session/history',{'session_id':f'r_{room}','limit':100},user)['messages']
            async def exercise():
                nonlocal adapter
                users=[];clients=[]
                async def offsets(topic):
                    values=await asyncio.to_thread(compose,'exec','-T','kafka','/opt/kafka/bin/kafka-get-offsets.sh',
                        '--bootstrap-server','kafka:29092','--topic',topic)
                    return {int(line.split(':')[1]):int(line.split(':')[2]) for line in values.strip().splitlines()}
                async def settled(topic,group,target):
                    deadline=time.monotonic()+60
                    while time.monotonic()<deadline:
                        state=await asyncio.to_thread(compose,'exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh',
                            '--bootstrap-server','kafka:29092','--group',group,'--describe')
                        rows=[line.split() for line in state.splitlines() if topic in line]
                        done={int(row[2]):int(row[3]) if row[3].isdigit() else -1 for row in rows if len(row)>5}
                        # An unused partition has no committed offset ('-').
                        # Every partition containing a record must cross the
                        # captured end offset; empty partitions need no commit.
                        if set(done)==set(target) and all(n==0 or done[p]>=n for p,n in target.items()):return done
                        await asyncio.sleep(.5)
                    raise AssertionError(topic+' consumer offsets did not settle')
                try:
                    for name in ('A','B','C','Outsider'):
                        credentials={'account':'public_'+secrets.token_hex(8),'name':name,'password':secrets.token_hex(24)}
                        users.append({**call('/api/register',credentials),'credentials':credentials})
                    a,b,c,d=users
                    room=call('/api/spaces/create',{'name':'真实公共 Agent 代码协作'},a)['room_id']
                    for member in (b,c):
                        call('/api/spaces/invite',{'room_id':room,'user_id':member['user_id']},a)
                        call('/api/spaces/accept',{'room_id':room},member)
                    call('/api/chatroom/join',{'room_id':room,'user_id':d['user_id']},d,403)
                    for agent in ('public-pi','public-hermes'):call('/api/chatroom/agents/install',{'room_id':room,'agent_id':agent},a)
                    call('/api/chatroom/workspace/set',{'room_id':room,'project_id':'spark-push','generation':0},a)
                    call('/api/chatroom/workspace/set',{'room_id':room,'project_id':'','generation':1},b,403)
                    clients.extend([await Client(base,u).connect(room) for u in (a,b,c)])
                    normal=await asyncio.gather(*(client.send(room,'Kafka accepted_ack confirms broker persistence, not SQL or reading.') for client in clients))
                    for _,ack in normal:assert ack['type']=='accepted_ack'
                    for client in clients:
                        await until(lambda:all(any(e.get('msg_id')==ack['msg_id'] and e.get('type')=='chatroom' for e in client.events) for _,ack in normal))
                    assert call('/api/chatroom/agents/list',{'room_id':room},a)['requests']==[]
                    check('three independent users exchange real messages; normal chat does not invoke models')
                    prompt=('Use your read tool to read PROJECT_FILES.txt, probe.cpp and logic/message_reservation.cpp. '
                            'Report the EXACT acceptance nonce, cite MySqlMessageReservations::Reserve with one concrete review finding. '
                            'Then summarize the Kafka discussion with a task title and two suggested steps. Do not run shell or claim tests passed.')
                    requests=[];payloads=[];finals=[]
                    for client,agent in zip(clients,('public-pi','public-hermes')):
                        payload,ack=await client.send(room,prompt,agent);assert ack['type']=='accepted_ack'
                        request=rid(ack['msg_id'],agent);requests.append(request);payloads.append(payload)
                        for watcher in clients:await until(lambda:watcher.final(request),240)
                        answer=clients[2].final(request);finals.append(answer)
                        assert answer.get('agent_status')=='completed' and not answer.get('content',{}).get('error'),(agent,answer.get('agent_status'))
                        assert nonce in json.dumps(answer) and 'Reserve' in json.dumps(answer)
                        assert answer['actor_user_id']==client.user['user_id'] and answer['agent_id']==agent
                        turn=store.get_turn(request)
                        assert turn['status']=='completed' and turn['metadata']['project_snapshot']['file_count']>=2
                        check(agent+' reads the real nonce and C++ file; all three clients see its attributed durable final')
                    turns=[store.get_turn(request) for request in requests]
                    await until(lambda:store.get('hermes-native:public-hermes:run',requests[1]).get('delivery_state')=='broker_confirmed',30)
                    for topic,group in [('ai_request','spark_push_hermes_bridge'),('ai_reply','spark_push_group_hermes_reply')]:
                        await settled(topic,group,await offsets(topic))
                    for topic in ('ai_request.dlq','ai_reply.dlq'):assert sum((await offsets(topic)).values())==0
                    check('original Native room broker receipt succeeds and both consumers commit without dead letters')
                    assert len({t['metadata']['agent_route']['backend_session_ref'] for t in turns})==2
                    assert all(t['metadata']['agent_route']['conversation_id']==f'r_{room}' for t in turns)
                    assert any(e.get('type')=='hermes_delta' for e in clients[2].events)
                    # Native transcripts prove actual tool outputs rather than
                    # inferring execution solely from an assistant statement.
                    native_db=native.root/'service/state.db'
                    def transcript_state():
                        with sqlite3.connect(f'file:{native_db}?mode=ro',uri=True) as db:
                            messages=db.execute('SELECT * FROM messages ORDER BY id').fetchall()
                            sessions=db.execute('SELECT id FROM sessions ORDER BY id').fetchall()
                        pi={str(p.relative_to(root/'gateway/service/rooms')):hashlib.sha256(p.read_bytes()).hexdigest()
                            for p in (root/'gateway/service/rooms').rglob('*.jsonl')}
                        return {'hermes_messages':len(messages),'hermes_sessions':len(sessions),
                            'hermes_transcript_sha256':hashlib.sha256(json.dumps([messages,sessions],default=str).encode()).hexdigest(),
                            'pi_sessions':pi}
                    transcripts=transcript_state()
                    with sqlite3.connect(f'file:{native_db}?mode=ro',uri=True) as db:
                        native_tools=db.execute("SELECT content FROM messages WHERE role='tool'").fetchall()
                    assert any(nonce in str(row[0]) for row in native_tools)
                    assert any('MySqlMessageReservations::Reserve' in str(row[0]) for row in native_tools)
                    pi_tools=[]
                    for path in (root/'gateway/service/rooms').rglob('*.jsonl'):
                        for line in path.read_text().splitlines():
                            event=json.loads(line);message=event.get('message',{})
                            if message.get('role')=='toolResult':pi_tools.append(message)
                    assert any(nonce in json.dumps(row) for row in pi_tools)
                    assert any('MySqlMessageReservations::Reserve' in json.dumps(row) for row in pi_tools)
                    report['tool_receipts']={'pi_results':len(pi_tools),'hermes_results':len(native_tools),
                        'source_sha256':hashlib.sha256((source/'logic/message_reservation.cpp').read_bytes()).hexdigest()}
                    check('real native tool transcripts confirm file contents; Agents use separate sessions and share one IM timeline')
                    owned_files=[native.root/'service'/name for name in ('.spark-public-runtime.json','config.yaml','auth.json')]
                    owned_before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in owned_files}
                    adapter.stop()
                    await asyncio.to_thread(native.stop_process);await asyncio.to_thread(native.launch)
                    adapter=HermesNativeAdapter(entry,AgentStore(root/'native-probe'))
                    assert adapter._capabilities(15)['features']['spark_public_profile_v1']==caps['features']['spark_public_profile_v1']
                    assert owned_before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in owned_files}
                    check('Native process restart reuses the owned Profile without rewriting config, model auth or identity')
                    await asyncio.to_thread(native.stop_process)
                    native_port=int(entry['base_url'].split(':')[-1].split('/')[0])
                    offline=subprocess.check_output([entry['windows_python'],'-c',
                        'import socket,sys;s=socket.socket();s.settimeout(2);print(s.connect_ex(("127.0.0.1",int(sys.argv[1]))));s.close()',str(native_port)],text=True)
                    assert int(offline.strip())!=0
                    report['native_offline_before_replay']=True
                    task=call('/api/chatroom/tasks/create',{'room_id':room,'request_id':requests[0],
                        'title':'Review Kafka reservation','description':'Review the Agent analysis',
                        'steps':['Inspect duplicate reservation','Check outbox recovery']},a)
                    task=call('/api/chatroom/tasks/update',{'room_id':room,'task_id':task['task_id'],
                        'revision':1,'claim':True,'status':'running','summary':'C takes over the code review'},c)
                    assert call('/api/chatroom/tasks/list',{'room_id':room},b)['tasks'][0]['summary']==task['summary']
                    for client,payload in zip(clients,payloads):
                        _,ack=await client.send(room,payload['content']['text'],payload['agent_mentions'][0],payload['client_msg_id'])
                        assert ack['type']=='accepted_ack'
                    await asyncio.sleep(2)
                    assert turns==[store.get_turn(request) for request in requests]
                    check('tasks and handoff are persisted; duplicate client messages preserve completed execution records')
                    before=[json.loads(sql("SELECT result FROM room_agent_request WHERE request_id='"+r+"'")) for r in requests]
                    originals=[json.loads(sql("SELECT payload FROM room_agent_request WHERE request_id='"+r+"'")) for r in requests]
                    publications=[json.loads(sql("SELECT reply FROM room_agent_publication WHERE request_id='"+r+"'")) for r in requests]
                    history_before=history(c,room)
                    publication_hashes=[sql("SELECT input_digest FROM room_agent_publication WHERE request_id='"+r+"'") for r in requests]
                    request_before=await offsets('ai_request');reply_before=await offsets('ai_reply')
                    call('/api/spaces/remove',{'room_id':room,'user_id':b['user_id']},a)
                    _,denied=await clients[1].send(room,'forbidden','public-hermes');assert denied.get('code')==403
                    call('/api/session/history',{'session_id':f'r_{room}'},b,403)
                    call('/api/chatroom/agents/list',{'room_id':room},b,403)
                    # Disable both entries, restart Connector and Bridge, then
                    # replay exact Kafka inputs after member/workspace revoke.
                    call('/api/chatroom/workspace/set',{'room_id':room,'project_id':'','generation':1},a)
                    for disabled in catalog['agents']:disabled['room_enabled']=False
                    registry.write_text(json.dumps(catalog))
                    await asyncio.to_thread(gateway.stop);await asyncio.to_thread(gateway.start)
                    await asyncio.to_thread(compose,'restart','agent-bridge')
                    content=''.join(v['session_id']+':'+v['agent_id']+'\t'+json.dumps(v)+'\n' for v in originals)
                    await asyncio.to_thread(compose,'exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh',
                        '--bootstrap-server','kafka:29092','--topic','ai_request','--property','parse.key=true','--property','key.separator=\t',input=content)
                    request_after=await offsets('ai_request')
                    assert sum(request_after.values())==sum(request_before.values())+2
                    committed=await settled('ai_request','spark_push_hermes_bridge',request_after)
                    reply_after=await offsets('ai_reply')
                    assert sum(reply_after.values())==sum(reply_before.values())+2
                    wire=await asyncio.to_thread(compose,'exec','-T','kafka','/opt/kafka/bin/kafka-console-consumer.sh',
                        '--bootstrap-server','kafka:29092','--topic','ai_reply','--from-beginning',
                        '--max-messages',str(sum(reply_after.values())),'--timeout-ms','10000')
                    replies=[json.loads(line) for line in wire.splitlines() if line.startswith('{')]
                    diagnostic=output.with_suffix('.replies.private.json')
                    fd=os.open(diagnostic,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
                    with os.fdopen(fd,'w') as stream:json.dump({'saved':publications,'wire':replies},stream,ensure_ascii=False)
                    stable=('request_id','session_id','user_id','bot_user_id','client_msg_id','collaboration','ok','text','citations')
                    for request,original in zip(requests,publications):
                        matching=[v for v in replies if v.get('request_id')==request]
                        assert len(matching)==2
                        assert all({k:v.get(k) for k in stable}=={k:original.get(k) for k in stable} for v in matching)
                        assert all(comparable_metadata(v.get('response_metadata',{}))==
                                   comparable_metadata(original.get('response_metadata',{})) for v in matching)
                    report['kafka_replay']={'request_before':request_before,'request_after':request_after,'committed':committed,
                        'reply_before':reply_before,'reply_after':reply_after,'exact_results_replayed':2}
                    # Logic must consume both replay replies, not merely leave
                    # them queued while the old history still looks correct.
                    done=await settled('ai_reply','spark_push_group_hermes_reply',reply_after)
                    report['kafka_replay']['reply_committed']=done
                    # Independently redeliver the original ai_reply events.
                    content=''.join(v['request_id']+'\t'+json.dumps(v)+'\n' for v in publications)
                    await asyncio.to_thread(compose,'exec','-T','kafka','/opt/kafka/bin/kafka-console-producer.sh',
                        '--bootstrap-server','kafka:29092','--topic','ai_reply','--property','parse.key=true','--property','key.separator=\t',input=content)
                    redelivered=await offsets('ai_reply')
                    assert sum(redelivered.values())==sum(reply_after.values())+2
                    report['kafka_replay']['independent_reply_redelivery']=redelivered
                    report['kafka_replay']['redelivery_committed']=await settled('ai_reply','spark_push_group_hermes_reply',redelivered)
                    assert before==[json.loads(sql("SELECT result FROM room_agent_request WHERE request_id='"+r+"'")) for r in requests]
                    assert publications==[json.loads(sql("SELECT reply FROM room_agent_publication WHERE request_id='"+r+"'")) for r in requests]
                    assert publication_hashes==[sql("SELECT input_digest FROM room_agent_publication WHERE request_id='"+r+"'") for r in requests]
                    assert history_before==history(c,room)
                    assert turns==[store.get_turn(r) for r in requests]
                    with sqlite3.connect(f'file:{native_db}?mode=ro',uri=True) as db:
                        assert len(native_tools)==db.execute("SELECT COUNT(*) FROM messages WHERE role='tool'").fetchone()[0]
                    replay_pi=[]
                    for path in (root/'gateway/service/rooms').rglob('*.jsonl'):
                        replay_pi.extend(json.loads(line)['message'] for line in path.read_text().splitlines()
                            if json.loads(line).get('message',{}).get('role')=='toolResult')
                    assert len(replay_pi)==len(pi_tools)
                    assert transcripts==transcript_state()
                    report['transcripts_unchanged']={'pi_sessions':len(transcripts['pi_sessions']),
                        'hermes_messages':transcripts['hermes_messages'],'hermes_sessions':transcripts['hermes_sessions']}
                    for topic in ('ai_request.dlq','ai_reply.dlq'):
                        assert sum((await offsets(topic)).values())==0,topic
                    for request in requests:assert sum(request in json.dumps(m) for m in history(c,room))==1
                    check('Kafka redelivery after revoke, catalog disable and Connector/Bridge restart preserves one original result')
                    for client in clients:await client.close()
                    clients.clear()
                    await asyncio.to_thread(compose,'down','--remove-orphans')
                    await asyncio.to_thread(compose,'up','-d','--wait','--wait-timeout','180','--pull','never')
                    for user in (a,c):
                        user['token']=call('/api/login',user['credentials'])['token']
                        rows=history(user,room)
                        for request in requests:assert sum(request in json.dumps(m) for m in rows)==1
                        assert call('/api/chatroom/tasks/list',{'room_id':room},user)['tasks'][0]['summary']==task['summary']
                    b['token']=call('/api/login',b['credentials'])['token']
                    call('/api/session/history',{'session_id':f'r_{room}'},b,403)
                    check('full Compose restart and fresh login retain final messages, claimed task and membership revocation')
                finally:
                    await asyncio.gather(*(client.close() for client in clients),return_exceptions=True)
            asyncio.run(exercise());report['ok']=True
        finally:
            if adapter:adapter.stop()
            gateway.stop()
            if native.log:native.log.flush()
            for source_path,suffix in [(native.root/'runtime.log','.native.private.log'),(root/'gateway/gateway.log','.gateway.private.log')]:
                if source_path.exists():
                    target=output.with_suffix(suffix);shutil.copyfile(source_path,target);target.chmod(0o600)
            native.close()
            if started:
                try:compose('logs','--no-color','--tail','150','logic','agent-bridge')
                finally:compose('down','-v','--remove-orphans')
            report['limitations']=['not browser acceptance','Windows shared storage does not prove OS read-only mounts',
                'CANN/private Agent regression and unknown-run failure injection are separate tests']
            output.write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({'ok':report['ok'],'checks':len(report['checks'])}),flush=True)


if __name__=='__main__':main()
