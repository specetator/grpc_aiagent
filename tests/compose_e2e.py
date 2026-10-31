#!/usr/bin/env python3
"""Real release Compose stack, same-origin HTTP/WS and normal restart recovery.

Uses owned project/volumes and a copied example environment. No host C++ tools,
SQL mutations, model credentials or internal RPC access are used by the test.
Python 3.11 with websockets is needed only by this acceptance harness.
"""
import argparse
import asyncio
import base64
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import websockets

ROOT = Path(__file__).resolve().parents[1]

def http(base, path, data=None, token=None):
    headers={'Content-Type':'application/json'}
    if token: headers['Authorization']='Bearer '+token
    request=urllib.request.Request(base+path,None if data is None else json.dumps(data).encode(),headers)
    try:
        with urllib.request.urlopen(request,timeout=15) as response: return response.read()
    except urllib.error.HTTPError as error:
        return error.read()

async def exercise(base, compose, check):
    def call(path,body,user=None,expected=0):
        result=json.loads(http(base,path,body,user['token'] if user else None))
        if result.get('code')!=expected: raise AssertionError(path+': '+str(result.get('code'))+' '+result.get('message',''))
        return result.get('data',{})
    credentials=[{'account':'release_'+secrets.token_hex(8),'password':secrets.token_hex(24),'name':'Team '+str(i)} for i in range(11)]
    users=[call('/api/register',identity) for identity in credentials]
    a,b,c=users[:3]
    room=call('/api/spaces/create',{'name':'项目开发组','description':'Compose acceptance'},a)['room_id']
    for user in users[1:10]:
        call('/api/spaces/invite',{'room_id':room,'user_id':user['user_id']},a)
        call('/api/spaces/accept',{'room_id':room},user)
    call('/api/chatroom/join',{'room_id':room,'user_id':users[-1]['user_id']},users[-1],403)
    call('/api/session/history',{'session_id':f'r_{room}'},users[-1],403)
    check('private space invites and outsider rejection through the public gateway')
    sockets=[];tasks=[];events=[]
    async def connect(user):
        ws=await websockets.connect(base.replace('http','ws',1)+'/ws?token='+user['token'],proxy=None)
        inbox=[];sockets.append(ws);events.append(inbox)
        async def read():
            try:
                async for raw in ws: inbox.append(json.loads(raw))
            except websockets.ConnectionClosed: pass
        tasks.append(asyncio.create_task(read()))
        return ws,inbox
    async def wait(inbox,predicate,start=0):
        async with asyncio.timeout(30):
            while True:
                for item in inbox[start:]:
                    if predicate(item):return item
                await asyncio.sleep(.02)
    async def send(ws,body,inbox):
        start=len(inbox)
        await ws.send(json.dumps(body))
        ack=await wait(inbox,lambda e:e.get('client_msg_id')==body['client_msg_id'] and e.get('type') in ('accepted_ack','error'),start)
        if ack.get('type')!='accepted_ack':raise AssertionError('message rejected: '+str(ack.get('code')))
        return ack
    messages=[{'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'gRPC delivery discussion '+str(i)}} for i in range(10)]
    try:
        for user in users[:10]:
            ws,inbox=await connect(user)
            await ws.send(json.dumps({'type':'chatroom_join','group_id':room}))
            joined=await wait(inbox,lambda e:e.get('op')=='chatroom_join')
            assert joined['type']=='ack'
        acknowledgements=await asyncio.gather(*(send(ws,msg,inbox) for ws,msg,inbox in zip(sockets,messages,events)))
        for inbox in events:
            for msg in messages:
                await wait(inbox,lambda e:e.get('client_msg_id')==msg['client_msg_id'] and e.get('type')=='chatroom')
        check('ten simultaneous authenticated WebSockets receive all ten group messages')
        reference=None
        for inbox in events:
            sequence=sorted((e['msg_seq'],e['client_msg_id']) for e in inbox if e.get('type')=='chatroom' and any(e.get('client_msg_id')==m['client_msg_id'] for m in messages))
            assert len(set(sequence))==10
            if reference is None:reference=set(sequence)
            else:assert set(sequence)==reference
        duplicate=await send(sockets[0],messages[0],events[0])
        assert duplicate['msg_seq']==acknowledgements[0]['msg_seq']
        check('duplicate client message preserves its canonical sequence')
        single={'type':'single_chat','to_user_id':b['user_id'],'client_msg_id':secrets.token_hex(12),'content':{'text':'ordinary direct message'}}
        await send(sockets[0],single,events[0])
        await wait(events[1],lambda e:e.get('client_msg_id')==single['client_msg_id'] and e.get('type')=='single_chat')
        check('ordinary direct chat works with Agent functionality disabled')
        image=base64.b64encode(bytes.fromhex('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082')).decode()
        attachment=call('/api/attachment/upload',{'session_id':f'r_{room}','name':'team.png','data':image},a)
        original_attachment=call('/api/attachment/get',{'id':attachment['id']},c)
        assert original_attachment['data']==image
        call('/api/attachment/get',{'id':attachment['id']},users[-1],403)
        check('shared attachment is visible only to room members')
        await sockets[1].close()
        offline={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'persist while disconnected'}}
        await send(sockets[0],offline,events[0])
        _,reconnected=await connect(b)
        await wait(reconnected,lambda e:e.get('client_msg_id')==offline['client_msg_id'] and e.get('type')=='chatroom')
        check('WebSocket reconnect restores a message sent while offline')
        for _ in range(100):
            history=call('/api/session/history',{'session_id':f'r_{room}'},a)['messages']
            if len(history)>=11: break
            await asyncio.sleep(.1)
        assert len(history)==11
        check('SQL-backed public history contains one final row per accepted group message')
        for ws in sockets:await ws.close()
        await asyncio.gather(*tasks)
        compose('down')
        compose('up','-d','--wait','--wait-timeout','180')
        relogged=[call('/api/login',identity) for identity in credentials[:3]]
        for user in relogged:
            current=call('/api/session/history',{'session_id':f'r_{room}'},user)['messages']
            assert current==history
            assert len(call('/api/spaces/members',{'room_id':room},user)['members'])==10
            assert call('/api/attachment/get',{'id':attachment['id']},user)==original_attachment
        check('normal full-stack down/up preserves login, membership, exact history and attachment bytes')
        restored,inbox=await connect(relogged[1])
        await restored.send(json.dumps({'type':'chatroom_join','group_id':room}))
        await wait(inbox,lambda e:e.get('op')=='chatroom_join')
        post={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'continue after restart'}}
        ack=await send(restored,post,inbox)
        assert ack['msg_seq']>max(m['msg_seq'] for m in history)
        check('restarted services continue using the same monotonic conversation sequence')
    finally:
        for ws in sockets:await ws.close()
        await asyncio.gather(*tasks,return_exceptions=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='spark-push:dev')
    parser.add_argument('--port',type=int,default=0)
    parser.add_argument('--out',required=True)
    args=parser.parse_args()
    output=Path(args.out).resolve();output.parent.mkdir(parents=True,exist_ok=True)
    project='spark-release-'+secrets.token_hex(6)
    with socket.socket() as listener:
        listener.bind(('127.0.0.1',args.port));port=listener.getsockname()[1]
    receipt={'ok':False,'mode':'real Docker Compose, Agents disabled','source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'checks':[]}
    with tempfile.TemporaryDirectory(prefix='spark-compose-') as temporary:
        envfile=Path(temporary)/'.env';envfile.write_bytes((ROOT/'.env.example').read_bytes())
        envfile.chmod(0o600)
        environment={key:value for key,value in os.environ.items() if not key.startswith(('SPARK_','COMPOSE_','KERNEL_','HERMES_','PI_'))}
        environment.update(SPARK_PUSH_IMAGE=args.image,SPARK_PUSH_PORT=str(port),SPARK_PUSH_BIND='127.0.0.1')
        base=['docker','compose','--env-file',str(envfile),'-p',project,'-f',str(ROOT/'docker-compose.yml')]
        def compose(*arguments):
            result=subprocess.run(base+list(arguments),cwd=ROOT,env=environment,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=300)
            with output.with_suffix('.private.log').open('a') as log:
                if arguments[0]!='config':log.write(result.stdout)
            output.with_suffix('.private.log').chmod(0o600)
            if result.returncode:raise RuntimeError('Compose failed: '+' '.join(arguments))
            return result.stdout
        def check(description):
            receipt['checks'].append(description);print(description,flush=True)
        try:
            compose('up','-d','--wait','--wait-timeout','180','--pull','never')
            public='http://127.0.0.1:'+str(port)
            assert b'sameOrigin: true' in http(public,'/runtime-config.js')
            assert b'Spark' in http(public,'/chatroom.html')
            check('fresh copied environment starts all services with one public origin')
            config=json.loads(compose('config','--format','json'))
            assert all(not service.get('ports') for key,service in config['services'].items() if key!='gateway')
            check('database, Redis, Kafka, internal RPC and metrics have no published host ports')
            asyncio.run(exercise(public,compose,check))
            receipt['ok']=True
        finally:
            compose('logs','--no-color','--tail','100')
            compose('down','-v','--remove-orphans')
            output.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(receipt,ensure_ascii=False))

if __name__=='__main__':main()
