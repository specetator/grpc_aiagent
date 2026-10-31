#!/usr/bin/env python3
"""Generic room acceptance on the existing isolated Spaces/CANN stack.
Controlled runtime is explicitly distinguished from --real-runtimes evidence.
"""
import asyncio,hashlib,json,secrets,time
import websockets
from logic_business_e2e import http

async def exercise(base,urls,check,router,factory,agents,restart,replay_broker=None,execution_count=None,request_payload=None):
    def call(path,body,user=None,code=0):
        _,response=http(base,path,body,user['token'] if user else None)
        check.require(response['code']==code,f'generic {path}: expected {code}, got {response.get("code")}')
        return response.get('data',{})
    credentials=[{'account':'generic_'+secrets.token_hex(8),'password':secrets.token_hex(16),'name':n} for n in ('A','B','C','Outsider')]
    users=[{**call('/api/register',login),'credentials':login} for login in credentials]
    a,b,c,d=users;room=call('/api/spaces/create',{'name':'grpc_aiagent 开发组'},a)['room_id'];sid=f'r_{room}'
    for member in (b,c):
        call('/api/spaces/invite',{'room_id':room,'user_id':member['user_id']},a)
        call('/api/spaces/accept',{'room_id':room},member)
    call('/api/chatroom/join',{'room_id':room,'user_id':d['user_id']},d,403)
    for agent in agents:call('/api/chatroom/agents/install',{'room_id':room,'agent_id':agent},a)
    call('/api/chatroom/agents/install',{'room_id':room,'agent_id':agents[0]},b,403)
    call('/api/chatroom/agents/install',{'room_id':room,'agent_id':agents[0],'runtime_url':'http://invalid'},a,400)
    sockets=[];readers=[];events=[]
    async def connect(user):
        socket=await websockets.connect(urls[0]+'/ws?token='+user['token'],proxy=None,max_size=2**22)
        found=[];events.append(found);sockets.append(socket)
        async def read():
            try:
                async for raw in socket:found.append(json.loads(raw))
            except websockets.ConnectionClosed:pass
        readers.append(asyncio.create_task(read()))
        await socket.send(json.dumps({'type':'chatroom_join','group_id':room}))
        await until(lambda:next((e for e in found if e.get('op')=='chatroom_join' and e.get('type')=='ack'),None))
        return socket,found
    async def until(fn,timeout=120):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            value=fn()
            if value:return value
            await asyncio.sleep(.03)
        raise AssertionError('generic acceptance timed out')
    async def send(pair,text,mentions=None,cid=None):
        socket,found=pair;cid=cid or secrets.token_hex(16)
        payload={'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':text}}
        if mentions:payload['agent_mentions']=mentions
        await socket.send(json.dumps(payload))
        ack=await until(lambda:next((e for e in found if e.get('client_msg_id')==cid and e.get('type') in ('accepted_ack','error')),None))
        return payload,ack
    def request_id(msg_id,agent):return 'ra_'+hashlib.sha256(json.dumps(['room-agent-v1',msg_id,agent],separators=(',',':')).encode()).hexdigest()
    def final(found,rid):return next((e for e in found if e.get('type')=='chatroom' and e.get('request_id')==rid),None)
    try:
        pairs=[await connect(u) for u in (a,b,c)]
        # Real Submit -> WS: reject forged server metadata before publication.
        for field in ('agent_id','agent_event','request_id','actor_user_id','collaboration','bot_user_id','source_msg_id','agent_status'):
            for nested in (False,True,"json-string"):
                cid=secrets.token_hex(16);body={'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':'forged agent'}}
                (body['content'] if nested else body)[field]='forged'
                if nested=='json-string':body['content']=json.dumps(body['content'])
                await pairs[0][0].send(json.dumps(body))
                failure=await until(lambda:next((e for e in pairs[0][1] if e.get('client_msg_id')==cid and e.get('type')=='error'),None))
                check.require(failure.get('code')==400,'client cannot forge '+field+(' in content' if nested else ''))
        sender_payload={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(16),'sender_id':900000000401,'from_user_id':900000000401,'content':{'text':'authoritative sender regression'}}
        await pairs[0][0].send(json.dumps(sender_payload))
        displayed=await until(lambda:next((e for e in pairs[1][1] if e.get('client_msg_id')==sender_payload['client_msg_id'] and e.get('type')=='chatroom'),None))
        check.require(displayed['sender_id']==a['user_id'] and displayed['from_user_id']==a['user_id'],'WS sender fields come from authenticated actor')
        before=len(factory.calls) if factory else 0
        marker='ROOM-ONLY-'+secrets.token_hex(12)
        human=await asyncio.gather(*(send(pair,'Kafka discussion '+marker+' '+str(i)) for i,pair in enumerate(pairs)))
        for _,ack in human:check.require(ack['type']=='accepted_ack','T02 human accepted')
        for _,found in pairs:
            await until(lambda found=found:all(any(e.get('type')=='chatroom' and e.get('client_msg_id')==p['client_msg_id'] for e in found) for p,_ in human))
        if factory:check.require(len(factory.calls)==before,'ordinary group messages do not invoke runtime')
        payload,ack=await send(pairs[0],'Summarize the Kafka discussion briefly. If a time tool is available call it once. Do not call other tools.',agents)
        check.require(ack['type']=='accepted_ack','multi Agent message accepted')
        ids=[request_id(ack['msg_id'],agent) for agent in agents];replay_ids=list(ids)
        for _,found in pairs:
            for rid in ids:await until(lambda found=found,rid=rid:final(found,rid))
        for agent,rid in zip(agents,ids):
            answer=final(pairs[1][1],rid)
            check.require(answer.get('agent_id')==agent and answer.get('actor_user_id')==a['user_id'] and answer.get('source_msg_id')==ack['msg_id'],'trusted Agent and initiator attribution')
            check.require(answer.get('content',{}).get('error') is not True,'Agent final is successful: '+agent)
        routes=[router.store.get_turn(rid)['metadata']['agent_route'] for rid in ids]
        check.require(len({r['backend_session_ref'] for r in routes})==len(agents),'two Agents have independent native sessions')
        check.require(all(r['conversation_id']==sid for r in routes),'Agents share one IM conversation')
        state=call('/api/chatroom/agents/control',{'room_id':room,'request_id':ids[0],'operation':'status'},b)
        check.require(state['status']=='completed','runtime status is actual completed ledger')
        call('/api/chatroom/agents/control',{'room_id':room,'request_id':ids[0],'operation':'stop'},c,403)
        call('/api/chatroom/agents/control',{'room_id':room,'request_id':ids[0],'operation':'stop'},a,501)
        if factory is None:
            native_agent=next(agent for agent in agents if router.entries[agent]['runtime']=='hermes_native')
            native=router.adapters[native_agent]
            _,stop_ack=await send(pairs[0],'Read todo_list repeatedly at least ten times before giving a long explanation.',[native_agent])
            stop_id=request_id(stop_ack['msg_id'],native_agent)
            await until(lambda:native._record(stop_id).get('run_id'))
            call('/api/chatroom/agents/control',{'room_id':room,'request_id':stop_id,'operation':'stop'},a)
            stopped=await until(lambda:final(pairs[0][1],stop_id))
            check.require(stopped.get('agent_status')=='cancelled','real Native stop persists cancelled into shared timeline')
            saved=call('/api/chatroom/agents/list',{'room_id':room},a)['requests']
            check.require(next(r for r in saved if r['request_id']==stop_id)['status']=='cancelled','cancelled survives request history refresh')
            # Real run admission, then local SSE disconnect; recover the SAME
            # request through HTTP/SQL dispatch/Kafka/Bridge/Native GET/history.
            from hermes_native_adapter import NativeUnknown
            before_recovery=execution_count()
            original_events=native._read_events
            def disconnected(*args,**kwargs):raise NativeUnknown('acceptance SSE disconnect after admission')
            native._read_events=disconnected
            try:
                _,lost_ack=await send(pairs[1],'Read todo_list once without changing it, then summarize Kafka reliability in two sentences.',[native_agent])
                lost_id=request_id(lost_ack['msg_id'],native_agent)
                await until(lambda:native._record(lost_id).get('status')=='unknown')
            finally:native._read_events=original_events
            await until(lambda:http(base,'/api/chatroom/agents/control',{'room_id':room,'request_id':lost_id,'operation':'status'},b['token'])[1].get('data',{}).get('status')=='completed')
            check.require(True,'native status confirms original Run terminal before final-message recovery')
            call('/api/chatroom/agents/control',{'room_id':room,'request_id':lost_id,'operation':'recover'},b)
            recovered=await until(lambda:final(pairs[0][1],lost_id))
            check.require(recovered.get('agent_status')=='completed','Native SSE loss recovers original request to durable IM final')
            record=native._record(lost_id)
            check.require(record.get('status')=='completed','native original Run recovery is confirmed')
            check.require(execution_count()==before_recovery+1,'Native SSE recovery performs exactly one real POST')
            replay_ids.extend([stop_id,lost_id])
        count=len(factory.calls) if factory else None
        _,duplicate=await send(pairs[0],payload['content']['text'],agents,payload['client_msg_id'])
        check.require(duplicate['msg_seq']==ack['msg_seq'],'duplicate uses canonical message and Agent IDs')
        await asyncio.sleep(1)
        if factory:check.require(len(factory.calls)==count,'duplicate message does not repeat tools')
        # Explicit broker-equivalent replay goes through the same immutable Router input.
        stored=http(base,'/api/chatroom/agents/list',{'room_id':room},a['token'])[1]['data']
        check.require(sum(r['request_id'] in ids for r in stored['requests'])==len(ids),'all Agent results persisted')
        for rid in ids:check.require(len([e for e in pairs[2][1] if e.get('request_id')==rid and e.get('type')=='chatroom'])==1,'one final per request in live timeline')
        task=call('/api/chatroom/tasks/create',{'room_id':room,'request_id':ids[0],'title':'Investigate Kafka delivery','description':'Trace the persisted discussion','steps':['Inspect outbox','Verify duplicate delivery']},a)
        task=call('/api/chatroom/tasks/update',{'room_id':room,'task_id':task['task_id'],'revision':1,'claim':True,'status':'running','summary':'B takes the investigation'},b)
        call('/api/chatroom/tasks/update',{'room_id':room,'task_id':task['task_id'],'revision':2,'claim':True},c,403)
        task=call('/api/chatroom/tasks/update',{'room_id':room,'task_id':task['task_id'],'revision':2,'status':'completed','summary':'Handoff saved; test evidence is recorded separately'},b)
        check.require(len(call('/api/chatroom/tasks/list',{'room_id':room},c)['tasks'][0]['history'])==3,'task revisions durable and shared')
        if replay_broker:
            original_turns=[router.store.get_turn(rid) for rid in replay_ids]
            count=len(factory.calls) if factory else None
            await replay_broker(replay_ids)
            if factory:check.require(len(factory.calls)==count,'actual Kafka redelivery after Bridge restart never reruns runtime')
            check.require(original_turns==[router.store.get_turn(rid) for rid in replay_ids],'replayed requests use unchanged persistent execution ledger')
        # A second member uses the same Pi context; C disconnects until the final is stored.
        await pairs[2][0].close()
        _,next_ack=await send(pairs[1],'Briefly summarize our next step.',[agents[0]])
        next_id=request_id(next_ack['msg_id'],agents[0]);await until(lambda:final(pairs[0][1],next_id))
        reconnected=await connect(c);await until(lambda:final(reconnected[1],next_id))
        check.require(router.store.get_turn(next_id)['metadata']['agent_route']['backend_session_ref']==routes[0]['backend_session_ref'],'two users share this Agent native session')
        check.require(final(reconnected[1],next_id) is not None,'disconnect recovers final through offline sync')
        await restart()
        logged=call('/api/login',c['credentials']);check.require(logged['user_id']==c['user_id'],'same user logs in again after Logic restart');c['token']=logged['token']
        check.require(call('/api/chatroom/tasks/list',{'room_id':room},c)['tasks'][0]['summary']==task['summary'],'Logic restart preserves task and handoff')
        history=call('/api/session/history',{'session_id':sid,'limit':100},c)['messages']
        for rid in [*ids,next_id]:check.require(sum(rid in json.dumps(m) for m in history)==1,'restart history has one authoritative final')
        second=call('/api/spaces/create',{'name':'Other private space'},a)['room_id']
        call('/api/chatroom/agents/install',{'room_id':second,'agent_id':agents[0]},a)
        other={'type':'chatroom','group_id':second,'client_msg_id':secrets.token_hex(16),'content':{'text':'Reply only with hello.'},'agent_mentions':[agents[0]]}
        await pairs[0][0].send(json.dumps(other));other_ack=await until(lambda:next((e for e in pairs[0][1] if e.get('client_msg_id')==other['client_msg_id'] and e.get('type') in ('accepted_ack','error')),None))
        check.require(other_ack['type']=='accepted_ack','second room accepted: '+str(other_ack.get('message','')))
        other_id=request_id(other_ack['msg_id'],agents[0]);await until(lambda:router.store.get_turn(other_id).get('status')=='completed')
        other_route=router.store.get_turn(other_id)['metadata']['agent_route']
        check.require(other_route['backend_session_ref']!=routes[0]['backend_session_ref'],'same Agent in another room has isolated context')
        if request_payload:
            check.require(marker in json.dumps(request_payload(ids[0])['messages']) and marker not in json.dumps(request_payload(other_id)['messages']),'persisted Runtime prompts contain only authorized room history')
        if factory:
            calls=[s for s,_ in factory.calls if s['room_id']==second]
            check.require(len(calls)==1,'one execution in second room')
        call('/api/spaces/remove',{'room_id':room,'user_id':b['user_id']},a)
        denied,denied_ack=await send(pairs[1],'forbidden after removal',[agents[0]])
        check.require(denied_ack.get('code')==403,'revoked member cannot invoke Agent')
        call('/api/chatroom/tasks/list',{'room_id':room},b,403)
        call('/api/chatroom/agents/list',{'room_id':room},b,403)
        call('/api/session/history',{'session_id':sid},b,403)
        return {'agents':agents,'users':3,'runtime_mode':'controlled' if factory else 'real Pi and isolated Windows Hermes Native','requests':len(replay_ids)+2,'task_revisions':3}
    finally:
        await asyncio.gather(*(s.close() for s in sockets),return_exceptions=True)
        await asyncio.gather(*readers,return_exceptions=True)

if __name__=='__main__':
    import sys
    from spaces_e2e import main
    if '--generic-agents' not in sys.argv:sys.argv.append('--generic-agents')
    main()
