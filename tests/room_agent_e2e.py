#!/usr/bin/env python3
"""Opt-in room collaboration test against dedicated MySQL/Redis/Kafka.

Runs REAL Comet/Logic/Job/Bridge/Gateway. Only the model/tool decision is a
deterministic adapter; compilation/testing uses the actual bubblewrap runner.
Never prints credentials or puts them in tracked results.
"""
import argparse
import asyncio
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
import websockets
import grpc
from grpc_tools import protoc
import importlib

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'cannbot/scripts'))
sys.path.insert(0,str(ROOT/'inference/benchmarks'))
from agent_router import AgentRouter,AgentStore
from pi_gateway import GatewayState,make_handler
from room_agent import run_tests,workspace_manifest
from run_app_matrix import free_port,write_config
from logic_business_e2e import http,Checks
BOT=900000000001


class ControlledFactory:
    def __init__(self,root):
        self.root=root;self.calls=[];self.active=0;self.peak_active=0
        self.runner=root/'runner.py'
        self.runner.write_text("import subprocess\nsubprocess.run(['g++','operator.cpp','-o','.operator-test'],check=True)\nsubprocess.run(['./.operator-test'],check=True)\nprint('operator precision checks passed')\n")
    def is_ready(self): return True
    def for_scope(self,scope):
        factory=self
        class Bound:
            def chat(self,message,on_delta,timeout_s,*args,**kwargs):
                factory.active+=1;factory.peak_active=max(factory.peak_active,factory.active)
                workspace=factory.root/f"room_{scope['room_id']}_{scope['task_id']}"
                workspace.mkdir(exist_ok=True)
                factory.calls.append({**scope,'request_id':kwargs.get('request_id')})
                time.sleep(.05)
                progress=kwargs.get('on_progress')
                if progress: progress('受控算子测试：执行中')
                # Deliberately checks only the CURRENT user request marker,
                # not historical discussion or a previous developer's input.
                current=message.rsplit('当前请求',1)[-1]
                tests=[]
                if 'WRITE_BAD' in current or 'WRITE_GOOD' in current:
                    if scope['can_develop']:
                        good='WRITE_GOOD' in current
                        (workspace/'operator.cpp').write_text('int add(int a,int b){return a'+('+' if good else '-')+'b;}\nint main(){return add(2,3)==5?0:1;}\n')
                        tests=[run_tests(workspace,factory.root/'receipts',factory.runner)]
                        text='code updated; actual precision '+('passed' if tests[0]['passed'] else 'failed')
                    else: text='read-only member: file modification denied'
                elif 'WAIT_HANDOFF' in current:
                    time.sleep(.8);text='continuing room task after sender disconnected'
                else: text='shared workspace files: '+','.join(p.name for p in workspace.glob('*.cpp'))
                if on_delta: on_delta(text[:20]);on_delta(text[20:])
                factory.active-=1
                return text,{'workspace_checkpoint':{**workspace_manifest(workspace),'tests':tests}}
        return Bound()


async def receive(socket,predicate,timeout=40):
    async with asyncio.timeout(timeout):
        while True:
            event=json.loads(await socket.recv())
            if predicate(event): return event


async def exercise(base,ws_base,factory,check,stub,pb):
    def call(path,body,user=None,code=0):
        _,response=http(base,path,body,user['token'] if user else None)
        check.require(response['code']==code,path+' expected '+str(code))
        return response.get('data',{})
    users=[]
    for name in ('A','B','Observer','Outsider'):
        credentials={'account':'room_'+secrets.token_hex(10),'password':secrets.token_hex(16),'name':name}
        users.append(call('/api/register',credentials))
    a,b,c,d=users
    admin=call('/api/login',{'account':os.environ['SPARK_PUSH_ADMIN_ACCOUNT'],'password':os.environ['SPARK_PUSH_ADMIN_PASSWORD']})
    room=call('/api/admin/chatroom/create',{'name':'CANN collaboration regression','owner_id':a['user_id']},admin)['room_id']
    for user in (a,b,c): call('/api/chatroom/join',{'room_id':room,'user_id':user['user_id']},user)
    sid=f'r_{room}'
    config={'room_id':room,'title':'Add operator','spec':'controlled host C++ precision test; not an Ascend hardware test',
            'developer_user_ids':[b['user_id']]}
    call('/api/chatroom/agent/configure',config,b,403)
    task=call('/api/chatroom/agent/configure',config,a)
    forged=pb.UpstreamMessageRequest(from_user_id=b['user_id'],scene='chatroom',group_id=room,
        client_msg_id='forged-'+secrets.token_hex(8),content_json=json.dumps({'content':{'text':'@CANN WRITE_GOOD'}}))
    denied=stub.SendUpstreamMessage(forged,timeout=10)
    check.require(denied.error.code==401 and denied.ack_stage==pb.ACK_STAGE_UNSPECIFIED,'room RPC without bearer denied before acceptance')
    forged.auth_token=a['token']
    denied=stub.SendUpstreamMessage(forged,timeout=10)
    check.require(denied.error.code==403 and denied.ack_stage==pb.ACK_STAGE_UNSPECIFIED,'forged room sender denied despite a valid other-user bearer')
    check.require(call('/api/chatroom/agent/task',{'room_id':room},c)['can_develop']==False,'member read-only by default')
    call('/api/chatroom/agent/task',{'room_id':room},d,403)
    call('/api/chatroom/agent/checkpoint',{'room_id':room,'revision':1,'patch':{'summary':'forged'}},c,403)
    checkpoint=call('/api/chatroom/agent/checkpoint',{'room_id':room,'revision':1,
        'patch':{'summary':'A designed the operator','next_steps':'B verify precision'}},a)
    call('/api/chatroom/agent/checkpoint',{'room_id':room,'revision':1,'patch':{'summary':'stale'}},b,409)

    async def connect(user): return await websockets.connect(ws_base+'/ws?token='+user['token'],max_size=2**20,proxy=None)
    sockets=[await connect(user) for user in users]
    aa,bb,cc,dd=sockets
    for socket in sockets: await socket.send(json.dumps({'type':'chatroom_join','group_id':room}))
    async def submit(socket,text):
        cid='room-e2e-'+secrets.token_hex(8)
        body={'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':text}}
        await socket.send(json.dumps(body))
        ack=await receive(socket,lambda e:e.get('type')=='accepted_ack' and e.get('client_msg_id')==cid)
        return body,ack
    def final_for(request): return lambda e:e.get('type')=='chatroom' and e.get('request_id')==request and e.get('from_user_id')==BOT
    async def task_view(predicate):
        for _ in range(200):
            view=call('/api/chatroom/agent/task',{'room_id':room},b)
            if predicate(view): return view
            await asyncio.sleep(.05)
        raise AssertionError('room task result not durable before deadline')
    async def persisted(user,predicate):
        for _ in range(200):
            _,result=http(base,'/api/session/history',{'session_id':sid,'limit':100},user['token'])
            if result['code']==0 and predicate(result['data']['messages']): return result['data']['messages']
            await asyncio.sleep(.05)
        raise AssertionError('room history not persisted before deadline')
    try:
        normal,ack=await submit(aa,'ordinary developer discussion')
        await receive(bb,lambda e:e.get('client_msg_id')==normal['client_msg_id'] and e.get('type')=='chatroom')
        await asyncio.sleep(.2);check.require(len(factory.calls)==0,'ordinary discussion does not invoke Agent')
        body,ack=await submit(aa,'@CANN WRITE_BAD')
        request=ack['msg_id']
        progress=await receive(bb,lambda e:e.get('type')=='hermes_delta' and e.get('request_id')==request)
        check.require(progress.get('msg_seq',0)==0,'ephemeral progress does not advance cursor')
        answers=await asyncio.gather(*(receive(s,final_for(request)) for s in (aa,bb,cc)))
        check.require(len({e['msg_seq'] for e in answers})==1,'all room members receive one sequenced final answer')
        view=await task_view(lambda v:any(r['request_id']==request and r['result'] for r in v['requests']))
        first=next(r for r in view['requests'] if r['request_id']==request)
        check.require(first['result']['workspace_checkpoint']['tests'][0]['passed']==False,'real failing precision receipt persisted')
        await aa.send(json.dumps(body))
        duplicate=await receive(aa,lambda e:e.get('type')=='accepted_ack' and e.get('client_msg_id')==body['client_msg_id'])
        check.require(duplicate['msg_id']==request and duplicate['msg_seq']==ack['msg_seq'],'duplicate room request retains ID and sequence')
        await asyncio.sleep(.2);check.require(len(factory.calls)==1,'duplicate request does not execute tools again')

        _,second=await submit(bb,'@CANN WRITE_GOOD')
        final=await receive(cc,final_for(second['msg_id']))
        check.require('passed' in final['content']['text'],'second developer receives actual passing result')
        view=await task_view(lambda v:any(r['request_id']==second['msg_id'] and r['result'] for r in v['requests']))
        latest=next(r for r in view['requests'] if r['request_id']==second['msg_id'])
        check.require(latest['result']['workspace_checkpoint']['tests'][0]['passed'],'real successful precision receipt persisted')
        check.require(view['task']['task_id']==task['task_id'] and view['task']['summary']=='A designed the operator','handoff preserves task and summary')
        check.require([x['actor_user_id'] for x in factory.calls[:2]]==[a['user_id'],b['user_id']],'separate actor attribution in shared task')

        _,readonly=await submit(cc,'@CANN WRITE_BAD')
        readonly_final=await receive(bb,final_for(readonly['msg_id']))
        check.require('denied' in readonly_final['content']['text'],'read-only member cannot change workspace')
        await dd.send(json.dumps({'type':'chatroom','group_id':room,'client_msg_id':'outsider-test','content':{'text':'@CANN malicious'}}))
        error=await receive(dd,lambda e:e.get('type')=='error')
        check.require(error.get('code',403)==403,'outsider WS submit rejected')
        try:
            await receive(dd,lambda e:e.get('session_id')==sid and e.get('type') in ('chatroom','hermes_delta'),.5)
            check.require(False,'outsider received shared Agent content')
        except TimeoutError: check.require(True,'unauthorized WS presence receives no room Agent content')

        _,offline=await submit(aa,'@CANN WAIT_HANDOFF')
        await aa.close()
        resumed=await receive(bb,final_for(offline['msg_id']))
        check.require('disconnected' in resumed['content']['text'],'sender disconnect does not cancel room task')
        rows=await persisted(b,lambda rows:any(m['sender_id']==BOT and offline['msg_id'] in json.dumps(m) for m in rows))
        check.require(not any(m['msg_type']=='hermes_delta' for m in rows),'history contains final messages, no deltas')
        reconnected=await connect(a);sockets.append(reconnected)
        await reconnected.send(json.dumps({'type':'sync_after','session_id':sid,'after_seq':0,'limit':100}))
        recovered=await receive(reconnected,final_for(offline['msg_id']))
        check.require(recovered['msg_seq']==resumed['msg_seq'],'reconnected member recovers same final sequence')
        concurrent=await asyncio.gather(submit(bb,'@CANN inspect shared code B'),submit(reconnected,'@CANN inspect shared code A'))
        await asyncio.gather(*(receive(socket,final_for(item[1]['msg_id'])) for socket,item in zip((bb,reconnected),concurrent)))
        ordered=sorted((item[1] for item in concurrent),key=lambda ack:ack['msg_seq'])
        check.require([call['request_id'] for call in factory.calls[-2:]]==[ack['msg_id'] for ack in ordered],
                      'simultaneous users execute in server message order')
        check.require(factory.peak_active==1,'same workspace has only one active execution')
        call('/api/chatroom/unsubscribe',{'room_id':room,'user_id':c['user_id']},c)
        call('/api/chatroom/agent/task',{'room_id':room},c,403)
        call('/api/session/history',{'session_id':sid},c,403)
        _,after_leave=await submit(bb,'@CANN inspect after unsubscribe')
        await receive(bb,final_for(after_leave['msg_id']))
        try:
            await receive(cc,lambda e:e.get('request_id')==after_leave['msg_id'],.5)
            check.require(False,'unsubscribed online socket received new Agent output')
        except TimeoutError: check.require(True,'unsubscribed online socket cannot receive new Agent output')
        history_rows=await persisted(b,lambda rows:any(after_leave['msg_id'] in json.dumps(m) and m['sender_id']==BOT for m in rows))
        check.require(not any(u['token'] in json.dumps(history_rows) for u in users),'transport bearer never enters room history')
        return {'model_adapter':'deterministic, not a real LLM','actual_test_runner':'bubblewrap + g++ precision executable',
                'agent_executions':len(factory.calls),'room_members_tested':3,'peak_workspace_executions':factory.peak_active}
    finally:
        await asyncio.gather(*(s.close() for s in sockets),return_exceptions=True)


async def exercise_live(base,ws_base,check):
    def call(path,body,user=None):
        _,result=http(base,path,body,user['token'] if user else None)
        check.require(result['code']==0,path+' live success')
        return result['data']
    users=[]
    for name in ('LiveA','LiveB'):
        users.append(call('/api/register',{'account':'live_room_'+secrets.token_hex(8),
                         'password':secrets.token_hex(16),'name':name}))
    a,b=users
    admin=call('/api/login',{'account':os.environ['SPARK_PUSH_ADMIN_ACCOUNT'],'password':os.environ['SPARK_PUSH_ADMIN_PASSWORD']})
    room=call('/api/admin/chatroom/create',{'name':'Real Pi room collaboration','owner_id':a['user_id']},admin)['room_id']
    for user in users: call('/api/chatroom/join',{'room_id':room,'user_id':user['user_id']},user)
    task=call('/api/chatroom/agent/configure',{'room_id':room,'title':'host Add collaboration smoke',
        'spec':'本轮仅验证协作，不讨论 CANN API。C++ int add(a,b)=a+b; main 检查 add(2,3)==5。',
        'developer_user_ids':[b['user_id']]},a)
    sockets=[await websockets.connect(ws_base+'/ws?token='+u['token'],proxy=None,max_size=2**20) for u in users]
    try:
        for socket in sockets: await socket.send(json.dumps({'type':'chatroom_join','group_id':room}))
        prompts=[
          '本轮仅验证协作文件工具。请实际调用 write 创建 operator.cpp，内容严格为：int add(int a,int b){return a+b;}\nint main(){return add(2,3)==5?0:1;}\n。然后把交接说明发给房间，不要调用测试。',
          '我是接手的另一位开发者。请实际调用 read 读取 operator.cpp，然后调用 room_run_tests 执行真实测试。不要修改文件，仅根据回执简短报告。']
        revisions=[]
        for index,prompt in enumerate(prompts):
            cid='live-room-'+secrets.token_hex(8)
            await sockets[index].send(json.dumps({'type':'chatroom','group_id':room,'client_msg_id':cid,'content':{'text':'@CANN '+prompt}}))
            ack=await receive(sockets[index],lambda e:e.get('type')=='accepted_ack' and e.get('client_msg_id')==cid)
            finals=await asyncio.gather(*(receive(s,lambda e:e.get('type')=='chatroom' and e.get('request_id')==ack['msg_id'] and e.get('from_user_id')==BOT,150) for s in sockets))
            check.require(finals[0]['msg_seq']==finals[1]['msg_seq'],'live model final delivered to both members')
            view=call('/api/chatroom/agent/task',{'room_id':room},b)
            request=next(r for r in view['requests'] if r['request_id']==ack['msg_id'])
            checkpoint=request['result']['workspace_checkpoint']
            check.require(any(f['path']=='operator.cpp' for f in checkpoint['files']),'real Pi source checkpoint persisted')
            revisions.append(checkpoint['code_revision'])
            if index==1:
                check.require(bool(checkpoint['tests']) and all(t['passed'] for t in checkpoint['tests']),
                              'real Pi invoked sandbox runner and persisted passing receipt')
            print('live room turn '+str(index+1)+' verified',flush=True)
        check.require(revisions[0]==revisions[1],'second user used unchanged shared source version')
        check.require(view['task']['task_id']==task['task_id'],'real users retain one room task')
        return {'model_adapter':'REAL installed Pi, configured default provider/model',
                'actual_test_runner':'bubblewrap + g++ precision executable','agent_executions':2,'room_members_tested':2}
    finally: await asyncio.gather(*(s.close() for s in sockets),return_exceptions=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secret-file',required=True);parser.add_argument('--out',required=True)
    parser.add_argument('--live-pi',action='store_true',help='Use the real configured Pi model instead of the deterministic adapter')
    args=parser.parse_args();out=Path(args.out).resolve();out.mkdir(parents=True,exist_ok=True)
    start=time.monotonic();check=Checks();processes=[];logs=[]
    result={'schema':'sparkpush.room_agent_e2e.v1','ok':False}
    env=dict(os.environ,SPARK_PUSH_MYSQL_PASSWORD=json.loads(Path(args.secret_file).read_text())['mysql_password'],
             SPARK_PUSH_HERMES_ENABLED='true',SPARK_PUSH_ADMIN_ACCOUNT='room_agent_admin',
             SPARK_PUSH_ADMIN_PASSWORD=secrets.token_hex(24),SPARK_PUSH_HERMES_API_KEY=secrets.token_hex(24))
    for key in ('SPARK_PUSH_ADMIN_ACCOUNT','SPARK_PUSH_ADMIN_PASSWORD'): os.environ[key]=env[key]
    server=None;thread=None
    try:
      with tempfile.TemporaryDirectory(prefix='spark-room-agent-e2e-') as temp:
        private=Path(temp);private.chmod(0o700)
        if protoc.main(['protoc','-I'+str(ROOT/'proto'),'--python_out='+temp,'--grpc_python_out='+temp,
                        str(ROOT/'proto/spark_push.proto')])!=0: raise RuntimeError('test protobuf generation failed')
        sys.path.insert(0,temp)
        pb=importlib.import_module('spark_push_pb2');rpc=importlib.import_module('spark_push_pb2_grpc')
        default_adapter=None
        if args.live_pi:
            from room_agent_live_pi import build_live_factory
            default_adapter,factory=build_live_factory(private)
            runner=private/'runner.py'
            runner.write_text("import subprocess\nsubprocess.run(['g++','operator.cpp','-o','.operator-test'],check=True)\nsubprocess.run(['./.operator-test'],check=True)\nprint('host precision verified')\n")
            os.environ['SPARK_PUSH_ROOM_TEST_RUNNER']=str(runner)
        else: factory=ControlledFactory(private)
        entries=[{'id':'pi','name':'CANN test','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':BOT}]
        router=AgentRouter(entries,{'pi':default_adapter or factory},AgentStore(private/'state'))
        router.room_adapter=factory
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(GatewayState(router,env['SPARK_PUSH_HERMES_API_KEY'],'pi-agent')))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        ports={k:free_port() for k in ('logic','http','ws','comet','jobm','cometm')}
        prefix='room_'+secrets.token_hex(6)+'_'
        topics={k:prefix+k for k in ('single','group','broadcast','persist','ai_request','ai_delta','ai_reply')}
        common={'kafka_brokers':'127.0.0.1:29092','kafka_consumer_group':prefix+'group',
                'mysql_port':3308,'mysql_user':'root','mysql_db':'spark_push','mysql_password':'${SPARK_PUSH_MYSQL_PASSWORD}'}
        common.update({'kafka_'+k+'_topic':v for k,v in topics.items()})
        configs={
          'logic':dict(common,listen_addr='127.0.0.1',listen_port=ports['logic'],http_port=ports['http'],
                       redis_port=6381,redis_db=11,hermes_enabled='true',room_agent_enabled='true',agent_bot_users=''),
          'comet':dict(listen_addr='127.0.0.1',listen_port=ports['ws'],comet_grpc_port=ports['comet'],
                       metrics_port=ports['cometm'],logic_grpc_target=f"127.0.0.1:{ports['logic']}"),
          'job':dict(common,comet_targets=f"comet-1=127.0.0.1:{ports['comet']}",metrics_port=ports['jobm']),
          'hermes_bridge':dict(kafka_brokers='127.0.0.1:29092',kafka_consumer_group=prefix+'bridge',
                       kafka_request_topic=topics['ai_request'],kafka_delta_topic=topics['ai_delta'],kafka_reply_topic=topics['ai_reply'],
                       hermes_base_url=f'http://127.0.0.1:{server.server_port}/v1',hermes_api_key='${SPARK_PUSH_HERMES_API_KEY}',
                       inference_backend='pi',hermes_streaming='true',processing_workers=2)}
        try:
          for name,config in configs.items():
            cfg=write_config(private,name,config)
            log=open(out/(name+'.log'),'w');logs.append(log)
            binary=name if name=='hermes_bridge' else name+'_server'
            processes.append(subprocess.Popen(['stdbuf','-oL','-eL',str(ROOT/'build-wsl'/name/binary),'--config',str(cfg)],
                             cwd=ROOT,env=env,stdout=log,stderr=subprocess.STDOUT))
          base=f"http://127.0.0.1:{ports['http']}"
          for _ in range(100):
            if any(p.poll() is not None for p in processes): raise RuntimeError('service startup failed')
            try: http(base,'/api/login',{});break
            except Exception: time.sleep(.1)
          time.sleep(5)  # New Kafka groups and Comet streams finish assignment.
          if args.live_pi:
            result.update(asyncio.run(exercise_live(base,f"ws://127.0.0.1:{ports['ws']}",check)))
          else:
            with grpc.insecure_channel(f"127.0.0.1:{ports['logic']}") as channel:
              grpc.channel_ready_future(channel).result(timeout=10)
              result.update(asyncio.run(exercise(base,f"ws://127.0.0.1:{ports['ws']}",factory,check,rpc.LogicServiceStub(channel),pb)))
          result['ok']=True
        finally:
          for process in reversed(processes):
            if process.poll() is None: process.terminate()
          for process in reversed(processes):
            try: process.wait(timeout=10)
            except subprocess.TimeoutExpired: process.kill();process.wait()
    except Exception as error:
        result['error_type']=type(error).__name__
        result['error']=str(error) if isinstance(error,(AssertionError,RuntimeError)) else 'execution failed; inspect private logs'
    finally:
        if server: server.shutdown();server.server_close()
        if thread: thread.join()
        for log in logs: log.close()
        result.update(checks=check.names,checks_passed=len(check.names),duration_s=time.monotonic()-start)
        (out/'room-agent-e2e.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k!='checks'},ensure_ascii=False))
    return 0 if result['ok'] else 1

if __name__=='__main__':sys.exit(main())
