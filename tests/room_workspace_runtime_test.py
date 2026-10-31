#!/usr/bin/env python3
"""Opt-in real Pi/Hermes read-only tools with a contract authority fixture.

Uses actual models/native tools and real repository C++; this is NOT IM E2E.
Final product acceptance must additionally use the actual Logic authority.
"""
import argparse,hashlib,json,os,queue,secrets,shutil,subprocess,sys,tempfile,threading
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'cannbot/scripts'))
from room_workspace import ProjectSnapshots,execution_namespace
from room_agent import PublicRoomPiFactory
from pi_gateway import PiRpcClient
from agent_router import AgentStore
from hermes_native_adapter import HermesNativeAdapter
from hermes_room_runtime_fixture import HermesRoomRuntime


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',required=True)
    parser.add_argument('--runtime',choices=['pi','hermes','both'],default='both')
    args=parser.parse_args()
    receipt={'ok':False,'mode':'real models and native read tools; contract HTTP authority, not IM E2E','checks':[]}
    with tempfile.TemporaryDirectory(prefix='spark-public-code-',dir='/mnt/f') as directory:
        root=Path(directory);root.chmod(0o700)
        source=root/'source';(source/'logic').mkdir(parents=True)
        shutil.copyfile(ROOT/'logic/message_reservation.cpp',source/'logic/message_reservation.cpp')
        nonce=secrets.token_hex(12)
        (source/'probe.cpp').write_text('// Isolated acceptance nonce: '+nonce+'\nint spark_workspace_probe() { return 17; }\n')
        catalog={'schema':1,'projects':[{'id':'spark-push','name':'Spark Push','root':str(source),'paths':['logic','probe.cpp']}]}
        catalog_file=root/'projects.json';catalog_file.write_text(json.dumps(catalog))
        snapshots=ProjectSnapshots(catalog,root/'snapshots')
        grants={};authority_calls=[];key=secrets.token_hex(32)
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                scope=grants.get(value.get('request_id')) if self.headers.get('Authorization')=='Bearer '+key else None
                authority_calls.append(bool(scope))
                self.send_response(200);self.end_headers()
                self.wfile.write(json.dumps({'code':0 if scope else 403,'data':{'request_id':value.get('request_id'),'collaboration':scope}}).encode())
        server=ThreadingHTTPServer(('0.0.0.0',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        host=subprocess.check_output(['hostname','-I'],text=True).split()[0]
        origin=f'http://{host}:{server.server_port}'
        environment={'SPARK_PUSH_ROOM_PROJECT_CATALOG':str(catalog_file),'SPARK_WORKSPACE_AUTH_ORIGIN':origin,
            'SPARK_PUSH_WORKSPACE_AUTH_KEY':key,'SPARK_PUSH_PUBLIC_WORKSPACE_ROOT':str(root/'snapshots'),
            'SPARK_PUSH_PUBLIC_PI_MODELS_FILE':'/home/peco/.pi-spark-agent/models.json',
            'SPARK_PUSH_PUBLIC_PI_ENV_FILE':'/home/peco/.pi-spark-agent/.env'}
        native=None;adapter=None
        pi_reads=[]
        class ObservedPi(PiRpcClient):
            def _spawn(self):
                super()._spawn()
                pending={};cwd=self.cwd
                class Events(queue.Queue):
                    def put(self,event,*args,**kwargs):
                        call=event.get('toolCallId')
                        if event.get('type')=='tool_execution_start' and event.get('toolName')=='read':
                            pending[call]=event.get('args',{}).get('path')
                        elif event.get('type')=='tool_execution_end' and call in pending:
                            output='\n'.join(item.get('text','') for item in event.get('result',{}).get('content',[]) if item.get('type')=='text')
                            pi_reads.append({'request_id':cwd.parent.name,'path':pending.pop(call),
                                'ok':not event.get('isError',False) and bool(output),
                                'reserve_seen':'MySqlMessageReservations::Reserve' in output})
                        return super().put(event,*args,**kwargs)
                # No prompt/tool is submitted until start() completes.
                self.events=Events(maxsize=4096)
        def grant(agent,runtime,actor=7,room=1):
            request_id='ra_'+hashlib.sha256(secrets.token_bytes(32)).hexdigest()
            value=dict(kind='room_agent_v1',room_id=room,agent_id=agent,actor_user_id=actor,owner_user_id=7,
                       bot_user_id=900000000402 if runtime=='hermes_native' else 900000000401,
                       generation=1,workspace_generation=1,project_id='spark-push',policy='read_only',
                       runtime=runtime,request_id=request_id,source_msg_id='r_1-1')
            grants[request_id]=value;return value
        prompt=('Use your read tool to read PROJECT_FILES.txt, then probe.cpp and logic/message_reservation.cpp. '
                'Report the EXACT acceptance nonce from probe.cpp, name MySqlMessageReservations::Reserve, '
                'and give one concrete code review observation about its transaction or duplicate handling. '
                'Cite both relative paths. Do not run shell or claim tests passed.')
        def check(ok,label):
            if not ok:raise AssertionError(label)
            receipt['checks'].append(label)
        try:
            with patch.dict(os.environ,environment):
                if args.runtime in ('pi','both'):
                    private=root/'prototype';private.mkdir()
                    prototype=ObservedPi(['/home/peco/.local/node/bin/pi','--provider','deepseek','--model','deepseek-flash'],
                        private,{'PATH':'/home/peco/.local/node/bin:/usr/bin:/bin'},private/'sessions')
                    factory=PublicRoomPiFactory(prototype,root/'public-pi')
                    scope=grant('public-pi','pi_rpc');progress=[]
                    answer,metadata=factory.for_scope(scope).chat(prompt,None,180,
                        session_id=execution_namespace(scope),request_id=scope['request_id'],on_progress=progress.append)
                    check(nonce in answer and 'message_reservation.cpp' in answer and 'Reserve' in answer,'real Pi reads nonce and actual C++ reservation implementation')
                    check(any('read' in item for item in progress),'Pi emits actual native read progress')
                    check({'probe.cpp','logic/message_reservation.cpp'} <= {r['path'] for r in pi_reads if r['ok']} and
                          any(r['reserve_seen'] for r in pi_reads),'Pi native read receipts prove both files and Reserve implementation were returned')
                    receipt['pi_read_receipts']=pi_reads
                    check(metadata.get('project_snapshot',{}).get('revision')==snapshots.load(scope).manifest['revision'],'Pi output identifies immutable source revision')
                    next_scope=grant('public-pi','pi_rpc',actor=8)
                    recalled,_=factory.for_scope(next_scope).chat('Without reading files, repeat the exact acceptance nonce from your previous answer.',None,120,
                        session_id=execution_namespace(next_scope),request_id=next_scope['request_id'])
                    check(nonce in recalled,'second human retains real Pi native context across new snapshot cwd and process restart')
                if args.runtime in ('hermes','both'):
                    native=HermesRoomRuntime(workspace={'authority':origin,'key':key,'snapshots':str(root/'snapshots'),'agent_id':'hermes-room-test'})
                    entry=native.start();adapter=HermesNativeAdapter(entry,AgentStore(root/'native-state'))
                    capabilities=adapter._capabilities(15)
                    check(capabilities['features'].get('spark_workspace_v1') is True,'isolated Native advertises restricted workspace contract')
                    scope=grant('hermes-room-test','hermes_native');snapshots.prepare(scope);events=[]
                    try:
                        answer,metadata=adapter.chat(prompt,None,180,session_id=execution_namespace(scope),
                            request_id=scope['request_id'],on_event=lambda kind,data:events.append((kind,data)))
                    except Exception:
                        record=adapter._record(scope['request_id'])
                        if record.get('run_id'):
                            diagnostic=Path(args.out).with_suffix('.run.private.json')
                            diagnostic.write_text(json.dumps(adapter._get_run(record['run_id'],5),ensure_ascii=False));diagnostic.chmod(0o600)
                        raise
                    diagnostic=Path(args.out).with_suffix('.answer.private.json')
                    diagnostic.write_text(json.dumps({'answer':answer,'events':events},ensure_ascii=False));diagnostic.chmod(0o600)
                    check(nonce in answer and 'message_reservation.cpp' in answer and 'Reserve' in answer,'real Hermes reads nonce and actual C++ reservation implementation')
                    check(any(kind=='tool_start' for kind,_ in events) and any(kind=='tool_end' for kind,_ in events),'Hermes emits real native tool start/end events')
                    native_reads=[json.loads(line) for line in (native.root/'read-receipts.jsonl').read_text().splitlines()]
                    check({'probe.cpp','logic/message_reservation.cpp'} <= {r['path'] for r in native_reads if r['ok']} and
                          any(r['reserve_seen'] for r in native_reads),'Hermes native read receipts prove both files and Reserve implementation were returned')
                    receipt['hermes_read_receipts']=native_reads
                    before=len(authority_calls)
                    replay,_=adapter.chat(prompt,None,15,session_id=execution_namespace(scope),request_id=scope['request_id'])
                    check(replay==answer and len(authority_calls)==before,'Native completed request replays without another tool execution')
                    next_scope=grant('hermes-room-test','hermes_native',actor=8);snapshots.prepare(next_scope)
                    recalled,_=adapter.chat('Without reading files, repeat the exact acceptance nonce from your previous answer.',None,120,
                        session_id=execution_namespace(next_scope),request_id=next_scope['request_id'])
                    check(nonce in recalled,'second human retains real Hermes native context across independent requests')
            receipt['ok']=True
        finally:
            if adapter:adapter.stop()
            if native:
                if native.log:native.log.flush()
                if (native.root/'runtime.log').exists():
                    diagnostic=Path(args.out).with_suffix('.native.private.log')
                    shutil.copyfile(native.root/'runtime.log',diagnostic);diagnostic.chmod(0o600)
                native.close()
            server.shutdown();server.server_close();thread.join()
            receipt['authority_calls']=len(authority_calls)
            receipt['source_cpp_sha256']=hashlib.sha256((ROOT/'logic/message_reservation.cpp').read_bytes()).hexdigest()
            Path(args.out).write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps(receipt,ensure_ascii=False))

if __name__=='__main__':main()
