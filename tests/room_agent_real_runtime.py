#!/usr/bin/env python3
"""T14: real isolated Windows Hermes API + actual default-profile model.
Requires explicit user authorization to use the local profile's model auth.
No mock model/runtime; only one transport interruption is injected after POST.
"""
import argparse,json,sys,time,threading,concurrent.futures,subprocess
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'cannbot/scripts'))
from hermes_room_runtime_fixture import HermesRoomRuntime
from hermes_native_adapter import HermesNativeAdapter,NativeUnknown,NativeTerminal
from agent_router import AgentStore

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--out',required=True);args=parser.parse_args()
    output=Path(args.out);output.parent.mkdir(parents=True,exist_ok=True)
    receipt={'ok':False,'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),
             'working_tree':True,'model':'authorized Windows default profile model','runtime':'real Windows Hermes Native','checks':[]}
    fixture=HermesRoomRuntime();adapters=[];posts=[]
    def require(condition,name):
        if not condition:raise AssertionError(name)
        receipt['checks'].append(name)
    def make(entry,store):
        adapter=HermesNativeAdapter(entry,store);adapters.append(adapter);admit=adapter._admit
        def tracked(*args,**kwargs):posts.append(args[0]);return admit(*args,**kwargs)
        adapter._admit=tracked;return adapter
    try:
        entry=fixture.start();store=AgentStore(fixture.root/'spark-state');adapter=make(entry,store)
        adapter._capabilities(10)
        enabled=[t['name'] for t in adapter._json('/toolsets')['data'] if t['enabled']]
        require(enabled==['todo'],'native runtime exposes only per-Agent plan tool; file/shell/memory/session search disabled')
        events=[];prompt='Call todo_list with NO parameters to READ your plan. Then answer with one short sentence. Do not edit the plan.'
        text,meta=adapter.chat(prompt,None,120,session_id='roomagent_100_hermes-room-test',request_id='native-real-tools',on_event=lambda kind,data:events.append((kind,data)))
        require(bool(text),'real model produces completed native output')
        require(any(kind=='tool_start' for kind,_ in events) and any(kind=='tool_end' for kind,_ in events),'real tool start/end events observed')
        before=len(posts);same,_=adapter.chat(prompt,None,10,session_id='roomagent_100_hermes-room-test',request_id='native-real-tools')
        require(same==text and len(posts)==before,'same request replays stored result without another POST')
        restored=make(entry,AgentStore(fixture.root/'spark-state'))
        same,_=restored.chat(prompt,None,10,session_id='roomagent_100_hermes-room-test',request_id='native-real-tools')
        require(same==text and len(posts)==before,'adapter restart recovers durable native result without repeating tools')
        # Drop local SSE handling only AFTER admission. Recovery must GET the same Run.
        lost=make(entry,store)
        def disconnect(*args,**kwargs):raise NativeUnknown('injected transport loss after native admission')
        lost._read_events=disconnect
        try:lost.chat('Reply with the word recovered.',None,30,session_id='roomagent_101_hermes-room-test',request_id='native-real-recover')
        except NativeUnknown:pass
        else:raise AssertionError('expected unknown after dropped transport')
        record=lost._record('native-real-recover');require(record['status']=='unknown' and bool(record['run_id']),'disconnect remains unknown with original Run ID')
        before=len(posts);deadline=time.monotonic()+120
        while time.monotonic()<deadline:
            status=restored._get_run(record['run_id'],5)['status']
            if status in {'completed','failed','cancelled','interrupted'}:break
            time.sleep(.2)
        recovered,_=restored.recover_turn('roomagent_101_hermes-room-test','native-real-recover',{})
        require(bool(recovered) and len(posts)==before,'unknown recovers original real Run by GET with no second POST')
        # Native stop targets the exact request, independently of the execution lock.
        stopping=make(entry,store)
        def long_turn():
            try:return stopping.chat('Read todo_list repeatedly at least ten times before a final short answer.',None,120,session_id='roomagent_102_hermes-room-test',request_id='native-real-stop')
            except (NativeTerminal,NativeUnknown):return None
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future=pool.submit(long_turn);deadline=time.monotonic()+30
            while time.monotonic()<deadline and not stopping._record('native-real-stop').get('run_id'):time.sleep(.05)
            record=stopping._record('native-real-stop');require(bool(record.get('run_id')),'stop target is a real admitted Run')
            result=stopping.room_control('roomagent_102_hermes-room-test','native-real-stop','stop')
            deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                status=stopping._get_run(record['run_id'],5)['status']
                if status in {'cancelled','failed','interrupted','completed'}:break
                time.sleep(.1)
            require(status in {'cancelled','interrupted'},'real stop reaches non-success terminal state')
            future.result(timeout=30)
        before=len(posts)
        try:restored.recover_turn('roomagent_102_hermes-room-test','native-real-stop',{})
        except NativeTerminal:pass
        else:raise AssertionError('cancelled execution must not become success')
        require(len(posts)==before,'cancelled result recovery never reruns tools')
        try:stopping.room_control('roomagent_102_hermes-room-test','native-real-stop','approve','invented')
        except ValueError:pass
        else:raise AssertionError('approval must stay unavailable in public read-only mode')
        require(True,'public approval unavailable; no invented success')
        # Fault only this fixture's model selection. The real provider rejects
        # the nonexistent model and Hermes must persist run.failed.
        failing=make(entry,store);session='roomagent_103_hermes-room-test'
        state=failing._state(session);state['selection']={'provider':'openai-codex','public':{'id':'spark-nonexistent-acceptance-model'}}
        store.put(failing.namespace,session,state)
        try:failing.chat('Reply briefly.',None,60,session_id=session,request_id='native-real-failure')
        except NativeTerminal:pass
        else:raise AssertionError('invalid model must fail as a real native Run')
        failed=failing._record('native-real-failure')
        require(failed.get('status')=='failed' and failing._get_run(failed['run_id'],5)['status']=='failed','actual provider failure persists run.failed, not a successful partial response')
        before=len(posts)
        try:restored.recover_turn(session,'native-real-failure',{})
        except NativeTerminal:pass
        else:raise AssertionError('failed Run must not recover as success')
        require(len(posts)==before,'failed Run recovery never reposts execution')
        receipt['ok']=True
    except Exception as exc:
        receipt['error']=type(exc).__name__+': '+str(exc)
        if (fixture.root/'runtime.log').exists():
            # Private local diagnostics, not part of the public test receipt.
            diagnostics=output.with_suffix('.runtime.log');diagnostics.write_bytes((fixture.root/'runtime.log').read_bytes());diagnostics.chmod(0o600)
        raise
    finally:
        for adapter in adapters:adapter.stop()
        fixture.close();receipt['original_profile_unchanged']=True;receipt['native_post_count']=len(posts)
        output.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({k:v for k,v in receipt.items() if k!='checks'},ensure_ascii=False))
if __name__=='__main__':main()
