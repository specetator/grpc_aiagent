#!/usr/bin/env python3
"""Real OS processes/locks, plus optional actual isolated Native startup/stop.

The process fixtures do not call models or stand in for Agent execution tests.
An OS reboot is not performed; reboot-audit success has separate contract tests.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import queue
import secrets
import signal
import subprocess
import sys
import tempfile
import threading
import time

ROOT=Path(__file__).resolve().parents[1]
SCRIPTS=ROOT/'cannbot/scripts'
OWNER=r'''
import sys,json,os,subprocess
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from public_runtime import PublicHome,process_identity
root=Path(sys.argv[2]);mode=sys.argv[3]
identity=json.loads(sys.argv[4])
with PublicHome(root,identity) as home:
    if identity['runtime']=='hermes_native' and not (root/'config.yaml').exists():
        policy={'platform_toolsets':{'api_server':['todo','spark_workspace']},'tools':{'tool_search':{'enabled':'off'}},
            'memory':{'memory_enabled':False,'user_profile_enabled':False},'mcp_servers':{},'checkpoints':{'enabled':False},'approvals':{'mode':'manual'}}
        home.write_new('config.yaml',json.dumps(policy))
    if not (root/'ledger-fixture.json').exists():home.write_new('ledger-fixture.json','{"status":"unknown","request_id":"unchanged"}')
    if mode!='exit':
        child=None
        if mode=='orphan':
            code="import sys,time;from pathlib import Path;p=Path(sys.argv[1]);\nwhile True:\n with p.open('a') as f:f.write('x')\n time.sleep(.05)"
            child=subprocess.Popen([sys.executable,'-c',code,str(root/'orphan-progress')],stdin=subprocess.DEVNULL,
                                   stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
        print(json.dumps({'state':'running','pid':os.getpid(),'child_pid':child.pid if child else None,
                          'child_identity':process_identity(child.pid) if child else None}),flush=True)
        sys.stdin.readline()
if mode=='linger':
    print(json.dumps({'state':'clean-but-alive','pid':os.getpid()}),flush=True)
    sys.stdin.readline()
'''


def event(process):
    """Bound process handshakes on both Windows pipes and POSIX pipes."""
    result=queue.Queue()
    def read():
        try:result.put(process.stdout.readline())
        except Exception as error:result.put(error)
    threading.Thread(target=read,daemon=True).start()
    try:value=result.get(timeout=45)
    except queue.Empty:raise TimeoutError('lifecycle process did not reach its handshake') from None
    if isinstance(value,Exception):raise value
    return json.loads(value)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--windows-python')
    parser.add_argument('--native-live',action='store_true')
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    receipt={'ok':False,'mode':'real OS process/lock integration; no model invocation',
             'checks':[],'sources':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
             for p in [SCRIPTS/name for name in ('public_runtime.py','public_runtime_maintenance.py',
                 'public_agent_gateway.py','hermes_public_gateway.py','pi_gateway.py','room_agent.py','agent_router.py')]
                 + [ROOT/'tests/public_runtime_fixture.py',Path(__file__)]}}
    def check(label):receipt['checks'].append(label);print(label,flush=True)
    def win(path):return subprocess.check_output(['wslpath','-w',str(path)],text=True).strip()
    try:
        with tempfile.TemporaryDirectory(prefix='spark-runtime-lifecycle-',dir='/mnt/f' if args.windows_python else None) as temporary:
            root=Path(temporary)
            for platform,python in [('posix',sys.executable)]+([('nt',args.windows_python)] if args.windows_python else []):
                path=lambda value:win(value) if platform=='nt' else str(value)
                service=root/platform;service.mkdir()
                home=service/'home';config=service/'config.json';registry=service/'agents.json'
                registry.write_text(json.dumps({'schema':1,'agents':[{'id':'pi','runtime':'pi_rpc',
                    'owner_user_ids':[],'room_enabled':True,'room_policy':'read_only'}]}))
                config.write_text(json.dumps({'schema':1,'id':'lifecycle-test','home':path(home),'registry':path(registry)}))
                identity={'runtime':'public_gateway','id':'lifecycle-test'}
                if platform=='nt':
                    identity={'runtime':'hermes_native','agent_id':'public-hermes','profile':'spark-public-lifecycle'}
                    config.write_text(json.dumps({'schema':1,'home':path(home),'profile':identity['profile'],'agent_id':identity['agent_id']}))
                command=[python,path(SCRIPTS/'public_runtime_maintenance.py')]
                def maintenance(operation='check',expected=0):
                    result=subprocess.run(command+[operation,'--runtime',identity['runtime'],'--config',path(config)],
                                          capture_output=True,text=True,timeout=30)
                    assert result.returncode==expected,(platform,operation,result.stderr[-300:])
                    value=json.loads(result.stdout);assert value['ok']==(expected==0)
                    return value
                def owner(mode):
                    return subprocess.Popen([python,'-c',OWNER,path(SCRIPTS),path(home),mode,json.dumps(identity)],
                                            stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                maintenance(expected=1);assert not home.exists()
                proc=owner('linger')
                try:
                    assert event(proc)['state']=='running'
                    maintenance(expected=1)
                    proc.stdin.write('stop\n');proc.stdin.flush()
                    assert event(proc)['state']=='clean-but-alive'
                    maintenance(expected=1)
                    proc.communicate('exit\n',timeout=30);assert proc.returncode==0
                finally:
                    if proc.poll() is None:proc.kill();proc.wait(timeout=15)
                evidence=maintenance();assert evidence['platform']==platform
                before={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in home.iterdir() if p.is_file()}
                batch=secrets.token_hex(16)
                lease=subprocess.Popen(command+['freeze','--runtime',identity['runtime'],'--config',path(config),'--batch-id',batch],
                                       stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                try:
                    ready=event(lease);assert ready['state']=='frozen' and ready['batch_id']==batch
                    contender=owner('exit');contender.communicate(timeout=30);assert contender.returncode!=0
                    output,error=lease.communicate(json.dumps({'operation':'release','batch_id':batch})+'\n',timeout=30)
                    assert lease.returncode==0 and json.loads(output)['state']=='released',error[-300:]
                finally:
                    if lease.poll() is None:lease.kill();lease.wait(timeout=15)
                assert before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in home.iterdir() if p.is_file()}
                check(platform+': missing home refused; active/closed-but-alive owner refused; exited owner freezes and blocks restart without changing files')
                for invalid in ('', '{\n', json.dumps({'operation':'release','batch_id':'wrong-batch'})+'\n'):
                    lease=subprocess.Popen(command+['freeze','--runtime',identity['runtime'],'--config',path(config),'--batch-id',batch],
                                           stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
                    try:
                        assert event(lease)['state']=='frozen'
                        contender=owner('exit');contender.communicate(timeout=30);assert contender.returncode!=0
                        output,error=lease.communicate(invalid,timeout=30)
                        values=[json.loads(line) for line in output.splitlines()]
                        assert lease.returncode==1 and values and values[-1]['state']=='refused'
                        assert not any(value.get('state')=='released' or value.get('ok') for value in values)
                    finally:
                        if lease.poll() is None:lease.kill();lease.wait(timeout=15)
                    maintenance()
                    assert before=={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in home.iterdir() if p.is_file()}
                check(platform+': EOF, malformed JSON and wrong batch fail without release success; lock is held until failure and files stay unchanged')
                proc=owner('orphan' if platform=='posix' else 'wait');child=None
                try:
                    info=event(proc);assert info['state']=='running'
                    if platform=='posix':
                        sys.path.insert(0,str(SCRIPTS))
                        from public_runtime import process_identity
                        child=(info['child_pid'],info['child_identity'])
                    proc.kill();proc.wait(timeout=15)
                    if child:
                        assert process_identity(child[0])==child[1]
                        progress=home/'orphan-progress'
                        deadline=time.monotonic()+5
                        while not progress.exists() and time.monotonic()<deadline:time.sleep(.02)
                        count=progress.stat().st_size;time.sleep(.15)
                        assert progress.stat().st_size>count
                    maintenance(expected=1)
                    restarted=owner('exit');restarted.communicate(timeout=30);assert restarted.returncode==0
                    maintenance(expected=1);maintenance('audit-after-reboot',expected=1)
                    assert json.loads((home/'.spark-runtime-state.json').read_text())['unclean_latched']
                    assert (home/'ledger-fixture.json').read_text()=='{"status":"unknown","request_id":"unchanged"}'
                finally:
                    if proc.poll() is None:proc.kill();proc.wait(timeout=15)
                    if child and process_identity(child[0])==child[1]:
                        os.kill(child[0],signal.SIGTERM)
                        deadline=time.monotonic()+5
                        while process_identity(child[0])==child[1] and time.monotonic()<deadline:time.sleep(.02)
                        assert process_identity(child[0]) is None
                check(platform+': forced owner termination remains ineligible after restart; same-boot audit refused and fixture journal unchanged')
                if child:check('posix: an actual independently grouped writer survives parent kill; maintenance refuses throughout and the owned writer is reaped')
            if args.native_live:
                from public_runtime_fixture import PublicHermesRuntime
                snapshots=root/'native-snapshots';snapshots.mkdir()
                runtime=PublicHermesRuntime(workspace={'key':secrets.token_hex(32),'authority':'http://127.0.0.1:19999',
                                                      'snapshots':str(snapshots)})
                try:
                    # Real installed API/tool discovery, but no Agent request;
                    # there is no fake authorization response or model success.
                    runtime.start()
                    command=[runtime.python,win(SCRIPTS/'public_runtime_maintenance.py'),'check','--runtime','hermes_native',
                             '--config',win(runtime.root/'public-config.json')]
                    active=subprocess.run(command,capture_output=True,text=True,timeout=30);assert active.returncode==1
                    runtime.stop_process()
                    stopped=subprocess.run(command,capture_output=True,text=True,timeout=30)
                    assert stopped.returncode==0,stopped.stderr[-300:]
                    assert json.loads(stopped.stdout)['lifecycle']['phase']=='clean'
                    check('actual isolated Windows Native API starts, refuses live freeze, and reaches provably stopped state through operator stdin shutdown')
                finally:runtime.close()
                check('original Windows model profile hashes preserved; only owned Native fixture removed')
            receipt['ok']=True
    finally:
        args.out.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({'ok':receipt['ok'],'checks':len(receipt['checks'])}),flush=True)


if __name__=='__main__':main()
