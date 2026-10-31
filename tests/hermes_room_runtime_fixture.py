"""Opt-in Windows Hermes Native fixture using an existing profile's MODEL auth.

Never launches its Telegram gateway or copies its memory, sessions, tools, .env,
Skills, working directory or refresh tokens. Owns only a fresh temporary root.
"""
import base64,copy,hashlib,json,os,secrets,shutil,subprocess,time
from pathlib import Path
import yaml

BOOTSTRAP=r'''
import asyncio,json,os,sys
from pathlib import Path
root=Path(sys.argv[1]);source=Path(sys.argv[2]);port=int(sys.argv[3])
keep={k:v for k,v in os.environ.items() if k.upper() in {'SYSTEMROOT','WINDIR','PATH','PATHEXT','COMSPEC','TEMP','TMP','HTTP_PROXY','HTTPS_PROXY','NO_PROXY'}}
os.environ.clear();os.environ.update(keep)
os.environ.update(HERMES_HOME=str(root),HOME=str(root/'home'),USERPROFILE=str(root/'home'),
                  HERMES_QUIET='1',TERMINAL_CWD=str(root/'workspace'),HERMES_EXEC_ASK='1',PYTHONUTF8='1')
os.chdir(root/'workspace');sys.path.insert(0,str(source))
from gateway.config import PlatformConfig
from gateway.platforms.api_server import APIServerAdapter
workspace_config=root/'spark-workspace.json'
expected_tools={'todo'}
if workspace_config.exists():
    workspace=json.loads(workspace_config.read_text())
    # The Windows native read tool uses Git Bash internally. WSL bash.exe
    # cannot resolve its /f/... paths. This is operator fixture configuration,
    # never a model-visible shell tool or a copy of the private profile's env.
    os.environ['HERMES_GIT_BASH_PATH']=workspace['git_bash']
    sys.path.insert(0,workspace['scripts'])
    from room_workspace import ProjectSnapshots,WorkspaceAuthority
    from hermes_public_workspace import public_api_adapter
    from tools import file_tools
    import threading
    receipt_lock=threading.Lock()
    native_read=file_tools.read_file_tool
    def diagnostic_read(*args,**kwargs):
        result=native_read(*args,**kwargs)
        value=json.loads(result)
        print('public_native_read',json.dumps({k:value[k] for k in ('success','error','note') if k in value}),flush=True)
        relative=Path(args[0]).relative_to(Path(workspace['snapshots'])).parts
        receipt={'request_id':relative[0],'path':'/'.join(relative[2:]),
                 'ok':not value.get('error') and bool(value.get('content')),
                 'reserve_seen':'MySqlMessageReservations::Reserve' in value.get('content','')}
        with receipt_lock:
            with (root/'read-receipts.jsonl').open('a') as output:output.write(json.dumps(receipt)+'\n')
        return result
    file_tools.read_file_tool=diagnostic_read
    APIServerAdapter=public_api_adapter(APIServerAdapter,
        WorkspaceAuthority(workspace['authority'],workspace['key']),
        ProjectSnapshots({'schema':1,'projects':[]},Path(workspace['snapshots'])),workspace['agent_id'],'spark-room-test-default')
    expected_tools.add('spark_workspace')
async def main():
    adapter=APIServerAdapter(PlatformConfig(enabled=True,extra={'host':'127.0.0.1','port':port,
        'key':(root/'api-key').read_text().strip(),'model_name':'spark-room-test-default'}))
    if not await adapter.connect():raise RuntimeError('isolated API startup failed')
    # Tool discovery imports can be slow on a cold Windows interpreter. Ready
    # means the actual policy catalog is queryable, not just a bound socket.
    import aiohttp
    async with aiohttp.ClientSession() as client:
        async with client.get(f'http://127.0.0.1:{port}/v1/toolsets',headers={'Authorization':'Bearer '+(root/'api-key').read_text().strip()},timeout=aiohttp.ClientTimeout(total=45)) as response:
            if response.status!=200:raise RuntimeError('tool policy preflight failed')
            catalog=await response.json()
            if {row['name'] for row in catalog['data'] if row['enabled']}!=expected_tools:raise RuntimeError('unexpected public tools')
    (root/'ready').write_text('ready')
    try:
        while not (root/'stop').exists():await asyncio.sleep(.2)
    finally:await adapter.disconnect()
asyncio.run(main())
'''

class HermesRoomRuntime:
    def __init__(self, profile=Path('/mnt/f/hermes'), source=Path('/mnt/f/hermes/hermes-agent'), workspace=None):
        self.profile,self.source=Path(profile),Path(source)
        self.python=str(self.source/'venv/Scripts/python.exe')
        self.root=self.profile.parent/('spark-room-test-'+secrets.token_hex(8))
        self.hashes={p:hashlib.sha256((self.profile/p).read_bytes()).hexdigest() for p in ('config.yaml','.env','auth.json') if (self.profile/p).exists()}
        self.proc=None;self.log=None
        self.workspace=workspace
    @staticmethod
    def win(path):return subprocess.check_output(['wslpath','-w',str(path)],text=True).strip()
    def start(self):
        self.root.mkdir(mode=0o700)
        for name in ('home','workspace'): (self.root/name).mkdir()
        original=yaml.safe_load((self.profile/'config.yaml').read_text())
        model=original.get('model',{})
        if model.get('provider')!='openai-codex':raise ValueError('fixture currently supports the authorized default Codex model profile only')
        auth=json.loads((self.profile/'auth.json').read_text())
        state=copy.deepcopy(auth['providers']['openai-codex'])
        token=state['tokens']['access_token'];payload=token.split('.')[1]
        expiry=json.loads(base64.urlsafe_b64decode(payload+'='*((-len(payload))%4))).get('exp',0)
        if expiry<time.time()+900:raise ValueError('model access token expires too soon; fixture never refreshes the original account')
        state['tokens'].pop('refresh_token',None)
        isolated={'version':auth.get('version',1),'active_provider':'openai-codex','providers':{'openai-codex':state}}
        (self.root/'auth.json').write_text(json.dumps(isolated));(self.root/'auth.json').chmod(0o600)
        config={'model':{'default':model['default'],'provider':'openai-codex'},
            'agent':{'max_turns':6},'platform_toolsets':{'api_server':['todo']},
            'memory':{'memory_enabled':False,'user_profile_enabled':False},'mcp_servers':{},
            'approvals':{'mode':'manual'},'terminal':{'cwd':self.win(self.root/'workspace')},
            'gateway':{'api_server':{'max_concurrent_runs':2}},'checkpoints':{'enabled':False}}
        if self.workspace:
            # Public policy requires an explicit native tool surface. Do not
            # allow the generic deferred-tool bridge to conceal that surface.
            config['tools']={'tool_search':{'enabled':'off'}}
            config['platform_toolsets']['api_server'].append('spark_workspace')
            public={**self.workspace,'scripts':self.win(Path(__file__).resolve().parents[1]/'cannbot/scripts'),
                    'git_bash':self.win(Path('/mnt/f/Git/bin/bash.exe')),
                    'snapshots':self.win(Path(self.workspace['snapshots']))}
            (self.root/'spark-workspace.json').write_text(json.dumps(public))
            (self.root/'spark-workspace.json').chmod(0o600)
        (self.root/'config.yaml').write_text(yaml.safe_dump(config))
        key=secrets.token_hex(32);(self.root/'api-key').write_text(key);(self.root/'api-key').chmod(0o600)
        (self.root/'api.env').write_text('API_SERVER_KEY='+key+'\n');(self.root/'api.env').chmod(0o600)
        bootstrap=self.root/'bootstrap.py';bootstrap.write_text(BOOTSTRAP)
        port=int(subprocess.check_output([self.python,'-c','import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'],text=True).strip())
        self.log=open(self.root/'runtime.log','w')
        self.proc=subprocess.Popen([self.python,self.win(bootstrap),self.win(self.root),self.win(self.source),str(port)],stdout=self.log,stderr=subprocess.STDOUT)
        for _ in range(300):
            if (self.root/'ready').exists():break
            if self.proc.poll() is not None:raise RuntimeError('isolated Hermes startup failed; inspect owned runtime.log')
            time.sleep(.2)
        else:raise TimeoutError('isolated Hermes startup timed out')
        self.entry={'id':'hermes-room-test','name':'Hermes','runtime':'hermes_native','owner_user_ids':[],
            'bot_user_id':900000000402,'room_enabled':True,'room_policy':'read_only','room_test_only':True,
            'profile':'spark-room-test-default','native_interactive_approvals':False,
            'transport':'windows_stdio','windows_python':self.python,'base_url':f'http://127.0.0.1:{port}/v1',
            'expected_model':'spark-room-test-default','credential_env_file':str(self.root/'api.env'),'credential_env_key':'API_SERVER_KEY'}
        if self.workspace:self.entry['room_workspace_contract']=1
        return self.entry
    def close(self):
        if self.proc and self.proc.poll() is None:
            (self.root/'stop').touch()
            try:self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:self.proc.terminate();self.proc.wait(timeout=10)
        if self.log:self.log.close()
        changed=[name for name,digest in self.hashes.items() if hashlib.sha256((self.profile/name).read_bytes()).hexdigest()!=digest]
        if changed:raise RuntimeError('original profile files changed during testing: '+','.join(changed))
        if self.root.exists():shutil.rmtree(self.root)
