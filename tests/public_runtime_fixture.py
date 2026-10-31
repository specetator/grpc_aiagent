"""Opt-in real public launchers; no fake runtime or permission authority."""
import json
import hashlib
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import time

from hermes_room_runtime_fixture import HermesRoomRuntime

ROOT=Path(__file__).resolve().parents[1]


class PublicHermesRuntime(HermesRoomRuntime):
    def start(self):
        self.root.mkdir(mode=0o700)
        model,auth=self.model_credentials()
        auth_file=self.root/'provision-auth.json'
        auth_file.write_text(json.dumps(auth));auth_file.chmod(0o600)
        key=secrets.token_hex(32)
        (self.root/'api-key').write_text(key);(self.root/'api-key').chmod(0o600)
        (self.root/'api.env').write_text('API_SERVER_KEY='+key+'\n');(self.root/'api.env').chmod(0o600)
        keys=self.root/'keys.json'
        keys.write_text(json.dumps({'SPARK_PUSH_WORKSPACE_AUTH_KEY':self.workspace['key']}));keys.chmod(0o600)
        port=int(subprocess.check_output([self.python,'-c','import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'],text=True).strip())
        config={'schema':1,'profile':'spark-public-hermes','agent_id':'public-hermes','source':self.win(self.source),
            'home':self.win(self.root/'service'),'model':model,'model_auth_file':self.win(auth_file),
            'keys_file':self.win(keys),'api_key_file':self.win(self.root/'api-key'),
            'authority_origin':self.workspace['authority'],'snapshots':self.win(Path(self.workspace['snapshots'])),
            'git_bash':self.win(Path('/mnt/f/Git/bin/bash.exe')),'port':port}
        path=self.root/'public-config.json';path.write_text(json.dumps(config));path.chmod(0o600)
        self.command=[self.python,self.win(ROOT/'cannbot/scripts/hermes_public_gateway.py'),'--config',self.win(path),'--control-stdin']
        self.launch()
        self.entry={'id':'public-hermes','name':'Hermes','runtime':'hermes_native','owner_user_ids':[],
            'bot_user_id':900000000402,'room_enabled':True,'room_policy':'read_only','room_public_profile':True,
            'profile':'spark-public-hermes','room_workspace_contract':1,'native_interactive_approvals':False,
            'transport':'windows_stdio','windows_python':self.python,'base_url':f'http://127.0.0.1:{port}/v1',
            'expected_model':'spark-public-hermes','credential_env_file':str(self.root/'api.env'),'credential_env_key':'API_SERVER_KEY'}
        return self.entry

    def launch(self):
        self.log=open(self.root/'runtime.log','w')
        self.proc=subprocess.Popen(self.command,stdin=subprocess.PIPE,stdout=self.log,stderr=subprocess.STDOUT)
        for _ in range(600):
            if 'Public Hermes Native ready' in (self.root/'runtime.log').read_text(errors='replace'):return
            if self.proc.poll() is not None:raise RuntimeError('public Native startup failed; inspect owned runtime.log')
            time.sleep(.2)
        raise TimeoutError('public Native startup timeout')

    def stop_process(self):
        forced=False
        if self.proc and self.proc.poll() is None:
            try:self.proc.stdin.write(b'shutdown\n');self.proc.stdin.flush()
            except (BrokenPipeError,OSError):pass
            try:self.proc.wait(timeout=120)
            except subprocess.TimeoutExpired:
                forced=True;self.proc.kill();self.proc.wait(timeout=10)
        if self.proc and self.proc.stdin:self.proc.stdin.close()
        if self.log:self.log.close();self.log=None
        if forced:raise TimeoutError('public Native did not finish controlled shutdown')

    def close(self):
        try:self.stop_process()
        finally:super().close()


class PublicAgentGateway:
    def __init__(self,root,registry,projects,keys,authority,snapshots,port):
        self.root=Path(root);self.proc=None;self.log=None
        self.config={'schema':1,'id':'acceptance','home':str(self.root/'service'),
            'registry':str(registry),'projects':str(projects),'keys_file':str(keys),'authority_origin':authority,
            'snapshots':str(snapshots),'host':'0.0.0.0','port':port,
            'pi':{'command':'/home/peco/.local/node/bin/pi','provider':'deepseek','model':'deepseek-flash',
                  'models_file':'/home/peco/.pi-spark-agent/models.json','model_env_file':'/home/peco/.pi-spark-agent/.env'}}

    def start(self):
        self.root.mkdir(mode=0o700,exist_ok=True)
        # Explicit test provisioning into owned public files. The backup
        # utility must never copy an entire private provider catalog or .env.
        if not (self.root/'public-models.json').exists():
            sys.path.insert(0,str(ROOT/'cannbot/scripts'))
            from public_runtime import model_environment
            pi=self.config['pi'];source=Path(pi['models_file']);environment=Path(pi['model_env_file'])
            self.private_hashes={path:hashlib.sha256(path.read_bytes()).hexdigest() for path in (source,environment)}
            provider=json.loads(source.read_text())['providers'][pi['provider']]
            selected={**provider,'models':[m for m in provider['models'] if m['id']==pi['model']]}
            (self.root/'public-models.json').write_text(json.dumps({'providers':{pi['provider']:selected}}))
            (self.root/'public-model.env').write_text(''.join(k+'='+v+'\n' for k,v in model_environment(environment).items()))
            for name in ('public-models.json','public-model.env'):(self.root/name).chmod(0o600)
        self.config['pi'].update(models_file=str(self.root/'public-models.json'),model_env_file=str(self.root/'public-model.env'))
        path=self.root/'config.json';path.write_text(json.dumps(self.config));path.chmod(0o600)
        self.log=open(self.root/'gateway.log','w')
        command=[sys.executable,str(ROOT/'cannbot/scripts/public_agent_gateway.py'),'--config',str(path)]
        if getattr(self,'native_fault_marker',None):
            # Fault injection loses real Native event/status responses in this
            # owned Gateway process. Admission and model/tool execution remain
            # real; no response, authorization or successful result is mocked.
            bootstrap=self.root/'fault-gateway.py'
            bootstrap.write_text('''import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1])
from hermes_native_adapter import HermesNativeAdapter,NativeUnknown
from public_agent_gateway import main
marker=Path(sys.argv[2]);original_read=HermesNativeAdapter._read_events;original_recover=HermesNativeAdapter.recover_turn
def read(self,*args,**kwargs):
 if marker.exists():raise NativeUnknown('Injected loss after actual Native admission')
 return original_read(self,*args,**kwargs)
def recover(self,*args,**kwargs):
 if marker.exists():raise NativeUnknown('Injected Native status transport outage')
 return original_recover(self,*args,**kwargs)
HermesNativeAdapter._read_events=read;HermesNativeAdapter.recover_turn=recover
sys.argv=[sys.argv[0],*sys.argv[3:]]
raise SystemExit(main())
''')
            command=[sys.executable,str(bootstrap),str(ROOT/'cannbot/scripts'),str(self.native_fault_marker),'--config',str(path)]
        self.proc=subprocess.Popen(command,
                                   stdout=self.log,stderr=subprocess.STDOUT,start_new_session=True)
        for _ in range(300):
            if 'Public Agent Gateway ready' in (self.root/'gateway.log').read_text(errors='replace'):return
            if self.proc.poll() is not None:raise RuntimeError('public Gateway startup failed; inspect owned gateway.log')
            time.sleep(.2)
        raise TimeoutError('public Gateway startup timeout')

    def stop(self):
        if self.proc and self.proc.poll() is None:
            os.killpg(self.proc.pid,signal.SIGTERM)
            try:self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:os.killpg(self.proc.pid,signal.SIGKILL);self.proc.wait(timeout=10)
        if self.log:self.log.close();self.log=None
        if hasattr(self,'private_hashes') and any(hashlib.sha256(p.read_bytes()).hexdigest()!=h for p,h in self.private_hashes.items()):
            raise RuntimeError('private Pi model configuration changed during fixture')
