"""Opt-in real public launchers; no fake runtime or permission authority."""
import json
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
        self.command=[self.python,self.win(ROOT/'cannbot/scripts/hermes_public_gateway.py'),'--config',self.win(path)]
        self.launch()
        self.entry={'id':'public-hermes','name':'Hermes','runtime':'hermes_native','owner_user_ids':[],
            'bot_user_id':900000000402,'room_enabled':True,'room_policy':'read_only','room_public_profile':True,
            'profile':'spark-public-hermes','room_workspace_contract':1,'native_interactive_approvals':False,
            'transport':'windows_stdio','windows_python':self.python,'base_url':f'http://127.0.0.1:{port}/v1',
            'expected_model':'spark-public-hermes','credential_env_file':str(self.root/'api.env'),'credential_env_key':'API_SERVER_KEY'}
        return self.entry

    def launch(self):
        self.log=open(self.root/'runtime.log','w')
        self.proc=subprocess.Popen(self.command,stdout=self.log,stderr=subprocess.STDOUT)
        for _ in range(600):
            if 'Public Hermes Native ready' in (self.root/'runtime.log').read_text(errors='replace'):return
            if self.proc.poll() is not None:raise RuntimeError('public Native startup failed; inspect owned runtime.log')
            time.sleep(.2)
        raise TimeoutError('public Native startup timeout')

    def stop_process(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:self.proc.wait(timeout=15)
            except subprocess.TimeoutExpired:self.proc.kill();self.proc.wait(timeout=10)
        if self.log:self.log.close();self.log=None

    def close(self):
        self.stop_process()
        super().close()


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
        path=self.root/'config.json';path.write_text(json.dumps(self.config));path.chmod(0o600)
        self.log=open(self.root/'gateway.log','w')
        self.proc=subprocess.Popen([sys.executable,str(ROOT/'cannbot/scripts/public_agent_gateway.py'),'--config',str(path)],
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
