#!/usr/bin/env python3
"""Owned full Compose + three independent browsers, optionally real public Agents.

Creates users and all collaboration data through the product Web UI. No routes,
models, WS frames or authentication are mocked. Agent mode uses the explicitly
authorized Windows default model credential in fresh public runtime homes only.
"""
import argparse
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

from connector_deployment_e2e import ROOT, free_port, http


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='spark-push:web-collaboration')
    parser.add_argument('--agents',action='store_true')
    parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args();output=args.out.resolve();output.parent.mkdir(parents=True,exist_ok=True)
    # Fail before starting Compose or reading authorized model configuration
    # when the external browser dependency has not been installed/configured.
    subprocess.run(['node','-e',"require(process.env.SPARK_PLAYWRIGHT_MODULE || 'playwright')"],cwd=ROOT,check=True)
    receipt={'ok':False,'mode':'real Compose, three independent Chromium processes; '+('actual public Pi/Hermes' if args.agents else 'Agents disabled'),
             'source_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),'checks':[]}
    receipt['image_id']=subprocess.check_output(['docker','image','inspect',args.image,'--format','{{.Id}}'],text=True).strip()
    receipt['sources']={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for directory in ('web_demo/static','logic/services','logic/repositories')
                        for p in (ROOT/directory).iterdir() if p.is_file() and p.suffix in {'.html','.js','.css','.cpp','.h'}}
    for name in ('logic/http_server.cpp','logic/http_server.h','logic/main.cpp','deploy/Caddyfile','Dockerfile',
                 'tests/collaboration_web_e2e.py','tests/collaboration_web_live_browser_test.cjs'):
        receipt['sources'][name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    screenshots=output.parent/(output.stem+'-screenshots');screenshots.mkdir(exist_ok=True)
    native=gateway=process=None;started=False;job_paused=False
    with tempfile.TemporaryDirectory(prefix='spark-web-e2e-',dir='/mnt/f' if args.agents else None) as temporary:
        root=Path(temporary);root.chmod(0o755)
        environment={k:v for k,v in os.environ.items() if not k.startswith(('SPARK_','COMPOSE_','HERMES_','PI_','KERNEL_'))}
        port=free_port();environment.update(SPARK_PUSH_IMAGE=args.image,SPARK_PUSH_PORT=str(port),SPARK_PUSH_BIND='127.0.0.1')
        envfile=root/'.env';envfile.touch(mode=0o600)
        options=['--env-file',str(envfile),'-p','spark-web-'+secrets.token_hex(6),'-f',str(ROOT/'docker-compose.yml')]
        nonce=secrets.token_hex(12)
        def compose(*arguments):
            result=subprocess.run(['docker','compose',*options,*arguments],cwd=ROOT,env=environment,
                                  stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,timeout=300)
            target=output.with_suffix('.compose.private.log')
            with os.fdopen(os.open(target,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600),'w') as stream:stream.write(result.stdout)
            if result.returncode:raise RuntimeError('owned Compose operation failed: '+arguments[0])
            return result.stdout
        try:
            if args.agents:
                from public_runtime_fixture import PublicAgentGateway,PublicHermesRuntime
                source=root/'source';(source/'logic').mkdir(parents=True)
                shutil.copyfile(ROOT/'logic/message_reservation.cpp',source/'logic/message_reservation.cpp')
                (source/'probe.cpp').write_text('// Acceptance nonce: '+nonce+'\nint spark_workspace_probe() { return 17; }\n')
                config=root/'agents';config.mkdir(mode=0o755)
                projects=config/'projects.json';projects.write_text(json.dumps({'schema':1,'projects':[{'id':'spark-push','name':'Spark Push 示例代码','root':str(source),'paths':['probe.cpp','logic']}]}))
                registry=config/'agents.json';catalog=json.loads((ROOT/'deploy/agents/agents.json.example').read_text())
                keys={name:secrets.token_hex(32) for name in ('SPARK_PUSH_HERMES_API_KEY','SPARK_PUSH_WORKSPACE_AUTH_KEY')}
                keyfile=root/'connector-keys.json';keyfile.write_text(json.dumps(keys));keyfile.chmod(0o600)
                host=subprocess.check_output(['hostname','-I'],text=True).split()[0];authport,gatewayport=free_port(),free_port();authority=f'http://{host}:{authport}'
                environment.update(keys,SPARK_PUSH_AGENT_CONFIG_DIR=str(config),SPARK_PUSH_AGENT_AUTH_PORT=str(authport),SPARK_PUSH_AGENT_AUTH_BIND=host,SPARK_PUSH_AGENT_GATEWAY_ORIGIN=f'http://{host}:{gatewayport}')
                options.extend(['-f',str(ROOT/'docker-compose.agents.yml'),'--profile','agents'])
                native=PublicHermesRuntime(workspace={'authority':authority,'key':keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'],'snapshots':str(root/'snapshots')})
                catalog['agents'][1]=native.start();registry.write_text(json.dumps(catalog))
                gateway=PublicAgentGateway(root/'gateway',registry,projects,keyfile,authority,root/'snapshots',gatewayport);gateway.start()
            started=True;compose('up','-d','--wait','--wait-timeout','180','--pull','never')
            receipt['served_assets']={}
            for name in ('index.html','collaboration_ui.js','collaboration_ui.css','spaces.js','room_agents.js','room_agent_channel.js'):
                status,body=http(f'http://127.0.0.1:{port}','/'+name)
                assert status==200 and hashlib.sha256(body).hexdigest()==receipt['sources']['web_demo/static/'+name], 'served asset differs from tested source: '+name
                receipt['served_assets'][name]=hashlib.sha256(body).hexdigest()
            receipt['checks'].append('fresh owned full Compose started with healthy services')
            control=root/'control';control.mkdir();browser_report=root/'browser.json'
            command=['node',str(ROOT/'tests/collaboration_web_live_browser_test.cjs'),'--base',f'http://127.0.0.1:{port}',
                     '--control',str(control),'--out',str(browser_report),'--screenshots',str(screenshots)]
            if args.agents:command.extend(['--agents','--nonce',nonce])
            log=output.with_suffix('.browser.private.log')
            with os.fdopen(os.open(log,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600),'w') as stream:
                process=subprocess.Popen(command,cwd=ROOT,env=os.environ.copy(),stdout=stream,stderr=subprocess.STDOUT)
                deadline=time.monotonic()+(1800 if args.agents else 420)
                restarted=False;fault_controls=set()
                while process.poll() is None:
                    for request in sorted(control.glob('*.job-*.request')):
                        if request.name in fault_controls:continue
                        operation='unpause' if '.job-resume.' in request.name else 'pause'
                        compose(operation,'job');job_paused=operation=='pause'
                        container=compose('ps','-q','job').strip()
                        paused=subprocess.check_output(['docker','inspect','--format','{{.State.Paused}}',container],text=True).strip()=='true'
                        assert paused==job_paused,'Job pause state does not match fault control'
                        fault_controls.add(request.name);request.with_suffix('.ready').write_text('ready')
                    if (control/'restart.request').exists() and not restarted:
                        compose('down','--remove-orphans');compose('up','-d','--wait','--wait-timeout','180','--pull','never')
                        restarted=True;(control/'restart.ready').write_text('ready')
                        receipt['checks'].append('full Compose down/up preserves owned volumes')
                    if time.monotonic()>deadline:raise TimeoutError('browser acceptance deadline exceeded')
                    time.sleep(.25)
                if browser_report.exists():receipt['browser']=json.loads(browser_report.read_text())
                if process.returncode:raise RuntimeError('browser acceptance failed; inspect private browser log')
                assert restarted,'browser never exercised full service restart'
            if not receipt.get('browser',{}).get('ok'):raise RuntimeError('browser did not report acceptance')
            if args.agents:
                sys.path.insert(0,str(ROOT/'cannbot/scripts'))
                from agent_router import AgentStore
                store=AgentStore(root/'gateway/service/router')
                turns=[store.get_turn(request['request_id']) for request in receipt['browser']['agent_requests']]
                assert all(turn and turn['status']=='completed' for turn in turns)
                routes=[turn['metadata']['agent_route'] for turn in turns]
                assert len({route['backend_session_ref'] for route in routes})==2
                assert len({route['conversation_id'] for route in routes})==1
                with sqlite3.connect(f'file:{native.root}/service/state.db?mode=ro',uri=True) as database:
                    native_tools=database.execute("SELECT content FROM messages WHERE role='tool'").fetchall()
                assert any(nonce in str(row[0]) for row in native_tools),'no real Native tool receipt for nonce'
                assert any('MySqlMessageReservations::Reserve' in str(row[0]) for row in native_tools),'no real Native C++ read receipt'
                receipt['native_tool_receipts']={'count':len(native_tools),'sha256':hashlib.sha256(json.dumps(native_tools).encode()).hexdigest(),
                                                'source_sha256':hashlib.sha256((source/'logic/message_reservation.cpp').read_bytes()).hexdigest()}
                receipt['checks'].append('actual completed Router turns use distinct Runtime sessions; Native tool outputs contain the source nonce and C++ function')
            receipt['ok']=True
        finally:
            if process and process.poll() is None:
                process.terminate()
                try:process.wait(timeout=10)
                except subprocess.TimeoutExpired:process.kill();process.wait()
            cleanup_errors=[]
            for log_path,label in ((root/'gateway/gateway.log','gateway'),(native.root/'runtime.log' if native else root/'absent','native')):
                if log_path.exists():
                    target=output.with_suffix('.'+label+'.private.log');shutil.copyfile(log_path,target);target.chmod(0o600)
            for resource in (gateway,native):
                if resource:
                    try:resource.stop() if resource is gateway else resource.close()
                    except Exception as error:cleanup_errors.append(type(error).__name__)
            if started:
                if job_paused:compose('unpause','job')
                try:compose('logs','--no-color','--tail','100','logic','comet','job')
                finally:compose('down','-v','--remove-orphans')
            receipt['limitations']=['browser test is one-machine acceptance, not production HA or fresh-Linux-host proof',
                                   'real CANN/private Agent and Kafka injection remain separate regressions']
            if cleanup_errors:receipt['ok']=False;receipt['cleanup_errors']=cleanup_errors
            output.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({'ok':receipt['ok'],'mode':receipt['mode'],'browser_checks':len(receipt.get('browser',{}).get('checks',[]))}),flush=True)
            if cleanup_errors:raise RuntimeError('owned runtime cleanup failed')


if __name__=='__main__':main()
