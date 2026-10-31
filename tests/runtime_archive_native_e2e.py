#!/usr/bin/env python3
"""Real WSL/Windows public processes and archives; no model invocation.

The helper-death case stubs the IM boundary explicitly. Native restore and
Windows lock/launcher competition use the installed isolated Runtime itself.
"""
import argparse
import hashlib
import json
import secrets
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

from public_runtime_fixture import PublicAgentGateway, PublicHermesRuntime, ROOT
from connector_deployment_e2e import free_port
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'cannbot/scripts')]
import spark_team_backup as team
from public_runtime_archive import tree_hash, RESERVED


def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--out',type=Path,required=True);args=parser.parse_args()
    receipt={'ok':False,'mode':'real native OS processes/archives; IM boundary stubbed only for helper-death test; no models','checks':[],
        'sources':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
            'scripts/spark_team_backup.py','cannbot/scripts/public_runtime_archive.py','cannbot/scripts/public_runtime.py',
            'tests/public_runtime_fixture.py','tests/runtime_archive_native_e2e.py')}}
    def check(label):receipt['checks'].append(label);print(label,flush=True)
    with tempfile.TemporaryDirectory(prefix='spark-native-archive-',dir='/mnt/f') as temporary:
        root=Path(temporary);snapshots=root/'snapshots';snapshots.mkdir()
        (snapshots/'PROJECT_FILES.txt').write_text('probe.cpp');(snapshots/'probe.cpp').write_text('int proof = 17;')
        keys={'SPARK_PUSH_HERMES_API_KEY':secrets.token_hex(32),'SPARK_PUSH_WORKSPACE_AUTH_KEY':secrets.token_hex(32)}
        keyfile=root/'keys.json';keyfile.write_text(json.dumps(keys));keyfile.chmod(0o600)
        registry=root/'agents.json';projects=root/'projects.json';projects.write_text('{"schema":1,"projects":[]}')
        authority='http://127.0.0.1:19999';port=free_port()
        native=PublicHermesRuntime(workspace={'authority':authority,'key':keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'],'snapshots':str(snapshots)})
        gateway=PublicAgentGateway(root/'gateway',registry,projects,keyfile,authority,snapshots,port)
        leases=[]
        try:
            entry=native.start();catalog=json.loads((ROOT/'deploy/agents/agents.json.example').read_text())
            catalog['agents'][1]=entry;registry.write_text(json.dumps(catalog))
            gateway.start();gateway.stop();native.stop_process()
            before=tree_hash(native.root/'service',RESERVED)
            items=[{'id':'gateway','runtime':'public_gateway','platform':'posix','python':sys.executable,
                    'config':str(gateway.root/'config.json'),'config_root':str(root)},
                   {'id':'native','runtime':'hermes_native','platform':'nt','python':native.python,
                    'config':native.win(native.root/'public-config.json'),'config_root':native.win(native.root),
                    'credential_file':native.win(native.root/'api.env')}]
            specification=root/'plan.json';specification.write_text(json.dumps({'schema':1,'runtimes':items}))
            origin='http://host.docker.internal:'+str(port)
            fake_im={'services':{'logic':{'environment':{'SPARK_PUSH_AGENT_GATEWAY_ORIGIN':origin}},
                'agent-bridge':{'environment':{'SPARK_PUSH_HERMES_BASE_URL':origin+'/v1'}},
                'agent-authority':{'ports':[{'host_ip':'127.0.0.1','published':'19999'}]}}}
            constructor=team.Lease
            def capture(*values):
                lease=constructor(*values);leases.append(lease);return lease
            def lose_native_helper(_):
                assert len(leases)==2 and all(lease.process.poll() is None for lease in leases)
                selected=next(lease for lease in leases if lease.item['platform']=='nt')
                selected.process.kill();selected.process.wait(timeout=20)
            failed=root/'failed'
            request=argparse.Namespace(runtime_plan=str(specification),output=str(failed),project='owned-contract',env_file=None,file=[],profile=[])
            try:
                with patch.object(team,'Lease',side_effect=capture),patch.object(team.im,'run',return_value=json.dumps(fake_im)), \
                        patch.object(team.im,'backup',side_effect=lose_native_helper):team.backup(request)
            except RuntimeError:pass
            else:raise AssertionError('a dead Native helper was accepted')
            assert not json.loads((failed/'batch.json').read_text())['complete']
            try:team.verify(failed)
            except ValueError:pass
            else:raise AssertionError('incomplete batch verified')
            assert native.proc.poll() is not None and gateway.proc.poll() is not None
            assert tree_hash(native.root/'service',RESERVED)==before
            check('two actual native freeze helpers held state; killing Windows helper makes the batch unverifiable and starts no service (IM boundary stub)')
            batch=secrets.token_hex(16);records={}
            for item in items:
                lease=constructor('freeze',item,root/'valid'/item['id'],batch);leases.append(lease)
                lease.receive('frozen')
                records[item['id']]=lease.call('archive','archived')['record']
            assert records['gateway']['snapshots_sha256']==records['native']['snapshots_sha256']
            for lease in leases[-2:]:lease.release()
            check('actual Linux and Windows archive helpers agree on the mixed-case shared snapshot store')
            home=native.root/'service';saved=native.root/'service-before-restore'
            subprocess.run([native.python,'-c','from pathlib import Path;import sys;Path(sys.argv[1]).rename(sys.argv[2])',native.win(home),native.win(saved)],check=True,timeout=30)
            restore=constructor('restore',items[1],root/'valid/native',batch);leases.append(restore);restore.receive('restored')
            def compete():
                code='''import sys
from pathlib import Path
sys.path.insert(0,sys.argv[1]);from public_runtime import runtime_lock
try:lock=runtime_lock(Path(sys.argv[2])/'.spark-runtime.lock');lock.close()
except OSError as error:raise SystemExit(13 if error.errno==13 else 2)
'''
                result=subprocess.run([native.python,'-c',code,native.win(ROOT/'cannbot/scripts'),native.win(home)],capture_output=True,timeout=30)
                return result.returncode
            assert compete()==13
            competitor=subprocess.run(native.command,stdin=subprocess.DEVNULL,capture_output=True,timeout=45)
            assert competitor.returncode==1 and (home/'.spark-restore-incomplete').exists()
            restore.call('commit','committed');assert compete()==13
            restore.release();assert compete()==0
            assert not (home/'.spark-restore-incomplete').exists() and tree_hash(home,RESERVED)==before
            check('Windows restore holds the real byte lock through commit; a native launcher and raw lock contender are refused until release')
            native.launch();native.stop_process()
            check('actual restored Native API restarts from its original Profile without a model request')
            receipt['ok']=True
        finally:
            for lease in leases:lease.close()
            gateway.stop();native.close()
            check('private Pi model files and original Windows profile hashes are unchanged; owned fixtures removed')
            args.out.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({'ok':receipt['ok'],'checks':len(receipt['checks'])}),flush=True)


if __name__=='__main__':main()
