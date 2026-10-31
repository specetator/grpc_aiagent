#!/usr/bin/env python3
"""Actual Compose cold backup/restore, including accepted Kafka backlog.

Only owned projects, volumes, accounts and temporary directories are touched.
External Agent runtimes are explicitly outside this core-IM test.
Run serially on a Docker daemon: cold-backup preflight requires an exclusive
maintenance window, including no other acceptance test's transient helpers.
"""
import argparse
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import re
import shutil
import subprocess
import sys
import tempfile
import tarfile
import time

import websockets
from connector_deployment_e2e import ROOT, free_port
from release_identity import source_commit, running_images
from compose_e2e import http
sys.path.insert(0, str(ROOT / 'scripts'))
import export_cann_documents as cann


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='spark-push:web-collaboration')
    parser.add_argument('--out',type=Path,required=True)
    parser.add_argument('--cann-documents',action='store_true',help='also archive and restore an owned public document fixture')
    args=parser.parse_args();output=args.out.resolve();output.parent.mkdir(parents=True,exist_ok=True)
    receipt={'ok':False,'mode':'real Docker Compose cold IM backup/restore; no model or mocked service','checks':[],
             'source_commit':source_commit(ROOT),'scenario':'cann_documents' if args.cann_documents else 'im_backup',
             'sources':{name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in (
                 'scripts/spark_backup.py','scripts/spark_team_backup.py','scripts/export_cann_documents.py',
                 'tests/backup_restore_e2e.py','docker-compose.cann.yml')}}
    receipt['image_id']=subprocess.check_output(['docker','image','inspect',args.image,'--format','{{.Id}}'],text=True).strip()
    project='spark-backup-'+secrets.token_hex(5);restored=project+'-restored'
    environment={k:v for k,v in os.environ.items() if not k.startswith(('SPARK_','COMPOSE_','HERMES_','PI_','KERNEL_'))}
    port,target_port=free_port(),free_port()
    # Deliberately includes Compose interpolation syntax; test-only credential.
    test_password='backup$literal${fixture}'+secrets.token_hex(8)
    environment.update(SPARK_PUSH_IMAGE=args.image,SPARK_PUSH_BIND='127.0.0.1',SPARK_PUSH_PORT=str(port),
                       SPARK_PUSH_MYSQL_PASSWORD=test_password)
    log=output.with_suffix('.private.log')
    def run(command,expected=0,binary=False):
        result=subprocess.run(command,cwd=ROOT,env=environment,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=not binary,timeout=900)
        with os.fdopen(os.open(log,os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600),'w') as stream:
            stream.write(result.stdout.decode(errors='backslashreplace') if binary else result.stdout)
        if expected is not None and result.returncode!=expected:raise RuntimeError('acceptance command returned unexpected status: '+command[0])
        return result.stdout
    def check(text):receipt['checks'].append(text);print(text,flush=True)
    with tempfile.TemporaryDirectory(prefix='spark-backup-e2e-') as temporary:
        root=Path(temporary);envfile=root/'.env';envfile.touch(mode=0o600)
        source=['docker','compose','--env-file',str(envfile),'-p',project,'-f',str(ROOT/'docker-compose.yml')]
        deployment=root/'restored';bundle=root/('im' if args.cann_documents else 'backup')
        destination=['docker','compose','-p',restored,'-f',str(deployment/'compose.json')]
        cli=[sys.executable,str(ROOT/'scripts/spark_backup.py')]
        create=cli+['backup','--project',project,'--env-file',str(envfile),'-f',str(ROOT/'docker-compose.yml'),'--output',str(bundle),'--leave-stopped']
        recover=cli+['restore','--backup',str(bundle),'--project',restored,'--output',str(deployment),'--port',str(target_port)]
        base='http://127.0.0.1:'+str(port)
        if args.cann_documents:
            generation = '20261101T000000Z-' + secrets.token_hex(6)
            doc_id, chunk_id = 'doc_' + secrets.token_hex(12), 'chk_' + secrets.token_hex(12)
            knowledge = root / 'knowledge';public = root / 'public-data'
            documents = knowledge / 'data/generations' / generation / 'documents';documents.mkdir(parents=True)
            value = {'generation': generation, 'document': {'doc_id': doc_id, 'content': '公开原文 ' + secrets.token_hex(16)},
                     'chunks': [{'doc_id': doc_id, 'chunk_id': chunk_id, 'content': '公开片段 ' + secrets.token_hex(16)}]}
            pointer = knowledge / 'data/current.json';pointer.write_text(json.dumps({'generation': generation}));pointer.chmod(0o600)
            document = documents / (doc_id + '.json');document.write_text(json.dumps(value));document.chmod(0o600)
            original_files = {path: (path.read_bytes(), path.stat().st_mode) for path in (pointer, document)}
            cann.export_documents(knowledge, public)
            public_before = cann.exported_snapshot(public)
            environment['SPARK_PUSH_CANN_DATA_DIR'] = str(public)
            source += ['-f', str(ROOT / 'docker-compose.cann.yml')]
            create += ['-f', str(ROOT / 'docker-compose.cann.yml')]
        def api(path,body,user=None,expected=0,origin=None):
            value=json.loads(http(origin or base,path,body,user['token'] if user else None))
            assert value.get('code')==expected,(path,value.get('code'))
            return value.get('data',{})
        def poll(function,timeout=30):
            deadline=time.monotonic()+timeout
            while time.monotonic()<deadline:
                value=function()
                if value:return value
                time.sleep(.1)
            raise TimeoutError('acceptance condition not met')
        def secret_hashes(prefix):
            result={}
            for volume in ('spark_admin','spark_security'):
                code="import pathlib,hashlib,json;print(json.dumps({str(p.relative_to('/state')):hashlib.sha256(p.read_bytes()).hexdigest() for p in pathlib.Path('/state').iterdir() if p.is_file()}))"
                value=run(['docker','run','--rm','--network','none','--read-only','--user','0','--mount','type=volume,src='+prefix+'_'+volume+',dst=/state,readonly','--entrypoint','python3',args.image,'-c',code])
                result[volume]=json.loads(value)
            return result
        def states(command):
            ids=run(command+['ps','-aq']).split()
            return {item['Config']['Labels']['com.docker.compose.service']:item['State']['Running']
                    for item in json.loads(run(['docker','inspect',*ids]))}
        def kafka_record(command,client_id):
            delimiter='\nSPARK_BACKUP_RECORD_END\n'
            data=run(command+['exec','-T','kafka','/opt/kafka/bin/kafka-console-consumer.sh',
                '--bootstrap-server','kafka:29092','--topic','persist_message','--from-beginning',
                '--timeout-ms','2000','--property','print.partition=true','--property','print.offset=true',
                '--property','line.separator='+delimiter],None,binary=True)
            records=[]
            for match in re.finditer(rb'Partition:(\d+)\tOffset:(\d+)\t(.*?)'+re.escape(delimiter.encode()),data,re.DOTALL):
                if client_id.encode() in match[3]:
                    records.append({'partition':int(match[1]),'offset':int(match[2]),
                                    'value_sha256':hashlib.sha256(match[3]).hexdigest()})
            assert records,'accepted message is missing from the real broker log'
            return records
        def committed(command):
            data=run(command+['exec','-T','kafka','/opt/kafka/bin/kafka-consumer-groups.sh',
                '--bootstrap-server','kafka:29092','--describe','--group','spark_push_group_persist'])
            return {int(parts[2]):int(parts[3]) for line in data.splitlines()
                    if len(parts:=line.split())>=4 and parts[0]=='spark_push_group_persist'
                    and parts[1]=='persist_message' and parts[3].isdigit()}
        async def send(user,body):
            async with websockets.connect(base.replace('http','ws',1)+'/ws?token='+user['token'],proxy=None) as socket:
                await socket.send(json.dumps(body))
                async with asyncio.timeout(30):
                    async for raw in socket:
                        frame=json.loads(raw)
                        if frame.get('client_msg_id')==body['client_msg_id'] and frame.get('type') in {'accepted_ack','error'}:
                            assert frame['type']=='accepted_ack';return frame
        cleanup_errors=[]
        try:
            run(source+['up','-d','--wait','--wait-timeout','180','--pull','never'])
            receipt['release_services']=running_images(ROOT,args.image,project)
            source_init=json.loads(run(['docker','inspect',run(source+['ps','-aq','kafka-init']).strip()]))[0]['Config']['Cmd']
            credentials=[{'account':'backup_'+secrets.token_hex(8),'password':secrets.token_hex(16),'name':name} for name in ('Alice','Bob','Outsider')]
            a,b,outsider=[api('/api/register',value) for value in credentials]
            if args.cann_documents:
                document_query = {'doc_id': doc_id, 'chunk_id': chunk_id}
                document_before = api('/api/knowledge/document', document_query, a)
                assert document_before == {'generation': generation, 'document': value['document'], 'chunk': value['chunks'][0]}
                api('/api/knowledge/document', document_query, None, 401)
                assert run(source+['exec','-T','logic','id','-u']).strip() == '10001'
                initial_containers = json.loads(run(['docker','inspect',*run(source+['ps','-aq']).split()]))
                initial_started = {item['Id']:item['State']['StartedAt'] for item in initial_containers}
                invalid = create.copy();invalid[invalid.index('--output')+1] = str(root/'invalid-documents-backup')
                (public/'index.sqlite').write_bytes(b'not an exported document')
                assert 'unexpected public document directory contents' in run(invalid,1)
                (public/'index.sqlite').unlink()
                assert not (root/'invalid-documents-backup').exists()
                after = json.loads(run(['docker','inspect',*initial_started]))
                assert {item['Id']:item['State']['StartedAt'] for item in after} == initial_started
                public_before = cann.exported_snapshot(public)
                check('real UID 10001 serves exact exported document/chunk; invalid source tree is rejected before stopping containers or creating a backup')
            room=api('/api/spaces/create',{'name':'备份恢复协作空间'},a)['room_id'];sid='r_'+str(room)
            api('/api/spaces/invite',{'room_id':room,'user_id':b['user_id']},a);api('/api/spaces/accept',{'room_id':room},b)
            members_before=api('/api/spaces/members',{'room_id':room},a)
            original={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'已持久化的协作消息'}}
            asyncio.run(send(a,original))
            history_before=poll(lambda:api('/api/session/history',{'session_id':sid},a).get('messages'))
            direct={'type':'single_chat','to_user_id':b['user_id'],'client_msg_id':secrets.token_hex(12),'content':{'text':'备份前的私聊'}}
            direct_ack=asyncio.run(send(a,direct));direct_sid=direct_ack['session_id']
            def direct_history():
                value=json.loads(http(base,'/api/session/history',{'session_id':direct_sid},b['token']))
                if value.get('code')==404:return None
                assert value.get('code')==0
                return value['data']['messages']
            direct_before=poll(direct_history)
            png=base64.b64encode(bytes.fromhex('89504e470d0a1a0a0000000d49484452000000010000000108060000001f15c4890000000b49444154789c636000020000050001a5f645400000000049454e44ae426082')).decode()
            attachment=api('/api/attachment/upload',{'session_id':sid,'name':'team.png','data':png},a)
            attachment_before=api('/api/attachment/get',{'id':attachment['id']},b)
            api('/api/attachment/get',{'id':attachment['id']},outsider,403)
            security_before=secret_hashes(project)
            check('real users, invited membership, direct/room history and authorized attachment exist before backup')
            run(source+['stop','--timeout','20','job'])
            pending={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'Kafka 已接受但 Job 尚未落库'}}
            pending_ack=asyncio.run(send(a,pending))
            assert not any(message['msg_id']==pending_ack['msg_id'] for message in api('/api/session/history',{'session_id':sid},a)['messages'])
            broker_before=kafka_record(source,pending['client_msg_id']);offsets_before=committed(source)
            assert all(offsets_before.get(record['partition'],0)<=record['offset'] for record in broker_before)
            receipt['kafka_backlog']={'records':broker_before,'consumer_offsets_before':offsets_before}
            check('stopped real Job leaves a broker-recorded, uncommitted Kafka-accepted message absent from SQL')
            previous_states=states(source)
            bad_link=['docker','run','--rm','--network','none','--mount','type=volume,src='+project+'_spark_attachments,dst=/state','--entrypoint','python3',args.image,'-c']
            run(bad_link+["from pathlib import Path;Path('/state/unsafe-link').symlink_to('/etc/passwd')"])
            failed=create.copy();failed.remove('--leave-stopped');failed[failed.index('--output')+1]=str(root/'failed-backup')
            assert 'unsafe or duplicate archive member' in run(failed,1)
            assert states(source)==previous_states and previous_states['job'] is False
            assert json.loads((root/'failed-backup/manifest.json').read_text())['complete'] is False
            run(bad_link+["from pathlib import Path;Path('/state/unsafe-link').unlink()"])
            check('an actual unsafe archive failure resumes previously running services while leaving Job stopped and the manifest incomplete')
            backup_result=json.loads(run(create));assert backup_result['ok'] and backup_result['scope']=='im-only'
            assert json.loads(run(cli+['verify',str(bundle)]))['ok']
            manifest=json.loads((bundle/'manifest.json').read_text())
            receipt['backup_manifest']={key:manifest[key] for key in ('schema','scope','backup_id','application_revision','tool_checkout_revision','volumes','bindings','images','files')}
            assert len(manifest['volumes'])==6 and manifest['complete']
            assert not run(source+['ps','-q']).strip()
            assert bundle.stat().st_mode & 0o777==0o700
            assert all(path.stat().st_mode & 0o777==0o600 for path in bundle.iterdir())
            check('cold backup includes all six stopped volumes, checksums, binding config and actual immutable image identities')
            if args.cann_documents:
                assert cann.exported_snapshot(public) == public_before
                assert all((path.read_bytes(),path.stat().st_mode)==original for path,original in original_files.items())
                cann_bindings = [(key,record) for key,record in manifest['bindings'].items() if record.get('kind')=='cann_documents']
                assert len(cann_bindings)==1
                cann_key,cann_record = cann_bindings[0]
                assert cann_record['documents']==public_before[0]
                public.rename(root/'moved-public-data')
                assert not public.exists()
                check('document archive records its generation and content hashes; original bytes/modes stay intact and source export is moved away before restore')
            # Missing/corrupt sets must fail before any target state is created.
            copied=root/'bad';shutil.copytree(bundle,copied)
            broken=copied/manifest['volumes']['spark_mysql_data']['archive'];original_bytes=broken.read_bytes();broken.unlink()
            bad_recover=recover.copy();bad_recover[bad_recover.index('--backup')+1]=str(copied)
            run(bad_recover,1);assert not deployment.exists()
            broken.write_bytes(original_bytes+b'corrupt');run(bad_recover,1);assert not deployment.exists()
            assert not run(['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+restored]).strip()
            check('missing/corrupt archive sets are rejected before target files or volumes are created')
            if args.cann_documents:
                # Malformed content with a valid, recomputed outer checksum.
                bad_docs=root/'bad-documents';shutil.copytree(bundle,bad_docs)
                archive_path=bad_docs/cann_record['archive']
                expanded=root/'expanded';expanded.mkdir()
                with tarfile.open(archive_path) as archive:archive.extractall(expanded)
                (expanded/'content/index.sqlite').write_bytes(b'not a public document')
                with tarfile.open(archive_path,'w:gz') as archive:archive.add(expanded/'content',arcname='content')
                bad_manifest=json.loads((bad_docs/'manifest.json').read_text())
                bad_manifest['files'][cann_record['archive']]={'sha256':hashlib.sha256(archive_path.read_bytes()).hexdigest(),'bytes':archive_path.stat().st_size}
                (bad_docs/'manifest.json').write_text(json.dumps(bad_manifest))
                invalid=recover.copy();invalid[invalid.index('--backup')+1]=str(bad_docs)
                assert 'unexpected public document archive member' in run(invalid,1)
                assert not deployment.exists()
                assert not run(['docker','volume','ls','-q','--filter','label=com.docker.compose.project='+restored]).strip()
                check('recomputed archive checksum cannot hide an extra index; real restore rejects it before target files or volumes exist')
            collision=restored+'_spark_security'
            volumes_before=set(run(['docker','volume','ls','-q']).split())
            run(['docker','volume','create',collision])
            run(['docker','run','--rm','--network','none','--user','0','--mount','type=volume,src='+collision+',dst=/state','--entrypoint','python3',args.image,'-c',"from pathlib import Path;Path('/state/sentinel').write_text('preserve')"])
            run(recover,1)
            assert run(['docker','run','--rm','--network','none','--mount','type=volume,src='+collision+',dst=/state,readonly','--entrypoint','cat',args.image,'/state/sentinel']).strip()=='preserve'
            assert not deployment.exists()
            assert set(run(['docker','volume','ls','-q']).split())==volumes_before|{collision}
            run(['docker','volume','rm',collision])
            stopped=run(['docker','create','--label','com.docker.compose.project='+restored,
                         '--network','none','--entrypoint','/bin/true',args.image]).strip()
            run(recover,1)
            assert not deployment.exists() and set(run(['docker','volume','ls','-q']).split())==volumes_before
            assert json.loads(run(['docker','inspect',stopped]))[0]['State']['Status']=='created'
            run(['docker','rm',stopped])
            check('existing late target volume and a stopped target container are each refused without modifying existing state or creating volumes')
            restored_result=json.loads(run(recover));assert restored_result['ok'] and not restored_result['agents_enabled']
            restored_config=json.loads((deployment/'compose.json').read_text())
            assert not {'agent-bridge','agent-authority'} & set(restored_config['services'])
            for flag in ('SPARK_PUSH_HERMES_ENABLED','SPARK_PUSH_ROOM_AGENT_ENABLED','SPARK_PUSH_ROOM_HERMES_PUBLIC'):
                assert restored_config['services']['logic']['environment'][flag]=='false'
            assert not any(states(source).values())
            actual=json.loads(run(['docker','inspect',*run(destination+['ps','-aq']).split()]))
            assert all(item['Image']==manifest['images'][item['Config']['Labels']['com.docker.compose.service']]['id'] for item in actual)
            bootstrap=next(item for item in actual if item['Config']['Labels']['com.docker.compose.service']=='bootstrap')
            assert 'SPARK_PUSH_MYSQL_PASSWORD='+test_password in bootstrap['Config']['Env']
            restored_init=next(item for item in actual if item['Config']['Labels']['com.docker.compose.service']=='kafka-init')['Config']['Cmd']
            assert restored_init==source_init and '"$topic"' in restored_init[0]
            code="from pathlib import Path; wanted={'mysql_host','redis_host','kafka_brokers'}; print('\\n'.join(line for line in Path('/tmp/spark-logic.conf').read_text().splitlines() if line.split('=',1)[0] in wanted))"
            endpoints=dict(line.split('=',1) for line in run(destination+['exec','-T','logic','python3','-c',code]).splitlines())
            assert endpoints=={'mysql_host':'mysql','redis_host':'redis','kafka_brokers':'kafka:29092'}
            receipt['restored_store_endpoints']=endpoints
            check('restoration uses exact original image IDs with no Agent bridge/authority and all Logic Agent gates disabled; source remains stopped')
            check('literal dollar credentials survive Compose re-parsing and restored stores use only target-project service DNS names')
            restored_base='http://127.0.0.1:'+str(target_port)
            users=[api('/api/login',value,origin=restored_base) for value in credentials]
            if args.cann_documents:
                mount=restored_config['services']['logic']['volumes'][cann_record['index']]
                expected_path=deployment/'bindings'/cann_key/'content'
                assert mount['source']==str(expected_path) and mount['read_only'] is True
                assert mount.get('bind',{}).get('create_host_path',False) is False
                logic_container=next(item for item in actual if item['Config']['Labels']['com.docker.compose.service']=='logic')
                running_mount=next(item for item in logic_container['Mounts'] if item['Destination']=='/run/spark-cann/data')
                assert running_mount['Type']=='bind' and running_mount['Source']==str(expected_path) and running_mount['RW'] is False
                assert logic_container['Config']['Labels']['com.docker.compose.project']==restored
                assert logic_container['State']['Running'] is True
                # Exercise the batch tool's real enabled-plan generator and
                # Docker Compose parser, without starting an Agent Runtime.
                import spark_team_backup as team
                team.enabled_compose(root, deployment, restored)
                enabled=json.loads((deployment/'compose.agents.json').read_text())
                enabled_mount=enabled['services']['logic']['volumes'][cann_record['index']]
                assert enabled_mount==mount
                enabled_command=['docker','compose','--env-file','/dev/null','-p',restored,'-f',str(deployment/'compose.agents.json')]
                assert json.loads(run(enabled_command+['config','--format','json']))['services']['logic']['volumes'][cann_record['index']]==mount
                assert not public.exists() and cann.exported_snapshot(expected_path)[0]==public_before[0]
                assert run(destination+['exec','-T','logic','id','-u']).strip()=='10001'
                for user in users:
                    assert api('/api/knowledge/document',document_query,user,origin=restored_base)==document_before
                api('/api/knowledge/document',document_query,None,401,restored_base)
                assert all((path.read_bytes(),path.stat().st_mode)==original for path,original in original_files.items())
                receipt['cann_documents']={**public_before[0],'logic_uid':10001,'original_export_absent':True,
                    'response_sha256':hashlib.sha256(json.dumps(document_before,sort_keys=True).encode()).hexdigest(),
                    'original_source_bytes_and_modes_unchanged':True,'runtime_and_rag_index_included':False,
                    'restored_project':restored,'logic_container_id':logic_container['Id'],
                    'running_mount':{key:running_mount[key] for key in ('Type','Source','Destination','RW')},
                    'source_project_stopped':True,'fresh_logins':len(users),'enabled_plan_real_compose_validated':True,
                    'enabled_plan_agents_started':False}
                check('new project serves identical public document/chunk at UID 10001 from its restored readonly binding while the original export path is absent')
            current=lambda:api('/api/session/history',{'session_id':sid},users[0],origin=restored_base)['messages']
            history=poll(lambda:current() if any(message['msg_id']==pending_ack['msg_id'] for message in current()) else None)
            assert len(history)==2 and history[0]==history_before[0]
            assert [message['msg_id'] for message in history].count(pending_ack['msg_id'])==1
            for user in users[:2]:
                assert api('/api/spaces/members',{'room_id':room},user,origin=restored_base)==members_before
                assert api('/api/session/history',{'session_id':direct_sid},user,origin=restored_base)['messages']==direct_before
                assert api('/api/attachment/get',{'id':attachment['id']},user,origin=restored_base)==attachment_before
            api('/api/session/history',{'session_id':sid},users[2],403,restored_base)
            api('/api/attachment/get',{'id':attachment['id']},users[2],403,restored_base)
            assert secret_hashes(restored)==security_before
            assert kafka_record(destination,pending['client_msg_id'])==broker_before
            def backlog_committed():
                value=committed(destination)
                return value if all(value.get(record['partition'],0)>record['offset'] for record in broker_before) else None
            receipt['kafka_backlog']['consumer_offsets_after']=poll(backlog_committed)
            offsets=run(destination+['exec','-T','kafka','/opt/kafka/bin/kafka-get-offsets.sh','--bootstrap-server','kafka:29092','--topic','persist_message.dlq','--time','-1'])
            dlq=[int(line.rsplit(':',1)[1]) for line in offsets.splitlines() if line.startswith('persist_message.dlq:')]
            assert len(dlq)==3 and all(value==0 for value in dlq)
            check('fresh project restores accounts, exact history, membership, attachment bytes and secret hashes; queued Kafka message persists once without DLQ')
            base=restored_base
            after={'type':'chatroom','group_id':room,'client_msg_id':secrets.token_hex(12),'content':{'text':'恢复后继续协作'}}
            ack=asyncio.run(send(users[0],after));assert ack['msg_seq']>pending_ack['msg_seq']
            poll(lambda:len(current())==3)
            run(destination+['stop','--timeout','20','job'])
            restored_states=states(destination)
            second=cli+['backup','--project',restored,'-f',str(deployment/'compose.json'),'--output',str(root/'second-backup')]
            assert json.loads(run(second))['ok']
            assert len(current())==3 and states(destination)==restored_states and restored_states['job'] is False
            check('restored deployment continues its sequence and a normal backup resumes the previous running services')
            receipt['ok']=True
        except BaseException:
            # Keep failure evidence before owned containers are removed. These
            # stacks contain only generated fixture users/configuration.
            if (deployment/'compose.json').exists():
                run(destination+['ps','-a'],None)
                run(destination+['logs','--no-color','--tail','80'],None)
            raise
        finally:
            for command in (destination,source):
                if command is destination and not (deployment/'compose.json').exists():continue
                try:run(command+['down','-v','--remove-orphans'])
                except Exception as error:cleanup_errors.append(type(error).__name__)
            # Only this random test project's resources, including a partial restore.
            subprocess.run(['docker','rm','spark-restore-'+restored],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            for logical in ('spark_admin','spark_security','spark_mysql_data','spark_redis_data','spark_kafka_data','spark_attachments'):
                subprocess.run(['docker','volume','rm',restored+'_'+logical],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            if cleanup_errors:receipt['ok']=False;receipt['cleanup_errors']=cleanup_errors
            output.write_text(json.dumps(receipt,ensure_ascii=False,indent=2)+'\n')
            print(json.dumps({'ok':receipt['ok'],'checks':len(receipt['checks'])}),flush=True)
            if cleanup_errors:raise RuntimeError('owned backup test cleanup failed')


if __name__=='__main__':main()
