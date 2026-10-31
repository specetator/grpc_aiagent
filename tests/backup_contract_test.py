"""Backup preflight tests; actual Docker restore is exercised separately."""
import argparse
import copy
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
spec = importlib.util.spec_from_file_location('spark_backup', Path(__file__).resolve().parents[1] / 'scripts/spark_backup.py')
backup = importlib.util.module_from_spec(spec)
spec.loader.exec_module(backup)


class BackupPreflight(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bundle = self.root / 'bundle';self.bundle.mkdir()
        image = 'sha256:' + 'a' * 64
        services = {name: {'image': image} for name in backup.SERVICE_NAMES - {'agent-bridge','agent-authority'}}
        services['gateway']['volumes'] = [{'type':'bind','source':'gateway-0','target':'/etc/caddy/Caddyfile','read_only':True}]
        self.config = {'name':'source','services':services,'volumes':{name:{'name':'source_'+name} for name in backup.VOLUMES}}
        self.manifest = {'schema':'sparkpush.cold-backup.v1','complete':True,'scope':'im-only','backup_id':'f'*32,
            'application_revision':None,'tool_checkout_revision':'b'*40,
            'images':{name:{'id':image,'digests':[],'os':'linux','architecture':'amd64'} for name in services},
            'volumes':{name:{'archive':'volume-'+name+'.tar.gz','source_name':'source_'+name} for name in backup.VOLUMES},
            'bindings':{'gateway-0':{'archive':'binding-gateway-0.tar.gz','service':'gateway','index':0}},'files':{}}
        for value in self.manifest['volumes'].values():
            self.archive(value['archive'], 'data', b'owned test data')
        self.archive('binding-gateway-0.tar.gz','content',b':9010 { respond "fixture" }')
        self.save()

    def archive(self, name, member, data=b'', kind=tarfile.REGTYPE, link=''):
        with tarfile.open(self.bundle/name,'w:gz') as archive:
            info=tarfile.TarInfo(member);info.type=kind;info.linkname=link
            info.size=len(data) if kind==tarfile.REGTYPE else 0
            archive.addfile(info,io.BytesIO(data) if info.size else None)

    def save(self):
        (self.bundle/'compose.json').write_text(json.dumps(self.config))
        self.manifest['files']={path.name:{'sha256':backup.digest(path),'bytes':path.stat().st_size}
            for path in self.bundle.iterdir() if path.name!='manifest.json'}
        (self.bundle/'manifest.json').write_text(json.dumps(self.manifest))

    def rejected_without_docker(self):
        args=argparse.Namespace(backup=str(self.bundle),project='restored',output=str(self.root/'target'),port=9010,bind='127.0.0.1')
        with patch.object(backup,'run',side_effect=AssertionError('Docker touched before valid preflight')) as command:
            with self.assertRaises((ValueError,FileNotFoundError)):
                backup.restore(args)
            command.assert_not_called()
        self.assertFalse((self.root/'target').exists())

    def add_cann(self):
        self.generation = '20261101T000000Z-123456789abc'
        self.doc_id, self.chunk_id = 'doc_' + 'a' * 24, 'chk_' + 'b' * 24
        source = self.root / 'knowledge'
        docs = source / 'data/generations' / self.generation / 'documents'
        docs.mkdir(parents=True)
        (source / 'data/current.json').write_text(json.dumps({'generation': self.generation}))
        value = {'generation': self.generation, 'document': {'doc_id': self.doc_id, 'content': 'public'},
                 'chunks': [{'doc_id': self.doc_id, 'chunk_id': self.chunk_id, 'content': 'public chunk'}]}
        (docs / (self.doc_id + '.json')).write_text(json.dumps(value))
        self.public = self.root / 'public-data'
        backup.cann.export_documents(source, self.public)
        snapshot = backup.cann.exported_snapshot(self.public)
        record = {'archive': 'binding-logic-0.tar.gz', 'service': 'logic', 'index': 0, 'kind': 'cann_documents'}
        record['documents'] = backup.copy_cann_binding(self.public, self.bundle / record['archive'], snapshot)
        self.manifest['bindings']['logic-0'] = record
        self.config['services']['logic'].update(environment={'SPARK_PUSH_CANN_KNOWLEDGE_ROOT': '/run/spark-cann'},
            volumes=[{'type': 'bind', 'source': 'logic-0', 'target': '/run/spark-cann/data',
                      'read_only': True, 'bind': {}}])
        self.save()

    def test_cann_archive_preserves_source_and_restores_public_permissions(self):
        self.add_cann()
        before = backup.cann.exported_snapshot(self.public)
        _, manifest, config = backup.verify(self.bundle)
        self.assertEqual(manifest['bindings']['logic-0']['documents'], before[0])
        self.assertEqual(config, self.config)
        target = self.root / 'extracted';target.mkdir()
        with tarfile.open(self.bundle / 'binding-logic-0.tar.gz') as archive:
            archive.extractall(target, members=backup.safe_members(archive))
        self.assertEqual(backup.cann.exported_snapshot(target / 'content')[0], before[0])
        self.assertEqual(backup.cann.exported_snapshot(self.public), before)

    def test_cann_bad_mount_or_tree_is_rejected_before_stop(self):
        self.add_cann()
        caddy = self.root / 'Caddyfile';caddy.write_text('fixture')
        original = copy.deepcopy(self.config)
        original['services']['gateway']['volumes'][0]['source'] = str(caddy)
        original['services']['logic']['volumes'][0]['source'] = str(self.public)
        args = argparse.Namespace(project='source', output=str(self.root/'backup-output'), env_file=None,
                                  file=None, profile=[], leave_stopped=False)
        def reject(config):
            with patch.object(backup, 'run', return_value=json.dumps(config).encode()) as command:
                with self.assertRaises(ValueError): backup.backup(args)
                self.assertEqual(command.call_count, 1)
                self.assertEqual(command.call_args.args[0][-3:], ['config', '--format', 'json'])
            self.assertFalse((self.root / 'backup-output').exists())
        for change in ({'target': '/unknown'}, {'read_only': False}, {'bind': {'create_host_path': True}},
                       {'bind': {'propagation': 'shared'}}, {'target': '/run/spark-cann'}):
            config = copy.deepcopy(original);config['services']['logic']['volumes'][0].update(change)
            reject(config)
        config = copy.deepcopy(original)
        config['services']['logic']['environment']['SPARK_PUSH_CANN_KNOWLEDGE_ROOT'] = '/private'
        reject(config)
        config = copy.deepcopy(original)
        config['services']['web']['volumes'] = config['services']['logic'].pop('volumes')
        reject(config)
        for extra in ('index.sqlite', 'source.py', 'generations/another-generation'):
            path = self.public / extra;path.write_text('not public export');reject(original);path.unlink()
        pointer = self.public / 'current.json';pointer.chmod(0o600)
        reject(original);pointer.chmod(0o644)
        link = self.root / 'linked-pointer';os.link(pointer, link)
        reject(original);link.unlink()
        alias = self.root / 'alias';alias.symlink_to(self.public, target_is_directory=True)
        config = copy.deepcopy(original);config['services']['logic']['volumes'][0]['source'] = str(alias)
        reject(config)

    def test_rehashed_cann_archive_still_requires_structure_and_document_identity(self):
        self.add_cann()
        archive_path = self.bundle / 'binding-logic-0.tar.gz'
        with tarfile.open(archive_path) as archive:
            original = [(copy.copy(member), archive.extractfile(member).read() if member.isfile() else b'')
                        for member in archive.getmembers()]
        doc_path = 'content/generations/' + self.generation + '/documents/' + self.doc_id + '.json'
        cases = ('extra-index', 'extra-generation', 'missing-directory', 'generation', 'doc-id', 'chunk-id',
                 'chunk-owner', 'oversized', 'symlink', 'hardlink', 'unreadable', 'directory-mode', 'nested-file')
        for case in cases:
            with self.subTest(case=case):
                items = copy.deepcopy(original)
                if case.startswith('extra') or case == 'nested-file':
                    name = {'extra-index': 'content/index.sqlite', 'extra-generation': 'content/generations/extra',
                            'nested-file': doc_path + '/nested'}[case]
                    info = tarfile.TarInfo(name);info.mode = 0o644
                    items.append((info, b'x'))
                elif case == 'missing-directory':
                    items = [(info, raw) for info, raw in items if info.name != 'content/generations']
                else:
                    for index, (info, raw) in enumerate(items):
                        if case == 'directory-mode' and info.name == 'content': info.mode = 0o700
                        if info.name != doc_path: continue
                        value = json.loads(raw)
                        if case == 'generation': value['generation'] = '20261101T000001Z-123456789abc'
                        elif case == 'doc-id': value['document']['doc_id'] = 'doc_' + 'c' * 24
                        elif case == 'chunk-id': value['chunks'][0]['chunk_id'] = '../wrong'
                        elif case == 'chunk-owner': value['chunks'][0]['doc_id'] = 'doc_' + 'c' * 24
                        elif case in ('symlink', 'hardlink'):
                            info.type = tarfile.SYMTYPE if case == 'symlink' else tarfile.LNKTYPE
                            info.linkname = 'content/current.json'
                        elif case == 'unreadable': info.mode = 0o600
                        raw = b' ' * (backup.cann.DOCUMENT_LIMIT + 1) if case == 'oversized' else json.dumps(value).encode()
                        items[index] = (info, raw)
                with tarfile.open(archive_path, 'w:gz') as archive:
                    for info, raw in items:
                        info.size = len(raw) if info.isfile() else 0
                        archive.addfile(info, io.BytesIO(raw) if info.isfile() else None)
                self.save()  # Recomputed SHA does not bypass semantic preflight.
                self.rejected_without_docker()

    def test_cann_mapping_and_descriptor_must_agree(self):
        self.add_cann()
        original = copy.deepcopy(self.manifest['bindings']['logic-0'])
        for field, value in (('kind', 'configuration'), ('kind', 'unknown'), ('documents', {})):
            self.manifest['bindings']['logic-0'] = {**original, field: value}
            self.save();self.rejected_without_docker()
        self.manifest['bindings']['logic-0'] = {k: v for k, v in original.items() if k != 'kind'}
        self.save();self.rejected_without_docker()

    def test_cann_mutation_during_copy_is_not_a_complete_snapshot(self):
        self.add_cann()
        before = backup.cann.exported_snapshot(self.public)
        original = backup.copy_binding
        def mutate(source, destination):
            original(source, destination)
            path = self.public / 'current.json'
            path.write_bytes(path.read_bytes() + b' ')
        with patch.object(backup, 'copy_binding', side_effect=mutate):
            with self.assertRaisesRegex(ValueError, 'changed'):
                backup.copy_cann_binding(self.public, self.root / 'changed.tar.gz', before)

    def test_team_enabled_plan_uses_archived_cann_path(self):
        self.add_cann()
        import spark_team_backup as team
        batch = self.root / 'batch';batch.mkdir();shutil.copytree(self.bundle, batch / 'im')
        target = self.root / 'restored';target.mkdir()
        disabled = copy.deepcopy(self.config);disabled['networks'] = {'default': {'name': 'restored_default'}}
        disabled['services']['gateway']['ports'] = [{'target': 9010, 'published': '9012'}]
        (target / 'compose.json').write_text(json.dumps(disabled))
        self.public.rename(self.root / 'moved-public')
        with patch.object(team.im, 'run') as command:
            team.enabled_compose(batch, target, 'restored')
            command.assert_called_once()
        enabled = json.loads((target / 'compose.agents.json').read_text())
        mount = enabled['services']['logic']['volumes'][0]
        self.assertEqual(mount['source'], str(target / 'bindings/logic-0/content'))
        self.assertTrue(mount['read_only'])
        self.assertEqual(backup.cann.exported_snapshot(Path(mount['source']))[0],
                         self.manifest['bindings']['logic-0']['documents'])

    def test_complete_bundle(self):
        directory,manifest,config=backup.verify(self.bundle)
        self.assertEqual(directory,self.bundle)
        self.assertEqual(set(manifest['volumes']),backup.VOLUMES)
        self.assertEqual(config,self.config)

    def test_missing_archive(self):
        (self.bundle/'volume-spark_mysql_data.tar.gz').unlink()
        self.rejected_without_docker()

    def test_corrupt_archive(self):
        with (self.bundle/'volume-spark_mysql_data.tar.gz').open('ab') as output:output.write(b'corruption')
        self.rejected_without_docker()

    def test_incomplete_marker_and_revision(self):
        original=copy.deepcopy(self.manifest)
        for field,value in [('complete',False),('application_revision','invalid')]:
            self.manifest=copy.deepcopy(original);self.manifest[field]=value;self.save();self.rejected_without_docker()

    def test_unsafe_archive_members_even_with_matching_checksum(self):
        filename='volume-spark_attachments.tar.gz'
        for name,kind,link in [('../escape',tarfile.REGTYPE,''),('/escape',tarfile.REGTYPE,''),
                               ('link',tarfile.SYMTYPE,'/etc/passwd'),('link',tarfile.LNKTYPE,'../escape'),
                               ('device',tarfile.CHRTYPE,''),('fifo',tarfile.FIFOTYPE,'')]:
            with self.subTest(name=name,kind=kind):
                self.archive(filename,name,b'',kind,link);self.save();self.rejected_without_docker()

    def test_duplicate_volume_archive(self):
        self.manifest['volumes']['spark_mysql_data']['archive']='volume-spark_redis_data.tar.gz'
        self.save();self.rejected_without_docker()

    def test_bind_mapping_escape(self):
        self.manifest['bindings']['../../escape']=self.manifest['bindings'].pop('gateway-0')
        self.config['services']['gateway']['volumes'][0]['source']='../../escape'
        self.save();self.rejected_without_docker()

    def test_unpacked_host_bind_and_privileged_override(self):
        original=copy.deepcopy(self.config)
        for field,value in [('volumes',[{'type':'bind','source':'/var/run/docker.sock','target':'/docker.sock'}]),('privileged',True)]:
            self.config=copy.deepcopy(original);self.config['services']['logic'][field]=value
            self.save();self.rejected_without_docker()

    def test_unsupported_compose_resources(self):
        original=copy.deepcopy(self.config)
        for scope,field,value in [('top','secrets',{'private':{'file':'/etc/shadow'}}),
                ('top','configs',{'private':{'file':'/etc/shadow'}}),
                ('service','container_name','source-logic'),('service','network_mode','host'),
                ('service','ports',[{'target':9000,'published':'9000'}]),
                ('service','extra_hosts',{'mysql':'192.0.2.1'})]:
            with self.subTest(field=field):
                self.config=copy.deepcopy(original)
                target=self.config if scope=='top' else self.config['services']['logic']
                target[field]=value;self.save();self.rejected_without_docker()

    def test_duplicate_service_containers(self):
        container={'Config':{'Labels':{'com.docker.compose.service':'job'}}}
        with patch.object(backup,'run',side_effect=['id1 id2',json.dumps([container,container])]):
            with self.assertRaisesRegex(ValueError,'multiple containers'):
                backup.source_containers(['docker','compose'])

    def test_external_volume_user(self):
        info={'Labels':{'com.docker.compose.project':'source'},'Driver':'local','Options':None}
        external={'Id':'external','Mounts':[{'Type':'volume','Name':'source_spark_mysql_data'}]}
        with patch.object(backup,'inspect',return_value=info), patch.object(backup,'run',side_effect=['external',json.dumps([external])]):
            with self.assertRaisesRegex(ValueError,'another container'):
                backup.exclusive_volumes(self.config,'source',{})

    def test_duplicate_source_volumes(self):
        self.config['volumes']['spark_mysql_data']['name']='source_spark_redis_data'
        with patch.object(backup,'run',side_effect=AssertionError('must refuse before Docker')):
            with self.assertRaisesRegex(ValueError,'unique'):
                backup.exclusive_volumes(self.config,'source',{})

    def test_archive_without_tar_suffix_still_validated(self):
        old='volume-spark_attachments.tar.gz';new='renamed.bin'
        self.archive(old,'link',kind=tarfile.SYMTYPE,link='/etc/passwd')
        (self.bundle/old).rename(self.bundle/new)
        self.manifest['volumes']['spark_attachments']['archive']=new
        self.save();self.rejected_without_docker()

    def test_output_inside_source_rejected(self):
        self.config['services']['gateway']['volumes'][0]['source']=str(self.root/'configuration')
        with patch.object(backup,'inspect',return_value={'Mountpoint':str(self.root/'volume')}):
            for output in (self.root/'configuration/backup',self.root/'volume/backup'):
                with self.assertRaisesRegex(ValueError,'outside every source'):
                    backup.outside_sources(output,self.config)
                self.assertFalse(output.exists())

    def test_dollar_paths_rejected_before_docker(self):
        args=argparse.Namespace(backup=str(self.bundle),project='restored',output=str(self.root/'${fixture}'),port=9010,bind='127.0.0.1')
        with patch.object(backup,'run',side_effect=AssertionError('must refuse before Docker')):
            with self.assertRaisesRegex(ValueError,'dollar signs'):backup.restore(args)
        self.config['services']['gateway']['volumes'][0]['source']='/operator/$$name'
        with self.assertRaisesRegex(ValueError,'dollar signs'):backup.supported_config(self.config)

    def test_resume_only_original_container_ids(self):
        containers={name:{'Id':name+'-id'} for name in ('mysql','gateway','job')}
        with patch.object(backup,'run') as command, patch.object(backup,'wait_running'):
            backup.resume_containers(['docker','compose'],containers,['mysql','gateway'])
            self.assertEqual([call.args[0] for call in command.call_args_list],
                             [['docker','start','mysql-id'],['docker','start','gateway-id']])

    def test_missing_core_service_rejected(self):
        del self.config['services']['job']
        self.save();self.rejected_without_docker()

    def test_normalized_agent_host_mapping(self):
        self.config['services']['logic']['extra_hosts']=['host.docker.internal=host-gateway']
        backup.supported_config(self.config)
        self.config['services']['logic']['extra_hosts']=['mysql=192.0.2.1']
        with self.assertRaises(ValueError):backup.supported_config(self.config)


if __name__=='__main__':unittest.main()
