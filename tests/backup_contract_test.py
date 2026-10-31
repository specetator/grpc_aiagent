"""Backup preflight tests; actual Docker restore is exercised separately."""
import argparse
import copy
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

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
