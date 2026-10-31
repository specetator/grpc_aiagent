"""Real process/lock/file round trips with synthetic state, not model E2E."""
import argparse
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'scripts'), str(ROOT / 'cannbot/scripts')]
import spark_team_backup as team
import public_runtime_archive as state


class RuntimeArchiveTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='spark-runtime-archive-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.public = self.root / 'public';self.public.mkdir()
        self.home = self.public / 'home';self.snapshots = self.public / 'snapshots';self.snapshots.mkdir()
        (self.snapshots / 'proof.cpp').write_text('int actual_fixture_bytes = 17;\n')
        (self.snapshots / 'PROJECT_FILES.txt').write_text('mixed case ordering is portable')
        self.models = {'providers': {'fixture': {'models': [{'id': 'fixture'}]}}}
        values = {'models.json': self.models, 'keys.json': {'SPARK_PUSH_HERMES_API_KEY': 'g' * 32, 'SPARK_PUSH_WORKSPACE_AUTH_KEY': 'w' * 32},
            'agents.json': {'schema': 1, 'agents': [{'id': 'pi', 'runtime': 'pi_rpc', 'owner_user_ids': [], 'room_enabled': True, 'room_policy': 'read_only'}]},
            'projects.json': {'schema': 1, 'projects': []}}
        for name, value in values.items():(self.public / name).write_text(json.dumps(value))
        self.config = {'schema': 1, 'id': 'fixture', 'home': str(self.home), 'snapshots': str(self.snapshots),
            'registry': str(self.public / 'agents.json'), 'projects': str(self.public / 'projects.json'),
            'keys_file': str(self.public / 'keys.json'), 'authority_origin': 'http://127.0.0.1:19999',
            'pi': {'command': sys.executable, 'provider': 'fixture', 'model': 'fixture', 'models_file': str(self.public / 'models.json')}}
        self.config_path = self.public / 'gateway.json';self.config_path.write_text(json.dumps(self.config))
        code = '''import json,sys
from public_runtime import PublicHome
from pathlib import Path
cfg=json.loads(Path(sys.argv[1]).read_text())
with PublicHome(cfg['home'],{'runtime':'public_gateway','id':cfg['id']}) as home:
 home.write_new('public-models.json',Path(cfg['pi']['models_file']).read_text())
 home.directory('router')
 home.write_new('router/unknown.json','{"status":"unknown","request_id":"keep-original"}')
'''
        subprocess.run([sys.executable, '-c', code, str(self.config_path)], cwd=ROOT / 'cannbot/scripts', check=True)
        self.item = {'id': 'gateway', 'runtime': 'public_gateway', 'platform': 'posix', 'python': sys.executable,
                     'config': str(self.config_path), 'config_root': str(self.public)}
        self.batch = 'a' * 32;self.bundle = self.root / 'bundle';self.leases = []
        self.addCleanup(lambda: [lease.close() for lease in self.leases])

    def helper(self, operation):
        lease = team.Lease(operation, self.item, self.bundle, self.batch);self.leases.append(lease)
        return lease

    def archive(self):
        lease = self.helper('freeze');lease.receive('frozen')
        record = lease.call('archive', 'archived')['record'];lease.release()
        return record

    def hide_state(self):
        self.home.rename(self.root / 'old-home');self.snapshots.rename(self.public / 'old-snapshots')

    def args(self):
        return argparse.Namespace(runtime='public_gateway', config=str(self.config_path), config_root=str(self.public),
                                  credential_file=None, directory=str(self.bundle), batch_id=self.batch)

    def launcher_attempt(self):
        code = "from public_runtime import PublicHome;h=PublicHome(__import__('sys').argv[1],{'runtime':'public_gateway','id':'fixture'});h.__enter__();h.__exit__()"
        return subprocess.run([sys.executable, '-c', code, str(self.home)], cwd=ROOT / 'cannbot/scripts', capture_output=True, timeout=15)

    def test_real_freeze_restore_holds_lock_and_preserves_unknown(self):
        self.snapshots.chmod(0o500)
        record = self.archive();self.hide_state()
        before = {p: (p.read_bytes(), p.stat().st_mode) for p in self.public.iterdir() if p.is_file()}
        state.preflight_restore(self.args())
        lease = self.helper('restore');lease.receive('restored')
        self.assertNotEqual(self.launcher_attempt().returncode, 0)
        self.assertTrue((self.home / state.INCOMPLETE).exists())
        lease.call('commit', 'committed')
        self.assertNotEqual(self.launcher_attempt().returncode, 0)
        lease.release()
        self.assertFalse((self.home / state.INCOMPLETE).exists())
        self.assertEqual(state.tree_hash(self.home, state.RESERVED), record['home_sha256'])
        self.assertEqual(self.snapshots.stat().st_mode & 0o777, 0o500)
        self.assertEqual((self.home / 'router/unknown.json').read_text(), '{"status":"unknown","request_id":"keep-original"}')
        self.assertEqual(before, {p: (p.read_bytes(), p.stat().st_mode) for p in before})
        self.assertEqual(self.launcher_attempt().returncode, 0)

    def test_eof_before_batch_commit_leaves_launcher_disabled(self):
        self.archive();self.hide_state()
        lease = self.helper('restore');lease.receive('restored');lease.close()
        self.assertEqual(lease.process.returncode, 1)
        self.assertTrue((self.home / state.INCOMPLETE).exists())
        self.assertNotEqual(self.launcher_attempt().returncode, 0)

    def test_missing_public_configuration_is_restored_without_overwrite(self):
        record = self.archive();self.hide_state()
        path = self.public / 'keys.json';path.unlink()
        lease = self.helper('restore');lease.receive('restored');lease.call('commit', 'committed');lease.release()
        self.assertEqual(state.digest(path), record['files'][str(path)]['sha256'])

    def test_existing_home_or_changed_config_fails_before_writes(self):
        self.archive()
        with self.assertRaises(ValueError):state.preflight_restore(self.args())
        self.hide_state()
        path = self.public / 'keys.json';path.write_text('{"keep":"operator edit"}')
        with self.assertRaises(ValueError):state.preflight_restore(self.args())
        self.assertFalse(self.home.exists());self.assertFalse(self.snapshots.exists())
        self.assertEqual(path.read_text(), '{"keep":"operator edit"}')

    def test_corrupt_archive_and_wrong_batch_fail_before_targets(self):
        self.archive();self.hide_state()
        wrong = self.args();wrong.batch_id = 'b' * 32
        with self.assertRaises(ValueError):state.preflight_restore(wrong)
        with (self.bundle / 'state.tar.gz').open('ab') as output:output.write(b'corrupt')
        with self.assertRaises(ValueError):state.preflight_restore(self.args())
        self.assertFalse(self.home.exists());self.assertFalse(self.snapshots.exists())

    def test_private_model_file_is_not_collected(self):
        private = self.root / 'private-models.json';private.write_text(json.dumps(self.models))
        self.config['pi']['models_file'] = str(private);self.config_path.write_text(json.dumps(self.config))
        lease = self.helper('freeze')
        with self.assertRaises(RuntimeError):lease.receive('frozen')
        self.assertFalse(self.bundle.exists())
        self.assertEqual(json.loads(private.read_text()), self.models)

    def test_symlink_state_is_refused(self):
        (self.home / 'escape').symlink_to(self.public / 'keys.json')
        lease = self.helper('freeze')
        with self.assertRaises(RuntimeError):lease.receive('frozen')
        self.assertFalse(self.bundle.exists())

    def test_dependency_change_is_checked_before_mutation(self):
        self.archive();self.hide_state()
        manifest = self.bundle / 'runtime.json';value = json.loads(manifest.read_text())
        value['dependencies'][sys.executable] = '0' * 64;manifest.write_text(json.dumps(value))
        with self.assertRaises(ValueError):state.preflight_restore(self.args())
        self.assertFalse(self.home.exists())

    def test_im_output_overlap_is_refused_before_any_targets(self):
        record = self.archive();self.hide_state()
        for target in (self.home, self.home / 'deployment', self.home.parent, self.snapshots / 'im',
                       self.public / 'keys.json' / 'nested'):
            with self.subTest(target=target), self.assertRaises(ValueError):
                team.restore_paths(target, self.bundle, [self.item], {'gateway': record})
            self.assertFalse(self.home.exists());self.assertFalse(self.snapshots.exists())
        team.restore_paths(self.root / 'separate-im', self.bundle, [self.item], {'gateway': record})

    def test_same_native_identity_cannot_substitute_another_endpoint(self):
        public = self.archive()
        credential = str(self.public / 'native-api.env')
        entry = {'id': 'hermes', 'runtime': 'hermes_native', 'profile': 'spark-public-test',
                 'expected_model': 'spark-public-test', 'transport': 'http', 'credential_env_file': credential,
                 'base_url': 'http://127.0.0.1:18644/v1'}
        public['agents'].append(entry)
        native = {'identity': {'agent_id': 'hermes', 'profile': 'spark-public-test'}, 'home': str(self.public / 'native'),
                  'platform': 'posix', 'listener': {'host': '127.0.0.1', 'port': 18644}, 'credential_file': credential,
                  'snapshots': public['snapshots'], 'snapshots_sha256': public['snapshots_sha256'],
                  'authority_origin': public['authority_origin']}
        items = [self.item, {**self.item, 'id': 'hermes', 'runtime': 'hermes_native'}]
        records = {'gateway': public, 'hermes': native}
        team.relationships(items, records)
        for field, value in [('port', 18645), ('host', '127.0.0.2')]:
            changed = copy.deepcopy(records);changed['hermes']['listener'][field] = value
            with self.assertRaises(ValueError):team.relationships(items, changed)
        changed = copy.deepcopy(records);changed['gateway']['agents'][-1]['expected_model'] = 'another-profile'
        with self.assertRaises(ValueError):team.relationships(items, changed)

    def test_gateway_endpoint_does_not_accept_a_different_port_or_remote_host(self):
        listener = {'host': '0.0.0.0', 'port': 8643}
        team.endpoint_matches('http://host.docker.internal:8643/v1', listener, '/v1', docker_host=True)
        team.endpoint_matches('http://localhost:18644/v1', {'host':'127.0.0.1','port':18644}, '/v1')
        for url in ('http://127.0.0.1:8644/v1', 'http://192.0.2.1:8643/v1',
                    'http://127.0.0.1:8643/wrong', 'http://user:password@127.0.0.1:8643/v1',
                    'http://127.0.0.1:8643/v1', 'http://localhost:8643/v1', 'http://[::1]:8643/v1'):
            with self.assertRaises(ValueError):team.endpoint_matches(url, listener, '/v1', docker_host=True)

    def test_real_helper_death_cannot_complete_a_batch(self):
        # Contract-only IM boundary; the helper itself is a real subprocess
        # holding the original home lock and producing an actual archive.
        self.config['host']='0.0.0.0';self.config_path.write_text(json.dumps(self.config))
        specification=self.root/'plan.json';specification.write_text(json.dumps({'schema':1,'runtimes':[self.item]}))
        source={'services':{
            'logic':{'environment':{'SPARK_PUSH_AGENT_GATEWAY_ORIGIN':'http://host.docker.internal:8643'}},
            'agent-bridge':{'environment':{'SPARK_PUSH_HERMES_BASE_URL':'http://host.docker.internal:8643/v1'}},
            'agent-authority':{'ports':[{'host_ip':'127.0.0.1','published':'19999'}]}}}
        leases=[];constructor=team.Lease
        def capture(*args):
            lease=constructor(*args);leases.append(lease);return lease
        def stop_helper(_):
            leases[0].process.terminate();leases[0].process.wait(timeout=10)
        output=self.root/'batch'
        args=argparse.Namespace(runtime_plan=str(specification),output=str(output),project='fixture',env_file=None,file=[],profile=[])
        with patch.object(team,'Lease',side_effect=capture), patch.object(team.im,'run',return_value=json.dumps(source)), \
                patch.object(team.im,'backup',side_effect=stop_helper):
            with self.assertRaises(RuntimeError):team.backup(args)
        self.assertFalse(json.loads((output/'batch.json').read_text())['complete'])
        with self.assertRaises(ValueError):team.verify(output)
        self.assertEqual((self.home/'router/unknown.json').read_text(),'{"status":"unknown","request_id":"keep-original"}')


if __name__ == '__main__':unittest.main()
