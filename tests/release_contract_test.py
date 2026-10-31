"""Local Git/archive and mocked Docker contracts; not release acceptance."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import package_release as package
import verify_release as verify
from clean_linux_release_e2e import cleanup_owned


class ReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.repo = self.root / 'repo'; self.repo.mkdir()
        self.git('init', '-q')
        for name in package.COMPOSE_FILES:
            (self.repo / name).write_bytes((ROOT / name).read_bytes())
        (self.repo / 'README.md').write_text('committed source\n')
        self.git('add', '.')
        self.commit()
        self.application = {'reference': package.IMAGE + '@sha256:' + 'a' * 64,
                            'builder_image_id': 'sha256:' + 'b' * 64, 'platform_manifest': 'sha256:'+'c'*64, 'config_digest': 'sha256:'+'b'*64, 'platform': 'linux/amd64', 'revision': self.revision}
        self.dependencies = {tag: {'reference': tag.split(':')[0] + '@sha256:' + str(i) * 64,
                                  'builder_image_id': 'sha256:' + str(i) * 64, 'platform_manifest': 'sha256:'+format(i+8,'x')*64, 'config_digest': 'sha256:'+str(i+4)*64, 'platform': 'linux/amd64'}
                             for i, tag in enumerate(package.DEPENDENCIES, 1)}

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.repo, stderr=subprocess.DEVNULL).decode().strip()

    def commit(self):
        self.git('-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture')
        self.revision = self.git('rev-parse', 'HEAD')

    def build(self, destination='output'):
        output = self.root / destination
        manifest = package.package(self.repo, self.revision, 'v0.1.0', self.application, self.dependencies, output)
        return output, manifest

    def extract(self, output):
        target = self.root / 'extracted';target.mkdir()
        with tarfile.open(output / 'spark-push-v0.1.0-linux-amd64.tar.gz') as archive:
            # Trusted archive generated above; the negative link case never gets this far.
            archive.extractall(target)
        return target / 'spark-push-v0.1.0'

    def test_only_committed_bytes_and_all_runtime_images_are_pinned(self):
        (self.repo / 'README.md').write_text('uncommitted change')
        (self.repo / '.env').write_text('PRIVATE_FIXTURE=not-for-release')
        output, manifest = self.build()
        target = self.extract(output)
        self.assertEqual((target / 'README.md').read_text(), 'committed source\n')
        self.assertFalse((target / '.env').exists())
        self.assertEqual(verify.verify_files(target), manifest)
        compose = package.yaml.safe_load((target / 'docker-compose.yml').read_text())
        for service in compose['services'].values():
            self.assertNotIn('build', service)
            self.assertIn('@sha256:', service['image'])
            self.assertEqual(service['platform'], 'linux/amd64')
        other, _ = self.build('second-output')
        self.assertEqual(verify.digest(output / 'spark-push-v0.1.0-linux-amd64.tar.gz'),
                         verify.digest(other / 'spark-push-v0.1.0-linux-amd64.tar.gz'))

    def test_staged_new_paths_do_not_silently_change_the_source_set(self):
        (self.repo / 'new.txt').write_text('new')
        self.git('add', 'new.txt')
        with self.assertRaises(ValueError): self.build()
        self.assertFalse((self.root / 'output').exists())

    def test_tracked_links_and_private_environment_are_rejected(self):
        for name in ('link', '.env'):
            with self.subTest(name=name):
                if name == 'link': (self.repo / name).symlink_to('/etc/passwd')
                else: (self.repo / name).write_text('private fixture')
                self.git('add', name);self.commit()
                with self.assertRaises(ValueError): self.build()
                self.assertFalse((self.root / 'output').exists())
                self.git('rm', name);self.commit()

    def test_modified_distribution_file_or_symlink_fails_verification(self):
        output, _ = self.build();target = self.extract(output)
        (target / 'README.md').write_text('different')
        with self.assertRaises(ValueError): verify.verify_files(target)
        (target / 'README.md').unlink();(target / 'README.md').symlink_to(self.repo / 'README.md')
        with self.assertRaises(ValueError): verify.verify_files(target)

    def test_image_digest_revision_and_platform_must_match(self):
        labels = {'org.opencontainers.image.source': package.SOURCE, 'org.opencontainers.image.revision': self.revision}
        data = {'Os': 'linux', 'Architecture': 'amd64', 'Id': self.application['builder_image_id'],
                'RepoDigests': [self.application['reference']], 'Config': {'Labels': labels}}
        with patch.object(package, 'run', return_value=json.dumps([data]).encode()), patch.object(package, 'registry_identity', return_value={k:self.application[k] for k in ('platform_manifest','config_digest')}):
            self.assertEqual(package.inspect_image(self.application['reference'], self.revision), self.application)
        for key, bad in [('Architecture', 'arm64'), ('RepoDigests', []), ('Config', {'Labels': {}})]:
            altered = {**data, key: bad}
            with self.subTest(key=key), patch.object(package, 'run', return_value=json.dumps([altered]).encode()), patch.object(package, 'registry_identity', return_value={k:self.application[k] for k in ('platform_manifest','config_digest')}):
                with self.assertRaises(ValueError): package.inspect_image(self.application['reference'], self.revision)

    def test_registry_selects_only_the_requested_platform(self):
        platform = self.application['platform_manifest'];config = self.application['config_digest']
        selected = {'digest': platform, 'platform': {'os':'linux','architecture':'amd64'}}
        index = {'manifests':[selected, {'digest':'sha256:'+'d'*64,'platform':{'os':'linux','architecture':'arm64'}}]}
        with patch.object(package,'run',side_effect=[json.dumps(index).encode(),json.dumps({'config':{'digest':config}}).encode()]):
            self.assertEqual(package.registry_identity(self.application['reference']), {'platform_manifest':platform,'config_digest':config})
        with patch.object(package,'run',return_value=json.dumps({'manifests':[selected,selected]}).encode()):
            with self.assertRaises(ValueError): package.registry_identity(self.application['reference'])

    def test_classic_and_containerd_ids_still_require_the_same_registry_graph(self):
        value = {'application': self.application, 'dependencies': {}, 'revision':self.revision, 'source':package.SOURCE}
        for local_id in verify.image_ids(self.application):
            actual = {'Id':local_id,'RepoDigests':[self.application['reference']],'Os':'linux','Architecture':'amd64',
                      'Config':{'Labels':{'org.opencontainers.image.revision':self.revision,'org.opencontainers.image.source':package.SOURCE}}}
            with patch.object(verify,'docker',return_value=[actual]):
                self.assertEqual(verify.verify_images(value), {self.application['reference']:local_id})
            for key,bad in [('Id','sha256:'+'f'*64),('RepoDigests',[]),('Architecture','arm64'),('Config',{})]:
                with self.subTest(local_id=local_id,key=key), patch.object(verify,'docker',return_value=[{**actual,key:bad}]):
                    with self.assertRaises(ValueError): verify.verify_images(value)

    def test_final_gate_rejects_missing_mock_or_mismatched_receipts(self):
        output, manifest = self.build()
        identities = {x['reference']: x['builder_image_id'] for x in [self.application, *self.dependencies.values()]}
        services = {role: {'reference': ref, 'image_id': identities[ref]} for role, ref in manifest['services'].items()}
        evidence = {'schema': 'sparkpush.release-acceptance.v1', 'accepted': True, 'revision': self.revision,
                    'image': self.application['reference'], 'archive_sha256': verify.digest(output / 'spark-push-v0.1.0-linux-amd64.tar.gz'), 'checks': {}}
        for name in verify.REQUIRED_CHECKS:
            receipt = {'ok': True, 'scenario': name, 'source_commit': self.revision, 'image_id': self.application['builder_image_id'],
                       'release_services': services, 'sources': {'README.md': manifest['files']['README.md']['sha256']}}
            path = output / (name + '.json');path.write_text(json.dumps(receipt))
            evidence['checks'][name] = {'exit_code': 0, 'kind': 'real', 'receipt': path.name, 'sha256': verify.digest(path)}
        path = output / 'acceptance.json';path.write_text(json.dumps(evidence))
        self.assertEqual(verify.verify_evidence(output, path), evidence)
        for name in verify.REQUIRED_CHECKS: (output / (name + '.exit')).write_text('0\n')
        self.assertEqual(verify.collect_evidence(output, output / 'collected.json'), evidence)
        (output / 'web_im.exit').write_text('1\n')
        with self.assertRaises(ValueError): verify.collect_evidence(output, output / 'failed.json')
        self.assertFalse((output / 'failed.json').exists())
        for field, bad in [('exit_code', False), ('kind', 'mock'), ('receipt', '../escape.json'), ('sha256', '0'*64)]:
            changed = copy.deepcopy(evidence);changed['checks']['web_im'][field] = bad;path.write_text(json.dumps(changed))
            with self.subTest(field=field):
                with self.assertRaises(ValueError): verify.verify_evidence(output, path)
        changed = copy.deepcopy(evidence);changed['checks'].pop('web_agents');path.write_text(json.dumps(changed))
        with self.assertRaises(ValueError): verify.verify_evidence(output, path)
        # A rehashed receipt still cannot change the tested source or container.
        receipt_path = output / 'web_im.json';original = json.loads(receipt_path.read_text())
        for field, bad in [('scenario', 'web_agents'), ('source_commit', 'c'*40), ('image_id', 'sha256:'+'c'*64),
                           ('sources', {'README.md': 'c'*64}), ('release_services', {})]:
            receipt_path.write_text(json.dumps({**original, field: bad}))
            changed = copy.deepcopy(evidence);changed['checks']['web_im']['sha256'] = verify.digest(receipt_path)
            path.write_text(json.dumps(changed))
            with self.subTest(field=field):
                with self.assertRaises(ValueError): verify.verify_evidence(output, path)

    def test_cleanup_queries_owned_resources_even_without_a_successful_create_reply(self):
        remaining = {'container': ['owned-container'], 'volume': ['owned-volume']}
        removed = []
        def command(args):
            if args[2] == 'ls':
                self.assertEqual(args[-1], 'label=spark.clean.acceptance=unique-test')
                return SimpleNamespace(stdout='\n'.join(remaining[args[1]]))
            removed.append(args)
            remaining[args[1]] = []
            return SimpleNamespace(stdout='')
        cleanup_owned('unique-test', command)
        self.assertEqual(removed, [['docker','container','rm','-fv','owned-container'],
                                  ['docker','volume','rm','owned-volume']])

    def test_cleanup_does_not_claim_success_while_owned_resources_remain(self):
        def command(args): return SimpleNamespace(stdout='still-present' if args[2]=='ls' else '')
        with self.assertRaises(RuntimeError): cleanup_owned('unique-test', command)


if __name__ == '__main__': unittest.main()
