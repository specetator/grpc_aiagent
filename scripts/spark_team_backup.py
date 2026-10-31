#!/usr/bin/env python3
"""Cold IM + public Runtime batches on one host, retaining original Runtime paths."""
import argparse
import copy
import json
import ipaddress
import os
from pathlib import Path
import queue
import re
import subprocess
import sys
import tarfile
import threading
import uuid
from urllib.parse import urlsplit

import spark_backup as im

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'cannbot/scripts/public_runtime_archive.py'


def plan(value):
    if value.get('schema') != 1 or not isinstance(value.get('runtimes'), list) or not 1 <= len(value['runtimes']) <= 12:
        raise ValueError('invalid public Runtime plan')
    ids = set()
    for item in value['runtimes']:
        if (set(item) - {'id', 'runtime', 'platform', 'python', 'config', 'config_root', 'credential_file'} or
                not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', item.get('id', '')) or item['id'] in ids or
                item.get('runtime') not in {'public_gateway', 'hermes_native'} or item.get('platform') not in {'posix', 'nt'} or
                not all(isinstance(item.get(k), str) and item[k] for k in ('python', 'config', 'config_root'))):
            raise ValueError('invalid or duplicate public Runtime descriptor')
        ids.add(item['id'])
    if sum(item['runtime'] == 'public_gateway' for item in value['runtimes']) != 1:
        raise ValueError('a batch requires exactly one public Gateway')
    return value


def native_path(path, item):
    return subprocess.check_output(['wslpath', '-w', str(path)], text=True).strip() if item['platform'] == 'nt' else str(path)


def host_path(path, item):
    return Path(subprocess.check_output(['wslpath', '-u', path], text=True).strip() if item['platform'] == 'nt' else path).resolve()


def helper_command(operation, item, directory, batch):
    command = [item['python'], native_path(HELPER, item), operation, '--runtime', item['runtime'],
        '--config', item['config'], '--config-root', item['config_root'],
        '--directory', native_path(directory, item), '--batch-id', batch]
    if item.get('credential_file'):command += ['--credential-file', item['credential_file']]
    return command


class Lease:
    def __init__(self, operation, item, directory, batch):
        self.batch, self.item = batch, item
        self.process = subprocess.Popen(helper_command(operation, item, directory, batch),
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
        self.events = queue.Queue()
        def read():
            try:
                for line in self.process.stdout:
                    if len(line) > 1024 * 1024:raise ValueError('oversized helper response')
                    self.events.put(json.loads(line))
                self.events.put(None)
            except Exception as error:self.events.put(error)
        self.reader = threading.Thread(target=read, daemon=True);self.reader.start()

    def receive(self, expected):
        try:value = self.events.get(timeout=300)
        except queue.Empty:raise TimeoutError('native helper did not finish ' + expected) from None
        if not isinstance(value, dict) or not value.get('ok') or value.get('state') != expected:
            detail = value.get('detail', '') if isinstance(value, dict) else ''
            raise RuntimeError('native helper refused ' + expected + ': ' + self.item['id'] + ': ' + detail)
        return value

    def call(self, operation, expected):
        if self.process.poll() is not None:raise RuntimeError('native helper exited before batch completion')
        self.process.stdin.write(json.dumps({'operation': operation, 'batch_id': self.batch}) + '\n')
        self.process.stdin.flush()
        return self.receive(expected)

    def release(self):
        self.call('release', 'released')
        if self.process.wait(timeout=30):raise RuntimeError('native freeze release failed')

    def close(self):
        if self.process.stdin and not self.process.stdin.closed:self.process.stdin.close()
        try:self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:self.process.kill();self.process.wait(timeout=10)
        self.reader.join(timeout=5)
        self.process.stdout.close()


def held(leases):
    if any(lease.process.poll() is not None for lease in leases):
        raise RuntimeError('a native freeze helper exited during the batch')


def endpoint_matches(url, listener, path, *, docker_host=False):
    value = urlsplit(url)
    if (value.scheme != 'http' or value.username or value.password or value.query or value.fragment or
            value.path.rstrip('/') != path or (value.port or 80) != listener['port']):
        raise ValueError('Runtime endpoint scheme, path or port differs from its listener')
    target = '127.0.0.1' if value.hostname == 'localhost' else value.hostname
    bind = '127.0.0.1' if listener['host'] == 'localhost' else listener['host']
    if target == 'host.docker.internal':
        if not docker_host or bind != '0.0.0.0':raise ValueError('Docker host alias requires the Gateway wildcard listener')
        return
    address = ipaddress.ip_address(target)
    if address.is_unspecified:raise ValueError('a target endpoint cannot use a wildcard address')
    if docker_host and address.is_loopback:
        raise ValueError('a container loopback endpoint cannot identify the host Gateway')
    if bind != '0.0.0.0':
        if target != bind:raise ValueError('Runtime endpoint host differs from its listener')
    elif not address.is_loopback:
        local = subprocess.check_output(['hostname', '-I'], text=True).split()
        if target not in local:raise ValueError('Runtime endpoint does not belong to this host')


def deployment_endpoints(config, public):
    logic = config['services']['logic']['environment']
    bridge = config['services']['agent-bridge']['environment']
    endpoint_matches(logic['SPARK_PUSH_AGENT_GATEWAY_ORIGIN'], public['listener'], '', docker_host=True)
    endpoint_matches(bridge['SPARK_PUSH_HERMES_BASE_URL'], public['listener'], '/v1', docker_host=True)
    if bridge['SPARK_PUSH_HERMES_BASE_URL'].removesuffix('/v1').rstrip('/') != logic['SPARK_PUSH_AGENT_GATEWAY_ORIGIN'].rstrip('/'):
        raise ValueError('Logic and Bridge use different Gateway endpoints')
    ports = config['services']['agent-authority'].get('ports', [])
    if len(ports) != 1:raise ValueError('missing authority endpoint')
    endpoint_matches(public['authority_origin'], {'host': ports[0].get('host_ip', '0.0.0.0'), 'port': int(ports[0]['published'])}, '')


def relationships(descriptors, records):
    gateway = next(item for item in descriptors if item['runtime'] == 'public_gateway')
    public = records[gateway['id']]
    native = {records[item['id']]['identity']['agent_id']: (item, records[item['id']])
              for item in descriptors if item['runtime'] == 'hermes_native'}
    expected = {entry['id']: entry for entry in public['agents'] if entry['runtime'] == 'hermes_native'}
    if len(native) != len(descriptors) - 1 or set(native) != set(expected):
        raise ValueError('every catalog Native, including disabled history, needs its own inventory entry')
    roots = []
    snapshot = host_path(public['snapshots'], gateway)
    for item in descriptors:
        record = records[item['id']]
        if record['platform'] != item['platform']:raise ValueError('helper native platform differs')
        roots.append(host_path(record['home'], item))
        if host_path(record['snapshots'], item) != snapshot:
            raise ValueError('Native must use the same physical snapshot store as the Gateway')
        if record['snapshots_sha256'] != public['snapshots_sha256']:
            raise ValueError('Native and Gateway snapshot contents differ')
    roots.append(snapshot)
    for i, first in enumerate(roots):
        if any(first == second or first in second.parents or second in first.parents for second in roots[i + 1:]):
            raise ValueError('Runtime state roots overlap')
    for agent_id, (item, record) in native.items():
        entry = expected[agent_id]
        if entry['profile'] != record['identity']['profile'] or not entry['credential_env_file']:
            raise ValueError('Native profile or credential mapping differs')
        if entry['expected_model'] != record['identity']['profile']:
            raise ValueError('Native advertised model identity differs')
        if (entry.get('transport') or 'http') != ('windows_stdio' if item['platform'] == 'nt' else 'http'):
            raise ValueError('Native transport does not reach its original native platform')
        endpoint_matches(entry['base_url'], record['listener'], '/v1')
        if record['authority_origin'] != public['authority_origin']:raise ValueError('Runtime authority endpoints differ')
        if host_path(entry['credential_env_file'], gateway) != host_path(record['credential_file'], item):
            raise ValueError('Gateway credential is not part of the corresponding Native backup')
    return gateway


def runtime_config(directory, record):
    with tarfile.open(directory / 'state.tar.gz') as archive:
        return json.loads(archive.extractfile(record['files'][record['config']]['member']).read())


def link_im(directory, manifest, config, descriptors, records):
    """Verify this IM and these Runtime states belonged to the same deployment."""
    gateway = relationships(descriptors, records)
    public = records[gateway['id']]
    own = runtime_config(directory / gateway['id'], public)
    if not {'agent-bridge', 'agent-authority'} <= set(config['services']):
        raise ValueError('the source deployment must contain the public Connector services')
    deployment_endpoints(config, public)
    mount_index = next((i for i, mount in enumerate(config['services']['logic'].get('volumes', []))
                        if mount.get('target') == '/run/spark-agent-config'), None)
    binding = next((entry for entry in manifest['bindings'].values()
                    if entry['service'] == 'logic' and entry['index'] == mount_index), None)
    if not binding:raise ValueError('IM has no archived public catalog mount')
    with tarfile.open(directory / 'im' / binding['archive']) as archive:
        for field, name in [('registry', 'agents.json'), ('projects', 'projects.json')]:
            import hashlib
            value = archive.extractfile('content/' + name).read()
            if hashlib.sha256(value).hexdigest() != public['files'][own[field]]['sha256']:
                raise ValueError('IM and Gateway catalogs differ')
    with tarfile.open(directory / 'im' / manifest['volumes']['spark_security']['archive']) as archive:
        member = next(m for m in archive.getmembers() if m.name.removeprefix('./') == 'security.json')
        security = json.loads(archive.extractfile(member).read())
    env = config['services']['logic']['environment']
    keys = {}
    for variable, field in [('SPARK_PUSH_HERMES_API_KEY', 'gateway'), ('SPARK_PUSH_WORKSPACE_AUTH_KEY', 'workspace')]:
        keys[variable] = (env.get(variable) or '').replace('$$', '$') or security[field]
    for item in descriptors:
        record = records[item['id']]
        cfg = runtime_config(directory / item['id'], record)
        with tarfile.open(directory / item['id'] / 'state.tar.gz') as archive:
            runtime_keys = json.loads(archive.extractfile(record['files'][cfg['keys_file']]['member']).read())
        expected = keys if item['runtime'] == 'public_gateway' else {'SPARK_PUSH_WORKSPACE_AUTH_KEY': keys['SPARK_PUSH_WORKSPACE_AUTH_KEY']}
        if any(runtime_keys.get(k) != v for k, v in expected.items()):raise ValueError('IM and Runtime keys differ')
        if cfg['authority_origin'] != own['authority_origin']:raise ValueError('Runtime authority origins differ')


def backup(args):
    specification = plan(json.loads(Path(args.runtime_plan).read_text()))
    directory = Path(args.output).resolve()
    if directory.exists():raise ValueError('batch output must not exist')
    batch = uuid.uuid4().hex
    record = {'schema': 'sparkpush.team-backup.v1', 'batch_id': batch, 'complete': False,
              'plan': specification, 'files': {}, 'scope': 'im-and-public-runtimes'}
    leases, records = [], {}
    try:
        for item in specification['runtimes']:
            lease = Lease('freeze', item, directory / item['id'], batch);leases.append(lease)
            records[item['id']] = lease.receive('frozen')['record']
        gateway = relationships(specification['runtimes'], records)
        source_config = json.loads(im.run(im.compose_command(args) + ['config', '--format', 'json']))
        deployment_endpoints(source_config, records[gateway['id']])
        for item in specification['runtimes']:
            for field in ('home', 'snapshots'):
                source = host_path(records[item['id']][field], item)
                if directory == source or source in directory.parents:raise ValueError('batch output is inside Runtime state')
        directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        im.save_json(directory / 'batch.json', record)
        held(leases)
        for lease in leases:records[lease.item['id']] = lease.call('archive', 'archived')['record']
        im_args = copy.copy(args);im_args.output = str(directory / 'im');im_args.leave_stopped = True
        im.backup(im_args)
        held(leases)
        _, manifest, config = im.verify(directory / 'im')
        link_im(directory, manifest, config, specification['runtimes'], records)
        record['files'] = {str(path.relative_to(directory)): {'sha256': im.digest(path), 'bytes': path.stat().st_size}
                           for path in directory.rglob('*') if path.is_file() and path.name != 'batch.json'}
        held(leases)
        record['complete'] = True;im.save_json(directory / 'batch.json', record)
        try:
            for lease in leases:lease.release()
        except BaseException:
            record['complete'] = False;im.save_json(directory / 'batch.json', record);raise
        return {'ok': True, 'batch_id': batch, 'backup': str(directory), 'source_left_stopped': True}
    finally:
        for lease in leases:lease.close()


def verify(directory):
    directory = Path(directory).resolve()
    manifest_path = directory / 'batch.json'
    if manifest_path.is_symlink() or manifest_path.stat().st_size > 1024 * 1024:raise ValueError('invalid batch manifest')
    record = json.loads(manifest_path.read_text())
    if (record.get('schema') != 'sparkpush.team-backup.v1' or record.get('complete') is not True or
            not re.fullmatch('[0-9a-f]{32}', record.get('batch_id', ''))):raise ValueError('incomplete Runtime batch')
    specification = plan(record['plan'])
    _, manifest, config = im.verify(directory / 'im')
    expected = {'im/manifest.json'} | {'im/' + name for name in manifest['files']}
    expected |= {item['id'] + '/' + name for item in specification['runtimes'] for name in ('runtime.json', 'state.tar.gz')}
    if set(record['files']) != expected:raise ValueError('batch inventory is incomplete')
    for name, value in record['files'].items():
        path = directory / name
        if path.is_symlink() or path.stat().st_size != value['bytes'] or im.digest(path) != value['sha256']:
            raise ValueError('batch file is missing or corrupt')
    records = {item['id']: json.loads((directory / item['id'] / 'runtime.json').read_text()) for item in specification['runtimes']}
    if any(value.get('batch_id') != record['batch_id'] for value in records.values()):raise ValueError('Runtime batch IDs differ')
    link_im(directory, manifest, config, specification['runtimes'], records)
    return directory, record, records


def enabled_compose(directory, target, project):
    _, manifest, original = im.verify(directory / 'im')
    disabled = json.loads((target / 'compose.json').read_text())
    config = copy.deepcopy(original)
    config.update(name=project, networks=disabled['networks'], volumes=disabled['volumes'])
    config['services']['gateway']['ports'] = disabled['services']['gateway']['ports']
    for key, record in manifest['bindings'].items():
        bind = target / 'bindings' / key
        if not bind.exists():
            bind.mkdir(mode=0o700, parents=True)
            with tarfile.open(directory / 'im' / record['archive']) as archive:archive.extractall(bind, members=im.safe_members(archive))
        config['services'][record['service']]['volumes'][record['index']]['source'] = str(bind / 'content')
    im.run(['docker', 'compose', '--env-file', '/dev/null', '-p', project, '-f', '-', 'config', '--quiet'], input=json.dumps(config).encode())
    im.save_json(target / 'compose.agents.json', config)


def restore_paths(target, directory, descriptors, records):
    if target == directory or directory in target.parents:
        raise ValueError('restore output must be outside its immutable backup')
    for item in descriptors:
        record = records[item['id']]
        for native in (record['home'], record['snapshots'], *record['files']):
            path = host_path(native, item)
            if target == path or target in path.parents or path in target.parents:
                raise ValueError('IM restore output overlaps a Runtime state or configuration target')


def restore(args):
    directory, batch, records = verify(args.backup)
    items = sorted(batch['plan']['runtimes'], key=lambda item: item['runtime'] != 'public_gateway')
    target = Path(args.output).resolve()
    restore_paths(target, directory, items, records)
    # All Runtime checks and the IM preflight precede any target data write.
    for item in items:
        result = subprocess.run(helper_command('preflight', item, directory / item['id'], batch['batch_id']),
                                capture_output=True, text=True, timeout=120)
        if result.returncode or json.loads(result.stdout).get('state') != 'ready':
            raise ValueError('native Runtime restore preflight refused: ' + item['id'])
    im_args = copy.copy(args);im_args.backup = str(directory / 'im')
    im.restore_preflight(im_args)
    leases = []
    try:
        for item in items:
            lease = Lease('restore', item, directory / item['id'], batch['batch_id']);leases.append(lease)
            lease.receive('restored')
        held(leases)
        result = im.restore(im_args)
        im.save_json(target / 'team-restore.json', {'complete': False, 'batch_id': batch['batch_id'], 'agents_enabled': False})
        enabled_compose(directory, target, args.project)
        held(leases)
        for lease in leases:lease.call('commit', 'committed')
        for lease in leases:lease.release()
        im.save_json(target / 'team-restore.json', {'complete': True, 'batch_id': batch['batch_id'], 'agents_enabled': False,
            'runtime_plan': batch['plan'], 'next': 'Validate restored IM, then down compose.json for this target project without -v. Start the original public Runtimes and cold-start compose.agents.json with --profile agents; do not hot-switch Logic.'})
        return {**result, 'scope': 'im-and-public-runtimes', 'batch_id': batch['batch_id'], 'agents_enabled': False}
    finally:
        for lease in leases:lease.close()


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='operation', required=True)
    create = commands.add_parser('backup')
    create.add_argument('--runtime-plan', required=True)
    create.add_argument('--project', required=True)
    create.add_argument('--env-file')
    create.add_argument('-f', '--file', action='append')
    create.add_argument('--profile', action='append', default=[])
    create.add_argument('--output', required=True)
    check = commands.add_parser('verify');check.add_argument('backup')
    recover = commands.add_parser('restore')
    for name in ('backup', 'project', 'output'):recover.add_argument('--' + name, required=True)
    recover.add_argument('--bind', default='127.0.0.1')
    recover.add_argument('--port', type=int, default=9010)
    args = parser.parse_args()
    try:
        result = backup(args) if args.operation == 'backup' else restore(args) if args.operation == 'restore' else {'ok': bool(verify(args.backup))}
        print(json.dumps(result));return 0
    except Exception as error:
        print(json.dumps({'ok': False, 'error': type(error).__name__, 'detail': str(error) if isinstance(error, (ValueError, RuntimeError)) else 'batch operation failed'}))
        return 1


if __name__ == '__main__':raise SystemExit(main())
