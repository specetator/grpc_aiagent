#!/usr/bin/env python3
"""Cold backup/restore of one Spark Push Compose deployment (standard library).

IM state is one consistent set: MySQL, Kafka, Redis, attachments and both secret
volumes. External Agent runtime state is not part of these Docker volumes;
restoration deliberately keeps Agents disabled until that state is restored.
"""
import argparse
import copy
import datetime
import hashlib
import ipaddress
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import time
import uuid

ROOT = Path(__file__).resolve().parents[1]
VOLUMES = {'spark_admin', 'spark_security', 'spark_mysql_data', 'spark_redis_data',
           'spark_kafka_data', 'spark_attachments'}
DATA_SERVICES = {'mysql', 'redis', 'kafka'}
SERVICE_NAMES = {'bootstrap', 'migrate', 'kafka-init', 'logic', 'comet', 'job',
                 'web', 'gateway', 'mysql', 'redis', 'kafka', 'agent-bridge', 'agent-authority'}
CORE_SERVICES = SERVICE_NAMES - {'agent-bridge', 'agent-authority'}
PROJECT = re.compile(r'[a-z0-9][a-z0-9_-]{0,62}\Z')


def run(args, **kwargs):
    result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs)
    if result.returncode:
        # Compose config and Docker stderr can contain operator credentials.
        raise RuntimeError('command failed: ' + ' '.join(args[:3]))
    return result.stdout


def digest(path):
    result = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def save_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    with temporary.open('x', encoding='utf-8') as output:
        json.dump(value, output, ensure_ascii=False, indent=2)
        output.write('\n')
    temporary.chmod(0o600)
    temporary.replace(path)


def validate_tar(path):
    """Only directories and regular files; no links, devices or path escapes."""
    with tarfile.open(path, 'r:*') as archive:
        safe_members(archive)


def safe_members(archive):
    names = set()
    members = archive.getmembers()
    for member in members:
        name = PurePosixPath(member.name)
        if (name.is_absolute() or '..' in name.parts or '\\' in member.name or
                not (member.isdir() or member.isfile()) or name in names):
            raise ValueError('unsafe or duplicate archive member: ' + member.name + ':' + repr(member.type))
        names.add(name)
        if member.mode & 0o6000:
            raise ValueError('set-id files are not supported in state backups')
    if not names:
        raise ValueError('empty archive')
    return members


def inspect(kind, name):
    result = subprocess.run(['docker', kind, 'inspect', name], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if result.returncode:
        return None
    return json.loads(result.stdout)[0]


def compose_command(args):
    command = ['docker', 'compose', '--project-directory', str(ROOT)]
    if args.env_file:
        command += ['--env-file', str(Path(args.env_file).resolve())]
    command += ['-p', args.project]
    for filename in args.file or [str(ROOT / 'docker-compose.yml')]:
        command += ['-f', str(Path(filename).resolve())]
    for profile in args.profile:
        command += ['--profile', profile]
    return command


def source_containers(command):
    ids = run(command + ['ps', '-a', '-q'], text=True).split()
    if not ids:
        raise ValueError('deployment has no containers')
    values = json.loads(run(['docker', 'inspect', *ids]))
    result = {}
    for value in values:
        service = value['Config']['Labels']['com.docker.compose.service']
        if service in result:
            raise ValueError('multiple containers for one service are not supported')
        result[service] = value
    return result


def exclusive_volumes(config, project, containers):
    names = [value['name'] for value in config['volumes'].values()]
    if len(set(names)) != len(VOLUMES):
        raise ValueError('source volume names must be unique')
    for name in names:
        info = inspect('volume', name)
        if (not info or (info.get('Labels') or {}).get('com.docker.compose.project') != project or
                info.get('Driver') != 'local' or info.get('Options')):
            raise ValueError('only exclusive local volumes owned by this project are supported')
    ids = run(['docker', 'ps', '-aq'], text=True).split()
    allowed = {value['Id'] for value in containers.values()}
    if ids:
        for value in json.loads(run(['docker', 'inspect', *ids])):
            if value['Id'] not in allowed and any(
                    mount.get('Type') == 'volume' and mount.get('Name') in names
                    for mount in value.get('Mounts', [])):
                raise ValueError('a source volume is also mounted by another container')


def supported_config(config):
    """Scope this utility to the official single-host deployment, not arbitrary Compose."""
    if (set(config.get('volumes', {})) != VOLUMES or set(config['services']) - SERVICE_NAMES or
            not CORE_SERVICES <= set(config['services']) or
            set(config) - {'name', 'services', 'volumes', 'networks', 'x-application'}):
        raise ValueError('unsupported deployment configuration')
    for volume in config['volumes'].values():
        if set(volume) - {'name'}:
            raise ValueError('custom or external volumes are not supported')
    if set(config.get('networks', {})) - {'default'}:
        raise ValueError('custom networks are not supported')
    for network in config.get('networks', {}).values():
        if set(network) - {'name', 'ipam'} or network.get('ipam'):
            raise ValueError('custom networks are not supported')
    fields = {'image', 'build', 'pull_policy', 'command', 'entrypoint', 'environment',
              'depends_on', 'healthcheck', 'init', 'logging', 'networks', 'read_only',
              'restart', 'security_opt', 'stop_grace_period', 'tmpfs', 'user', 'volumes',
              'ports', 'profiles', 'extra_hosts'}
    for name, service in config['services'].items():
        if set(service) - fields:
            raise ValueError('unsupported service override')
        if set(service.get('networks', {})) - {'default'} or any(service.get('networks', {}).values()):
            raise ValueError('custom service network settings are not supported')
        if service.get('extra_hosts') and (name not in {'logic', 'agent-bridge', 'agent-authority'} or
                service['extra_hosts'] not in ({'host.docker.internal': 'host-gateway'},
                                              ['host.docker.internal=host-gateway'])):
            raise ValueError('host routing overrides are not supported')
        ports = service.get('ports', [])
        if ports and (name not in {'gateway', 'agent-authority'} or len(ports) != 1 or
                      ports[0].get('target') != (9010 if name == 'gateway' else 9011) or ports[0].get('protocol', 'tcp') != 'tcp'):
            raise ValueError('only the standard gateway port is supported')
        if any('$' in mount.get('source', '') for mount in service.get('volumes', []) if mount['type'] == 'bind'):
            raise ValueError('bind paths containing dollar signs are not supported')


def outside_sources(destination, config):
    sources = [Path(mount['source']).resolve() for service in config['services'].values()
               for mount in service.get('volumes', []) if mount['type'] == 'bind']
    for definition in config['volumes'].values():
        info = inspect('volume', definition['name'])
        if not info or not info.get('Mountpoint'):
            raise ValueError('source volume mountpoint is unavailable')
        sources.append(Path(info['Mountpoint']).resolve())
    if any(destination == source or source in destination.parents for source in sources):
        raise ValueError('backup output must be outside every source mount')


def wait_running(command, services):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        containers = source_containers(command)
        if all(name in containers and containers[name]['State']['Running'] and
               containers[name]['State'].get('Health', {}).get('Status', 'healthy') == 'healthy'
               for name in services):
            return
        time.sleep(1)
    raise RuntimeError('original deployment did not become healthy after backup')


def resume_containers(command, containers, running):
    # `compose start SERVICE` can also start dependencies that were deliberately
    # stopped before backup. Start only the captured container identities.
    stores = [name for name in running if name in DATA_SERVICES]
    applications = [name for name in running if name not in DATA_SERVICES]
    for group in (stores, applications):
        if group:
            run(['docker', 'start', *[containers[name]['Id'] for name in group]])
            wait_running(command, group)


def copy_binding(source, destination):
    source = Path(source)
    if not source.exists() or source.is_symlink():
        raise ValueError('bind source must exist and cannot be a symlink')
    with tarfile.open(destination, 'w:gz', dereference=False) as archive:
        archive.add(source, arcname='content')
    destination.chmod(0o600)
    validate_tar(destination)


def volume_archive(image, volume, path, extract=False, mysql=False):
    arguments = ['docker', 'run', '--rm', '--network', 'none', '--read-only',
                 '--user', '0', '--security-opt', 'no-new-privileges',
                 '--mount', 'type=volume,src=' + volume + ',dst=/state' + ('' if extract else ',readonly'),
                 '--entrypoint', 'tar', image, '--numeric-owner', '-C', '/state']
    if extract:
        with path.open('rb') as stream:
            # -i is a Docker option and must precede the image.
            arguments.insert(3, '-i')
            run(arguments + ['-xzpf', '-'], stdin=stream)
    else:
        excluded=[]
        if mysql:
            # The pinned official MySQL image leaves this compatibility link
            # after shutdown and recreates it at startup. It is not database
            # state. Never allow arbitrary links through the restore archive.
            probe="import os,pathlib; p=pathlib.Path('/state/mysql.sock'); print(os.readlink(p) if p.is_symlink() else '')"
            target=run(['docker','run','--rm','--network','none','--read-only','--user','0',
                '--mount','type=volume,src='+volume+',dst=/state,readonly','--entrypoint','python3',image,'-c',probe],text=True).strip()
            if target:
                if target!='/var/run/mysqld/mysqld.sock':
                    raise ValueError('unexpected MySQL compatibility socket link')
                arguments += ['--exclude=./mysql.sock'];excluded.append('mysql.sock')
        with path.open('xb') as stream:
            path.chmod(0o600)
            result = subprocess.run(arguments + ['-czpf', '-', '.'], stdout=stream, stderr=subprocess.PIPE)
            if result.returncode:
                raise RuntimeError('volume archive failed')
        validate_tar(path)
        return excluded


def backup(args):
    if not PROJECT.fullmatch(args.project):
        raise ValueError('invalid Compose project name')
    destination = Path(args.output).resolve()
    command = compose_command(args)
    config = json.loads(run(command + ['config', '--format', 'json']))
    supported_config(config)
    containers = source_containers(command)
    exclusive_volumes(config, args.project, containers)
    hashes = dict(line.split() for line in run(command + ['config','--hash','*'],text=True).splitlines() if line.strip())
    if any(value['Config']['Labels'].get('com.docker.compose.config-hash') != hashes.get(name)
           for name,value in containers.items()):
        raise ValueError('Compose settings differ from deployed containers; back up the deployed configuration')
    if any(value['State'].get('Paused') for value in containers.values()):
        raise ValueError('unpause deployment containers before a normal cold backup')
    running = [name for name, value in containers.items() if value['State']['Running']]
    if set(running).intersection({'bootstrap','migrate','kafka-init'}):
        raise ValueError('wait for initialization/migration to finish before backup')
    outside_sources(destination, config)
    destination.mkdir(mode=0o700, parents=True, exist_ok=False)
    manifest = {'schema': 'sparkpush.cold-backup.v1', 'complete': False, 'scope': 'im-only',
                'backup_id': uuid.uuid4().hex, 'created_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
                'source_project': args.project, 'volumes': {}, 'bindings': {}, 'images': {},
                'external_runtime_state': 'not included; restore keeps Agents disabled',
                'external_project_sources': 'not included', 'files': {}}
    source_commit = subprocess.run(['git', '-C', str(ROOT), 'rev-parse', 'HEAD'], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True)
    manifest['tool_checkout_revision'] = source_commit.stdout.strip() if source_commit.returncode == 0 else None
    manifest['application_revision'] = None
    save_json(destination / 'manifest.json', manifest)
    stopped = False
    try:
        # Stop ingress and all application writers before cold-stopping stores.
        stopped = True
        writers = [name for name in running if name not in DATA_SERVICES]
        if writers:
            run(command + ['stop', '--timeout', '60', *writers])
        stores = [name for name in running if name in DATA_SERVICES]
        if stores:
            run(command + ['stop', '--timeout', '60', *stores])
        frozen = source_containers(command)
        if any(value['State']['Running'] for value in frozen.values()):
            raise RuntimeError('all deployment containers must be stopped before archiving')
        exclusive_volumes(config, args.project, frozen)
        for name, service in config['services'].items():
            image = inspect('image', containers[name]['Image'] if name in containers else service['image'])
            if not image:
                raise ValueError('every configured image must be present locally')
            if name == 'logic':
                manifest['application_revision'] = (image['Config'].get('Labels') or {}).get('org.opencontainers.image.revision')
            manifest['images'][name] = {'id': image['Id'], 'digests': image.get('RepoDigests', []),
                                       'os': image['Os'], 'architecture': image['Architecture']}
            service['image'] = image['Id']
            service.pop('build', None)
            service.pop('pull_policy', None)
            for index, mount in enumerate(service.get('volumes', [])):
                if mount['type'] == 'bind':
                    if mount['target'] not in {'/etc/caddy/Caddyfile', '/run/spark-agent-config'}:
                        raise ValueError('unsupported bind mount in production deployment')
                    key = name + '-' + str(index)
                    filename = 'binding-' + key + '.tar.gz'
                    copy_binding(mount['source'], destination / filename)
                    manifest['bindings'][key] = {'archive': filename, 'service': name, 'index': index}
                    mount['source'] = key
        helper = manifest['images']['logic']['id']
        for logical, definition in config['volumes'].items():
            volume = definition['name']
            info = inspect('volume', volume)
            if not info or (info.get('Labels') or {}).get('com.docker.compose.project') != args.project:
                raise ValueError('volume is not owned by the selected Compose project')
            filename = 'volume-' + logical + '.tar.gz'
            excluded=volume_archive(helper, volume, destination / filename, mysql=logical=='spark_mysql_data')
            manifest['volumes'][logical] = {'archive': filename, 'source_name': volume,'excluded_runtime_files':excluded}
        config.pop('x-application', None)
        save_json(destination / 'compose.json', config)
        for path in destination.iterdir():
            if path.name != 'manifest.json':
                manifest['files'][path.name] = {'sha256': digest(path), 'bytes': path.stat().st_size}
        current = source_containers(command)
        if (set(current) != set(frozen) or any(current[name]['State']['Running'] or
                current[name]['Id'] != previous['Id'] or
                current[name]['State']['StartedAt'] != previous['State']['StartedAt']
                for name, previous in frozen.items())):
            raise RuntimeError('deployment changed while the cold backup was being archived')
        exclusive_volumes(config, args.project, current)
        manifest['complete'] = True
        save_json(destination / 'manifest.json', manifest)
        return {'ok': True, 'scope': manifest['scope'], 'backup': str(destination), 'backup_id': manifest['backup_id']}
    finally:
        # Do not rotate credentials or recreate services while returning the
        # original deployment to its prior running/stopped state.
        if stopped and running and not args.leave_stopped:
            resume_containers(command, containers, running)


def verify(directory):
    directory = Path(directory).resolve()
    manifest_path = directory / 'manifest.json'
    if manifest_path.is_symlink() or manifest_path.stat().st_size > 1024 * 1024:
        raise ValueError('invalid backup manifest')
    manifest = json.loads(manifest_path.read_text())
    if (manifest.get('schema') != 'sparkpush.cold-backup.v1' or manifest.get('complete') is not True or
            manifest.get('scope') != 'im-only' or set(manifest.get('volumes', {})) != VOLUMES or
            not re.fullmatch(r'[0-9a-f]{32}', manifest.get('backup_id', ''))):
        raise ValueError('incomplete or unsupported backup')
    for field in ('application_revision', 'tool_checkout_revision'):
        if field not in manifest or (manifest[field] is not None and not re.fullmatch(r'[0-9a-f]{40}', manifest[field])):
            raise ValueError('invalid revision metadata')
    required = {'compose.json'} | {item['archive'] for group in ('volumes', 'bindings') for item in manifest[group].values()}
    if (set(manifest.get('files', {})) != required or
            len(required) != 1 + len(manifest['volumes']) + len(manifest['bindings'])):
        raise ValueError('backup file manifest is incomplete')
    for name, record in manifest['files'].items():
        path = directory / name
        if (Path(name).name != name or path.is_symlink() or not path.is_file() or
                path.stat().st_size != record['bytes'] or digest(path) != record['sha256']):
            raise ValueError('backup file is missing or corrupt')
        if name != 'compose.json':
            validate_tar(path)
    config = json.loads((directory / 'compose.json').read_text())
    supported_config(config)
    if set(config['services']) != set(manifest['images']):
        raise ValueError('image manifest is incomplete')
    bindings = {}
    for key, item in manifest['bindings'].items():
        if (not re.fullmatch(r'[a-z0-9_-]+', key) or item['service'] not in config['services'] or
                type(item['index']) is not int or item['index'] < 0):
            raise ValueError('invalid bind archive mapping')
        position = (item['service'], item['index'])
        if position in bindings:
            raise ValueError('duplicate bind archive mapping')
        bindings[position] = key
    observed = set()
    for name, service in config['services'].items():
        image = manifest['images'][name]
        if service.get('image') != image['id'] or not re.fullmatch(r'sha256:[0-9a-f]{64}', image['id']):
            raise ValueError('deployment image is not pinned')
        if not image.get('architecture') or image.get('os') != 'linux':
            raise ValueError('image platform is missing or unsupported')
        if any(service.get(field) for field in ('privileged','cap_add','devices','volumes_from','pid','ipc','network_mode')):
            raise ValueError('host access overrides are not supported in restore bundles')
        for index, mount in enumerate(service.get('volumes', [])):
            if mount.get('type') == 'volume':
                if mount.get('source') not in VOLUMES:
                    raise ValueError('unknown data volume')
            elif mount.get('type') == 'bind':
                position = (name, index)
                if (bindings.get(position) != mount.get('source') or
                        mount.get('target') not in {'/etc/caddy/Caddyfile','/run/spark-agent-config'}):
                    raise ValueError('unpackaged or unsafe bind mount')
                observed.add(position)
            else:
                raise ValueError('unsupported mount type')
    if observed != set(bindings):
        raise ValueError('unused bind archive mapping')
    return directory, manifest, config


def restore_preflight(args):
    directory, manifest, config = verify(args.backup)
    if not PROJECT.fullmatch(args.project):
        raise ValueError('invalid target Compose project name')
    ipaddress.ip_address(args.bind)
    if not 1 <= args.port <= 65535:
        raise ValueError('invalid published port')
    target = Path(args.output).resolve()
    if '$' in str(target):
        raise ValueError('restore paths containing dollar signs are not supported')
    if target.exists():
        raise ValueError('restore deployment directory must not exist')
    names = {logical: args.project + '_' + logical for logical in VOLUMES}
    # All checks above and these collision checks precede every state write.
    existing = set(run(['docker','volume','ls','-q'],text=True).split())
    if existing.intersection(names.values()):
        raise ValueError('a target volume already exists; choose a fresh project')
    if run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + args.project], text=True).strip():
        raise ValueError('target project already has containers')
    for record in manifest['images'].values():
        image = inspect('image', record['id'])
        if not image:
            if not record['digests']:
                raise ValueError('local-only image missing; load the exact source image before restoring')
            run(['docker', 'pull', record['digests'][0]])
            image = inspect('image', record['id'])
        if not image or image['Os'] != record['os'] or image['Architecture'] != record['architecture']:
            raise ValueError('restoration requires the original image and platform')
    return directory, manifest, config, target, names


def restore(args):
    directory, manifest, config, target, names = restore_preflight(args)
    config = copy.deepcopy(config)
    config['name'] = args.project
    for network in config.get('networks', {}).values():
        network.pop('name', None)
        if network.get('external'):
            raise ValueError('external networks are not supported')
    for logical in VOLUMES:
        config['volumes'][logical] = {'name': names[logical]}
    config['services'].pop('agent-bridge', None)
    config['services'].pop('agent-authority', None)
    for service in config['services'].values():
        for removed in ('agent-bridge', 'agent-authority'):
            service.get('depends_on', {}).pop(removed, None)
    logic = config['services']['logic'].setdefault('environment', {})
    config['services']['logic'].pop('extra_hosts', None)
    for key in ('SPARK_PUSH_HERMES_ENABLED', 'SPARK_PUSH_ROOM_AGENT_ENABLED', 'SPARK_PUSH_ROOM_HERMES_PUBLIC'):
        logic[key] = 'false'
    for port in config['services']['gateway'].get('ports', []):
        port['published'], port['host_ip'] = str(args.port), args.bind
    # Resolve final bind paths and validate the entire plan before any write.
    for key, record in manifest['bindings'].items():
        if record['service'] in config['services']:
            config['services'][record['service']]['volumes'][record['index']]['source'] = str(target / 'bindings' / key / 'content')
    run(['docker','compose','--env-file','/dev/null','-p',args.project,'-f','-','config','--quiet'],
        input=json.dumps(config).encode())
    helper = manifest['images']['logic']['id']
    # Docker names are claimed atomically, including across CLI processes.
    # This empty, never-started container prevents two restores from creating
    # the same named volume between the preflight check and extraction.
    guard = run(['docker','create','--name','spark-restore-' + args.project,
                 '--label','spark.backup_id=' + manifest['backup_id'], '--network','none',
                 '--read-only','--entrypoint','/bin/true',helper],text=True).strip()
    try:
        existing = set(run(['docker','volume','ls','-q'],text=True).split())
        if existing.intersection(names.values()) or target.exists():
            raise ValueError('restore target changed after preflight')
        return restore_state(args, directory, manifest, config, names, target, helper)
    finally:
        run(['docker','rm',guard])


def restore_state(args, directory, manifest, config, names, target, helper):
    target.mkdir(mode=0o700, parents=True)
    save_json(target / 'restore-state.json', {'complete': False, 'backup_id': manifest['backup_id']})
    for key, record in manifest['bindings'].items():
        if record['service'] not in config['services']:
            continue
        bind = target / 'bindings' / key
        bind.mkdir(parents=True, mode=0o700)
        with tarfile.open(directory / record['archive']) as archive:
            # Python 3.8+ compatible explicit extraction; no links/devices.
            archive.extractall(bind, members=safe_members(archive))
        config['services'][record['service']]['volumes'][record['index']]['source'] = str(bind / 'content')
    for logical, name in names.items():
        run(['docker', 'volume', 'create', '--label', 'com.docker.compose.project=' + args.project,
             '--label', 'com.docker.compose.volume=' + logical, '--label', 'spark.backup_id=' + manifest['backup_id'], name])
        volume_archive(helper, name, directory / manifest['volumes'][logical]['archive'], extract=True)
    # Compose's canonical JSON already escapes dollars in env/commands for
    # re-parsing. Keep those values unchanged; do not escape them a second time.
    save_json(target / 'compose.json', config)
    command = ['docker', 'compose', '-p', args.project, '-f', str(target / 'compose.json')]
    run(command + ['up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never'])
    save_json(target / 'restore-state.json', {'complete': True, 'backup_id': manifest['backup_id'], 'agents_enabled': False})
    return {'ok': True, 'scope': 'im-only', 'project': args.project, 'deployment': str(target), 'agents_enabled': False}


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='action', required=True)
    create = commands.add_parser('backup')
    create.add_argument('--project', default='sparkpush')
    create.add_argument('-f', '--file', action='append')
    create.add_argument('--profile', action='append', default=[])
    create.add_argument('--env-file')
    create.add_argument('--output', required=True)
    create.add_argument('--leave-stopped', action='store_true')
    check = commands.add_parser('verify');check.add_argument('backup')
    recover = commands.add_parser('restore')
    recover.add_argument('--backup', required=True)
    recover.add_argument('--project', required=True)
    recover.add_argument('--output', required=True)
    recover.add_argument('--port', type=int, default=9010)
    recover.add_argument('--bind', default='127.0.0.1')
    args = parser.parse_args()
    try:
        result = backup(args) if args.action == 'backup' else restore(args) if args.action == 'restore' else {'ok': bool(verify(args.backup)), 'scope': 'im-only'}
        print(json.dumps(result))
        return 0
    except (OSError, ValueError, RuntimeError, KeyError, tarfile.TarError) as error:
        print('Backup operation failed: ' + str(error))
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
