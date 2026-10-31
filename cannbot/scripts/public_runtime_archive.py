#!/usr/bin/env python3
"""Native-OS half of a cold public Runtime batch; never executes an Agent."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import stat
import sys
import tarfile

from public_runtime import PublicHome, runtime_lock, read_json, MODEL_KEYS
from public_runtime_maintenance import registered_home, emit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
from spark_backup import safe_members, digest, save_json

LOCK = '.spark-runtime.lock'
INCOMPLETE = '.spark-restore-incomplete'
RESERVED = {LOCK, INCOMPLETE}
CODE = ('public_runtime.py', 'public_runtime_maintenance.py', 'public_runtime_archive.py',
        'public_agent_gateway.py', 'hermes_public_gateway.py', 'pi_gateway.py',
        'room_agent.py', 'room_workspace.py', 'hermes_public_workspace.py',
        'agent_router.py', 'agent_scheduler.py', 'hermes_native_adapter.py', 'agent_events.py')


def plain(path, *, missing=False):
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts or path.resolve() != path:
        raise ValueError('only canonical absolute native paths are supported')
    for part in (path, *path.parents):
        try:info = part.lstat()
        except FileNotFoundError:
            if missing:continue
            raise
        if stat.S_ISLNK(info.st_mode) or getattr(info, 'st_file_attributes', 0) & 0x400:
            raise ValueError('links and Windows reparse points are not supported')
        if not (stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)):
            raise ValueError('only directories and regular files are supported')
        if stat.S_ISREG(info.st_mode) and info.st_nlink != 1:
            raise ValueError('hard-linked state is not supported')
    return path


def tree(root, excluded=()):
    root = plain(root)
    if not root.is_dir():raise ValueError('state root is not a directory')
    yield root, '.'
    def visit(directory):
        # WindowsPath ordering folds case; archive identity must use the same
        # byte-content/name order when WSL and Windows view one snapshot store.
        for path in sorted(directory.iterdir(), key=lambda value: value.name):
            relative = path.relative_to(root).as_posix()
            if relative in excluded:continue
            plain(path)
            yield path, relative
            if path.is_dir():yield from visit(path)
    yield from visit(root)


def tree_hash(root, excluded=()):
    value = hashlib.sha256()
    for path, relative in tree(root, excluded):
        item = [relative, 'directory' if path.is_dir() else digest(path)]
        value.update(json.dumps(item, ensure_ascii=True, separators=(',', ':')).encode() + b'\n')
    return value.hexdigest()


def archived_tree_hash(archive, prefix):
    members = [m for m in archive.getmembers() if PurePosixPath(m.name).parts[0] == prefix]
    by_name = {m.name: m for m in members}
    if prefix not in by_name or not by_name[prefix].isdir():raise ValueError('missing state root in archive')
    value = hashlib.sha256()
    for member in sorted(members, key=lambda m: PurePosixPath(m.name).parts):
        path = PurePosixPath(member.name)
        if len(path.parts) > 1 and (str(path.parent) not in by_name or not by_name[str(path.parent)].isdir()):
            raise ValueError('missing or non-directory archive parent')
        content = 'directory'
        if member.isfile():
            checksum = hashlib.sha256()
            with archive.extractfile(member) as source:
                for block in iter(lambda: source.read(1024 * 1024), b''):checksum.update(block)
            content = checksum.hexdigest()
        relative = PurePosixPath(*path.parts[1:]).as_posix()
        value.update(json.dumps([relative, content], ensure_ascii=True, separators=(',', ':')).encode() + b'\n')
    return value.hexdigest()


def config_paths(config_path, config, runtime, config_root, credential_file=None):
    root = plain(config_root, missing=True)
    paths = [Path(config_path), Path(config['keys_file'])]
    if runtime == 'public_gateway':
        paths += [Path(config[name]) for name in ('registry', 'projects')]
        paths.append(Path(config['pi']['models_file']))
        if config['pi'].get('model_env_file'):paths.append(Path(config['pi']['model_env_file']))
    else:
        paths.append(Path(config['api_key_file']))
        for name in ('model_env_file', 'model_auth_file'):
            if config.get(name):paths.append(Path(config[name]))
        if not credential_file:raise ValueError('Native requires its dedicated Gateway credential file')
        paths.append(Path(credential_file))
    result = sorted(set(paths))
    for path in result:
        if not path.is_absolute() or root not in path.parents:
            raise ValueError('configuration reference is outside the declared public config root')
        plain(path, missing=True)
    return result


def dependencies(config, runtime):
    paths = [Path(__file__).parent / name for name in CODE]
    paths.append(Path(sys.executable))
    if runtime == 'public_gateway':paths.append(Path(config['pi']['command']))
    else:
        source = Path(config['source'])
        paths += [source / name for name in ('gateway/platforms/api_server.py', 'run_agent.py', 'hermes_state.py', 'tools/file_tools.py')]
        if config.get('git_bash'):paths.append(Path(config['git_bash']))
    # Executable symlinks are allowed as dependencies, never as archived state.
    return {str(path): digest(path) for path in paths}


def inventory(args, home):
    config_path = plain(args.config)
    config = read_json(config_path)
    files = config_paths(config_path, config, args.runtime, args.config_root, args.credential_file)
    for path in files:
        plain(path)
        if not path.is_file() or path.stat().st_size > 65536:raise ValueError('invalid public configuration file')
    if args.runtime == 'public_gateway':
        # Backups never copy a private provider catalog or environment file.
        if read_json(config['pi']['models_file']) != read_json(home.root / 'public-models.json'):
            raise ValueError('provision a dedicated, selected public model file before backup')
    envfile = config.get('model_env_file') if args.runtime == 'hermes_native' else config['pi'].get('model_env_file')
    if envfile:
        allowed = MODEL_KEYS | ({'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY'} if args.runtime == 'hermes_native' else set())
        for line in Path(envfile).read_text(encoding='utf-8').splitlines():
            if line.strip() and not line.lstrip().startswith('#') and ('=' not in line or line.split('=', 1)[0].strip() not in allowed):
                raise ValueError('model environment must contain only dedicated public model settings')
    if args.runtime == 'hermes_native':
        values = [line.strip() for line in Path(args.credential_file).read_text().splitlines() if line.strip() and not line.lstrip().startswith('#')]
        if values != ['API_SERVER_KEY=' + Path(config['api_key_file']).read_text().strip()]:
            raise ValueError('Native Gateway credential does not match the public API key')
    snapshots = plain(config['snapshots'])
    if home.root == snapshots or home.root in snapshots.parents or snapshots in home.root.parents:
        raise ValueError('home and snapshot store must be separate directories')
    home.require_stopped()
    result = {'schema': 'sparkpush.runtime-archive.v1', 'batch_id': args.batch_id, 'runtime': args.runtime,
        'platform': os.name, 'identity': home.identity, 'home': str(home.root),
        'config': str(config_path), 'config_root': str(Path(args.config_root)),
        'credential_file': args.credential_file, 'snapshots': str(snapshots),
        'listener': {'host': config.get('host', '127.0.0.1'), 'port': config.get('port', 8643 if args.runtime == 'public_gateway' else 18644)},
        'authority_origin': config['authority_origin'],
        'home_sha256': tree_hash(home.root, RESERVED), 'snapshots_sha256': tree_hash(snapshots),
        'files': {str(p): {'member': 'config/' + str(i), 'sha256': digest(p)} for i, p in enumerate(files)},
        'dependencies': dependencies(config, args.runtime), 'lifecycle': home.state()}
    if args.runtime == 'public_gateway':
        registry = read_json(config['registry'])
        result['agents'] = [{k: entry.get(k) for k in ('id', 'runtime', 'profile', 'credential_env_file', 'base_url', 'transport', 'expected_model')}
                            for entry in registry['agents']]
    return result


def archive_state(args, home, record):
    output = plain(args.directory, missing=True)
    for source in (home.root, Path(record['snapshots'])):
        if output == source or source in output.parents:raise ValueError('archive output is inside source state')
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    path = output / 'state.tar.gz'
    with tarfile.open(path, 'w:gz', dereference=False) as archive:
        roots = [('home', home.root, RESERVED)]
        if args.runtime == 'public_gateway':roots.append(('snapshots', Path(record['snapshots']), ()))
        for prefix, root, exclusions in roots:
            for source, relative in tree(root, exclusions):
                archive.add(source, arcname=prefix if relative == '.' else prefix + '/' + relative, recursive=False)
        for source, item in record['files'].items():archive.add(source, arcname=item['member'], recursive=False)
    path.chmod(0o600)
    if inventory(args, home) != record:raise ValueError('public state changed during its freeze')
    record = {**record, 'archive_sha256': digest(path), 'archive_bytes': path.stat().st_size}
    save_json(output / 'runtime.json', record)
    validate_bundle(args)
    return record


def validate_bundle(args):
    directory = plain(args.directory)
    path = plain(directory / 'runtime.json')
    if path.stat().st_size > 1024 * 1024:raise ValueError('oversized Runtime manifest')
    record = json.loads(path.read_text())
    if (record.get('schema') != 'sparkpush.runtime-archive.v1' or record.get('batch_id') != args.batch_id or
            record.get('runtime') != args.runtime or record.get('platform') != os.name or
            record.get('config') != args.config or record.get('config_root') != args.config_root or
            record.get('credential_file') != args.credential_file):
        raise ValueError('Runtime archive identity or original native paths differ')
    archive_path = plain(directory / 'state.tar.gz')
    if archive_path.stat().st_size != record['archive_bytes'] or digest(archive_path) != record['archive_sha256']:
        raise ValueError('Runtime archive is missing or corrupt')
    with tarfile.open(archive_path) as archive:
        members = safe_members(archive)
        names = {member.name for member in members}
        expected_config = {item['member'] for item in record['files'].values()}
        if len(expected_config) != len(record['files']):raise ValueError('duplicate configuration mapping')
        for member in members:
            parts = PurePosixPath(member.name).parts
            if (not parts or member.name != PurePosixPath(member.name).as_posix() or any(':' in part for part in parts) or
                    (parts[0] not in {'home', 'snapshots'} and member.name not in expected_config) or
                    (parts[0] == 'snapshots' and args.runtime != 'public_gateway') or
                    (parts[0] == 'home' and len(parts) == 2 and parts[1] in RESERVED)):
                raise ValueError('unexpected or reserved Runtime archive member')
        if os.name == 'nt' and len({name.casefold() for name in names}) != len(names):
            raise ValueError('case-colliding Windows archive paths')
        if archived_tree_hash(archive, 'home') != record['home_sha256']:
            raise ValueError('Runtime contents differ from manifest')
        if args.runtime == 'public_gateway' and archived_tree_hash(archive, 'snapshots') != record['snapshots_sha256']:
            raise ValueError('snapshot contents differ from manifest')
        if not expected_config <= names:raise ValueError('configuration archive is incomplete')
        for original, item in record['files'].items():
            member = archive.getmember(item['member'])
            if not member.isfile() or member.size > 65536:raise ValueError('invalid configuration archive member')
            if hashlib.sha256(archive.extractfile(member).read()).hexdigest() != item['sha256']:
                raise ValueError('configuration archive digest differs')
        config = json.loads(archive.extractfile(record['files'][args.config]['member']).read())
        if (config['home'] != record['home'] or config['snapshots'] != record['snapshots'] or
                {str(p) for p in config_paths(args.config, config, args.runtime, args.config_root, args.credential_file)} != set(record['files'])):
            raise ValueError('archived paths do not match the original configuration')
        marker = json.loads(archive.extractfile('home/.spark-public-runtime.json').read())
        state = json.loads(archive.extractfile('home/.spark-runtime-state.json').read())
        if marker != record['identity'] or state != record['lifecycle'] or state['phase'] != 'clean' or state['unclean_latched']:
            raise ValueError('archived lifecycle is not clean')
        expected_identity = {'schema': 1, 'runtime': args.runtime}
        expected_identity.update({'id': config['id']} if args.runtime == 'public_gateway' else
                                 {'agent_id': config['agent_id'], 'profile': config['profile']})
        if marker != expected_identity:raise ValueError('archived service identity differs')
    if dependencies(config, args.runtime) != record['dependencies']:
        raise ValueError('install the original Runtime/code dependencies before restoring')
    return record


def preflight_restore(args):
    record = validate_bundle(args)
    for root in [record['home']] + ([record['snapshots']] if args.runtime == 'public_gateway' else []):
        if plain(root, missing=True).exists():raise ValueError('Runtime restore requires absent state directories')
    for original, item in record['files'].items():
        path = plain(original, missing=True)
        if path.exists() and (not path.is_file() or digest(path) != item['sha256']):
            raise ValueError('existing public configuration differs; nothing was overwritten')
    # Bind restored lifecycle to the original native OS before any mutation.
    from public_runtime import machine_identity, boot_identity, process_namespace, process_identity
    state = record['lifecycle']
    if state['machine_id'] != machine_identity():raise ValueError('cross-machine Runtime restore is unsupported')
    if state['boot_id'] == boot_identity() and (state['pid_namespace'] != process_namespace() or
            process_identity(state['pid']) == state['process_identity']):
        raise ValueError('original native Runtime has not provably exited')
    return record


def restore_state(args, record):
    home = plain(record['home'], missing=True)
    home.mkdir(mode=0o700, parents=True, exist_ok=False)
    lock = runtime_lock(home / LOCK, exclusive=True)
    try:
        (home / INCOMPLETE).write_text(args.batch_id)
        if args.runtime == 'public_gateway':Path(record['snapshots']).mkdir(mode=0o700, parents=True, exist_ok=False)
        configs = {item['member']: Path(original) for original, item in record['files'].items()}
        directories = []
        with tarfile.open(Path(args.directory) / 'state.tar.gz') as archive:
            for member in safe_members(archive):
                if member.name in configs:
                    target = configs[member.name]
                    if target.exists():
                        if digest(target) != record['files'][str(target)]['sha256']:raise ValueError('configuration changed during restore')
                        continue
                else:
                    parts = PurePosixPath(member.name).parts
                    target = Path(record['home'] if parts[0] == 'home' else record['snapshots']).joinpath(*parts[1:])
                plain(target, missing=True)
                if member.isdir():
                    target.mkdir(mode=0o700, parents=True, exist_ok=True)
                    directories.append((target, member.mode & 0o777))
                else:
                    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                    with target.open('xb') as output, archive.extractfile(member) as source:shutil.copyfileobj(source, output)
                    target.chmod(member.mode & 0o777)
        for target, mode in sorted(directories, key=lambda item: len(item[0].parts), reverse=True):target.chmod(mode)
        verify_restored(record)
        return lock
    except BaseException:
        lock.close()
        raise


def verify_restored(record):
    if tree_hash(record['home'], RESERVED) != record['home_sha256']:
        raise ValueError('restored Runtime state differs')
    if tree_hash(record['snapshots']) != record['snapshots_sha256']:
        raise ValueError('restored snapshot store differs')
    for path, item in record['files'].items():
        if digest(plain(path)) != item['sha256']:raise ValueError('restored public configuration differs')
    home = PublicHome(record['home'], record['identity'], create=False)
    if read_json(home.root / '.spark-public-runtime.json') != home.identity:raise ValueError('restored marker differs')
    home.require_stopped()


def command(batch):
    line = sys.stdin.readline(4097)
    value = json.loads(line)
    if len(line) > 4096 or set(value) != {'operation', 'batch_id'} or value['batch_id'] != batch:
        raise ValueError('invalid batch operation')
    return value['operation']


def main():
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('freeze', 'preflight', 'restore'))
    parser.add_argument('--runtime', required=True, choices=('public_gateway', 'hermes_native'))
    for field in ('config', 'config-root', 'directory', 'batch-id'):parser.add_argument('--' + field, required=True)
    parser.add_argument('--credential-file')
    args = parser.parse_args()
    for field in ('config', 'config_root', 'directory', 'credential_file'):
        if getattr(args, field):setattr(args, field, str(Path(getattr(args, field))))
    try:
        import re
        if not re.fullmatch('[0-9a-f]{32}', args.batch_id):raise ValueError('invalid batch identity')
        if args.operation == 'freeze':
            with registered_home(args.config, args.runtime) as home:
                record = inventory(args, home)
                emit({'ok': True, 'state': 'frozen', 'record': record})
                if command(args.batch_id) != 'archive':raise ValueError('expected archive operation')
                record = archive_state(args, home, record)
                emit({'ok': True, 'state': 'archived', 'record': record})
                if command(args.batch_id) != 'release':raise ValueError('expected release operation')
                current = inventory(args, home)
                if any(current[k] != record[k] for k in current):raise ValueError('state changed before release')
        else:
            record = preflight_restore(args)
            if args.operation == 'preflight':
                emit({'ok': True, 'state': 'ready', 'record': record});return 0
            lock = restore_state(args, record)
            try:
                emit({'ok': True, 'state': 'restored', 'record': record})
                if command(args.batch_id) != 'commit':raise ValueError('expected restore commit')
                verify_restored(record)
                emit({'ok': True, 'state': 'committed'})
                if command(args.batch_id) != 'release':raise ValueError('expected release operation')
                (Path(record['home']) / INCOMPLETE).unlink()
            finally:lock.close()
        emit({'ok': True, 'state': 'released'})
        return 0
    except Exception as error:
        emit({'ok': False, 'state': 'refused', 'error': type(error).__name__,
              'detail': str(error) if type(error) is ValueError else ''})
        return 1


if __name__ == '__main__':raise SystemExit(main())
