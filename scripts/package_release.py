#!/usr/bin/env python3
"""Package one committed tree and pinned Linux/amd64 images for distribution.

Maintainer tool: requires Docker, Git and PyYAML. Never reads working-tree file
contents, personal environment files or Runtime state into the archive.
"""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile

import yaml

ROOT = Path(__file__).resolve().parents[1]
SOURCE = 'https://github.com/specetator/grpc_aiagent'
IMAGE = 'ghcr.io/specetator/spark-push'
DEPENDENCIES = ('mysql:8.0', 'redis:7.2-alpine', 'apache/kafka:3.7.0', 'caddy:2.10-alpine')
COMPOSE_FILES = ('docker-compose.yml', 'docker-compose.agents.yml')


def run(*args, cwd=ROOT):
    return subprocess.check_output(args, cwd=cwd)


def sha(data):
    return hashlib.sha256(data).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2) + '\n').encode()


def tracked_tree(root, revision):
    if not re.fullmatch('[0-9a-f]{40}', revision):
        raise ValueError('a full committed revision is required')
    if run('git', 'rev-parse', 'HEAD', cwd=root).decode().strip() != revision:
        raise ValueError('check out the requested revision before packaging')
    tracked = set(run('git', 'ls-files', '-z', cwd=root).decode().rstrip('\0').split('\0'))
    files = {}
    with tarfile.open(fileobj=io.BytesIO(run('git', 'archive', '--format=tar', revision, cwd=root))) as archive:
        for member in archive.getmembers():
            if member.isdir():
                continue
            name = member.name
            if (not member.isfile() or name not in tracked or name in files or
                    PurePosixPath(name).is_absolute() or '..' in PurePosixPath(name).parts):
                raise ValueError('only committed regular files may enter a release')
            if name in {'.env', '.env.local', '.env.rpc'}:
                raise ValueError('private environment file is tracked; refuse release')
            files[name] = (archive.extractfile(member).read(), 0o755 if member.mode & 0o111 else 0o644)
    if set(files) != tracked:
        raise ValueError('index differs from committed tree or includes unsupported entries')
    return files


def registry_identity(reference):
    repository, digest = reference.split('@')
    value = json.loads(run('docker', 'buildx', 'imagetools', 'inspect', reference, '--raw'))
    if 'manifests' in value:
        selected = [item for item in value['manifests'] if item.get('platform', {}).get('os') == 'linux'
                    and item.get('platform', {}).get('architecture') == 'amd64']
        if len(selected) != 1:
            raise ValueError('registry image must select exactly one linux/amd64 manifest')
        digest = selected[0]['digest']
        if not re.fullmatch(r'sha256:[0-9a-f]{64}', digest): raise ValueError('invalid platform manifest')
        value = json.loads(run('docker', 'buildx', 'imagetools', 'inspect', repository + '@' + digest, '--raw'))
    config = value.get('config', {}).get('digest', '')
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', config): raise ValueError('invalid image config digest')
    return {'platform_manifest': digest, 'config_digest': config}


def inspect_image(reference, revision=None):
    data = json.loads(run('docker', 'image', 'inspect', reference))[0]
    if (data['Os'], data['Architecture']) != ('linux', 'amd64'):
        raise ValueError('release supports linux/amd64 only')
    repository = reference.split('@')[0].split(':')[0]
    digests = [value for value in data.get('RepoDigests', []) if value.startswith(repository + '@sha256:')]
    if not digests:
        raise ValueError('pull the image from its registry before packaging')
    pinned = reference if '@sha256:' in reference else digests[0]
    if pinned not in digests:
        raise ValueError('image registry digest does not match the requested reference')
    identity = registry_identity(pinned)
    if data['Id'] not in {pinned.split('@')[1], identity['platform_manifest'], identity['config_digest']}:
        raise ValueError('local image identity does not belong to the registry graph')
    result = {'reference': pinned, 'builder_image_id': data['Id'], 'platform': 'linux/amd64', **identity}
    if revision:
        labels = data.get('Config', {}).get('Labels') or {}
        if labels.get('org.opencontainers.image.revision') != revision or labels.get('org.opencontainers.image.source') != SOURCE:
            raise ValueError('application OCI source/revision differs from release source')
        result['revision'] = revision
    return result


def pin_compose(files, application, dependencies):
    services = {}
    for name in COMPOSE_FILES:
        value = yaml.safe_load(files[name][0])
        entries = list(value['services'].items())
        if 'x-application' in value:
            entries.append(('x-application', value['x-application']))
        for role, spec in entries:
            image = spec.get('image')
            if image is None:
                continue
            spec.pop('build', None)
            if image.startswith('${SPARK_PUSH_IMAGE:'):
                record = application
            elif image in dependencies:
                record = dependencies[image]
            else:
                raise ValueError('unrecognized release image: ' + image)
            spec['image'] = record['reference']
            spec['platform'] = record['platform']
            if role != 'x-application':
                services[role] = record['reference']
        files[name] = (yaml.safe_dump(value, sort_keys=False, allow_unicode=True).encode(), 0o644)
    return services


def package(root, revision, version, application, dependencies, output):
    if not re.fullmatch(r'v[0-9]+\.[0-9]+\.[0-9]+', version):
        raise ValueError('use the target semantic version, without an rc suffix')
    files = tracked_tree(root, revision)
    services = pin_compose(files, application, dependencies)
    manifest = {'schema': 'sparkpush.release.v1', 'version': version, 'revision': revision,
                'source': SOURCE, 'application': application, 'dependencies': dependencies,
                'services': services, 'generated_files': list(COMPOSE_FILES),
                'files': {name: {'sha256': sha(data), 'mode': mode} for name, (data, mode) in files.items()}}
    files['release.json'] = (json_bytes(manifest), 0o644)
    output.mkdir(parents=True, exist_ok=False)
    filename = 'spark-push-' + version + '-linux-amd64.tar.gz'
    with (output / filename).open('xb') as raw:
        with gzip.GzipFile(fileobj=raw, mode='wb', filename='', mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode='w', format=tarfile.PAX_FORMAT) as archive:
                for name, (data, mode) in sorted(files.items()):
                    info = tarfile.TarInfo('spark-push-' + version + '/' + name)
                    info.size, info.mode, info.mtime = len(data), mode, 0
                    archive.addfile(info, io.BytesIO(data))
    (output / 'release.json').write_bytes(files['release.json'][0])
    checksums = ''.join(sha((output / name).read_bytes()) + '  ' + name + '\n' for name in (filename, 'release.json'))
    (output / 'SHA256SUMS').write_text(checksums)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', required=True)
    parser.add_argument('--version', default='v0.1.0')
    parser.add_argument('--image', required=True, help='application registry reference pinned by digest')
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    if not re.fullmatch(re.escape(IMAGE) + r'@sha256:[0-9a-f]{64}', args.image):
        parser.error('the application must be the project GHCR image pinned by digest')
    application = inspect_image(args.image, args.revision)
    dependencies = {tag: inspect_image(tag) for tag in DEPENDENCIES}
    package(ROOT, args.revision, args.version, application, dependencies, args.output)
    print(json.dumps({'revision': args.revision, 'image': args.image, 'output': str(args.output)}))


if __name__ == '__main__':
    main()
