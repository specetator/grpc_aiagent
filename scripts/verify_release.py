#!/usr/bin/env python3
"""Verify a release bundle, pulled images, running containers or final evidence."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import subprocess
import tempfile

REQUIRED_CHECKS = ('web_im', 'web_agents', 'cross_room', 'team_backup',
                   'legacy_private', 'im_backup', 'cann_documents', 'clean_linux')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def safe_file(root, name):
    if not isinstance(name, str):
        raise ValueError('invalid manifest path')
    path = PurePosixPath(name)
    if path.is_absolute() or '..' in path.parts or str(path) != name:
        raise ValueError('invalid manifest path')
    target = root / name
    if any(p.is_symlink() for p in (target, *target.parents)) or not target.is_file():
        raise ValueError('release file is missing or is a link: ' + name)
    return target


def manifest(root):
    value = json.loads(safe_file(root, 'release.json').read_text())
    if (value.get('schema') != 'sparkpush.release.v1' or
            not re.fullmatch('[0-9a-f]{40}', value.get('revision', '')) or
            not re.fullmatch(r'ghcr\.io/specetator/spark-push@sha256:[0-9a-f]{64}', value['application']['reference']) or
            value['application']['revision'] != value['revision']):
        raise ValueError('invalid release identity')
    return value


def verify_files(root):
    value = manifest(root)
    for name, record in value['files'].items():
        path = safe_file(root, name)
        if digest(path) != record['sha256'] or path.stat().st_mode & 0o777 != record['mode']:
            raise ValueError('release file differs: ' + name)
    return value


def docker(*arguments):
    return json.loads(subprocess.check_output(['docker', *arguments]))


def image_ids(record):
    # Classic stores identify an image by its config; containerd stores can use
    # the index or selected manifest. All must belong to this immutable graph.
    values = {record['reference'].split('@')[1], record['platform_manifest'], record['config_digest']}
    if any(not re.fullmatch(r'sha256:[0-9a-f]{64}', value) for value in values):
        raise ValueError('invalid registry image identity')
    return values


def verify_images(value, pull=False):
    records = [value['application'], *value['dependencies'].values()]
    local_ids = {}
    for record in records:
        if pull:
            subprocess.run(['docker', 'pull', '--platform', 'linux/amd64', record['reference']], check=True)
        actual = docker('image', 'inspect', record['reference'])[0]
        if (actual['Id'] not in image_ids(record) or record['reference'] not in actual.get('RepoDigests', []) or
                actual['Os'] + '/' + actual['Architecture'] != record['platform']):
            raise ValueError('pulled image differs from release manifest')
        local_ids[record['reference']] = actual['Id']
        if record is value['application']:
            labels = actual.get('Config', {}).get('Labels') or {}
            if (labels.get('org.opencontainers.image.revision') != value['revision'] or
                    labels.get('org.opencontainers.image.source') != value['source']):
                raise ValueError('OCI source/revision mismatch')
    return local_ids


def verify_running(value, project):
    if not re.fullmatch('[a-z0-9][a-z0-9_-]{0,62}', project):
        raise ValueError('invalid owned Compose project')
    ids = subprocess.check_output(['docker', 'ps', '-aq', '--filter',
        'label=com.docker.compose.project=' + project], text=True).split()
    if not ids:
        raise ValueError('release project has no containers')
    # Containers must refer to the image inspected on this daemon, never to an
    # opaque ID copied from the CI daemon's storage implementation.
    images = verify_images(value)
    results = {}
    for container in docker('inspect', *ids):
        role = container['Config']['Labels']['com.docker.compose.service']
        expected = value['services'].get(role)
        if not expected or container['Image'] != images[expected] or container['Config']['Image'] != expected:
            raise ValueError('running service does not use the tested release image: ' + role)
        results[role] = {'container_id': container['Id'], 'image_id': container['Image'], 'reference': expected}
    if not {'logic', 'comet', 'job', 'web', 'gateway', 'mysql', 'redis', 'kafka'} <= results.keys():
        raise ValueError('release stack is incomplete')
    return results


def verify_evidence(root, evidence_path):
    value = manifest(root)
    evidence = json.loads(evidence_path.read_text())
    archive = 'spark-push-' + value['version'] + '-linux-amd64.tar.gz'
    if (evidence.get('schema') != 'sparkpush.release-acceptance.v1' or evidence.get('accepted') is not True or
            evidence.get('revision') != value['revision'] or
            evidence.get('image') != value['application']['reference'] or
            evidence.get('archive_sha256') != digest(safe_file(root, archive))):
        raise ValueError('final evidence does not identify this candidate')
    for name in REQUIRED_CHECKS:
        check = evidence.get('checks', {}).get(name, {})
        if (check.get('exit_code') != 0 or type(check.get('exit_code')) is not int or
                check.get('kind') != 'real' or check.get('receipt') != name + '.json'):
            raise ValueError('missing real acceptance: ' + name)
        path = safe_file(evidence_path.parent, check.get('receipt', ''))
        if digest(path) != check.get('sha256'):
            raise ValueError('acceptance receipt checksum mismatch: ' + name)
        receipt = json.loads(path.read_text())
        if (receipt.get('ok') is not True or receipt.get('source_commit') != value['revision'] or
                receipt.get('image_id') not in image_ids(value['application']) or receipt.get('scenario') != name):
            raise ValueError('acceptance receipt is for another candidate: ' + name)
        sources = receipt.get('source_sha256', receipt.get('sources', {}))
        if not sources or any(value['files'].get(file, {}).get('sha256') != checksum for file, checksum in sources.items()):
            raise ValueError('acceptance source differs from the tested bundle: ' + name)
        expected_images = {item['reference']: image_ids(item) for item in [value['application'], *value['dependencies'].values()]}
        running = receipt.get('release_services') or {}
        if not {'logic', 'comet', 'job', 'web', 'gateway', 'mysql', 'redis', 'kafka'} <= running.keys():
            raise ValueError('missing actual container identity evidence: ' + name)
        for role, container in running.items():
            expected = value['services'].get(role)
            if (not expected or container.get('reference') != expected or
                    container.get('image_id') not in expected_images[expected]):
                raise ValueError('running acceptance image differs: ' + name)
        if running['logic']['image_id'] != receipt['image_id']:
            raise ValueError('application ID is not the locally observed container image: ' + name)
    return evidence


def collect_evidence(root, destination):
    value = manifest(root)
    archive = 'spark-push-' + value['version'] + '-linux-amd64.tar.gz'
    result = {'schema': 'sparkpush.release-acceptance.v1', 'accepted': True,
              'revision': value['revision'], 'image': value['application']['reference'],
              'archive_sha256': digest(safe_file(root, archive)), 'checks': {}}
    for name in REQUIRED_CHECKS:
        receipt = safe_file(destination.parent, name + '.json')
        status = safe_file(destination.parent, name + '.exit').read_text().strip()
        if status != '0':
            raise ValueError('acceptance command did not exit successfully: ' + name)
        result['checks'][name] = {'exit_code': 0, 'kind': 'real', 'receipt': receipt.name, 'sha256': digest(receipt)}
    with tempfile.NamedTemporaryFile(mode='w', dir=destination.parent, suffix='.json', delete=False) as pending:
        json.dump(result, pending)
    try:
        verify_evidence(root, Path(pending.name))
        with destination.open('x') as output:
            output.write(json.dumps(result, sort_keys=True, indent=2) + '\n')
    finally:
        Path(pending.name).unlink()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('files', 'images', 'running', 'evidence', 'collect'))
    parser.add_argument('--directory', required=True, type=Path)
    parser.add_argument('--pull', action='store_true')
    parser.add_argument('--project')
    parser.add_argument('--evidence', type=Path)
    args = parser.parse_args()
    root = args.directory.absolute()
    value = verify_files(root) if args.operation == 'files' else manifest(root)
    result = {'verified': True, 'revision': value['revision'], 'image': value['application']['reference']}
    if args.operation == 'images':
        verify_images(value, args.pull)
    elif args.operation == 'running':
        result['services'] = verify_running(value, args.project or '')
    elif args.operation in ('evidence', 'collect'):
        if not args.evidence:
            parser.error('--evidence is required')
        if args.operation == 'collect': collect_evidence(root, args.evidence.absolute())
        else: verify_evidence(root, args.evidence)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    main()
