#!/usr/bin/env python3
"""Real anonymous bundle install in fresh Linux userspace and a separate daemon.

The owned Docker-in-Docker container is privileged, with its own empty data
volume. Only the distribution archive is mounted read-only; no host socket,
credentials, model configuration or existing application volumes are mounted.
This is not a VM or an independent-kernel test.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tarfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from verify_release import verify_files, digest


def cleanup_owned(name, run):
    # Creation can succeed on the daemon even when the CLI fails or times out.
    # Query the unique ownership label instead of relying on an acknowledged run.
    for kind in ('container', 'volume'):
        query = ['docker', kind, 'ls', '-q', '--filter', 'label=spark.clean.acceptance=' + name]
        if kind == 'container': query.insert(3, '-a')
        owned = run(query).stdout.split()
        if owned:
            run(['docker', kind, 'rm', *(['-fv'] if kind == 'container' else []), *owned])
        if run(query).stdout.strip():
            raise RuntimeError('owned clean Linux resources remain')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--bundle', required=True, type=Path)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--dind-image', default='docker:29.1.3-dind')
    args = parser.parse_args()
    manifest = verify_files(ROOT)
    bundle = args.bundle.resolve()
    prefix = 'spark-push-' + manifest['version'] + '/'
    # Verify the complete archive before extracting it even in an owned container.
    with tarfile.open(bundle) as archive:
        members = archive.getmembers()
        expected = {prefix + name: row['sha256'] for name, row in manifest['files'].items()}
        expected[prefix + 'release.json'] = digest(ROOT / 'release.json')
        if len(members) != len(expected) or {m.name for m in members} != expected.keys():
            raise ValueError('archive member set differs from release manifest')
        for member in members:
            if not member.isfile() or hashlib.sha256(archive.extractfile(member).read()).hexdigest() != expected[member.name]:
                raise ValueError('archive is not the verified distribution')
    output = args.out.resolve();output.parent.mkdir(parents=True, exist_ok=True)
    log = output.with_suffix('.private.log')
    name = 'spark-clean-' + secrets.token_hex(6);volume = name + '-data'
    report = {'ok': False, 'scenario': 'clean_linux', 'source_commit': manifest['revision'],
              'archive_sha256': digest(bundle),
              'mode': 'real anonymous install in fresh Alpine Linux userspace and independent Docker daemon on a WSL host; not a VM',
              'sources': {f: digest(ROOT / f) for f in ('tests/clean_linux_release_e2e.py', 'tests/compose_e2e.py',
                          'tests/release_identity.py', 'scripts/verify_release.py')}, 'checks': []}
    def run(command, expected=0, timeout=900):
        result = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
        with os.fdopen(os.open(log, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), 'w') as stream:
            stream.write(result.stdout);stream.write(result.stderr)
        if expected is not None and result.returncode != expected:
            raise RuntimeError('owned clean Linux command failed; see private log')
        return result
    def check(text):
        report['checks'].append(text);print(text, flush=True)
    try:
        run(['docker', 'pull', '--platform', 'linux/amd64', args.dind_image])
        outer = json.loads(run(['docker', 'image', 'inspect', args.dind_image]).stdout)[0]
        report['test_daemon_image'] = {'id': outer['Id'], 'digests': outer.get('RepoDigests', [])}
        assert not run(['docker', 'volume', 'ls', '-q', '--filter', 'name=^' + volume + '$']).stdout.strip()
        run(['docker', 'volume', 'create', '--label', 'spark.clean.acceptance=' + name, volume])
        run(['docker', 'run', '-d', '--privileged', '--name', name,
             '--label', 'spark.clean.acceptance=' + name,
             '--mount', 'type=volume,src=' + volume + ',dst=/var/lib/docker',
             '--mount', 'type=bind,src=' + str(bundle) + ',dst=/input/release.tar.gz,readonly',
             '--env', 'DOCKER_CONFIG=/tmp/anonymous', '--env', 'DOCKER_TLS_CERTDIR=',
             outer['Id'], 'dockerd', '--host=unix:///var/run/docker.sock'])
        deadline = time.monotonic() + 90
        while True:
            probe = run(['docker', 'exec', name, 'docker', 'info', '--format', '{{json .}}'], expected=None, timeout=20)
            if probe.returncode == 0: break
            if time.monotonic() > deadline: raise RuntimeError('owned nested daemon did not become ready')
            time.sleep(1)
        initial = json.loads(probe.stdout)
        assert initial['Images'] == initial['Containers'] == 0
        assert not run(['docker', 'exec', name, 'docker', 'volume', 'ls', '-q']).stdout.strip()
        report['initial_daemon'] = {k: initial[k] for k in ('ID', 'ServerVersion', 'Images', 'Containers', 'DockerRootDir')}
        check('independent Linux daemon begins with zero images, containers and application volumes')
        # Only test-harness dependencies; the installed product needs Docker alone.
        run(['docker', 'exec', name, 'sh', '-ec',
             'mkdir /tmp/anonymous /release; apk add --no-cache python3 py3-virtualenv; '
             'python3 -m virtualenv /test-python; /test-python/bin/pip install websockets==17.4; '
             'tar -xzf /input/release.tar.gz -C /release'])
        directory = '/release/' + prefix.rstrip('/')
        execute = ['docker', 'exec', '-w', directory, name]
        run([*execute, '/test-python/bin/python', 'scripts/verify_release.py', 'files', '--directory', '.'])
        run([*execute, '/test-python/bin/python', 'scripts/verify_release.py', 'images', '--directory', '.', '--pull'])
        assert not run(['docker', 'exec', name, 'sh', '-c', 'test ! -e /tmp/anonymous/config.json']).returncode
        check('all pinned application/dependency digests pull anonymously into the empty daemon; OCI revision and bundle checksums match')
        run([*execute, '/test-python/bin/python', 'tests/compose_e2e.py', '--image', manifest['application']['reference'],
             '--out', '/tmp/clean-result.json'])
        actual = json.loads(run(['docker', 'exec', name, 'cat', '/tmp/clean-result.json']).stdout)
        assert actual['ok'] is True and actual['source_commit'] == manifest['revision']
        report['image_id'] = actual['image_id']
        report['release_services'] = actual['release_services']
        report['im_checks'] = actual['checks']
        check('copied example environment and prebuilt images pass existing ten-user IM, attachment ACL, reconnect and normal-restart scenarios')
        assert not run(['docker', 'exec', name, 'docker', 'ps', '-aq']).stdout.strip()
        assert not run(['docker', 'exec', name, 'docker', 'volume', 'ls', '-q']).stdout.strip()
        check('existing acceptance cleans its nested project and volumes without touching host deployments')
        report['ok'] = True
    finally:
        cleanup_errors = []
        try: cleanup_owned(name, run)
        except Exception: cleanup_errors.append('owned clean environment removal could not be verified')
        if cleanup_errors:
            report['ok'] = False;report['cleanup_errors'] = cleanup_errors
        report['cleanup_complete'] = not cleanup_errors
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')
    if not report['ok']: raise RuntimeError('clean Linux acceptance did not complete')


if __name__ == '__main__': main()
