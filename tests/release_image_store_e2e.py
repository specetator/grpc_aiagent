#!/usr/bin/env python3
"""Real Docker store identity check using a public image, not Spark acceptance."""
import argparse
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
from package_release import inspect_image
from verify_release import image_ids
from clean_linux_release_e2e import cleanup_owned


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', required=True, type=Path)
    parser.add_argument('--image', default='caddy:2.10-alpine')
    args = parser.parse_args()
    report = {'ok': False, 'scope': 'actual public-image store identities; no Spark application or model acceptance', 'stores': {}}
    output = args.out.resolve()
    def run(command, expected=0):
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        with os.fdopen(os.open(output.with_suffix('.private.log'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), 'w') as stream:
            stream.write(result.stdout);stream.write(result.stderr)
        if expected is not None and result.returncode != expected: raise RuntimeError('owned store test command failed')
        return result
    try:
        run(['docker', 'pull', '--platform', 'linux/amd64', args.image])
        run(['docker', 'pull', '--platform', 'linux/amd64', 'docker:29.1.3-dind'])
        record = inspect_image(args.image);report['registry_image'] = record
        for store, flag in [('classic', 'false'), ('containerd', 'true')]:
            name = 'spark-store-' + secrets.token_hex(6)
            try:
                run(['docker', 'volume', 'create', '--label', 'spark.clean.acceptance=' + name, name + '-data'])
                run(['docker', 'run', '-d', '--privileged', '--name', name, '--label', 'spark.clean.acceptance=' + name,
                     '--mount', 'type=volume,src=' + name + '-data,dst=/var/lib/docker', '--env', 'DOCKER_CONFIG=/tmp/anonymous',
                     'docker:29.1.3-dind', 'dockerd', '--host=unix:///var/run/docker.sock', '--feature=containerd-snapshotter=' + flag])
                for attempt in range(60):
                    probe = run(['docker', 'exec', name, 'docker', 'info', '--format', '{{json .}}'], expected=None)
                    if probe.returncode == 0: break
                    time.sleep(1)
                if probe.returncode: raise RuntimeError('nested store did not become ready')
                run(['docker', 'exec', name, 'docker', 'pull', '--platform', 'linux/amd64', record['reference']])
                actual = json.loads(run(['docker', 'exec', name, 'docker', 'image', 'inspect', record['reference']]).stdout)[0]
                assert actual['Id'] in image_ids(record) and record['reference'] in actual['RepoDigests']
                assert actual['Os'] + '/' + actual['Architecture'] == record['platform']
                container = run(['docker', 'exec', name, 'docker', 'create', record['reference'], '/bin/true']).stdout.strip()
                observed = json.loads(run(['docker', 'exec', name, 'docker', 'inspect', container]).stdout)[0]
                assert observed['Image'] == actual['Id'] and observed['Config']['Image'] == record['reference']
                report['stores'][store] = {'engine_image_id': actual['Id'], 'container_image_id': observed['Image'],
                                           'reference': observed['Config']['Image'], 'driver': json.loads(probe.stdout)['Driver']}
            finally:
                cleanup_owned(name, run)
        assert report['stores']['classic']['engine_image_id'] == record['config_digest']
        assert report['stores']['classic']['engine_image_id'] != report['stores']['containerd']['engine_image_id']
        report['ok'] = True
    finally:
        output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report))


if __name__ == '__main__': main()
