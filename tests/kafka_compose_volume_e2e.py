#!/usr/bin/env python3
"""Real production Compose Kafka restart and image-volume lifecycle regression."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='apache/kafka:3.7.0')
    parser.add_argument('--out', required=True, type=Path)
    args = parser.parse_args()
    output = args.out.resolve();output.parent.mkdir(parents=True, exist_ok=True)
    project = 'spark-kafka-mounts-' + secrets.token_hex(6)
    volume = project + '_spark_kafka_data'
    report = {'ok': False, 'mode': 'real production Compose Kafka; no model or mock',
              'sources': {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest()
                          for name in ('docker-compose.yml', 'tests/kafka_compose_volume_e2e.py')},
              'checks': []}
    environment = {k: v for k, v in os.environ.items() if not k.startswith(('SPARK_', 'COMPOSE_'))}
    def run(command, input=None):
        result = subprocess.run(command, cwd=ROOT, env=environment, input=input,
                                capture_output=True, text=True, timeout=300)
        with os.fdopen(os.open(output.with_suffix('.private.log'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), 'w') as log:
            log.write(result.stdout);log.write(result.stderr)
        if result.returncode: raise RuntimeError('owned Kafka regression command failed: ' + command[0])
        return result.stdout
    def check(text):
        report['checks'].append(text);print(text, flush=True)
    with tempfile.TemporaryDirectory(prefix=project) as temporary:
        override = Path(temporary) / 'images.json'
        override.write_text(json.dumps({'services': {role: {'image': args.image} for role in ('kafka', 'kafka-init')}}))
        base = ['docker', 'compose', '--env-file', '/dev/null', '-p', project,
                '-f', str(ROOT / 'docker-compose.yml'), '-f', str(override)]
        def compose(*args): return run([*base, *args])
        def containers():
            ids = run(['docker', 'ps', '-aq', '--filter', 'label=com.docker.compose.project=' + project]).split()
            return json.loads(run(['docker', 'inspect', *ids])) if ids else []
        def inspect_mounts():
            result = {}
            for container in containers():
                role = container['Config']['Labels']['com.docker.compose.service']
                assert role in ('kafka', 'kafka-init')
                mounts = {m['Destination']: m for m in container['Mounts']}
                ephemeral = {'/etc/kafka/secrets', '/mnt/shared/config'}
                if role == 'kafka-init': ephemeral.add('/var/lib/kafka/data')
                assert set(container['HostConfig'].get('Tmpfs', {})) == ephemeral
                # Docker records tmpfs separately from its volume/bind Mounts.
                live_tmpfs = {}
                if role == 'kafka':
                    assert container['State']['Running']
                    live = compose('exec', '-T', 'kafka', 'cat', '/proc/mounts')
                    types = {line.split()[1]: line.split()[2] for line in live.splitlines()}
                    live_tmpfs = {path: types[path] for path in ephemeral}
                    assert set(live_tmpfs.values()) == {'tmpfs'}
                else:
                    assert container['State']['Status'] == 'exited' and container['State']['ExitCode'] == 0
                volumes = [m['Name'] for m in mounts.values() if m['Type'] == 'volume']
                assert volumes == ([volume] if role == 'kafka' else [])
                if role == 'kafka': assert mounts['/var/lib/kafka/data']['Name'] == volume
                result[role] = {'image_id': container['Image'], 'mount_types': {path: mount['Type'] for path, mount in mounts.items()},
                                'tmpfs_paths': sorted(ephemeral), 'live_tmpfs_types': live_tmpfs, 'volumes': volumes}
            assert set(result) == {'kafka', 'kafka-init'}
            return result
        def kafka(tool, *args, input=None):
            return run([*base, 'exec', '-T', 'kafka', '/opt/kafka/bin/' + tool + '.sh',
                        '--bootstrap-server', '127.0.0.1:29092', *args], input)
        def identity():
            text = compose('exec', '-T', 'kafka', 'cat', '/var/lib/kafka/data/meta.properties')
            return dict(line.split('=', 1) for line in text.splitlines() if line and not line.startswith('#'))
        assert not containers()
        assert not run(['docker', 'volume', 'ls', '-q', '--filter', 'name=^' + volume + '$']).strip()
        try:
            compose('up', '-d', '--no-build', '--pull', 'never', '--wait', '--wait-timeout', '180', 'kafka', 'kafka-init')
            compose('wait', 'kafka-init')
            report['before'] = inspect_mounts()
            # Audit the same init service's live mount namespace without changing
            # its image or mounts; the production initializer has already exited.
            probe = compose('run', '--rm', '--no-deps', '-T', '--entrypoint', '/bin/cat', 'kafka-init', '/proc/mounts')
            types = {line.split()[1]: line.split()[2] for line in probe.splitlines()}
            report['init_live_mount_types'] = {path: types[path] for path in report['before']['kafka-init']['tmpfs_paths']}
            assert set(report['init_live_mount_types'].values()) == {'tmpfs'}
            topics = set(kafka('kafka-topics', '--list').splitlines())
            expected = {'push_single', 'push_group', 'push_to_comet', 'broadcast_task', 'persist_message',
                        'ai_request', 'ai_delta', 'ai_reply', 'ai_request.dlq', 'ai_reply.dlq', 'persist_message.dlq'}
            assert expected <= topics
            check('production broker and topic initializer start with five tmpfs mounts and one named data volume')
            topic = 'volume-proof-' + secrets.token_hex(6);marker = secrets.token_hex(24)
            kafka('kafka-topics', '--create', '--topic', topic, '--partitions', '1', '--replication-factor', '1')
            kafka('kafka-console-producer', '--topic', topic, input=marker + '\n')
            kafka('kafka-consumer-groups', '--group', topic, '--topic', topic, '--reset-offsets', '--to-offset', '1', '--execute')
            before = identity();assert before['cluster.id']
            compose('down')
            assert not containers()
            assert run(['docker', 'volume', 'ls', '-q', '--filter', 'name=^' + volume + '$']).strip() == volume
            compose('up', '-d', '--no-build', '--pull', 'never', '--wait', '--wait-timeout', '180', 'kafka', 'kafka-init')
            compose('wait', 'kafka-init')
            report['after'] = inspect_mounts();assert report['before'] == report['after']
            assert identity() == before
            assert kafka('kafka-console-consumer', '--topic', topic, '--partition', '0', '--offset', '0', '--max-messages', '1', '--timeout-ms', '10000').strip() == marker
            rows = [line.split() for line in kafka('kafka-consumer-groups', '--group', topic, '--describe').splitlines() if topic in line]
            assert rows and rows[0][3] == '1'
            report['cluster_id'] = before['cluster.id']
            check('ordinary down/up preserves cluster identity, exact record and committed consumer offset without anonymous volumes')
            report['ok'] = True
        finally:
            try:
                compose('down', '-v', '--remove-orphans')
                assert not containers()
                assert not run(['docker', 'volume', 'ls', '-q', '--filter', 'name=^' + volume + '$']).strip()
                report['cleanup_complete'] = True
            except Exception:
                report['cleanup_complete'] = False;report['ok'] = False
                raise
            finally:
                output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + '\n')


if __name__ == '__main__': main()
