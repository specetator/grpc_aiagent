"""Optional real cold-batch scenario used by the existing public model E2E."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def roundtrip(root, native, gateway, options, environment, port, output, cleanup, check, credentials):
    """Caller drained requests/clients; restore to a fresh owned Compose project."""
    native.stop_process();gateway.stop()
    batch = root / 'team-backup';deployment = root / 'team-restored'
    source_project = options[options.index('-p') + 1]
    project = source_project + '-restored'
    cleanup.append(project)
    specification = {'schema': 1, 'runtimes': [
        {'id': 'gateway', 'runtime': 'public_gateway', 'platform': 'posix', 'python': sys.executable,
         'config': str(gateway.root / 'config.json'), 'config_root': str(root)},
        {'id': 'hermes', 'runtime': 'hermes_native', 'platform': 'nt', 'python': native.python,
         'config': native.win(native.root / 'public-config.json'), 'config_root': native.win(native.root),
         'credential_file': native.win(native.root / 'api.env')}]}
    plan = root / 'runtime-plan.json';plan.write_text(json.dumps(specification));plan.chmod(0o600)
    cli = [sys.executable, str(ROOT / 'scripts/spark_team_backup.py')]
    environment = {key: value for key, value in environment.items() if key != 'COMPOSE_PROFILES'}
    def run(command, expected=0, input=None):
        result = subprocess.run(command, cwd=ROOT, env=environment, capture_output=True, text=True, timeout=900, input=input)
        path = output.with_suffix('.team.private.log')
        with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600), 'w') as log:
            log.write(result.stdout);log.write(result.stderr)
        if result.returncode != expected:raise RuntimeError('real team batch command failed: ' + command[2])
        return result.stdout
    run(cli + ['backup', '--runtime-plan', str(plan), '--project', source_project, '--env-file', str(root / '.env'),
        '-f', str(ROOT / 'docker-compose.yml'), '-f', str(ROOT / 'docker-compose.agents.yml'), '--profile', 'agents', '--output', str(batch)])
    run(cli + ['verify', str(batch)])
    manifest = json.loads((batch / 'batch.json').read_text())
    assert manifest['complete'] and len(manifest['plan']['runtimes']) == 2
    for name in ('gateway', 'hermes'):
        assert json.loads((batch / name / 'runtime.json').read_text())['batch_id'] == manifest['batch_id']
    check('real IM volumes and WSL Gateway/Windows Native archives form one batch; original services remain stopped')
    restore = cli + ['restore', '--backup', str(batch), '--project', project, '--output', str(deployment), '--port', str(port)]
    # Original state roots still exist: reject before any new project state.
    run(restore, 1);assert not deployment.exists()
    for path in (gateway.root / 'service', native.root / 'service', root / 'snapshots'):
        destination=path.with_name(path.name+'-before-restore')
        if path==native.root/'service':
            subprocess.run([native.python,'-c','from pathlib import Path;import sys;Path(sys.argv[1]).rename(sys.argv[2])',
                            native.win(path),native.win(destination)],check=True,timeout=30)
        else:path.rename(destination)
    damaged = batch / 'hermes/state.tar.gz';size = damaged.stat().st_size
    with damaged.open('ab') as stream:stream.write(b'controlled-corruption')
    run(restore, 1);assert not deployment.exists()
    with damaged.open('r+b') as stream:stream.truncate(size)
    assert not (gateway.root / 'service').exists() and not (native.root / 'service').exists()
    check('existing Runtime targets and a corrupt Native archive are refused before creating restore targets')
    public_files = [native.root / 'public-config.json', native.root / 'api-key', native.root / 'api.env',
                    gateway.root / 'config.json', gateway.root / 'public-models.json', gateway.root / 'public-model.env']
    before = {str(path): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mode) for path in public_files}
    # Exercise restoring a missing dedicated credential without touching a
    # personal profile or replacing any existing operator configuration.
    keys = root / 'connector-keys.json';key_digest = hashlib.sha256(keys.read_bytes()).hexdigest();keys.unlink()
    run(restore)
    assert before == {str(path): (hashlib.sha256(path.read_bytes()).hexdigest(), path.stat().st_mode) for path in public_files}
    assert hashlib.sha256(keys.read_bytes()).hexdigest() == key_digest
    disabled = json.loads((deployment / 'compose.json').read_text())
    assert 'agent-bridge' not in disabled['services'] and 'agent-authority' not in disabled['services']
    assert disabled['services']['logic']['environment']['SPARK_PUSH_HERMES_ENABLED'] == 'false'
    restore_receipt = json.loads((deployment / 'team-restore.json').read_text())
    assert restore_receipt['complete'] and restore_receipt['requires_cann_admission_verification'] is True
    check('fresh IM and original Runtime paths restored with Agents disabled; sessions/snapshots verified and existing public configs unchanged')
    def verify_mode(compose_options):
        running = run(['docker', 'compose', *compose_options, 'ps', '--services', '--status', 'running']).split()
        assert 'agent-bridge' not in running and 'agent-authority' not in running
        result = json.loads(run([sys.executable, str(ROOT / 'scripts/verify_public_agent_mode.py'),
            '--origin', 'http://127.0.0.1:' + str(port), '--credentials-stdin'],
            input=json.dumps({key: credentials[key] for key in ('account', 'password')})))
        assert result['verified'] is True and result['cann_enabled'] is False
        return result
    disabled_options = ['--env-file', '/dev/null', '-p', project, '-f', str(deployment / 'compose.json')]
    admission = {'disabled_im': verify_mode(disabled_options)}
    # Cold maintenance activation: retain the restored volumes while stopping
    # every old client before replacing Logic's configuration and container.
    run(['docker', 'compose', *disabled_options, 'down', '--remove-orphans'])
    enabled_im = ['--env-file', '/dev/null', '-p', project, '-f', str(deployment / 'compose.agents.json')]
    run(['docker', 'compose', *enabled_im, 'up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never'])
    admission['enabled_im_without_bridge'] = verify_mode(enabled_im)
    logic_id = run(['docker', 'compose', *enabled_im, 'ps', '-q', 'logic']).strip()
    assert logic_id
    check('restored disabled and enabled IM each prove boolean CANN admission false through authenticated CLI before public Runtimes or Bridge start')
    if getattr(gateway,'native_fault_marker',None):gateway.native_fault_marker.unlink(missing_ok=True)
    native.launch();gateway.start()
    restored_options = [*enabled_im, '--profile', 'agents']
    run(['docker', 'compose', *restored_options, 'up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never'])
    assert run(['docker', 'compose', *restored_options, 'ps', '-q', 'logic']).strip() == logic_id
    for role in ('logic', 'comet', 'job'):
        run(['docker', 'compose', *restored_options, 'exec', '-T', role, 'python3', '/app/deploy/health.py', role])
    expected_volumes={value['name'] for value in disabled['volumes'].values()}
    actual_volumes=set(run(['docker', 'volume', 'ls', '-q', '--filter', 'label=com.docker.compose.project='+project]).split())
    assert actual_volumes==expected_volumes
    check('operator cold-starts the enabled restored stack and original public Runtimes; actual readiness and all six volume identities are verified')
    return restored_options, {'batch_id': manifest['batch_id'], 'source_project': source_project, 'target_project': project,
        'default_agents_enabled': False, 'admission': admission, 'logic_not_recreated_when_bridge_added': True,
        'runtime_archives': {name: json.loads((batch / name / 'runtime.json').read_text())['archive_sha256']
                                                           for name in ('gateway', 'hermes')}}


def cleanup_project(project):
    """Only this test's registered, random project; also cleans partial restore."""
    def names(kind, label):
        command = ['docker', kind, 'ls', '-q', '--filter', 'label=' + label]
        if kind == 'container':command.insert(3, '-a')
        return subprocess.check_output(command, text=True).split()
    for kind, label in [('container', 'com.docker.compose.project=' + project),
                        ('volume', 'com.docker.compose.project=' + project),
                        ('network', 'com.docker.compose.project=' + project)]:
        values = names(kind, label)
        if values:
            subprocess.run(['docker', kind, 'rm', *(['--force'] if kind == 'container' else []), *values],
                           check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        assert not names(kind, label), 'owned project resources remain: ' + kind
