"""Owned real legacy Gateway/Compose fixture; never uses a personal workspace.

The normal Router, Pi RPC and native adapter do all execution. This module
only supplies trusted operator configuration and starts/stops test resources.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import signal
import subprocess
import sys
import threading
import time

from connector_deployment_e2e import ROOT, free_port, http
from hermes_room_runtime_fixture import HermesRoomRuntime

PI_BOT = 900000000001
HERMES_BOT = 900000000101


def serve(config):
    sys.path.insert(0, str(ROOT / 'cannbot/scripts'))
    from public_runtime import PublicHome, clean_environment, model_environment, install_shutdown, wait_process_group
    from public_agent_gateway import provision_models, PublicGatewayServer
    from pi_gateway import PiRpcClient, GatewayState, make_handler
    from agent_router import load_router
    # This is the existing general Gateway, deliberately not the public
    # launcher's admission policy. Nothing is installed in a personal home.
    with PublicHome(config['home'], {'runtime': 'legacy_test', 'id': 'acceptance'}) as home:
        pi = config['pi']
        models = provision_models(home, pi)
        agent_home = home.directory('prototype/agent-home')
        home.write_new('prototype/agent-home/models.json', models.read_text())
        working = home.directory('prototype/workspace')
        sessions = home.directory('prototype/sessions')
        env = clean_environment(home.directory('prototype/home'))
        env.update(model_environment(pi['model_env_file']))
        env.update(PI_CODING_AGENT_DIR=str(agent_home), CANN_KNOWLEDGE_ROOT=config['knowledge'])
        env['PATH'] = str(Path(pi['command']).parent) + os.pathsep + env['PATH']
        os.environ.clear()
        os.environ.update(clean_environment(home.directory('gateway-home')))
        os.environ.update(SPARK_PUSH_PI_WORKERS='1', SPARK_PUSH_ROOM_WORKSPACE_ROOT=str(home.directory('rooms')),
                          SPARK_PUSH_ROOM_TEST_RUNNER=config['runner'])
        argv = [pi['command'], '--mode', 'rpc', '--approve', '--session-dir', str(sessions),
                '--provider', pi['provider'], '--model', pi['model'], '--no-tools', '--no-extensions',
                '--no-skills', '--no-prompt-templates', '--no-themes', '--system-prompt',
                'You are Kernel Agent in an isolated private conversation. No tools are available.']
        prototype = PiRpcClient(argv, working, env, sessions)
        prototype.on_reap_failure = home.mark_unclean
        prototype.verify_reaped = wait_process_group
        router = server = None
        try:
            prototype.start()
            router = load_router(Path(config['registry']), prototype, home.directory('router'))
            # Exercise the actual Windows relay/capabilities before accepting
            # any IM request; never diagnose a miswired test port by submitting
            # a model turn and then treating its unknown state as retryable.
            for entry in router.entries.values():
                if entry['runtime'] == 'hermes_native':
                    router.adapters[entry['id']]._capabilities()
            # Observe the real admission decision, without replacing it. The
            # Bridge deliberately redacts HTTP error bodies in its final text.
            describe_route = router.describe_route
            admission_lock = threading.Lock()
            def observed_route(session, message=None, timeout_s=30):
                try:
                    return describe_route(session, message, timeout_s)
                except ValueError as error:
                    with admission_lock:
                        fd = os.open(home.root / 'admission-errors.jsonl', os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                        with os.fdopen(fd, 'w') as output:
                            output.write(json.dumps({'session_id': session, 'error': str(error)}) + '\n')
                    raise
            router.describe_route = observed_route
            server = PublicGatewayServer(('0.0.0.0', config['port']),
                make_handler(GatewayState(router, config['gateway_key'], 'legacy-acceptance')))
            install_shutdown(lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
            print('Owned legacy Gateway ready', flush=True)
            server.serve_forever()
        finally:
            errors = []
            for resource, method in ((server, 'server_close'), (router, 'stop'), (prototype, 'stop')):
                if resource:
                    try: getattr(resource, method)()
                    except BaseException as error: errors.append(error)
            if errors: raise errors[0]


class LegacyStack:
    def __init__(self, root, image, output, report):
        self.root, self.output, self.report = Path(root), Path(output), report
        self.proc = self.log = None
        self.started = False
        self.native = HermesRoomRuntime()
        self.root.chmod(0o755)
        self.config = self.root / 'agents'
        self.config.mkdir(mode=0o755)
        self.registry = self.config / 'agents.json'
        self.entries = [{'id': 'pi', 'name': 'Kernel Agent', 'runtime': 'pi_rpc',
                         'owner_user_ids': [], 'bot_user_id': PI_BOT}]
        self.write_registry()
        (self.config / 'projects.json').write_text(json.dumps({'schema': 1, 'projects': []}))
        self.envfile = self.root / '.env'
        self.envfile.write_text('')
        self.envfile.chmod(0o600)
        self.port, self.gateway_port, auth_port = free_port(), free_port(), free_port()
        host = subprocess.check_output(['hostname', '-I'], text=True).split()[0]
        self.base = f'http://127.0.0.1:{self.port}'
        self.key = secrets.token_hex(32)
        self.environment = {k: v for k, v in os.environ.items()
                            if not k.startswith(('SPARK_', 'COMPOSE_', 'HERMES_', 'PI_', 'KERNEL_'))}
        self.environment.update(SPARK_PUSH_IMAGE=image, SPARK_PUSH_PORT=str(self.port), SPARK_PUSH_BIND='127.0.0.1',
            SPARK_PUSH_CANN_DATA_DIR=str(self.root / 'public-cann-data'),
            SPARK_PUSH_HERMES_API_KEY=self.key, SPARK_PUSH_WORKSPACE_AUTH_KEY=secrets.token_hex(32),
            SPARK_PUSH_AGENT_CONFIG_DIR=str(self.config), SPARK_PUSH_AGENT_AUTH_PORT=str(auth_port),
            SPARK_PUSH_AGENT_AUTH_BIND=host, SPARK_PUSH_AGENT_GATEWAY_ORIGIN=f'http://{host}:{self.gateway_port}')
        self.project = 'spark-legacy-' + secrets.token_hex(6)
        self.options = ['--env-file', str(self.envfile), '-p', self.project, '-f', str(ROOT / 'docker-compose.yml'),
                        '-f', str(ROOT / 'docker-compose.legacy-cann.yml'), '-f', str(ROOT / 'docker-compose.cann.yml'), '--profile', 'agents']
        self.private_hashes = {p: hashlib.sha256(p.read_bytes()).hexdigest() for p in
            (Path('/home/peco/.pi-spark-agent/models.json'), Path('/home/peco/.pi-spark-agent/.env'))}

    def write_registry(self):
        self.registry.write_text(json.dumps({'schema': 1, 'default_agent': 'pi', 'bot_user_id': PI_BOT, 'agents': self.entries}))

    def compose(self, *args, input=None):
        result = subprocess.run(['docker', 'compose', *self.options, *args], cwd=ROOT, env=self.environment,
            input=input, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300)
        if result.returncode or args[0] in {'up', 'down', 'logs', 'restart'}:
            fd = os.open(self.output.with_suffix('.compose.private.log'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, 'w') as log: log.write(result.stdout)
        if result.returncode: raise RuntimeError('owned Compose operation failed: ' + args[0])
        return result.stdout

    def sql(self, query):
        return self.compose('exec', '-T', 'mysql', 'sh', '-c',
            'MYSQL_PWD="$(cat /run/spark-admin/mysql-root-password)" mysql -uroot --default-character-set=utf8mb4 --batch --raw --skip-column-names spark_push',
            input=query + ';\n').strip()

    def call(self, path, body, user=None, expected=0):
        status, raw = http(self.base, path, body, user['token'] if user else None)
        response = json.loads(raw)
        if response.get('code') != expected:
            fd = os.open(self.output.with_suffix('.http.private.log'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(fd, 'w') as output:
                output.write(json.dumps({'path': path, 'http_status': status, 'response': response}) + '\n')
        assert response.get('code') == expected, (path, response.get('code'), expected)
        if expected:
            self.report.setdefault('denials', []).append({'path': path, 'http_status': status,
                'wire_code': response['code'], 'actor_user_id': user['user_id'] if user else None})
        return response.get('data', {})

    def start_im(self):
        self.started = True
        self.compose('up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never')

    def preflight_knowledge(self):
        # Export only explicitly public documents; never chmod the original
        # 0600 knowledge files just to make container UID 10001 able to read.
        result = subprocess.run([sys.executable, str(ROOT / 'scripts/export_cann_documents.py'),
            '--knowledge-root', str(ROOT.parent / 'cann-agent-knowledge'),
            '--output', self.environment['SPARK_PUSH_CANN_DATA_DIR']], capture_output=True, text=True, check=True)
        self.report['knowledge_export'] = json.loads(result.stdout)
        # Actual Compose rendering, without starting databases or loading any
        # personal .env file. Missing bind directories must never be created.
        base = subprocess.run(['docker', 'compose', '--env-file', str(self.envfile), '-f', str(ROOT / 'docker-compose.yml'),
            'config', '--format', 'json'], env=self.environment, capture_output=True, text=True, check=True)
        plain = json.loads(base.stdout)
        assert not any(m.get('target') == '/run/spark-cann/data' for s in plain['services'].values() for m in s.get('volumes', []))
        effective = json.loads(self.compose('config', '--format', 'json'))
        assert effective['services']['logic']['environment']['SPARK_PUSH_ROOM_CANN_ENABLED'] == 'true'
        assert 'SPARK_PUSH_ROOM_AGENT_CATALOG' not in effective['services']['logic']['environment']
        assert 'agent-authority' not in effective['services']
        mounts = [m for m in effective['services']['logic']['volumes'] if m['target'] == '/run/spark-cann/data']
        # Compose may omit an explicitly false create_host_path in normalized
        # JSON. Check the source setting as well as the effective default.
        import yaml
        configured = yaml.safe_load((ROOT / 'docker-compose.cann.yml').read_text())
        assert configured['services']['logic']['volumes'][0]['bind']['create_host_path'] is False
        assert len(mounts) == 1 and mounts[0]['read_only'] and mounts[0].get('bind', {}).get('create_host_path', False) is False
        assert effective['services']['logic']['environment']['SPARK_PUSH_CANN_KNOWLEDGE_ROOT'] == '/run/spark-cann'
        missing = dict(self.environment)
        missing.pop('SPARK_PUSH_CANN_DATA_DIR')
        result = subprocess.run(['docker', 'compose', *self.options, 'config'], env=missing, capture_output=True, text=True)
        assert result.returncode != 0 and 'SPARK_PUSH_CANN_DATA_DIR' in result.stderr
        absent = self.root / 'not-a-knowledge-directory'
        self.started = True  # cleanup also owns a possible failed Docker create
        result = subprocess.run(['docker', 'run', '--rm', '--name', self.project + '-bind-probe',
            '--label', 'com.docker.compose.project=' + self.project, '--network', 'none', '--read-only',
            '--mount', 'type=bind,src=' + str(absent) + ',dst=/run/spark-cann/data,readonly',
            '--entrypoint', '/bin/true', self.environment['SPARK_PUSH_IMAGE']], capture_output=True, text=True)
        assert result.returncode != 0 and 'bind source path does not exist' in result.stderr and not absent.exists()
        self.report['knowledge_mount'] = {'base_has_no_dependency': True, 'required_variable_rejected': True,
            'missing_directory_rejected_without_creation': True, 'target': mounts[0]['target'], 'read_only': True}

    def start_gateway(self, owner):
        entry = self.native.start()
        # The existing transport uses this operator-only marker to permit an
        # isolated test loopback port. It does not enable room membership.
        entry = {k: v for k, v in entry.items() if not k.startswith('room_') or k == 'room_test_only'}
        entry.update(id='private-hermes', bot_user_id=HERMES_BOT, owner_user_ids=[owner['user_id']])
        self.entries.append(entry)
        self.write_registry()
        runner = self.root / 'runner.py'
        runner.write_text("import subprocess\nsubprocess.run(['g++','operator.cpp','-o','.operator-test'],check=True)\nsubprocess.run(['./.operator-test'],check=True)\nprint('host probe compiled and exited successfully')\n")
        self.gateway_config = {'home': str(self.root / 'gateway'), 'registry': str(self.registry),
            'port': self.gateway_port, 'gateway_key': self.key, 'runner': str(runner),
            'knowledge': str(ROOT.parent / 'cann-agent-knowledge'),
            'pi': {'command': '/home/peco/.local/node/bin/pi', 'provider': 'deepseek', 'model': 'deepseek-flash',
                   'models_file': '/home/peco/.pi-spark-agent/models.json', 'model_env_file': '/home/peco/.pi-spark-agent/.env'}}
        path = self.root / 'gateway-config.json'
        path.write_text(json.dumps(self.gateway_config))
        path.chmod(0o600)
        fd = os.open(self.root / 'gateway.log', os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        self.log = os.fdopen(fd, 'w')
        self.proc = subprocess.Popen([sys.executable, str(Path(__file__).resolve()), '--serve', str(path)],
                                     stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        for _ in range(300):
            if 'Owned legacy Gateway ready' in (self.root / 'gateway.log').read_text(errors='replace'): return
            if self.proc.poll() is not None: raise RuntimeError('owned legacy Gateway startup failed; inspect private log')
            time.sleep(.2)
        raise TimeoutError('owned legacy Gateway startup timed out')

    def stop_gateway(self):
        if self.proc and self.proc.poll() is None:
            os.kill(self.proc.pid, signal.SIGTERM)
            self.proc.wait(timeout=180)
        if self.log: self.log.close(); self.log = None

    def close(self):
        import shutil
        from team_backup_scenario import cleanup_project
        if self.started and not self.report.get('ok'):
            try: self.compose('logs', '--no-color', '--tail', '150', 'logic', 'comet', 'job', 'agent-bridge')
            except Exception: self.report['service_diagnostics_unavailable'] = True
        try:
            self.stop_gateway()
        finally:
            if self.native.log: self.native.log.flush()
            for source, suffix in ((self.root / 'gateway.log', '.gateway.private.log'),
                                   (self.native.root / 'runtime.log', '.native.private.log')):
                if source.exists():
                    target = self.output.with_suffix(suffix)
                    shutil.copyfile(source, target); target.chmod(0o600)
            try: self.native.close()
            finally:
                if self.started: cleanup_project(self.project)
        assert all(hashlib.sha256(p.read_bytes()).hexdigest() == digest for p, digest in self.private_hashes.items())
        self.report['cleanup'] = {'owned_compose_removed': True, 'owned_native_removed': not self.native.root.exists(),
            'private_pi_model_files_unchanged': True, 'original_windows_profile_unchanged': True}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--serve', required=True, type=Path)
    serve(json.loads(parser.parse_args().serve.read_text()))
