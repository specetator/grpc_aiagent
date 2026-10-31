#!/usr/bin/env python3
"""Real owned Compose stack: optional configuration, keys and Logic authority.

No model or Agent runtime is used. This tests the deployment/authorization
boundary; it is deliberately not evidence of native code analysis or browser E2E.
Requires Python 3.11 and websockets. Logs and temporary secrets stay private.
"""
import argparse
import asyncio
from contextlib import AsyncExitStack
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import websockets

ROOT = Path(__file__).resolve().parents[1]

def free_port():
    with socket.socket() as listener:
        listener.bind(('127.0.0.1', 0))
        return listener.getsockname()[1]

def http(base, path, data=None, key=None, method=None):
    headers = {'Content-Type': 'application/json'}
    if key:
        headers['Authorization'] = 'Bearer ' + key
    request = urllib.request.Request(base + path, None if data is None else json.dumps(data).encode(), headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()

async def send(base, user, room, mentions=False, observer=None):
    async with AsyncExitStack() as stack:
        async def connect(account):
            return await stack.enter_async_context(websockets.connect(
                base.replace('http', 'ws', 1) + '/ws?token=' + account['token'], proxy=None))
        watching = await connect(observer) if observer else None
        if watching:
            await watching.send(json.dumps({'type': 'chatroom_join', 'group_id': room}))
            async with asyncio.timeout(30):
                async for raw in watching:
                    event = json.loads(raw)
                    if event.get('op') == 'chatroom_join':
                        assert event['type'] == 'ack'
                        break
        ws = await connect(user)
        identity = secrets.token_hex(16)
        body = {'type': 'chatroom', 'group_id': room, 'client_msg_id': identity,
                'content': {'text': '@Pi summarize' if mentions else 'ordinary chat survives optional Agent failure'}}
        if mentions:
            body['agent_mentions'] = ['public-pi']
        await ws.send(json.dumps(body))
        async with asyncio.timeout(30):
            async for raw in ws:
                event = json.loads(raw)
                if event.get('client_msg_id') == identity and event.get('type') in ('accepted_ack', 'error'):
                    assert event['type'] == 'accepted_ack', event.get('code')
                    message_id = event['msg_id']
                    break
        if watching:
            async with asyncio.timeout(30):
                async for raw in watching:
                    event = json.loads(raw)
                    if event.get('client_msg_id') == identity and event.get('type') == 'chatroom':
                        assert event['msg_id'] == message_id
                        break
        return message_id

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image', default='spark-push:connector')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    project = 'spark-connector-' + secrets.token_hex(6)
    output = args.out.resolve()
    receipt = {'ok': False, 'mode': 'real Compose / MySQL / Redis / Kafka / Logic; no Agent runtime',
               'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(), 'checks': []}
    def check(description):
        receipt['checks'].append(description)
        print(description, flush=True)
    with tempfile.TemporaryDirectory(prefix='spark-connector-') as temporary:
        directory = Path(temporary)
        configuration = directory / 'agents'
        configuration.mkdir(mode=0o755)
        # Container UID 10001 must be able to traverse this test-owned directory.
        directory.chmod(0o755)
        registry = json.loads((ROOT / 'deploy/agents/agents.json.example').read_text())
        catalog = configuration / 'agents.json'
        catalog.write_text(json.dumps(registry))
        projects = configuration / 'projects.json'
        projects.write_text(json.dumps({'schema': 1, 'projects': [{'id': 'source', 'name': 'Test source'}]}))
        envfile = directory / '.env'
        envfile.write_text('')
        envfile.chmod(0o600)
        environment = {k: v for k, v in os.environ.items() if not k.startswith(('SPARK_', 'COMPOSE_', 'KERNEL_', 'HERMES_', 'PI_'))}
        port, authority_port = free_port(), free_port()
        environment.update(SPARK_PUSH_IMAGE=args.image, SPARK_PUSH_PORT=str(port), SPARK_PUSH_BIND='127.0.0.1',
            SPARK_PUSH_AGENT_CONFIG_DIR=str(configuration), SPARK_PUSH_AGENT_AUTH_PORT=str(authority_port),
            SPARK_PUSH_AGENT_GATEWAY_ORIGIN='http://host.docker.internal:1')
        options = ['--env-file', str(envfile), '-p', project, '-f', str(ROOT / 'docker-compose.yml'),
                   '-f', str(ROOT / 'docker-compose.agents.yml'), '--profile', 'agents']
        def compose(*arguments):
            result = subprocess.run(['docker', 'compose', *options, *arguments], cwd=ROOT, env=environment,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=300)
            if arguments[0] in {'up', 'down', 'logs'}:
                fd = os.open(output.with_suffix('.private.log'), os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
                with os.fdopen(fd, 'w') as log:
                    log.write(result.stdout)
            if result.returncode:
                raise RuntimeError('Compose operation failed: ' + arguments[0])
            return result.stdout
        base = f'http://127.0.0.1:{port}'
        authority = f'http://127.0.0.1:{authority_port}'
        def call(path, body, user=None, expected=0):
            result = json.loads(http(base, path, body, user['token'] if user else None)[1])
            assert result.get('code') == expected, (path, result.get('code'), expected)
            return result.get('data', {})
        def auth(key, request, expected=0):
            result = json.loads(http(authority, '/internal/room-agent/authorize', {'request_id': request}, key)[1])
            assert result.get('code') == expected, ('authorization', result.get('code'), expected)
            return result.get('data', {})
        try:
            compose('up', '-d', '--wait', '--wait-timeout', '180', '--pull', 'never')
            check('optional overlay starts without any reachable Connector or model')
            # Exercise bootstrap's actual locked upgrade, not a duplicate implementation.
            before = json.loads(compose('exec', '-T', 'logic', 'python3', '-c',
                "from pathlib import Path;print(Path('/run/spark-secrets/security.json').read_text())"))
            compose('run', '--rm', '--no-deps', '--entrypoint', 'python3', 'bootstrap', '-c',
                "import json;from pathlib import Path;p=Path('/run/spark-secrets/security.json');v=json.loads(p.read_text());v.pop('workspace');v['future_extension']='preserve';p.write_text(json.dumps(v))")
            compose('run', '--rm', '--no-deps', 'bootstrap')
            after = json.loads(compose('exec', '-T', 'logic', 'python3', '-c',
                "from pathlib import Path;print(Path('/run/spark-secrets/security.json').read_text())"))
            assert all(after[k] == v for k, v in before.items() if k != 'workspace')
            assert after['future_extension'] == 'preserve' and len(after['workspace']) == 64
            compose('run', '--rm', '--no-deps', 'bootstrap')
            assert after == json.loads(compose('exec', '-T', 'logic', 'python3', '-c',
                "from pathlib import Path;print(Path('/run/spark-secrets/security.json').read_text())"))
            compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic')
            check('bootstrap upgrades old credentials once without changing existing or unknown values')
            keyfile = directory / 'connector.json'
            exported = subprocess.run(['python3', str(ROOT / 'scripts/ops/export-agent-credentials.py'), str(keyfile), '--', *options],
                cwd=ROOT, env=environment, capture_output=True)
            assert exported.returncode == 0
            keys = json.loads(keyfile.read_text())
            assert set(keys) == {'SPARK_PUSH_HERMES_API_KEY', 'SPARK_PUSH_WORKSPACE_AUTH_KEY'}
            assert keys['SPARK_PUSH_HERMES_API_KEY'] == after['gateway'] and keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'] == after['workspace']
            assert keyfile.stat().st_mode & 0o777 == 0o600
            assert subprocess.run(['python3', str(ROOT / 'scripts/ops/export-agent-credentials.py'), str(keyfile), '--', *options],
                cwd=ROOT, env=environment, capture_output=True).returncode != 0
            check('empty Compose keys use persisted keys; export contains only two keys and refuses overwrite')
            users = [call('/api/register', {'account': 'connector_' + secrets.token_hex(8), 'password': secrets.token_hex(24), 'name': name}) for name in ('A', 'B', 'C')]
            a, b, c = users
            room = call('/api/spaces/create', {'name': 'Connector acceptance'}, a)['room_id']
            def chat():
                message = asyncio.run(send(base, a, room, observer=b))
                for _ in range(100):
                    history = call('/api/session/history', {'session_id': f'r_{room}'}, b)['messages']
                    if any(item.get('msg_id') == message for item in history):
                        return
                    time.sleep(.1)
                raise AssertionError('accepted message was not persisted in member history')
            def agent_request():
                source = asyncio.run(send(base, b, room, True, observer=a))
                for _ in range(100):
                    data = call('/api/chatroom/agents/list', {'room_id': room}, a)
                    request = next((r for r in data['requests'] if r.get('source_msg_id') == source), None)
                    if request:
                        return request['request_id']
                    time.sleep(.1)
                raise AssertionError('structured mention must persist a request')
            call('/api/spaces/invite', {'room_id': room, 'user_id': b['user_id']}, a)
            call('/api/spaces/accept', {'room_id': room}, b)
            call('/api/chatroom/agents/install', {'room_id': room, 'agent_id': 'public-pi'}, a)
            call('/api/chatroom/workspace/set', {'room_id': room, 'project_id': 'source', 'generation': 0}, a)
            chat()
            check('Connector-offline chat is Kafka-accepted, delivered to another member and present in SQL history')
            identity = agent_request()
            canonical = auth(keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'], identity)
            assert canonical['collaboration']['actor_user_id'] == b['user_id']
            assert canonical['collaboration']['room_id'] == room and canonical['collaboration']['project_id'] == 'source'
            for wrong in (None, a['token'], keys['SPARK_PUSH_HERMES_API_KEY'], 'wrong-key'):
                auth(wrong, identity, 401)
            forged = json.loads(http(authority, '/internal/room-agent/authorize',
                {'request_id': identity, 'actor_user_id': a['user_id']}, keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'])[1])
            assert forged['code'] == 400
            for method, path in [('GET', '/internal/room-agent/authorize'), ('POST', '/api/spaces/list'),
                                 ('GET', '/metrics'), ('POST', '/internal/room-agent/authorize/'),
                                 ('POST', '/prefix/internal/room-agent/authorize')]:
                assert http(authority, path, {} if method == 'POST' else None, method=method)[0] == 404
            check('exact callback route uses its separate key and returns only the stored request scope')
            call('/api/chatroom/workspace/set', {'room_id': room, 'project_id': '', 'generation': 1}, a)
            auth(keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'], identity, 403)
            call('/api/chatroom/workspace/set', {'room_id': room, 'project_id': 'source', 'generation': 2}, a)
            auth(keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'], identity, 403)
            member_request = agent_request()
            auth(keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'], member_request)
            call('/api/spaces/remove', {'room_id': room, 'user_id': b['user_id']}, a)
            auth(keys['SPARK_PUSH_WORKSPACE_AUTH_KEY'], member_request, 403)
            call('/api/chatroom/agents/list', {'room_id': room}, b, 403)
            call('/api/session/history', {'session_id': f'r_{room}'}, b, 403)
            check('workspace revoke/regrant and an independent member removal each invalidate authorized requests')
            call('/api/spaces/invite', {'room_id': room, 'user_id': b['user_id']}, a)
            call('/api/spaces/accept', {'room_id': room}, b)
            valid = catalog.read_text()
            scenarios = [('invalid catalog', 'catalog'), ('invalid project catalog', 'projects'),
                         ('invalid gateway origin', 'origin'), ('invalid authorization key', 'key'),
                         ('Agent bot ID already owned by a human', 'identity')]
            valid_projects = projects.read_text()
            for label, fault in scenarios:
                if fault == 'catalog': catalog.write_text('{"secret_parser_marker":')
                if fault == 'projects': projects.write_text('{"schema":1,"projects":[{"id":"bad/path"}]}')
                if fault == 'origin': environment['SPARK_PUSH_AGENT_GATEWAY_ORIGIN'] = 'http://host.invalid/path'
                if fault == 'key': environment['SPARK_PUSH_WORKSPACE_AUTH_KEY'] = 'short'
                if fault == 'identity':
                    collision = json.loads(valid)
                    collision['agents'].append({**collision['agents'][0], 'id': 'conflicting-bot', 'bot_user_id': b['user_id']})
                    catalog.write_text(json.dumps(collision))
                compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic')
                call('/api/chatroom/agents/list', {'room_id': room}, a, 503)
                call('/api/chatroom/agents/list', {'room_id': room}, c, 403)
                chat()
                call('/api/session/history', {'session_id': f'r_{room}'}, a)
                assert 'secret_parser_marker' not in compose('logs', '--no-color', '--tail', '100', 'logic')
                check(label + ' disables Agents while authenticated chat and history remain available')
                catalog.write_text(valid)
                projects.write_text(valid_projects)
                environment['SPARK_PUSH_AGENT_GATEWAY_ORIGIN'] = 'http://host.docker.internal:1'
                environment.pop('SPARK_PUSH_WORKSPACE_AUTH_KEY', None)
            compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic')
            restored = call('/api/chatroom/agents/list', {'room_id': room}, a)
            assert restored['agents'][0]['installed'] and any(r['request_id'] == identity for r in restored['requests'])
            chat()
            check('repair plus normal Logic restart preserves bindings and accepted requests')
            # This mutation is confined to the owned disposable test database.
            sql_prefix = 'MYSQL_PWD="$(cat /run/spark-admin/mysql-root-password)" mysql -uroot spark_push -e '
            compose('exec', '-T', 'mysql', 'sh', '-c', sql_prefix + '"RENAME TABLE room_agent_binding TO binding_fixture"')
            try:
                compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic')
                call('/api/chatroom/agents/list', {'room_id': room}, a, 503)
                chat()
                check('missing optional Agent schema disables Agents but preserves ordinary IM')
            finally:
                compose('exec', '-T', 'mysql', 'sh', '-c', sql_prefix + '"RENAME TABLE binding_fixture TO room_agent_binding"')
            # Existing invalid credentials are preserved, including values which
            # cannot be passed to execve. Empty project lists cannot mask this.
            projects.write_text('{"schema":1,"projects":[]}')
            credential_path = "from pathlib import Path;import json;p=Path('/run/spark-secrets/security.json');backup=p.with_name('security-fixture.json');"
            compose('run', '--rm', '--no-deps', '--entrypoint', 'python3', 'bootstrap', '-c',
                credential_path + 'backup.write_bytes(p.read_bytes());backup.chmod(0o600)')
            try:
                for label, expression in [('NUL', "'x'*32+chr(0)"), ('unencodable Unicode', "'x'*32+chr(0xd800)")]:
                    compose('run', '--rm', '--no-deps', '--entrypoint', 'python3', 'bootstrap', '-c',
                        credential_path + "v=json.loads(backup.read_text());v['workspace']=" + expression + ";p.write_text(json.dumps(v))")
                    compose('run', '--rm', '--no-deps', 'bootstrap')
                    compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic', 'comet', 'job')
                    call('/api/chatroom/agents/list', {'room_id': room}, a, 503)
                    chat()
                    unchanged = compose('exec', '-T', 'logic', 'python3', '-c',
                        credential_path + "assert json.loads(p.read_text())['workspace']==" + expression + ";print('preserved')")
                    assert unchanged.strip() == 'preserved'
                    check('persisted ' + label + ' key remains unchanged; IM starts and Agents report unavailable')
            finally:
                compose('run', '--rm', '--no-deps', '--entrypoint', 'python3', 'bootstrap', '-c',
                    credential_path + 'p.write_bytes(backup.read_bytes());backup.unlink()')
                projects.write_text(valid_projects)
            # Explicit nonempty overrides are used by both the process and the
            # exporter without changing persisted credentials.
            environment['SPARK_PUSH_WORKSPACE_AUTH_KEY'] = secrets.token_hex(32)
            environment['SPARK_PUSH_HERMES_API_KEY'] = secrets.token_hex(32)
            compose('up', '-d', '--no-deps', '--force-recreate', '--wait', 'logic', 'comet', 'job')
            override = json.loads(compose('exec', '-T', 'logic', 'python3', '/app/deploy/export_agent_credentials.py'))
            assert all(override[k] == environment[k] for k in override)
            auth(after['workspace'], identity, 401)
            auth(environment['SPARK_PUSH_WORKSPACE_AUTH_KEY'], identity, 403)  # stored grant was revoked
            assert after == json.loads(compose('exec', '-T', 'logic', 'python3', '-c',
                "from pathlib import Path;print(Path('/run/spark-secrets/security.json').read_text())"))
            chat()
            check('explicit key overrides agree with exported keys without rotating persistent credentials')
            receipt['image_id'] = subprocess.check_output(['docker', 'image', 'inspect', args.image, '--format', '{{.Id}}'], text=True).strip()
            receipt['ok'] = True
        finally:
            try:
                compose('logs', '--no-color', '--tail', '100')
            finally:
                compose('down', '-v', '--remove-orphans')
                output.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + '\n')
    print(json.dumps(receipt, ensure_ascii=False))

if __name__ == '__main__':
    main()
