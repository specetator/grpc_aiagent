"""Authenticated user connections. Only Logic may supply actor_user_id.

Each user has one connection per runtime. Shared contact IDs do not share
credentials or upstream sessions. No client-supplied executable is accepted.
"""
import hashlib
from contextlib import nullcontext
import json
import os
import re
import threading
import time
import uuid

from agent_cli_adapter import ExecutionUnknown
from agent_connection_transport import Endpoint, LIMIT
from agent_events import agent_event
from agent_router import SessionLocks

TYPES = {
    'dsh': ('DeepSeek Harness', 900000000301),
    'codex': ('Codex', 900000000302),
    'claude_code': ('Claude Code', 900000000303),
    'hermes': ('Hermes', 900000000304),
    'openclaw': ('OpenClaw', 900000000305),
}
SCHEMA = 'sparkpush.connector.v1'


class Connections:
    namespace = 'customer_connections_v1'
    def __init__(self, store):
        self.store, self.locks = store, SessionLocks()
        self.guard = lambda actor, runtime: nullcontext()
        self.probes = threading.BoundedSemaphore(4)
        self.vault = store.path.parent / 'connection_credentials'
        if self.vault.is_symlink():
            raise ValueError('credential directory cannot be a symlink')
        self.vault.mkdir(mode=0o700, exist_ok=True)
        self.vault.chmod(0o700)

    def credential_path(self, ref):
        if not isinstance(ref, str) or not re.fullmatch(r'[a-f0-9]{32}', ref):
            raise ValueError('invalid credential reference')
        path = self.vault / (ref + '.env')
        if path.is_symlink():
            raise ValueError('credential cannot be a symlink')
        return path

    def token(self, config):
        ref = config.get('key_ref')
        if not ref:
            return ''
        return self.credential_path(ref).read_text(encoding='ascii').removeprefix('CONNECTOR_KEY=').rstrip('\n')

    def write_token(self, token):
        ref = uuid.uuid4().hex
        fd = os.open(self.credential_path(ref), os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, 'w', encoding='ascii') as output:
            output.write('CONNECTOR_KEY=' + token + '\n')
            output.flush()
            os.fsync(output.fileno())
        return ref

    def remove_token(self, config):
        if config.get('key_ref'):
            self.credential_path(config['key_ref']).unlink(missing_ok=True)

    def key(self, actor, runtime):
        if type(actor) is not int or not 0 < actor < 2**63 or runtime not in TYPES:
            raise ValueError('invalid actor or Agent type')
        return str(actor) + ':' + runtime

    def get(self, actor, runtime):
        return self.store.get(self.namespace, self.key(actor, runtime)) or {}

    def public(self, runtime, config):
        return {'runtime':runtime, 'name':TYPES[runtime][0], 'bot_user_id':TYPES[runtime][1],
                'configured':bool(config.get('endpoint')), 'endpoint':config.get('endpoint', ''),
                'has_key':bool(config.get('key_ref')), 'revision':config.get('revision', '')}

    def probe(self, endpoint, token, expected='auto'):
        if expected not in {'auto', *TYPES}:
            raise ValueError('unsupported Agent type')
        if not self.probes.acquire(False):
            raise ValueError('连接检测繁忙，请稍后重试')
        try:
            try:
                result = Endpoint(endpoint, token).json('/v1/agent/capabilities', timeout=5)
            except ValueError:
                raise ValueError('连接地址或密钥无效；私网地址需要管理员配置允许列表') from None
            except Exception:
                raise ValueError('连接检测失败，请检查 Connector 地址、密钥和网络') from None
        finally:
            self.probes.release()
        runtime = result.get('runtime')
        if (result.get('schema') != SCHEMA or runtime not in TYPES or
                result.get('ready') is not True or result.get('capabilities', {}).get('chat') is not True):
            raise ValueError('端点不是受支持且已就绪的 Spark Agent Connector')
        if expected != 'auto' and runtime != expected:
            raise ValueError('检测到的 Agent 类型与选择不一致')
        return {'runtime':runtime, 'name':TYPES[runtime][0], 'capabilities':result['capabilities']}

    def manage(self, request):
        actor, action = request.get('actor_user_id'), request.get('action')
        self.key(actor, 'codex')
        # No arbitrary paths, URLs in unrelated fields, ownership, or argv.
        if set(request) - {'actor_user_id','action','runtime','endpoint','token','revision'}:
            raise ValueError('unsupported connection fields')
        if action == 'list':
            return {'connections':[self.public(rt, self.get(actor, rt)) for rt in TYPES]}
        runtime = request.get('runtime', 'auto')
        if action not in {'probe','save','delete'}:
            raise ValueError('unsupported connection action')
        if action == 'delete':
            with self.guard(actor, runtime), self.locks.hold(self.key(actor, runtime), 1):
                previous = self.get(actor, runtime)
                if request.get('revision') != previous.get('revision', ''):
                    raise ValueError('配置已变化，请刷新后重试')
                self.store.put(self.namespace, self.key(actor, runtime), {})
                self.remove_token(previous)
            return self.public(runtime, {})
        old = self.get(actor, runtime) if runtime in TYPES else {}
        endpoint = request.get('endpoint', '').rstrip('/')
        token = request.get('token') or (self.token(old) if old.get('endpoint') == endpoint else '')
        found = self.probe(endpoint, token, runtime)
        if action == 'probe':
            return found
        runtime = found['runtime']
        with self.guard(actor, runtime), self.locks.hold(self.key(actor, runtime), 1):
            previous = self.get(actor, runtime)
            if request.get('revision', '') != previous.get('revision', ''):
                raise ValueError('配置已变化，请刷新后重试')
            config = {'endpoint':endpoint, 'key_ref':self.write_token(token), 'revision':uuid.uuid4().hex}
            try:
                self.store.put(self.namespace, self.key(actor, runtime), config)
            except Exception:
                self.remove_token(config)
                raise
            self.remove_token(previous)
            return self.public(runtime, config)


class UserConnectionAdapter:
    def __init__(self, runtime, connections):
        self.runtime, self.connections = runtime, connections
        self.bot = TYPES[runtime][1]

    def visible_to(self, actor):
        return bool(self.connections.get(actor, self.runtime).get('endpoint'))

    def actor(self, session):
        match = re.fullmatch(r's_([1-9][0-9]*)_([1-9][0-9]*)', session or '')
        ids = [int(x) for x in match.groups()] if match else []
        if len(ids) != 2 or ids[0] >= ids[1] or ids.count(self.bot) != 1:
            raise ValueError('invalid authenticated customer conversation')
        return next(x for x in ids if x != self.bot)

    def binding(self, actor, session):
        config = self.connections.get(actor, self.runtime)
        if not config.get('endpoint'):
            raise ValueError('请先在“我的 Agent”中配置连接')
        scope = '\0'.join([str(actor), self.runtime, session, config['revision']])
        sid = 'spk_' + hashlib.sha256(scope.encode()).hexdigest()
        return Endpoint(config['endpoint'], self.connections.token(config)), sid

    def is_ready(self):
        return True

    def connection_scope(self, session):
        config = self.connections.get(self.actor(session), self.runtime)
        return {'runtime':'customer-' + self.runtime, 'connection_revision':config.get('revision', '')}

    def recover_turn(self, session, request_id, existing):
        endpoint, sid = self.binding(self.actor(session), session)
        try:
            result = endpoint.json('/v1/agent/result', {'session_id':sid, 'request_id':request_id})
        except Exception:
            return None
        if result.get('status')=='completed' and isinstance(result.get('text'), str) and result['text'].strip():
            return result['text'], {}
        return None

    def available_models(self):
        return []

    def control(self, request, timeout_s=30):
        session = request.get('session_id')
        actor = self.actor(session)
        if request.get('operation') not in {'new','new_session','status','help'}:
            return agent_event('assistant_final', {'text':'此连接暂不支持该操作，请发送 /help。'})
        with self.connections.locks.hold(self.connections.key(actor, self.runtime), min(timeout_s, 1)):
            endpoint, sid = self.binding(actor, session)
            try:
                event = endpoint.json('/v1/agent/control', {'session_id':sid,
                    'operation':request['operation'], 'command_seq':request.get('command_seq', 0)}, min(timeout_s, 10))
                # Reconstruct a plain event: remote presentation/actions are untrusted.
                text = event.get('data', {}).get('text')
                if not isinstance(text, str):
                    raise ValueError('invalid control response')
                from agent_events import command_card
                card = command_card('Agent 会话', ['new_confirm','cancel'] if request['operation']=='new' else ['new','status','help'])
                return agent_event('assistant_final', {'text':text, 'presentation':card})
            except Exception:
                raise ValueError('Agent 控制失败，请检查客户侧 Connector') from None

    def chat(self, message, on_delta, timeout_s, provider=None, model=None, session_id='',
             context_start_seq=0, retry=False, on_progress=None, request_id='', images=None):
        if retry or images:
            raise ValueError('客户连接当前不支持附件或重试')
        actor = self.actor(session_id)
        with self.connections.locks.hold(self.connections.key(actor, self.runtime), min(timeout_s, 1)):
            endpoint, sid = self.binding(actor, session_id)
            conn = response = None
            try:
                deadline = time.monotonic() + timeout_s
                conn, response = endpoint.open('/v1/agent/chat', {'session_id':sid,
                    'request_id':request_id, 'message':message}, timeout_s)
                final, total = None, 0
                while True:
                    if time.monotonic() >= deadline:
                        raise TimeoutError('connector deadline')
                    line = response.readline(LIMIT+1)
                    if not line:
                        break
                    total += len(line)
                    if len(line) > LIMIT or total > 32*LIMIT or not line.endswith(b'\n'):
                        raise ValueError('invalid connector stream size or framing')
                    e = json.loads(line)
                    if not isinstance(e, dict) or final is not None:
                        raise ValueError('invalid connector event ordering')
                    if e.get('type') == 'error':
                        raise ExecutionUnknown('客户 Agent 执行结果未知，请在客户侧检查')
                    if e.get('type') in {'delta','progress','final'}:
                        if not isinstance(e.get('text'), str):
                            raise ValueError('invalid connector text')
                        if e['type']=='delta' and on_delta:
                            on_delta(e['text'])
                        elif e['type']=='progress' and on_progress:
                            on_progress(e['text'])
                        elif e['type']=='final':
                            final = e['text']
                if not final or not final.strip():
                    raise ValueError('missing connector final')
                return final, {}
            except Exception:
                raise ExecutionUnknown('客户 Agent 执行结果未确认，未自动重跑；请检查 Connector') from None
            finally:
                if response:
                    response.close()
                if conn:
                    conn.close()
