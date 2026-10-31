#!/usr/bin/env python3
"""Run on the customer's Agent host; see docs/customer-agent-connections.md."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import threading

from agent_cli_adapter import CliAdapter
from agent_router import AgentStore, SessionLocks

SCHEMA = 'sparkpush.connector.v1'
RUNTIMES = {'dsh', 'codex', 'claude_code', 'hermes', 'openclaw'}


@contextmanager
def single_writer(root):
    fd = os.open(root / 'connector.lock', os.O_WRONLY | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield
    finally:
        os.close(fd)


def build_adapter(config, store):
    runtime = config.get('runtime')
    if runtime in {'dsh', 'codex', 'claude_code'}:
        return CliAdapter(config, store)
    if runtime == 'hermes':
        from hermes_adapter import HermesHttpAdapter
        return HermesHttpAdapter({**config, 'id': 'customer-hermes'}, store)
    if runtime == 'openclaw':
        from openclaw_adapter import OpenClawAdapter
        return OpenClawAdapter(config, store)
    raise ValueError('unsupported connector runtime')


def make_handler(adapter, runtime, store, token):
    locks, slots = SessionLocks(), threading.BoundedSemaphore(8)
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass  # Never log prompts, credentials or upstream error bodies.

        def json(self, code, payload):
            raw = json.dumps(payload, ensure_ascii=False).encode()
            self.send_response(code)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(raw)

        def authorized(self):
            if not hmac.compare_digest(self.headers.get('Authorization', ''), 'Bearer ' + token):
                self.json(401, {'error': 'unauthorized'})
                return False
            return True

        def do_GET(self):
            if not self.authorized():
                return
            if self.path != '/v1/agent/capabilities':
                self.json(404, {'error': 'not found'})
                return
            self.json(200, {'schema': SCHEMA, 'runtime': runtime,
                'ready': adapter.is_ready(),
                'capabilities': {'chat': True, 'sessions': True, 'new_session': True,
                                 'streaming': True, 'attachments': False, 'approval': False, 'stop': False}})

        def do_POST(self):
            if not self.authorized():
                return
            if self.path not in {'/v1/agent/chat', '/v1/agent/control', '/v1/agent/result'}:
                self.json(404, {'error': 'not found'})
                return
            if not slots.acquire(False):
                self.json(503, {'error': 'connector busy'})
                return
            streaming = False
            try:
                self.connection.settimeout(30)
                length = int(self.headers.get('Content-Length', '0'))
                if self.headers.get('Transfer-Encoding') or not 0 < length <= 1024*1024:
                    raise ValueError('invalid body length')
                body = json.loads(self.rfile.read(length))
                session = body.get('session_id')
                if not isinstance(session, str) or not session.startswith('spk_') or len(session) != 68:
                    raise ValueError('invalid connector session')
                if self.path.endswith('/result'):
                    request = body.get('request_id')
                    if not isinstance(request, str) or not request or len(request)>121:
                        raise ValueError('invalid request id')
                    rid = hashlib.sha256((session + '\0' + request).encode()).hexdigest()
                    result = store.get_turn(rid)
                    snapshot = store.get('connector_results', rid)
                    if snapshot.get('text'):
                        result = {'status':'completed', 'text':snapshot['text']}
                    self.json(200, {'status':result.get('status', 'unknown'),
                        'text':result.get('text', '') if result.get('status')=='completed' else ''})
                    return
                with locks.hold(session, 1):
                    if self.path.endswith('/control'):
                        if body.get('operation') not in {'new', 'new_session', 'status', 'help'}:
                            raise ValueError('unsupported control')
                        event = adapter.control(body, 10)
                        if body.get('operation') == 'new_session':
                            seq = body.get('command_seq', 0)
                            # The boundary and pending execution are ONE atomic
                            # record. A replay must not clear a newer execution.
                            store.update('connector_session', session,
                                lambda state: {'command_seq':seq} if seq > state.get('command_seq', 0) else state,
                                durable=True)
                        self.json(200, event)
                        return
                    request = body.get('request_id')
                    message = body.get('message')
                    if not isinstance(request, str) or not request or len(request) > 121:
                        raise ValueError('invalid request id')
                    if not isinstance(message, str) or not message.strip() or len(message.encode()) > 512*1024:
                        raise ValueError('invalid message')
                    rid = hashlib.sha256((session + '\0' + request).encode()).hexdigest()
                    digest = hashlib.sha256(message.encode()).hexdigest()
                    old = store.begin_turn(rid, session, digest)
                    self.send_response(200)
                    self.send_header('Content-Type', 'application/x-ndjson')
                    self.send_header('Connection', 'close')
                    self.end_headers()
                    streaming = True
                    def emit(event):
                        self.wfile.write(json.dumps(event, ensure_ascii=False).encode()+b'\n')
                        self.wfile.flush()
                    if old['status'] == 'completed':
                        emit({'type':'final', 'text':old['text'], 'metadata':old['metadata']})
                        return
                    if old['status'] != 'new':
                        emit({'type':'error', 'unknown':True})
                        return
                    try:
                        pending = store.get('connector_session', session).get('request_id')
                        if pending and not store.get('connector_results', pending).get('text'):
                            raise RuntimeError('previous execution unresolved')
                        store.update('connector_session', session, lambda state: {**state, 'request_id':rid}, durable=True)
                        text, metadata = adapter.chat(message, lambda t: emit({'type':'delta', 'text':t}),
                            600, session_id=session, request_id=rid,
                            on_progress=lambda t: emit({'type':'progress', 'text':t}))
                        # Persist success BEFORE writing the network terminal.
                        metadata = {'context_start_seq': metadata.get('context_start_seq', 0)}
                        store.update('connector_results', rid,
                                     lambda state: {'text':text, 'metadata':metadata, 'input_hash':digest}, durable=True)
                        store.complete_turn(rid, text, metadata)
                        store.update('connector_session', session,
                                     lambda state: {k:v for k,v in state.items() if k!='request_id'}, durable=True)
                        emit({'type':'final', 'text':text, 'metadata':metadata})
                    except Exception:
                        if store.get_turn(rid).get('status') != 'completed':
                            store.mark_turn_unknown(rid)
                        emit({'type':'error', 'unknown':True})
            except Exception:
                if not streaming:
                    self.json(400, {'error':'invalid request or connector unavailable'})
            finally:
                slots.release()
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', type=Path, required=True)
    parser.add_argument('--state-dir', type=Path, required=True)
    parser.add_argument('--listen', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8650)
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    token = os.environ.get('SPARK_AGENT_CONNECTOR_KEY', '')
    if len(token) < 24 or any(ord(c)<33 or ord(c)>126 for c in token):
        parser.error('SPARK_AGENT_CONNECTOR_KEY must contain at least 24 printable non-space characters')
    store = AgentStore(args.state_dir)
    with single_writer(args.state_dir):
        store.reclaim_running_turns(0)
        adapter = build_adapter(config, store)
        server = ThreadingHTTPServer((args.listen, args.port), make_handler(adapter, config['runtime'], store, token))
        try:
            server.serve_forever()
        finally:
            server.server_close()
            stop = getattr(adapter, 'stop', None)
            if stop:
                stop()

if __name__ == '__main__':
    main()
