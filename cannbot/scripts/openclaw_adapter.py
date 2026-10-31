"""OpenClaw Chat Completions adapter, used on the customer connector host."""
import hashlib
import json
import os
import time

from agent_cli_adapter import SessionAdapter, ExecutionUnknown
from agent_connection_transport import Endpoint, LIMIT


class OpenClawAdapter(SessionAdapter):
    def __init__(self, config, store):
        super().__init__(config, store)
        token = os.environ.get(config.get('token_env', 'OPENCLAW_GATEWAY_TOKEN'), '')
        self.endpoint = Endpoint(config['base_url'], token, trusted=True)
        self.agent = config.get('agent_id', 'main')
        if not isinstance(self.agent, str) or not self.agent or len(self.agent) > 128:
            raise ValueError('invalid OpenClaw agent id')

    def is_ready(self):
        return True  # configuration only; remote health is not inferred

    def chat(self, message, on_delta, timeout_s, provider=None, model=None, session_id='',
             context_start_seq=0, retry=False, on_progress=None, request_id='', images=None):
        state = self.prepare(message, session_id, retry, images)
        scope = hashlib.sha256((session_id + ':' + str(state['context_start_seq'])).encode()).hexdigest()
        payload = {'model': 'openclaw:' + self.agent, 'user': 'spark-' + scope,
                   'messages': [{'role': 'user', 'content': message}], 'stream': True}
        state['pending'] = True
        self.store.put(self.namespace, session_id, state)
        conn = response = None
        try:
            deadline = time.monotonic() + timeout_s
            conn, response = self.endpoint.open('/chat/completions', payload, timeout_s)
            chunks, fields, done, stopped, total = [], [], False, False, 0
            while True:
                if time.monotonic() >= deadline:
                    raise TimeoutError('OpenClaw deadline')
                line = response.readline(LIMIT+1)
                total += len(line)
                if total > 32*LIMIT or len(line) > LIMIT:
                    raise ValueError('OpenClaw stream limit')
                if not line:
                    break
                line = line.rstrip(b'\r\n')
                if line.startswith(b'data:'):
                    fields.append(line[5:].lstrip())
                elif not line and fields:
                    data, fields = b'\n'.join(fields), []
                    if data == b'[DONE]':
                        done = True
                        break
                    e = json.loads(data)
                    if not isinstance(e, dict) or e.get('error'):
                        raise ValueError('OpenClaw stream failed')
                    for choice in e.get('choices', []):
                        if choice.get('index', 0) != 0:
                            raise ValueError('multiple completion choices are unsupported')
                        delta = choice.get('delta') or {}
                        if delta.get('tool_calls'):
                            raise ValueError('expected a server Agent, received client tool calls')
                        text = delta.get('content')
                        if text:
                            if stopped or not isinstance(text, str):
                                raise ValueError('invalid text after terminal')
                            chunks.append(text)
                            if on_delta:
                                on_delta(text)
                        reason = choice.get('finish_reason')
                        if reason is not None:
                            if reason != 'stop':
                                raise ValueError('OpenClaw did not finish successfully')
                            stopped = True
            text = ''.join(chunks)
            if not done or not stopped or not text.strip():
                raise ValueError('OpenClaw final is incomplete')
            state['pending'] = False
            self.store.put(self.namespace, session_id, state)
            return text, {'context_start_seq': state['context_start_seq']}
        except Exception:
            raise ExecutionUnknown('OpenClaw execution was not confirmed; inspect the customer runtime') from None
        finally:
            if response:
                response.close()
            if conn:
                conn.close()
