"""Customer-side CLI adapters. Prompts use stdin, never a shell or argv.

Only the connector operator supplies binaries/cwd. IM users cannot choose them.
Official contracts and supported limitations: docs/customer-agent-connections.md.
"""
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import time

from agent_events import agent_event, command_card


class ExecutionUnknown(RuntimeError):
    execution_unknown = True


class SessionAdapter:
    def __init__(self, config, store):
        self.config, self.store = dict(config), store
        self.runtime = config['runtime']
        self.namespace = 'connector:' + self.runtime

    def state(self, session):
        if not isinstance(session, str) or not session or len(session) > 256:
            raise ValueError('invalid session')
        return self.store.get(self.namespace, session) or {'context_start_seq': 0}

    def available_models(self):
        return []  # No fabricated catalog; model is operator-owned in v1.

    def control(self, request, timeout_s=30):
        session, op = request.get('session_id'), request.get('operation')
        state = self.state(session)
        card = command_card('Agent 会话', ['new', 'status', 'help'])
        if op == 'new':
            text = '开启新上下文会保留 IM 历史，不撤销之前执行的工具操作。'
            card = command_card('新建上下文？', ['new_confirm', 'cancel'])
        elif op == 'new_session':
            seq = request.get('command_seq')
            if type(seq) is not int or seq <= 0 or seq < state['context_start_seq']:
                raise ValueError('stale or invalid command_seq')
            if seq > state['context_start_seq']:
                self.store.put(self.namespace, session, {'context_start_seq': seq})
            text = '已切换到新的上下文；下次发消息时创建 Agent 会话。'
        elif op == 'status':
            text = self.runtime + ('：上次执行结果未知，请在客户侧检查。' if state.get('pending') else '：连接已配置。')
        elif op == 'help':
            text = '支持连续对话、/new 和 /status。模型与工具权限在客户侧配置。当前不支持附件、重试、审批和停止命令。'
        else:
            text = '此连接暂不支持该操作，请发送 /help 查看可用功能。'
        return agent_event('assistant_final', {'text': text, 'presentation': card,
                                              'context_start_seq': state['context_start_seq']})

    def prepare(self, message, session, retry, images):
        if retry or images:
            raise ValueError('this connector does not support retry or attachments')
        if not isinstance(message, str) or not message.strip() or len(message.encode()) > 512*1024:
            raise ValueError('invalid prompt')
        state = self.state(session)
        if state.get('pending'):
            raise ExecutionUnknown('previous execution is unconfirmed; inspect the customer runtime')
        return state


class CliAdapter(SessionAdapter):
    def __init__(self, config, store):
        super().__init__(config, store)
        if self.runtime not in {'codex', 'claude_code', 'dsh'}:
            raise ValueError('unsupported CLI runtime')
        self.binary = config.get('binary', {'codex':'codex', 'claude_code':'claude', 'dsh':'dsh'}[self.runtime])
        self.cwd = Path(config['cwd']).resolve(strict=True)
        if not self.cwd.is_dir() or not isinstance(self.binary, str):
            raise ValueError('invalid connector workspace or executable')
        self.permission = config.get('permission_mode', 'read-only')
        if self.permission not in {'read-only', 'workspace-write'}:
            raise ValueError('permission_mode must be read-only or workspace-write')

    def is_ready(self):
        return shutil.which(self.binary) is not None

    def argv(self, state):
        upstream = state.get('upstream_session')
        if self.runtime == 'codex':
            args = [self.binary, 'exec', '--json', '--color', 'never', '--sandbox', self.permission,
                    '-c', 'approval_policy="never"']
            if self.config.get('model'):
                args += ['--model', self.config['model']]
            return args + (['resume', upstream, '-'] if upstream else ['-'])
        if self.runtime == 'claude_code':
            args = [self.binary, '-p', '--output-format', 'stream-json', '--verbose',
                    '--include-partial-messages', '--permission-mode', 'dontAsk']
            # A fixed tool allowlist, never bypassPermissions. Operator can opt
            # into writes; read-only excludes Bash and arbitrary MCP tools.
            args += ['--tools', 'Read,Glob,Grep' if self.permission == 'read-only' else 'Read,Glob,Grep,Edit,Write,Bash',
                     '--allowedTools', 'Read,Glob,Grep' if self.permission == 'read-only' else 'Read,Glob,Grep,Edit,Write,Bash']
            if self.config.get('model'):
                args += ['--model', self.config['model']]
            return args + (['--resume', upstream] if upstream else [])
        args = [self.binary, '--profile', 'headless', '--json', '--permission-mode', self.permission,
                '--approval-policy', 'never']
        return args + (['--session-id', upstream] if upstream else []) + ['-']

    def chat(self, message, on_delta, timeout_s, provider=None, model=None, session_id='',
             context_start_seq=0, retry=False, on_progress=None, request_id='', images=None):
        state = self.prepare(message, session_id, retry, images)
        if not self.is_ready():
            raise ValueError('runtime executable is not installed')
        deadline = time.monotonic() + max(0, timeout_s)
        state['pending'] = True
        self.store.put(self.namespace, session_id, state)
        parser = CliOutput(self.runtime, state.get('upstream_session'), on_delta, on_progress)
        proc = None
        try:
            proc = subprocess.Popen(self.argv(state), cwd=self.cwd, stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                    start_new_session=True)
            # Both pipes are nonblocking: large prompts cannot defeat timeout.
            with selectors.DefaultSelector() as sel:
                for stream in (proc.stdin, proc.stdout):
                    os.set_blocking(stream.fileno(), False)
                sel.register(proc.stdout, selectors.EVENT_READ)
                sel.register(proc.stdin, selectors.EVENT_WRITE)
                prompt, offset, buffer, total = message.encode(), 0, b'', 0
                while sel.get_map():
                    left = deadline-time.monotonic()
                    if left <= 0:
                        raise TimeoutError('runtime deadline')
                    for key, _ in sel.select(min(left, .2)):
                        if key.fileobj is proc.stdin:
                            offset += os.write(proc.stdin.fileno(), prompt[offset:offset+65536])
                            if offset == len(prompt):
                                sel.unregister(proc.stdin)
                                proc.stdin.close()
                        else:
                            chunk = os.read(proc.stdout.fileno(), 65536)
                            if not chunk:
                                sel.unregister(proc.stdout)
                                if buffer.strip():
                                    raise ValueError('truncated runtime JSONL')
                                continue
                            total += len(chunk)
                            buffer += chunk
                            if total > 32*1024*1024 or len(buffer) > 8*1024*1024:
                                raise ValueError('runtime output exceeds limit')
                            while b'\n' in buffer:
                                line, buffer = buffer.split(b'\n', 1)
                                if line.strip():
                                    parser.feed(json.loads(line))
                                    if parser.session:
                                        state['upstream_session'] = parser.session
                                        self.store.put(self.namespace, session_id, state)
            code = proc.wait(timeout=max(.001, deadline-time.monotonic()))
            result = parser.finish(code)
            state['pending'] = False
            self.store.put(self.namespace, session_id, state)
            return result, {'context_start_seq': state['context_start_seq']}
        except Exception:
            # Do not repeat a possibly executed tool, including malformed EOF.
            raise ExecutionUnknown('Agent execution was not confirmed; inspect the connector') from None
        finally:
            if proc:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                proc.wait()
                for stream in (proc.stdin, proc.stdout):
                    if not stream.closed:
                        stream.close()


class CliOutput:
    """Only native success terminals can turn a preview into a final reply."""
    def __init__(self, runtime, expected_session=None, delta=None, progress=None):
        self.runtime, self.session = runtime, expected_session
        self.delta, self.progress = delta, progress
        self.text, self.done, self.failed = '', False, False

    def bind(self, value):
        if not isinstance(value, str) or not value or len(value) > 256 or value.startswith('-'):
            raise ValueError('invalid runtime session')
        if self.session and self.session != value:
            raise ValueError('runtime changed session')
        self.session = value

    def feed(self, e):
        if not isinstance(e, dict) or self.done:
            raise ValueError('invalid event or event after terminal')
        kind = e.get('type')
        if kind in {'error', 'turn.failed'}:
            self.failed = True
        if self.runtime == 'codex':
            if kind == 'thread.started':
                self.bind(e.get('thread_id'))
            if kind == 'item.completed' and e.get('item', {}).get('type') == 'agent_message':
                self.text = e['item'].get('text', '')
                if self.delta and isinstance(self.text, str):
                    self.delta(self.text)
            if kind in {'item.started', 'item.completed'} and e.get('item', {}).get('type') in {'command_execution', 'mcp_tool_call', 'file_change'} and self.progress:
                self.progress('Agent 正在执行工具' if kind == 'item.started' else 'Agent 工具步骤已结束')
            if kind == 'turn.completed':
                self.done = True
        elif self.runtime == 'claude_code':
            if e.get('session_id') and not e.get('parent_tool_use_id'):
                self.bind(e['session_id'])
            if kind == 'stream_event' and not e.get('parent_tool_use_id'):
                d = e.get('event', {}).get('delta', {})
                if d.get('type') == 'text_delta' and isinstance(d.get('text'), str) and self.delta:
                    self.delta(d['text'])
            if kind == 'result':
                self.done, self.text = True, e.get('result', '')
                self.failed |= e.get('is_error') is not False or e.get('subtype') != 'success'
        else:
            if kind in {'session', 'final'}:
                self.bind(e.get('sessionId'))
            if kind == 'text' and isinstance(e.get('text'), str) and self.delta:
                self.delta(e['text'])
            if kind in {'tool_call', 'tool_result'} and self.progress:
                self.progress('Agent 正在执行工具' if kind == 'tool_call' else 'Agent 工具步骤已结束')
            if kind == 'final':
                self.done, self.text = True, e.get('text', '')
                self.failed |= (e.get('reason') or {}).get('kind') != 'completed'

    def finish(self, exit_code):
        if exit_code != 0 or not self.done or self.failed or not self.session or not isinstance(self.text, str) or not self.text.strip():
            raise ValueError('runtime did not return a successful final answer')
        return self.text
