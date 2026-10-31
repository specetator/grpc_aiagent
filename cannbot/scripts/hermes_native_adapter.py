"""Opt-in adapter for the installed Hermes 0.21.1 Runs API.

Agent execution belongs to Hermes. Spark keeps a durable admission/result journal
in AgentStore, never re-POSTs an ambiguous admission, and only publishes native
completed output. No Gateway, profile, or tool registry is created here.
"""
from __future__ import annotations

import codecs
import hashlib
import json
import re
import secrets
import threading
import time
from urllib.parse import quote

from agent_events import agent_event
from hermes_adapter import HermesHttpAdapter

_TERMINAL = {"completed", "failed", "cancelled", "interrupted"}
_SAFE_TOKEN = re.compile(r"[A-Za-z0-9_-]{1,80}")
_MAX_FRAME = 1024 * 1024
_MAX_TEXT = 16 * 1024 * 1024
_MAX_EVENTS = 100000


class NativeUnknown(RuntimeError):
    """Transport ambiguity is recoverable by GET, never by prompt resubmission."""
    execution_unknown = True


class NativeTerminal(RuntimeError):
    """Hermes explicitly reported a non-success terminal state."""
    execution_failed = True

    def __init__(self, message, status="failed"):
        super().__init__(message)
        self.terminal_status = "cancelled" if status in {"cancelled", "interrupted"} else "failed"


def _digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode()).hexdigest()


class HermesNativeAdapter(HermesHttpAdapter):
    accepts_runtime_events = True
    supports_native_runtime = True

    def __init__(self, config, store):
        if not isinstance(config.get('profile'), str) or not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', config['profile']):
            raise ValueError('Native requires an explicit trusted Profile identifier')
        super().__init__(config, store)
        self.namespace = "hermes-native:" + config["id"]
        self.active_lock = threading.RLock()
        self.active = {}
        self.control_pool = None
        # This is an operator capability, not a field supplied by a chat user.
        # The Bridge control lane is separately opt-in and has bounded lookahead.
        # Remain fail-closed unless the operator has verified the control host.
        self.interactive = config.get("native_interactive_approvals") is True
        self.approval_timeout = config.get("native_approval_timeout_s", 60)
        if type(self.approval_timeout) not in (int, float) or not 1 <= self.approval_timeout <= 300:
            raise ValueError("native_approval_timeout_s must be between 1 and 300")
        if self.relay_pool is not None:
            from windows_agent_transport import WindowsRelayPool
            # A saturated SSE pool must never starve stop, status or denial.
            self.control_pool = WindowsRelayPool(config["windows_python"], max_workers=2, port=self.relay_pool.port)

    def _scope(self, session):
        return "spark-im-" + _digest([self.namespace, self.config.get("profile"), session])

    def _open(self, path, payload=None, headers=None, timeout=10):
        if self.control_pool is None or path.endswith("/events"):
            return super()._open(path, payload, headers, timeout)
        from windows_agent_transport import WindowsResponse
        url = (self.base_url.removesuffix('/v1') if path.startswith('/api/') else self.base_url) + path
        return WindowsResponse(self.config['windows_python'], url, payload,
                               {'Authorization': 'Bearer ' + self.key,
                                'Content-Type': 'application/json', **(headers or {})},
                               max(0.1, timeout), pool=self.control_pool)

    def stop(self):
        with self.active_lock:
            sessions = list(self.active)
        for session in sessions:
            self._deny_if_waiting(session)
        super().stop()
        if self.control_pool:
            self.control_pool.close()

    def _capabilities(self, timeout=3):
        value = self._json('/capabilities', timeout=min(timeout, 3))
        features = value.get('features', {}) if isinstance(value, dict) else {}
        idem = features.get('runs_idempotency', {})
        if (not isinstance(idem, dict) or idem.get('supported') is not True or
                idem.get('durable') is not True or
                not all(features.get(k) is True for k in (
                    'run_submission', 'run_status', 'run_events_sse', 'run_stop',
                    'run_approval_response')) or value.get('model') != self.config['expected_model']):
            raise ValueError('Hermes 未确认所需 Runs API、持久幂等和 technical 身份；Native 未启用')
        if self.config.get('room_policy')=='read_only':
            catalog=self._json('/toolsets',timeout=min(timeout,3))
            rows=catalog.get('data') if isinstance(catalog,dict) else None
            # todo_list belongs to this AIAgent's own plan. No filesystem,
            # terminal, memory search, cross-session tools or delegation.
            if (not isinstance(rows,list) or any(not isinstance(row,dict) or
                (row.get('enabled') is True and row.get('name') not in {'todo'}) for row in rows)):
                raise ValueError('共享 Hermes 工具策略未经只读验证')
        return value

    def is_ready(self):
        try:
            self._capabilities()
            return True
        except Exception:
            return False

    def _record(self, request_id):
        return self.store.get(self.namespace + ':run', request_id)

    def _update_record(self, request_id, **fields):
        def mutate(record):
            if not record:
                raise RuntimeError('Native admission journal is missing')
            # A late timeout/control/event must not replace a durable terminal.
            if record.get('status') in _TERMINAL and 'status' in fields:
                return record
            return {**record, **fields, 'updated_at_ms': int(time.time() * 1000)}
        return self.store.update(self.namespace + ':run', request_id, mutate, durable=True)

    def _bound_record(self, session, request_id):
        record = self._record(request_id)
        if record and (record.get('session_id') != session or
                       record.get('profile') != self.config.get('profile') or
                       record.get('endpoint_hash') != _digest(self.base_url)):
            raise ValueError('Hermes run 不属于当前会话、Profile 或服务')
        return record

    def _ensure_session(self, session, state):
        upstream = self._upstream(session, state)
        if not state.get('upstream_session'):
            try:
                value = self._json('/api/sessions', {'id': upstream}, timeout=10)
            except RuntimeError:
                value = self._json('/api/sessions/' + quote(upstream, safe=''), timeout=10)
            created = value.get('session', {}) if isinstance(value, dict) else {}
            if created.get('id') != upstream or created.get('source') != 'api_server':
                raise ValueError('Hermes 未确认独立 API Session；拒绝借用其他表面的会话')
        return upstream

    def _guard_session(self, session, request_id=''):
        previous = self.store.get(self.namespace + ':current', session).get('request_id')
        if previous and previous != request_id:
            record = self._bound_record(session, previous)
            if record and record.get('status') not in _TERMINAL:
                # No second writer, even after the local chat worker has timed out.
                try:
                    self.recover_turn(session, previous, {})
                except NativeTerminal:
                    pass
                if self._record(previous).get('status') not in _TERMINAL:
                    raise NativeUnknown('上一 Hermes 任务尚未确认结束，未提交新任务')

    def chat(self, message, on_delta, timeout_s, provider=None, model=None, session_id='',
             context_start_seq=0, retry=False, on_progress=None, request_id='', images=None,
             on_event=None):
        del provider, model, context_start_seq
        if images:
            raise ValueError('Hermes Runs 字符串输入不支持图片；请使用 Pi 图片通路')
        if (not isinstance(request_id, str) or not request_id or len(request_id.encode()) > 121 or
                any(ord(c) < 33 or ord(c) == 127 for c in request_id)):
            raise ValueError('Native requires a stable Spark request_id')
        if not isinstance(session_id, str) or not session_id:
            raise ValueError('Native requires a trusted session identity')
        if timeout_s <= 0:
            raise TimeoutError('Native deadline exceeded before admission')
        input_hash = _digest([session_id, message, retry, self.config.get('profile')])
        record = self._bound_record(session_id, request_id)
        if record:
            if record.get('input_hash') != input_hash:
                raise ValueError('request_id was reused with different Agent input')
            return self.recover_turn(session_id, request_id, {})
        with self.active_lock:
            if session_id in self.active:
                raise NativeUnknown('Hermes 会话已有活动任务')
            self._guard_session(session_id, request_id)
            self.active[session_id] = {'request_id': request_id, 'run_id': '', 'status': 'preparing'}
        admitted = False
        deadline = time.monotonic() + timeout_s
        try:
            self._capabilities(min(timeout_s, 3))
            state = self._state(session_id)
            if retry:
                message = state.get('last_user', '')
            if not isinstance(message, str) or not message.strip():
                raise ValueError('当前 Hermes 上下文没有可执行的用户消息')
            upstream = self._ensure_session(session_id, state)
            state.update(upstream_session=upstream, last_user=message)
            self.store.put(self.namespace, session_id, state)
            creative = self._creative_prompt(state, message)
            body = {'input': message if not creative else creative + '\n\n用户消息：\n' + message,
                    'session_id': upstream}
            selection = state.get('selection')
            if selection:
                body.update(provider=selection['provider'], model=selection['public']['id'])
            if state.get('reasoning_level'):
                body['model_options'] = {'reasoning_effort': state['reasoning_level']}
            if len(json.dumps(body, ensure_ascii=False).encode()) > _MAX_FRAME:
                raise ValueError('Hermes input exceeds limit')
            with self.active_lock:
                if self.active[session_id].get('stop_requested'):
                    raise NativeTerminal('Hermes 任务已在提交前取消，未执行工具', 'cancelled')
            key = 'spk-' + _digest([self.namespace, session_id, request_id, self.config.get('profile')])
            journal = {'version': 1, 'request_id': request_id, 'input_hash': input_hash,
                       'session_id': session_id, 'hermes_session_id': upstream, 'run_id': '',
                       'idempotency_key': key, 'payload_hash': _digest(body), 'runtime': 'hermes_native',
                       'profile': self.config.get('profile'), 'endpoint_hash': _digest(self.base_url),
                       'status': 'admitting', 'event_position': 0, 'delivery_state': 'not_ready',
                       'created_at_ms': int(time.time() * 1000)}
            def claim(old):
                if old:
                    raise NativeUnknown('Native request already admitted; query original run')
                return journal
            self.store.update(self.namespace + ':run', request_id, claim, durable=True)
            self.store.update(self.namespace + ':current', session_id,
                              lambda _: {'request_id': request_id}, durable=True)
            admitted = True  # Durable intent precedes the only POST.
            self._preview(on_progress, 'Hermes · 正在提交 Runs API')
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                self._update_record(request_id, status='failed')
                raise TimeoutError('Native deadline exceeded before POST')
            response = self._admit('/runs', body,
                                   {'X-Hermes-Session-Key': self._scope(session_id),
                                    'Idempotency-Key': key}, remaining)
            run_id = response['run_id']
            self._update_record(request_id, run_id=run_id, status='queued')
            with self.active_lock:
                self.active[session_id].update(run_id=run_id, status='queued', deadline=deadline)
                stop_requested = self.active[session_id].get('stop_requested')
            if stop_requested:
                self.interrupt({'operation': 'stop', 'session_id': session_id},
                               max(.1, deadline - time.monotonic()))
            self._emit(on_progress, on_event, 'tool_update', 'Hermes · 已准入，等待原生事件', run_id)
            if response.get('replayed') is True:
                return self.recover_turn(session_id, request_id, {})
            return self._read_events(session_id, run_id, on_delta, on_progress, on_event, deadline)
        except NativeTerminal:
            raise
        except Exception as exc:
            if admitted:
                record = self._record(request_id)
                if record.get('status') not in _TERMINAL:
                    self._update_record(request_id, status='unknown')
                    if not isinstance(exc, NativeUnknown):
                        raise NativeUnknown('Hermes 执行状态未确认；保留原 Run，未重发输入') from None
            raise
        finally:
            self._deny_if_waiting(session_id)
            with self.active_lock:
                self.active.pop(session_id, None)

    def _admit(self, path, body, headers, timeout_s):
        try:
            with self._open(path, body, headers, timeout_s) as response:
                raw = response.read(_MAX_FRAME + 1)
            if len(raw) > _MAX_FRAME:
                raise ValueError('oversize admission')
            parsed = json.loads(raw)
            if (not isinstance(parsed, dict) or not isinstance(parsed.get('run_id'), str) or
                    not _SAFE_TOKEN.fullmatch(parsed['run_id'])):
                raise ValueError('invalid admission')
            return parsed
        except Exception:
            # A POST is NOT a lookup: even the same key may create a new run
            # after retention expiry or server state loss. Never compensate by
            # starting a second tool run and trying to stop it afterwards.
            raise NativeUnknown('Hermes 未返回可信 run_id；未再次 POST，需要核对准入结果') from None

    def _get_run(self, run_id, timeout_s):
        payload = self._json('/runs/' + quote(run_id, safe=''), timeout=timeout_s)
        if (not isinstance(payload, dict) or payload.get('run_id') != run_id or
                payload.get('status') not in _TERMINAL | {'queued', 'running', 'waiting_for_approval', 'stopping'}):
            raise NativeUnknown('Hermes Run 状态或身份无效')
        return payload

    def _post(self, path, payload, timeout_s):
        value = self._json(path, payload, timeout=timeout_s)
        if not isinstance(value, dict):
            raise NativeUnknown('Hermes control response is invalid')
        return value

    def _complete(self, session, request_id, run_id, status, output=None):
        if status != 'completed':
            self._update_record(request_id, status=status)
            raise NativeTerminal('Hermes run 终态为 run.' + status + '，未记为成功回答', status)
        if not isinstance(output, str) or not output.strip() or len(output.encode()) > _MAX_TEXT:
            raise NativeUnknown('Hermes 成功终态没有有效输出')
        metadata = self._metadata(self._state(session)) | {'run_id': run_id, 'runtime': 'hermes_native'}
        # Save output before returning to Router/Bridge, including downstream disconnects.
        saved = self._update_record(request_id, status='completed', output=output, metadata=metadata,
                                    delivery_state='result_ready')
        if saved['status'] != 'completed':
            raise NativeTerminal('Hermes 终态与已有账本冲突，未发布回答')
        return saved['output'], saved['metadata']

    def recover_turn(self, session_id, request_id, existing):
        del existing
        record = self._bound_record(session_id, request_id)
        if not record:
            return None
        if record.get('status') == 'completed':
            return record['output'], {**record['metadata'], 'recovered': True}
        if record.get('status') in _TERMINAL:
            raise NativeTerminal('Hermes run 终态为 ' + record['status'] + '，未记为成功回答', record['status'])
        run_id = record.get('run_id')
        if not isinstance(run_id, str) or not _SAFE_TOKEN.fullmatch(run_id):
            raise NativeUnknown('Hermes 准入结果未知且没有 run_id；禁止重发，请人工核对')
        try:
            payload = self._get_run(run_id, 5)
        except Exception:
            raise NativeUnknown('Hermes 原 Run 暂不可查询；未重新提交') from None
        status = payload['status']
        if status in _TERMINAL:
            text, metadata = self._complete(session_id, request_id, run_id, status, payload.get('output'))
            return text, {**metadata, 'recovered': True}
        self._update_record(request_id, status=status)
        raise NativeUnknown('Hermes run 仍在执行或等待审批；未重新提交')

    def _read_events(self, session, run_id, on_delta, on_progress, on_event, deadline):
        request_id = self.active[session]['request_id']
        decoder = codecs.getincrementaldecoder('utf-8')('strict')
        pending, lines, frame_bytes, count, preview_bytes = '', [], 0, 0, 0
        # Hermes 0.21.1 emits timestamp but NO event id / replay cursor. Preserve
        # identical text chunks: content hashing would eat repeated words/tools.
        try:
            with self._open('/runs/' + run_id + '/events', timeout=max(.1, deadline-time.monotonic())) as response:
                while time.monotonic() < deadline:
                    raw = response.readline(_MAX_FRAME + 1)
                    if not raw:
                        decoder.decode(b'', final=True)
                        break
                    frame_bytes += len(raw)
                    if frame_bytes > _MAX_FRAME:
                        raise NativeUnknown('Hermes stream frame exceeds limit')
                    pending += decoder.decode(raw)
                    while '\n' in pending:
                        line, pending = pending.split('\n', 1)
                        line = line.rstrip('\r')
                        if line.startswith('data:'):
                            lines.append(line[5:].removeprefix(' '))
                        if line:
                            continue
                        frame_bytes = len(pending.encode())
                        if not lines:
                            continue
                        payload = json.loads('\n'.join(lines))
                        lines = []
                        count += 1
                        if count > _MAX_EVENTS:
                            raise NativeUnknown('Hermes event limit exceeded')
                        if not isinstance(payload, dict) or payload.get('run_id') != run_id:
                            raise NativeUnknown('Hermes 事件属于其他 run 或格式无效')
                        kind = payload.get('event')
                        if not isinstance(kind, str):
                            raise NativeUnknown('Hermes event type is invalid')
                        if kind.startswith('run.') and kind[4:] in _TERMINAL:
                            self._update_record(request_id, event_position=count)
                            return self._complete(session, request_id, run_id, kind[4:], payload.get('output'))
                        if kind == 'message.delta':
                            delta = payload.get('delta')
                            if not isinstance(delta, str):
                                raise NativeUnknown('Hermes delta is invalid')
                            preview_bytes += len(delta.encode())
                            if preview_bytes > _MAX_TEXT:
                                raise NativeUnknown('Hermes preview exceeds limit')
                            self._preview(on_delta, delta)
                        else:
                            self._progress_event(session, run_id, payload, on_progress, on_event)
                        if count % 32 == 0 or kind == 'approval.request':
                            self._update_record(request_id, event_position=count)
        except (NativeTerminal, NativeUnknown):
            raise
        except (ValueError, UnicodeError):
            raise NativeUnknown('Hermes 事件 JSON/UTF-8 协议无效，未发布回答') from None
        except (OSError, RuntimeError) as exc:
            # The events queue is destructive, not a replay log. Never resubscribe
            # or resubmit. A pollable completed status is the recovery authority.
            self._update_record(request_id, transport_error=type(exc).__name__)
        self._deny_if_waiting(session)
        return self.recover_turn(session, request_id, {})

    @staticmethod
    def _preview(callback, *args):
        if callback:
            try:
                callback(*args)
            except (BrokenPipeError, ConnectionError, OSError):
                # Spark client delivery and Hermes execution have separate lives.
                pass

    def _emit(self, progress, event, kind, text, run_id):
        if event and kind in {'tool_start', 'tool_update', 'tool_end'}:
            self._preview(event, kind, {'text': text, 'run_id': run_id})
        else:
            self._preview(progress, text)

    def _progress_event(self, session, run_id, payload, progress, event):
        kind = payload.get('event')
        if kind in {'tool.started', 'tool.completed', 'tool.failed'}:
            name = payload.get('tool')
            name = name if isinstance(name, str) and _SAFE_TOKEN.fullmatch(name) else 'tool'
            started = kind == 'tool.started'
            label = ('工具开始：' if started else '工具结束：') + name
            if kind == 'tool.failed' or payload.get('error') is True:
                label += '（失败）'
            self._emit(progress, event, 'tool_start' if started else 'tool_end', label, run_id)
        elif kind in {'subagent.start', 'subagent.complete'}:
            self._emit(progress, event, 'tool_update',
                       'Hermes 子任务开始' if kind.endswith('start') else 'Hermes 子任务结束', run_id)
        elif kind == 'reasoning.available':
            self._preview(progress, 'Hermes 正在思考')
        elif kind == 'approval.request':
            self._approval(session, run_id, payload, progress, event)
        elif kind == 'approval.responded':
            with self.active_lock:
                current = self.active.get(session, {})
                if current.get('prompt_id') == payload.get('request_id'):
                    self._clear_approval(current)
        # Clarify is not exported by installed Runs API. Do not invent an endpoint.

    def _approval(self, session, run_id, payload, progress, event):
        prompt_id = payload.get('request_id')
        if not isinstance(prompt_id, str) or not prompt_id or len(prompt_id) > 256 or any(ord(c) < 33 for c in prompt_id):
            self._post('/runs/' + run_id + '/stop', {}, 5)
            raise NativeUnknown('Hermes 审批没有可信标识，已请求取消')
        with self.active_lock:
            current = self.active[session]
            if current.get('prompt_id') == prompt_id or prompt_id in current.get('resolved_prompts', set()):
                return
            # A second outstanding prompt cannot overwrite the authority of the first.
            if current.get('prompt_id'):
                self._deny_if_waiting(session)
            generation = current.get('generation', 0) + 1
            token = 'ap_' + secrets.token_hex(16)
            expiry = min(current['deadline'], time.monotonic() + self.approval_timeout)
            current.update(prompt_id=prompt_id, approval_token=token, generation=generation,
                           expires=expiry, deciding=False, status='waiting_for_approval')
            timer = threading.Timer(max(.001, expiry-time.monotonic()), self._expire_approval,
                                    (session, run_id, token))
            timer.daemon = True
            current['timer'] = timer
            self._update_record(current['request_id'], status='waiting_for_approval',
                                approval={'prompt_id': prompt_id, 'generation': generation,
                                          'expires_at_ms': int((time.time()+max(0,expiry-time.monotonic()))*1000)})
            timer.start()
        if not self.interactive or (progress is None and event is None) or 'once' not in payload.get('choices', []):
            self._deny_if_waiting(session)
            self._emit(progress, event, 'tool_update', '当前通路不支持交互审批，已拒绝本次工具请求', run_id)
            return
        self._emit(progress, event, 'tool_update',
                   '需要批准一次工具操作。使用 /approve ' + token + ' 或 /deny ' + token + '；超时拒绝。', run_id)

    @staticmethod
    def _clear_approval(current):
        if current.get('prompt_id'):
            current.setdefault('resolved_prompts', set()).add(current['prompt_id'])
        timer = current.pop('timer', None)
        if timer:
            timer.cancel()
        for key in ('prompt_id', 'approval_token', 'expires'):
            current.pop(key, None)
        current['deciding'] = False

    def _expire_approval(self, session, run_id, token):
        with self.active_lock:
            current = self.active.get(session, {})
            if current.get('run_id') != run_id or current.get('approval_token') != token:
                return
        self._deny_if_waiting(session, expected_token=token)

    def _deny_if_waiting(self, session, expected_token=None):
        with self.active_lock:
            current = self.active.get(session, {})
            if expected_token is not None and current.get('approval_token') != expected_token:
                return
            if not current.get('prompt_id') or current.get('deciding'):
                return
            current['deciding'] = True
            run_id, prompt_id = current['run_id'], current['prompt_id']
            self._clear_approval(current)
        try:
            value = self._post('/runs/' + run_id + '/approval', {'choice': 'deny', 'request_id': prompt_id}, 5)
            if (value.get('run_id') != run_id or value.get('request_id') != prompt_id or
                    value.get('choice') != 'deny' or type(value.get('resolved')) is not int or value['resolved'] != 1):
                raise NativeUnknown('Hermes 未确认拒绝审批')
        except Exception:
            try:
                self._post('/runs/' + run_id + '/stop', {}, 5)
            except Exception:
                # No success claimed. Hermes' own approval timeout also denies.
                pass

    def interrupt(self, request, timeout_s=30):
        session, operation = request.get('session_id'), request.get('operation')
        with self.active_lock:
            current = self.active.get(session, {})
            if operation == 'stop' and current and not current.get('run_id'):
                current['stop_requested'] = True
                return agent_event('assistant_final', {'text':
                    '已记录取消意图；若已开始提交，将在取得 run_id 后请求停止。尚未确认任务终止。'})
            run = dict(current)
        if operation == 'stop' and not run.get('run_id'):
            request_id = self.store.get(self.namespace + ':current', session).get('request_id')
            if request_id:
                run = self._bound_record(session, request_id)
                if run and not run.get('run_id') and run.get('status') not in _TERMINAL:
                    raise NativeUnknown('Hermes 准入结果未知且没有 run_id；无法确认停止，请人工核对')
        if not run.get('run_id') or run.get('status') in _TERMINAL:
            return agent_event('assistant_final', {'text': '当前没有可停止的 Hermes 活动 Run。'})
        run_id = run['run_id']
        if operation == 'stop':
            value = self._post('/runs/' + run_id + '/stop', {}, min(timeout_s, 10))
            if value.get('run_id') == run_id and value.get('status') in _TERMINAL:
                try:
                    self._complete(session, run['request_id'], run_id, value['status'], value.get('output'))
                except NativeTerminal:
                    pass
                return agent_event('assistant_final', {'text': 'Hermes 原任务已经结束：' + value['status']})
            if value.get('run_id') != run_id or value.get('status') not in {'stopping', 'cancelled'}:
                raise NativeUnknown('Hermes 未确认停止请求')
            self._update_record(run['request_id'], status='stopping')
            return agent_event('assistant_final', {'text': '已请求停止当前 Hermes 任务；等待原 Run 终态。'})
        if operation not in {'approve', 'deny'}:
            raise ValueError('Unsupported native control')
        token = request.get('argument') or request.get('approval_request_id')
        with self.active_lock:
            current = self.active.get(session, {})
            if (not self.interactive or not isinstance(token, str) or
                    current.get('run_id') != run_id or current.get('request_id') != run['request_id'] or
                    token != current.get('approval_token') or current.get('deciding') or
                    time.monotonic() >= current.get('expires', 0)):
                raise ValueError('审批请求过期、已处理或不属于当前任务')
            current['deciding'] = True
            prompt_id = current['prompt_id']
        choice = 'once' if operation == 'approve' else 'deny'
        try:
            value = self._post('/runs/' + run_id + '/approval',
                               {'choice': choice, 'request_id': prompt_id}, min(timeout_s, 10))
            if (value.get('run_id') != run_id or value.get('request_id') != prompt_id or
                    value.get('choice') != choice or type(value.get('resolved')) is not int or value['resolved'] != 1):
                raise NativeUnknown('Hermes 未确认一次性审批决定')
        except Exception:
            # An ambiguous approval response cannot leave an invisible waiter
            # accepting a later decision. Cancel that exact run, never retry allow.
            try:
                self._post('/runs/' + run_id + '/stop', {}, min(timeout_s, 5))
            except Exception:
                pass
            raise NativeUnknown('Hermes 审批结果未确认，已尝试取消原 Run') from None
        finally:
            with self.active_lock:
                current = self.active.get(session, {})
                if current.get('approval_token') == token:
                    self._clear_approval(current)
        return agent_event('assistant_final', {'text': '已把一次性决定送回 Hermes：' + choice + '。'})

    def room_control(self, session, request_id, operation, approval_token=None):
        """Control the exact saved Run, never whichever later task shares a session."""
        record=self._bound_record(session,request_id)
        capabilities={"status":True,"stop":True,"approve":self.interactive,"deny":self.interactive}
        if operation=="status":
            if not record:return {"status":"queued","capabilities":capabilities}
            status=record.get("status","unknown")
            if status not in _TERMINAL and record.get("run_id"):
                try: status=self._get_run(record["run_id"],3)["status"]
                except Exception: status="unknown"
            if status in {"admitting","preparing"}:status="unknown"
            return {"status":status,"capabilities":capabilities}
        if operation in {"approve","deny"} and not self.interactive:
            raise ValueError("交互审批未启用；默认拒绝")
        with self.active_lock:
            active=self.active.get(session,{})
            current=active.get("request_id") or self.store.get(self.namespace+':current',session).get('request_id')
            if current!=request_id:raise ValueError("目标请求不是当前活动任务，未操作其他任务")
            event=self.interrupt({"session_id":session,"operation":operation,"argument":approval_token},5)
        latest=self._bound_record(session,request_id)
        return {"status":latest.get("status","unknown"),"message":event["data"]["text"],"capabilities":capabilities}

    def control(self, request, timeout_s=30):
        operation, session = request.get('operation'), request.get('session_id', '')
        if operation in {'stop', 'approve', 'deny'}:
            return self.interrupt(request, timeout_s)
        if operation == 'confirm_reply':
            request_id = request.get('request_id')
            record = self._bound_record(session, request_id)
            if (not record or record.get('status') != 'completed' or
                    record.get('run_id') != request.get('run_id')):
                raise ValueError('Native reply receipt does not match a completed run')
            self._update_record(request_id, delivery_state='broker_confirmed')
            return agent_event('assistant_final', {'text': 'Native reply broker receipt recorded'})
        if operation == 'compact':
            return agent_event('assistant_final', {'text': '当前安装的 Runs API 没有 /compact 端点，未执行压缩。'})
        if operation in {'restart', 'restart_agent'}:
            raise ValueError('Native 不通过聊天重启共享 Profile；请由管理员排空任务后维护')
        if operation == 'status':
            caps = self._capabilities(min(3, timeout_s))
            request_id = self.store.get(self.namespace + ':current', session).get('request_id')
            record = self._bound_record(session, request_id) if request_id else {}
            status = 'idle'
            if record:
                status = record.get('status', 'unknown')
                if record.get('run_id'):
                    status = self._get_run(record['run_id'], min(3, timeout_s))['status']
            text = 'Hermes Runs API：可连接\n当前任务：' + status
            counts = [self._public_count(path, timeout_s) for path in ('/skills', '/toolsets')]
            text += '\nSkills/Toolsets：' + '/'.join(str(n) if n is not None else '查询不可用' for n in counts)
            text += '\n服务幂等保留秒数：' + str(caps['features']['runs_idempotency'].get('retention_seconds', '未知'))
            return agent_event('assistant_final', {'text': text, **self._metadata(self._state(session))})
        if operation == 'help':
            return agent_event('assistant_final', {'text':
                '/model 模型目录与会话选择\n/new 新上下文\n/status 真实 Run 状态\n'
                '/stop 请求停止\n/retry 明确重新执行（可能重复工具副作用）\n'
                '同 request_id 的投递恢复只查询原 Run，不重新执行。\n'
                'Runs API 不支持 /compact；交互审批默认关闭。'})
        if operation in {'new_session', 'set_model', 'reset_model', 'set_reasoning'}:
            self._guard_session(session)
        if operation == 'new_session':
            state = self._state(session)
            revision = request.get('command_seq')
            if type(revision) is not int or not 0 < revision < 2**63 or revision < state['context_start_seq']:
                raise ValueError('新建命令已过期')
            if revision > state['context_start_seq']:
                state = {k: v for k, v in state.items() if k in {
                    'model', 'selection', 'model_revision', 'creative_mode',
                    'persona', 'character', 'lorebook', 'pinned_facts'}}
                state['context_start_seq'] = revision
            # Create/verify before switching the local pointer. A lost response
            # may leave an unused API session, but never discards the old context.
            state['upstream_session'] = self._ensure_session(session, state)
            self.store.update(self.namespace, session, lambda _: state, durable=True)
            return agent_event('assistant_final', {'text':
                '已创建新的 Hermes 上下文；模型选择、聊天记录和长期记忆保留。',
                **self._metadata(state)})
        return super().control(request, timeout_s)

    def _public_count(self, path, timeout_s):
        try:
            payload = self._json(path, timeout=min(3, timeout_s))
        except Exception:
            return None
        data = payload.get('data') if isinstance(payload, dict) else None
        return len(data) if isinstance(data, list) and len(data) <= 10000 else None
