#!/usr/bin/env python3
"""Hermes Runs API adapter tests. No live technical profile or Gateway is started."""
import io
import json
from pathlib import Path
import tempfile
import threading
import unittest
import time
from unittest.mock import patch

from agent_events import agent_event
from agent_router import AgentRouter, AgentStore, load_router
from hermes_native_adapter import HermesNativeAdapter, NativeTerminal, NativeUnknown
from windows_agent_http import allowed_technical_path, streams_response


def frame(payload):
    return ("data: " + json.dumps(payload, ensure_ascii=False) + "\n\n").encode()


class Body:
    def __init__(self, raw):
        self.raw = io.BytesIO(raw)

    def read(self, size):
        return self.raw.read(size)

    def readline(self, size):
        return self.raw.readline(size)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class Chunked(Body):
    """One-byte reads so a multibyte UTF-8 character crosses readline calls."""

    def readline(self, size):
        return self.raw.read(1)


class GateBody:
    def __init__(self, first, rest, seen, release):
        self.buf = io.BytesIO(first)
        self.rest = rest
        self.seen = seen
        self.release = release
        self.phase = 0

    def read(self, size):
        return self.readline(size)

    def readline(self, size):
        line = self.buf.readline(size)
        if line:
            return line
        if self.phase == 0:
            self.phase = 1
            self.seen.set()
            if not self.release.wait(3):
                raise TimeoutError("approval wait exceeded")
            self.buf = io.BytesIO(self.rest)
            return self.buf.readline(size)
        return b""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakePi:
    def __init__(self):
        self.calls = []

    def is_ready(self):
        return True

    def chat(self, *args, **kwargs):
        self.calls.append(("chat", args, kwargs))
        return args[0], {}

    def control(self, request, timeout):
        self.calls.append(("control", request))
        return agent_event("assistant_final", {"text": "pi-control"})

    def stop(self):
        return None


class NativeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        (root / "env").write_text("API_SERVER_KEY=test-only-not-a-live-key\n")
        self.store = AgentStore(root / "state")
        self.adapter = HermesNativeAdapter({
            "id": "hermes-live",
            "name": "Hermes",
            "base_url": "http://127.0.0.1:8644/v1",
            "credential_env_file": str(root / "env"),
            "expected_model": "technical",
            "profile": "technical",
        }, self.store)
        self.calls = []
        self.run_posts = []
        self.events = self._success_events("最终回答")
        self.run_status = {"run_id": "run_abc", "status": "completed", "output": "最终回答"}
        self.event_factory = None
        self.adapter._open = self._open

    def tearDown(self):
        self.temp.cleanup()

    def _success_events(self, output):
        secret = "/home/secret/token"
        return b"".join([
            frame({"event": "message.delta", "run_id": "run_abc", "delta": "预"}),
            frame({"event": "message.delta", "run_id": "run_abc", "delta": "览"}),
            frame({"event": "message.delta", "run_id": "run_abc", "delta": "览"}),
            frame({"event": "tool.started", "run_id": "run_abc", "tool": "read_file", "preview": secret}),
            frame({"event": "tool.completed", "run_id": "run_abc", "tool": "read_file", "error": False}),
            frame({"event": "reasoning.available", "run_id": "run_abc", "text": "private chain " + secret}),
            frame({"event": "subagent.start", "run_id": "run_abc", "goal": secret}),
            frame({"event": "run.completed", "run_id": "run_abc", "output": output}),
            frame({"event": "message.delta", "run_id": "run_abc", "delta": "迟到"}),
        ])

    def _open(self, path, payload=None, headers=None, timeout=10):
        self.calls.append((path, payload, headers))
        if path == "/api/sessions" or (isinstance(path, str) and path.startswith("/api/sessions/")):
            if path.endswith("/model"):
                return Body(json.dumps({
                    "session_id": path.split("/")[3],
                    "runtime": {"provider": payload["provider"], "model": payload["model"],
                                "model_lock": "accepted"}}).encode())
            sid = payload['id'] if path == '/api/sessions' else path.split('/')[3]
            return Body(json.dumps({'session': {'id': sid, 'source': 'api_server'}}).encode())
        if path == '/capabilities':
            return Body(json.dumps({'model': 'technical', 'features': {
                'run_submission': True, 'run_status': True, 'run_events_sse': True,
                'run_stop': True, 'run_approval_response': True,
                'runs_idempotency': {'supported': True, 'durable': True, 'retention_seconds': 86400}
            }}).encode())
        if path == "/api/model/options":
            return Body(json.dumps({
                "provider": "custom:hyx", "model": "real-model",
                "providers": [{"slug": "custom:hyx", "authenticated": True, "models": ["real-model"]}]
            }).encode())
        if path == "/runs":
            scripted = self.run_posts.pop(0) if self.run_posts else {
                "run_id": "run_abc", "status": "started", "replayed": False}
            if isinstance(scripted, Exception):
                raise scripted
            return Body(json.dumps(scripted).encode())
        if path.endswith("/events"):
            if self.event_factory:
                return self.event_factory()
            return Body(self.events)
        if path.endswith('/stop'):
            return Body(json.dumps({'run_id': path.split('/')[2], 'status': 'stopping'}).encode())
        if path.endswith('/approval'):
            return Body(json.dumps({'run_id': path.split('/')[2], **payload, 'resolved': 1}).encode())
        if path.startswith("/runs/"):
            return Body(json.dumps(self.run_status).encode())
        if path in {"/models", "/skills", "/toolsets"}:
            return Body(json.dumps({"data": [{"id": "technical"}, {"id": "other"}]}).encode())
        raise AssertionError("unexpected Hermes path " + str(path))

    def _paths(self, suffix):
        return [path for path, _, _ in self.calls if path == suffix or path.endswith(suffix)]

    def test_success_uses_terminal_output_not_deltas_or_secrets(self):
        deltas, progress, events = [], [], []
        text, metadata = self.adapter.chat(
            "hello", deltas.append, 3, session_id="s_3_900000000101",
            request_id="req-1", on_progress=progress.append,
            on_event=lambda kind, data: events.append((kind, data["text"])))
        self.assertEqual(text, "最终回答")
        self.assertEqual(deltas, ["预", "览", "览"])
        self.assertEqual(metadata["run_id"], "run_abc")
        self.assertEqual(metadata["runtime"], "hermes_native")
        self.assertIn(("tool_start", "工具开始：read_file"), events)
        self.assertTrue(any(item[0] == "tool_update" and "子任务" in item[1] for item in events))
        blob = json.dumps({"progress": progress, "events": events}, ensure_ascii=False)
        self.assertNotIn("secret", blob)
        self.assertNotIn("private chain", blob)
        self.assertTrue(any("正在思考" in item for item in progress))
        posted = next(c for c in self.calls if c[0] == '/runs')
        self.assertNotIn('conversation_history', posted[1])
        self.assertRegex(posted[2]['Idempotency-Key'], r'^spk-[a-f0-9]{64}$')
        self.assertTrue(self.adapter.namespace.startswith("hermes-native:"))
        self.assertIsNone(self.store.get("hermes:hermes-live", "s_3_900000000101").get("upstream_session")
                          if isinstance(self.store.get("hermes:hermes-live", "s_3_900000000101"), dict) else None)

    def test_missing_terminal_failed_and_cancelled_are_not_success(self):
        self.events = frame({"event": "message.delta", "run_id": "run_abc", "delta": "只有预览"})
        self.run_status = {"run_id": "run_abc", "status": "running"}
        with self.assertRaises(RuntimeError) as ctx:
            self.adapter.chat("hello", None, 3, session_id="a", request_id="first")
        self.assertIn("仍在执行", str(ctx.exception))
        for status in ("failed", "cancelled", "interrupted"):
            self.events = frame({"event": "run." + status, "run_id": "run_abc", "output": "不要当成功"})
            with self.assertRaises(RuntimeError) as ctx:
                self.adapter.chat("hello", None, 3, session_id=status, request_id=status)
            self.assertIn(status, str(ctx.exception))

    def test_split_utf8_and_foreign_run_are_rejected(self):
        self.event_factory = lambda: Chunked(self._success_events("中文终态"))
        deltas = []
        text, _ = self.adapter.chat("hello", deltas.append, 3, session_id="a", request_id="utf8")
        self.assertEqual(text, "中文终态")
        self.assertEqual("".join(deltas), "预览览")
        self.event_factory = None
        self.events = frame({"event": "run.completed", "run_id": "run_other", "output": "错的"})
        with self.assertRaises(RuntimeError) as ctx:
            self.adapter.chat("hello", None, 3, session_id="foreign", request_id="foreign")
        self.assertIn("其他 run", str(ctx.exception))

    def test_conflict_and_lost_response_never_repost(self):
        for n, error in enumerate(("Hermes Agent API 返回 HTTP 409", "connection reset")):
            self.calls.clear()
            self.run_posts = [RuntimeError(error)]
            with self.assertRaises(NativeUnknown):
                self.adapter.chat("hello", None, 3, session_id='lost' + str(n), request_id='lost' + str(n))
            self.assertEqual(self._paths('/runs'), ['/runs'])
            with self.assertRaises(NativeUnknown):
                self.adapter.chat("hello", None, 3, session_id='lost' + str(n), request_id='lost' + str(n))
            self.assertEqual(self._paths('/runs'), ['/runs'])
        with self.assertRaises(ValueError):
            self.adapter.chat('hello', None, 3, session_id='no-key')

    def test_sessions_stay_isolated_and_new_keeps_memory_key(self):
        self.adapter.chat("one", None, 3, session_id="s_3_900000000101", request_id="u3")
        first = self.calls[-2] if self.calls[-1][0].endswith("/events") else self.calls[-1]
        run_call = next(item for item in self.calls if item[0] == "/runs")
        self.calls.clear()
        self.adapter.chat("two", None, 3, session_id="s_4_900000000101", request_id="u4")
        other = next(item for item in self.calls if item[0] == "/runs")
        self.assertNotEqual(run_call[1]["session_id"], other[1]["session_id"])
        self.assertNotEqual(run_call[2]["X-Hermes-Session-Key"], other[2]["X-Hermes-Session-Key"])
        self.assertNotIn("conversation_history", run_call[1])
        self.adapter.control({"operation": "new_session", "session_id": "s_3_900000000101", "command_seq": 10})
        self.calls.clear()
        self.adapter.chat("three", None, 3, session_id="s_3_900000000101", request_id="u3b")
        renewed = next(item for item in self.calls if item[0] == "/runs")
        self.assertNotEqual(run_call[1]["session_id"], renewed[1]["session_id"])
        self.assertEqual(run_call[2]["X-Hermes-Session-Key"], renewed[2]["X-Hermes-Session-Key"])
        self.assertIsNotNone(first)

    def test_compact_images_and_creative_projection(self):
        event = self.adapter.control({"operation": "compact", "session_id": "a"})
        self.assertIn("没有 /compact", event["data"]["text"])
        self.assertEqual(self.calls, [])
        with self.assertRaises(ValueError):
            self.adapter.chat("pic", None, 3, session_id="a", images=[{"id": "img"}])
        self.assertEqual(self.calls, [])
        self.adapter.control({"operation": "roleplay", "session_id": "a"})
        self.adapter.chat("出场", None, 3, session_id="a", request_id="creative-1")
        posted = next(item for item in self.calls if item[0] == "/runs")
        self.assertIn("剧情跑团", posted[1]["input"])
        self.assertIn("出场", posted[1]["input"])
        self.assertNotIn("conversation_history", posted[1])

    def test_stop_preempts_and_approval_once_or_deny(self):
        seen, release = threading.Event(), threading.Event()
        rest = frame({"event": "run.cancelled", "run_id": "run_abc"})
        self.event_factory = lambda: GateBody(b": keepalive\n\n", rest, seen, release)
        box = {}

        def run():
            try:
                self.adapter.chat("long", None, 3, session_id="a", request_id="stop-1")
                box["error"] = None
            except Exception as exc:
                box["error"] = exc

        worker = threading.Thread(target=run)
        worker.start()
        self.assertTrue(seen.wait(2))
        stopped = self.adapter.interrupt({"operation": "stop", "session_id": "a"}, 1)
        self.assertIn("已请求停止", stopped["data"]["text"])
        self.assertTrue(any(path.endswith("/stop") for path, _, _ in self.calls))
        release.set()
        worker.join(3)
        self.assertIsInstance(box["error"], RuntimeError)
        self.assertIn("run.cancelled", str(box["error"]))

        seen, release = threading.Event(), threading.Event()
        self.adapter.interactive = True
        first = frame({"event": "approval.request", "run_id": "run_abc", "request_id": "reqabc", 'choices': ['once', 'deny']})
        done = frame({"event": "run.completed", "run_id": "run_abc", "output": "已执行"})
        self.calls.clear()
        self.event_factory = lambda: GateBody(first, done, seen, release)

        def approve():
            try:
                if not seen.wait(2):
                    raise TimeoutError("approval event was not observed")
                try:
                    self.adapter.interrupt(
                        {"operation": "approve", "session_id": "b", "argument": "other"}, 1)
                    box["mismatch"] = "accepted"
                except ValueError:
                    box["mismatch"] = "rejected"
                box["decision"] = self.adapter.interrupt(
                    {"operation": "approve", "session_id": "b", "argument": self.adapter.active["b"]["approval_token"]}, 1)
            except Exception as exc:
                box["helper_error"] = exc
            finally:
                release.set()

        box.clear()
        helper = threading.Thread(target=approve)
        worker = threading.Thread(target=lambda: box.setdefault(
            "result", self.adapter.chat("need", None, 3, session_id="b", request_id="appr-1", on_progress=lambda _: None)))
        worker.start()
        helper.start()
        worker.join(3)
        helper.join(3)
        self.assertIsNone(box.get("helper_error"))
        self.assertEqual(box.get("mismatch"), "rejected")
        self.assertEqual(box["result"][0], "已执行")
        posted = next(payload for path, payload, _ in self.calls if path.endswith("/approval"))
        self.assertEqual(posted, {"choice": "once", "request_id": "reqabc"})

        self.calls.clear()
        self.events = first
        self.run_status = {"run_id": "run_abc", "status": "running"}
        self.event_factory = None
        with self.assertRaises(RuntimeError):
            self.adapter.chat("need", None, 3, session_id="c", request_id="deny-1")
        posted = next(payload for path, payload, _ in self.calls if path.endswith("/approval"))
        self.assertEqual(posted["choice"], "deny")

    def test_unknown_turn_reads_original_run_and_does_not_repost(self):
        pi = FakePi()
        router = AgentRouter(
            [{"id": "pi", "name": "Pi", "owner_user_ids": [], "bot_user_id": 900000000001},
             {"id": "hermes-live", "name": "Hermes", "owner_user_ids": [3], "bot_user_id": 900000000101}],
            {"pi": pi, "hermes-live": self.adapter}, self.store, default="pi")
        session = "s_3_900000000101"
        text, _ = router.chat("hello", None, 3, session_id=session, request_id="recover-1")
        self.assertEqual(text.startswith("【Hermes】"), True)
        runs = self._paths("/runs")
        with self.store.connect() as db:
            db.execute("UPDATE agent_turns SET status='unknown', error_code='process_restart' WHERE request_id='recover-1'")
        self.calls.clear()
        again, metadata = router.chat("hello", None, 3, session_id=session, request_id="recover-1")
        self.assertEqual(again, text)
        self.assertTrue(metadata.get("turn_recovered"))
        self.assertEqual(self._paths("/runs"), [])
        self.assertEqual(self.calls, [])  # durable Native output survives upstream retention
        self.assertEqual(runs, ["/runs"])
        with self.assertRaises(ValueError):
            router.chat("different", None, 3, session_id=session, request_id="recover-1")

    def test_router_stop_does_not_call_pi_and_native_stop_skips_the_lock(self):
        pi = FakePi()
        native = FakePi()
        native.interrupts = []

        def interrupt(request, timeout):
            native.interrupts.append(request)
            return agent_event("assistant_final", {"text": "native-stop"})

        native.interrupt = interrupt
        entered, release = threading.Event(), threading.Event()
        original = native.chat

        def slow(*args, **kwargs):
            entered.set()
            self.assertTrue(release.wait(2))
            return original(*args, **kwargs)

        native.chat = slow
        router = AgentRouter(
            [{"id": "pi", "name": "Pi", "owner_user_ids": [], "bot_user_id": 900000000001},
             {"id": "hermes-live", "name": "Hermes", "owner_user_ids": [3], "bot_user_id": 900000000101}],
            {"pi": pi, "hermes-live": native}, self.store, default="pi")
        refused = router.control({"session_id": "s_3_900000000001", "operation": "stop"})
        self.assertIn("暂不支持", refused["data"]["text"])
        self.assertEqual(pi.calls, [])
        compact = router.control({"session_id": "s_3_900000000001", "operation": "compact"})
        self.assertIn("暂不支持", compact["data"]["text"])
        self.assertEqual(pi.calls, [])
        worker = threading.Thread(target=lambda: router.chat(
            "long", None, 3, session_id="s_3_900000000101", request_id="lock-1"))
        worker.start()
        self.assertTrue(entered.wait(2))
        stopped = router.control({"session_id": "s_3_900000000101", "operation": "stop"})
        self.assertEqual(stopped["data"]["text"].split("\n")[-1], "native-stop")
        self.assertEqual(len(native.interrupts), 1)
        release.set()
        worker.join(3)

    def _router(self):
        return AgentRouter([
            {'id': 'pi', 'name': 'Pi', 'owner_user_ids': [], 'bot_user_id': 900000000001},
            {'id': 'hermes-live', 'name': 'Hermes', 'owner_user_ids': [3], 'bot_user_id': 900000000101}],
            {'pi': FakePi(), 'hermes-live': self.adapter}, self.store)

    def test_capabilities_missing_fails_before_admission(self):
        original = self.adapter._open
        self.adapter._open = lambda path, *a, **kw: Body(b'{}') if path == '/capabilities' else original(path, *a, **kw)
        with self.assertRaises(ValueError):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='caps')
        self.assertEqual(self._paths('/runs'), [])
        self.assertFalse(self.adapter.is_ready())

    def test_admission_intent_is_durable_before_post(self):
        original = self.adapter._open
        def opened(path, *a, **kw):
            if path == '/runs':
                record = self.adapter._record('intent')
                self.assertEqual(record['status'], 'admitting')
                self.assertEqual(record['run_id'], '')
                self.assertEqual(record['delivery_state'], 'not_ready')
                self.assertEqual(record['profile'], 'technical')
                self.assertEqual(len(record['input_hash']), 64)
            return original(path, *a, **kw)
        self.adapter._open = opened
        self.adapter.chat('hello', None, 3, session_id='a', request_id='intent')
        record = self.adapter._record('intent')
        self.assertEqual(record['status'], 'completed')
        self.assertEqual(record['delivery_state'], 'result_ready')
        self.assertGreater(record['event_position'], 0)

    def test_same_process_disconnect_is_unknown_then_get_recovers(self):
        router = self._router()
        session = 's_3_900000000101'
        self.events = frame({'event': 'message.delta', 'run_id': 'run_abc', 'delta': 'partial'})
        self.run_status = {'run_id': 'run_abc', 'status': 'running'}
        with self.assertRaises(NativeUnknown):
            router.chat('hello', None, 3, session_id=session, request_id='reconnect')
        self.assertEqual(self.store.get_turn('reconnect')['status'], 'unknown')
        self.run_status = {'run_id': 'run_abc', 'status': 'completed', 'output': 'recovered final'}
        self.calls.clear()
        answer, metadata = router.chat('hello', None, 3, session_id=session, request_id='reconnect')
        self.assertIn('recovered final', answer)
        self.assertTrue(metadata['turn_replayed'])
        self.assertEqual(self._paths('/runs'), [])
        self.assertEqual(self.store.get_turn('reconnect')['status'], 'completed')

    def test_bridge_crash_before_router_commit_reuses_native_output(self):
        router = self._router()
        session = 's_3_900000000101'
        with patch.object(self.store, 'complete_turn', side_effect=SystemExit):
            with self.assertRaises(SystemExit):
                router.chat('hello', None, 3, session_id=session, request_id='crash')
        self.assertEqual(self.store.get_turn('crash')['status'], 'running')
        router = self._router()  # startup reclaims leftover running turns
        self.calls.clear()
        answer, _ = router.chat('hello', None, 3, session_id=session, request_id='crash')
        self.assertIn('最终回答', answer)
        self.assertEqual(self.calls, [])

    def test_expired_admission_never_reposts_or_allows_another_writer(self):
        self.run_posts = [RuntimeError('lost response')]
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='expired')
        self.adapter._update_record('expired', created_at_ms=1)
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='expired')
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('other', None, 3, session_id='a', request_id='other')
        with self.assertRaises(NativeUnknown):
            self.adapter.control({'operation': 'new_session', 'session_id': 'a', 'command_seq': 7})
        self.assertEqual(self._paths('/runs'), ['/runs'])

    def test_status_and_stop_work_after_adapter_restart(self):
        self.events = b''
        self.run_status = {'run_id': 'run_abc', 'status': 'running'}
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='restart')
        self.assertEqual(self.adapter.active, {})
        value = self.adapter.control({'operation': 'status', 'session_id': 'a'})
        self.assertIn('running', value['data']['text'])
        self.calls.clear()
        self.adapter.interrupt({'operation': 'stop', 'session_id': 'a'})
        self.assertEqual(self._paths('/stop'), ['/runs/run_abc/stop'])
        self.assertEqual(self._paths('/runs'), [])

    def test_scope_and_input_conflicts_are_rejected(self):
        self.adapter.chat('hello', None, 3, session_id='a', request_id='bound')
        self.calls.clear()
        with self.assertRaises(ValueError):
            self.adapter.chat('other', None, 3, session_id='a', request_id='bound')
        with self.assertRaises(ValueError):
            self.adapter.recover_turn('b', 'bound', {})
        self.adapter.config['profile'] = 'other'
        with self.assertRaises(ValueError):
            self.adapter.recover_turn('a', 'bound', {})
        self.assertEqual(self.calls, [])

    def test_native_and_legacy_switch_cannot_replay_each_others_turn(self):
        router = self._router()
        session = 's_3_900000000101'
        router.chat('hello', None, 3, session_id=session, request_id='mode')
        router.adapters['hermes-live'] = FakePi()
        with self.assertRaises(ValueError):
            router.chat('hello', None, 3, session_id=session, request_id='mode')
        self.assertEqual(router.adapters['hermes-live'].calls, [])

    def test_native_identity_includes_deployment_scope(self):
        router = self._router()
        route = router.describe_route('s_3_900000000101')
        self.assertTrue(route['backend_session_ref'].startswith('agent:hermes-live:spark_pc:'))
        self.assertTrue(route['backend_session_ref'].endswith(':u3'))
        self.assertEqual(router.describe_route('s_3_900000000001')['backend_session_ref'], 's_3_900000000001')
        router.tenant_id = 'another'
        self.assertNotEqual(route['backend_session_ref'], router.describe_route('s_3_900000000101')['backend_session_ref'])

    def test_model_reasoning_and_new_use_actual_run_fields(self):
        self.adapter.control({'operation': 'set_model', 'session_id': 'a', 'command_seq': 1,
                              'target_provider': 'hermes-custom-hyx-' + __import__('hashlib').sha256(b'custom:hyx').hexdigest()[:6],
                              'target_model': 'real-model'})
        self.adapter.control({'operation': 'set_reasoning', 'session_id': 'a', 'level': 'high'})
        self.adapter.control({'operation': 'new_session', 'session_id': 'a', 'command_seq': 2})
        self.adapter.chat('hello', None, 3, session_id='a', request_id='model')
        body = next(c[1] for c in self.calls if c[0] == '/runs')
        self.assertEqual(body['provider'], 'custom:hyx')
        self.assertEqual(body['model'], 'real-model')
        # Parent /new intentionally retains only its established settings;
        # Native reasoning may be reselected without changing the Profile.
        self.adapter.control({'operation': 'set_reasoning', 'session_id': 'a', 'level': 'max'})
        self.calls.clear()
        self.adapter.chat('hello again', None, 3, session_id='a', request_id='model2')
        body = next(c[1] for c in self.calls if c[0] == '/runs')
        self.assertEqual(body['model_options'], {'reasoning_effort': 'max'})

    def test_non_api_session_is_never_adopted(self):
        original = self.adapter._open
        def opened(path, payload=None, *a, **kw):
            if path == '/api/sessions':
                return Body(json.dumps({'session': {'id': payload['id'], 'source': 'cli'}}).encode())
            return original(path, payload, *a, **kw)
        self.adapter._open = opened
        with self.assertRaises(ValueError):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='cli')
        self.assertEqual(self._paths('/runs'), [])

    def test_downstream_disconnect_does_not_abort_runtime_result(self):
        def disconnected(*args):
            raise BrokenPipeError()
        text, _ = self.adapter.chat('hello', disconnected, 3, session_id='a', request_id='gone',
                                    on_event=disconnected)
        self.assertEqual(text, '最终回答')
        self.assertEqual(self.adapter._record('gone')['status'], 'completed')

    def test_network_exception_queries_original_run(self):
        class Drop(Body):
            def readline(self, size):
                raise ConnectionResetError()
        self.event_factory = lambda: Drop(b'')
        text, _ = self.adapter.chat('hello', None, 3, session_id='a', request_id='reset')
        self.assertEqual(text, '最终回答')
        self.assertEqual(self._paths('/runs'), ['/runs'])
        self.assertIn('/runs/run_abc', self._paths('/runs/run_abc'))

    def test_oversize_fragmented_event_is_bounded(self):
        # A missing newline must be bounded across reads, not per chunk only.
        class Fragments(Body):
            def readline(self, size):
                return self.raw.read(65536)
        self.event_factory = lambda: Fragments(b'data: ' + b'x' * (1024 * 1024 + 1))
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='huge')
        self.assertEqual(self.adapter._record('huge')['status'], 'unknown')

    def test_default_and_missing_ui_approvals_deny_without_raw_preview(self):
        self.events = frame({'event': 'approval.request', 'run_id': 'run_abc', 'request_id': 'p',
                             'choices': ['once', 'deny'], 'command': 'secret token /private/path'}) + self.events
        progress = []
        self.adapter.chat('hello', None, 3, session_id='a', request_id='default-deny', on_progress=progress.append)
        posts = [c[1] for c in self.calls if c[0].endswith('/approval')]
        self.assertEqual(posts, [{'choice': 'deny', 'request_id': 'p'}])
        self.assertNotIn('secret', ''.join(progress))
        self.adapter.interactive = True
        self.calls.clear()
        self.adapter.chat('hello', None, 3, session_id='b', request_id='no-ui')
        self.assertEqual([c[1]['choice'] for c in self.calls if c[0].endswith('/approval')], ['deny'])

    def test_approval_timeout_while_reader_is_blocked(self):
        self.adapter.interactive = True
        self.adapter.approval_timeout = .05  # controlled timer, no real runtime
        seen, release = threading.Event(), threading.Event()
        first = frame({'event': 'approval.request', 'run_id': 'run_abc', 'request_id': 'p', 'choices': ['once', 'deny']})
        self.event_factory = lambda: GateBody(first, self.events, seen, release)
        original = self.adapter._open
        def opened(path, *a, **kw):
            value = original(path, *a, **kw)
            if path.endswith('/approval'):
                release.set()
            return value
        self.adapter._open = opened
        self.adapter.chat('hello', None, 3, session_id='a', request_id='timer', on_progress=lambda _: None)
        self.assertTrue(seen.is_set())
        self.assertEqual([c[1]['choice'] for c in self.calls if c[0].endswith('/approval')], ['deny'])

    def test_unauthorized_stop_and_approval_never_reach_runtime(self):
        router = self._router()
        for op in ('stop', 'approve', 'deny', 'status'):
            with self.assertRaises(ValueError):
                router.control({'session_id': 's_4_900000000101', 'operation': op, 'argument': 'forged'})
        self.assertEqual(self.calls, [])

    def test_duplicate_final_and_late_delta_cannot_replace_success(self):
        self.events += frame({'event': 'run.completed', 'run_id': 'run_abc', 'output': 'wrong'})
        deltas = []
        text, _ = self.adapter.chat('hello', deltas.append, 3, session_id='a', request_id='late')
        self.assertEqual(text, '最终回答')
        self.assertNotIn('迟到', deltas)
        self.adapter._update_record('late', status='unknown')
        self.assertEqual(self.adapter._record('late')['status'], 'completed')
        self.calls.clear()
        self.assertEqual(self.adapter.chat('hello', None, 3, session_id='a', request_id='late')[0], text)
        self.assertEqual(self.calls, [])

    def test_windows_control_pool_is_independent_of_sse_capacity(self):
        cfg = {**self.adapter.config, 'transport': 'windows_stdio', 'windows_python': 'unused-in-mock'}
        adapter = HermesNativeAdapter(cfg, self.store)
        try:
            with patch('windows_agent_transport.WindowsResponse', return_value=Body(b'{}')) as response:
                adapter._open('/runs/run_a/events')
                self.assertIs(response.call_args.kwargs['pool'], adapter.relay_pool)
                adapter._open('/runs/run_a/stop', {})
                self.assertIs(response.call_args.kwargs['pool'], adapter.control_pool)
                self.assertIsNot(adapter.control_pool, adapter.relay_pool)
        finally:
            adapter.stop()

    def test_late_duplicate_approval_is_not_presented_or_resolved_twice(self):
        approval = frame({'event': 'approval.request', 'run_id': 'run_abc',
                          'request_id': 'p', 'choices': ['once', 'deny']})
        self.events = approval + approval + self.events
        self.adapter.chat('hello', None, 3, session_id='a', request_id='duplicate-approval', on_progress=lambda _: None)
        self.assertEqual(len(self._paths('/approval')), 1)

    def test_loader_keeps_legacy_runtime_and_separate_namespace(self):
        root = Path(self.temp.name)
        registry = {
            "schema": 1, "default_agent": "pi",
            "agents": [
                {"id": "pi", "name": "Pi", "runtime": "pi_rpc", "owner_user_ids": [],
                 "bot_user_id": 900000000001},
                {"id": "hermes-technical", "name": "Legacy", "runtime": "hermes_http",
                 "owner_user_ids": [3], "bot_user_id": 900000000101,
                 "base_url": "http://127.0.0.1:8644/v1", "profile": "technical",
                 "expected_model": "technical", "credential_env_file": str(root / "env")},
                {"id": "hermes-live", "name": "Native", "runtime": "hermes_native",
                 "owner_user_ids": [3], "bot_user_id": 900000000211,
                 "base_url": "http://127.0.0.1:8644/v1", "profile": "technical",
                 "expected_model": "technical", "credential_env_file": str(root / "env")},
            ]}
        path = root / "agents.json"
        path.write_text(json.dumps(registry), encoding="utf-8")
        router = load_router(path, FakePi(), root / "router-state")
        self.assertEqual(router.adapters["hermes-technical"].namespace, "hermes:hermes-technical")
        self.assertEqual(router.adapters["hermes-live"].namespace, "hermes-native:hermes-live")
        self.assertTrue(router.routes_runtime_events)
        self.assertFalse(getattr(router.adapters["pi"], "accepts_runtime_events", False))
        router.stop()

    def test_failed_new_keeps_old_context_and_retry_uses_same_candidate(self):
        self.adapter.chat('hello', None, 3, session_id='a', request_id='before-new')
        before = self.adapter._state('a')
        original = self.adapter._open
        attempts = []
        def unavailable(path, payload=None, *args, **kwargs):
            if path.startswith('/api/sessions'):
                attempts.append(payload['id'] if payload else path.rsplit('/', 1)[-1])
                raise RuntimeError('service unavailable')
            return original(path, payload, *args, **kwargs)
        self.adapter._open = unavailable
        request = {'operation': 'new_session', 'session_id': 'a', 'command_seq': 7}
        with self.assertRaises(RuntimeError):
            self.adapter.control(request)
        self.assertEqual(self.adapter._state('a'), before)
        self.assertEqual(attempts[0], attempts[1])
        self.adapter._open = original
        self.adapter.control(request)
        after = self.adapter._state('a')
        self.assertEqual(after['upstream_session'], attempts[0])
        self.assertNotEqual(after['upstream_session'], before['upstream_session'])
        self.calls.clear()
        self.adapter.control(request)
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapter._state('a'), after)

    def test_completed_result_and_broker_receipt_are_immutable(self):
        text, metadata = self.adapter.chat('hello', None, 3, session_id='a', request_id='immutable')
        self.adapter.control({'operation': 'confirm_reply', 'session_id': 'a',
                              'request_id': 'immutable', 'run_id': 'run_abc'})
        again = self.adapter._complete('a', 'immutable', 'run_abc', 'completed', 'stale poll output')
        self.assertEqual(again, (text, metadata))
        self.assertEqual(self.adapter._record('immutable')['delivery_state'], 'broker_confirmed')

    def test_stop_during_preparation_prevents_submission(self):
        original = self.adapter._capabilities
        def capabilities(timeout):
            event = self.adapter.interrupt({'operation': 'stop', 'session_id': 'a'})
            self.assertIn('尚未确认', event['data']['text'])
            return original(timeout)
        self.adapter._capabilities = capabilities
        with self.assertRaises(NativeTerminal):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='prepare-stop')
        self.assertEqual(self._paths('/runs'), [])

    def test_stop_during_admission_is_sent_when_run_id_arrives(self):
        original = self.adapter._open
        def opened(path, *args, **kwargs):
            if path == '/runs':
                event = self.adapter.interrupt({'operation': 'stop', 'session_id': 'a'})
                self.assertIn('取消意图', event['data']['text'])
            return original(path, *args, **kwargs)
        self.adapter._open = opened
        self.events = frame({'event': 'run.cancelled', 'run_id': 'run_abc'})
        with self.assertRaises(NativeTerminal):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='admission-stop')
        self.assertEqual(self._paths('/runs'), ['/runs'])
        self.assertEqual(self._paths('/stop'), ['/runs/run_abc/stop'])
        self.assertEqual(self.adapter._record('admission-stop')['status'], 'cancelled')

    def test_unknown_admission_does_not_claim_no_active_run_on_stop(self):
        self.run_posts = [RuntimeError('lost response')]
        with self.assertRaises(NativeUnknown):
            self.adapter.chat('hello', None, 3, session_id='a', request_id='stop-unknown')
        with self.assertRaises(NativeUnknown):
            self.adapter.interrupt({'operation': 'stop', 'session_id': 'a'})
        self.assertEqual(self._paths('/stop'), [])
        self.assertEqual(self._paths('/runs'), ['/runs'])

    def test_invalid_denial_receipt_cancels_exact_run(self):
        self.events = frame({'event': 'approval.request', 'run_id': 'run_abc',
                             'request_id': 'p', 'choices': ['once', 'deny']}) + self.events
        original = self.adapter._open
        def opened(path, *args, **kwargs):
            if path.endswith('/approval'):
                return Body(b'{"resolved": 0}')
            return original(path, *args, **kwargs)
        self.adapter._open = opened
        self.adapter.chat('hello', None, 3, session_id='a', request_id='bad-denial')
        self.assertEqual(self._paths('/stop'), ['/runs/run_abc/stop'])

    def test_expired_timer_cannot_deny_replacement_approval(self):
        self.adapter.active['a'] = {'run_id': 'run_abc', 'prompt_id': 'new-prompt',
                                    'approval_token': 'new-token', 'deciding': False}
        self.adapter._deny_if_waiting('a', expected_token='old-token')
        self.assertEqual(self.calls, [])
        self.assertEqual(self.adapter.active['a']['approval_token'], 'new-token')


class RelayAllowlistTests(unittest.TestCase):
    def test_runs_routes_are_added_without_opening_other_paths(self):
        self.assertTrue(allowed_technical_path("/v1/chat/completions"))
        self.assertTrue(allowed_technical_path("/api/sessions"))
        self.assertTrue(allowed_technical_path("/api/sessions/spark-im-abc/model"))
        self.assertTrue(allowed_technical_path("/v1/runs"))
        self.assertTrue(allowed_technical_path("/v1/runs/run_abc"))
        self.assertTrue(allowed_technical_path("/v1/runs/run_abc/events"))
        self.assertTrue(allowed_technical_path("/v1/runs/run_abc/stop"))
        self.assertTrue(allowed_technical_path("/v1/runs/run_abc/approval"))
        self.assertTrue(allowed_technical_path("/v1/skills"))
        self.assertFalse(allowed_technical_path("/v1/runs/../admin"))
        self.assertFalse(allowed_technical_path("/v1/responses"))
        self.assertFalse(allowed_technical_path("/api/sessions/a/delete"))
        self.assertTrue(streams_response("/v1/runs/run_abc/events"))
        self.assertTrue(streams_response("/v1/chat/completions"))
        self.assertFalse(streams_response("/v1/runs"))
        self.assertFalse(streams_response("/v1/runs/run_abc/stop"))


if __name__ == "__main__":
    unittest.main()
