#!/usr/bin/env python3
"""Real local Spark HTTP/SSE contract with a fake Hermes server (not Hermes E2E)."""
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
import json
import threading
import unittest
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import HTTPError

from pi_gateway import GatewayState, make_handler
import test_hermes_native as fixtures
from test_hermes_native import GateBody, frame


class NativeGatewayTests(unittest.TestCase):
    setUp = fixtures.NativeTests.setUp
    tearDown = fixtures.NativeTests.tearDown
    _open = fixtures.NativeTests._open
    _success_events = fixtures.NativeTests._success_events
    _router = fixtures.NativeTests._router
    _paths = fixtures.NativeTests._paths

    @contextmanager
    def gateway(self):
        router = self._router()
        server = ThreadingHTTPServer(('127.0.0.1', 0), make_handler(GatewayState(router, 'test-key', 'test-model')))
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.url = 'http://127.0.0.1:' + str(server.server_port)
        try:
            yield router
        finally:
            server.shutdown()
            server.server_close()
            thread.join(2)

    def post(self, path, value):
        req = Request(self.url + path, json.dumps(value).encode(),
                      {'Authorization': 'Bearer test-key', 'Content-Type': 'application/json'})
        with build_opener(ProxyHandler({})).open(req, timeout=6) as response:
            return response.read().decode()

    def chat(self, request_id):
        return self.post('/v1/chat/completions', {
            'session_id': 's_3_900000000101', 'request_id': request_id,
            'messages': [{'role': 'user', 'content': 'hello'}], 'stream': True, 'agent_events': True})

    @staticmethod
    def parse_events(raw):
        return [json.loads(block.split('data: ', 1)[1]) for block in raw.split('\n\n')
                if block.startswith('event: agent.event\n')]

    def test_native_envelope_and_repeat_publish_survive_http_boundary(self):
        with self.gateway():
            first = self.parse_events(self.chat('http'))
            self.assertEqual(first[-1]['type'], 'assistant_final')
            self.assertEqual(first[-1]['envelope']['run_id'], 'run_abc')
            self.assertTrue(first[-1]['envelope']['terminal'])
            self.assertEqual([e['envelope']['sequence'] for e in first], list(range(len(first))))
            self.assertTrue(any(e['type'] == 'tool_start' for e in first))
            again = self.parse_events(self.chat('http'))
            self.assertTrue(all(e['envelope']['replayed'] for e in again))
            self.assertEqual([e['envelope']['event_id'] for e in first], [e['envelope']['event_id'] for e in again])
            self.assertEqual(self._paths('/runs'), ['/runs'])

    def test_unknown_http_error_cannot_poison_recovered_event_replay(self):
        self.events = b''
        self.run_status = {'run_id': 'run_abc', 'status': 'running'}
        with self.gateway() as router:
            first = self.chat('recover-http')
            self.assertIn('event: agent.event', first)
            self.assertTrue(type(self).parse_events(first)[-1]['data']['native_unconfirmed'])
            self.assertEqual(router.store.get_turn('recover-http')['status'], 'unknown')
            stored, _ = router.replay_events('recover-http')
            self.assertFalse(any(e['type'] == 'error' or e['envelope']['terminal'] for e in stored))
            self.run_status = {'run_id': 'run_abc', 'status': 'completed', 'output': 'recovered'}
            second = type(self).parse_events(self.chat('recover-http'))
            self.assertFalse(any(e['type'] == 'error' for e in second))
            self.assertEqual(second[-1]['type'], 'assistant_final')
            third = type(self).parse_events(self.chat('recover-http'))
            self.assertEqual(third[-1]['type'], 'assistant_final')
            self.assertEqual(self._paths('/runs'), ['/runs'])

    def test_broker_receipt_is_separate_from_result_and_idempotent(self):
        with self.gateway():
            self.chat('receipt')
            self.assertEqual(self.adapter._record('receipt')['delivery_state'], 'result_ready')
            # Simulated Kafka publish failure: retry output, do not confirm yet.
            self.chat('receipt')
            self.assertEqual(self._paths('/runs'), ['/runs'])
            value = {'operation': 'confirm_reply', 'session_id': 's_3_900000000101',
                     'request_id': 'receipt', 'run_id': 'foreign_run'}
            with self.assertRaises(HTTPError):
                self.post('/v1/agent/control', value)
            self.assertEqual(self.adapter._record('receipt')['delivery_state'], 'result_ready')
            value['run_id'] = 'run_abc'
            self.post('/v1/agent/control', value)
            self.post('/v1/agent/control', value)
            self.assertEqual(self.adapter._record('receipt')['delivery_state'], 'broker_confirmed')
            self.assertEqual(self._paths('/runs'), ['/runs'])

    def test_nonstream_unknown_preserves_recovery_classification(self):
        self.events = b''
        self.run_status = {'run_id': 'run_abc', 'status': 'running'}
        with self.gateway():
            with self.assertRaises(HTTPError) as exc:
                self.post('/v1/chat/completions', {
                    'session_id': 's_3_900000000101', 'request_id': 'nonstream',
                    'messages': [{'role': 'user', 'content': 'hello'}], 'stream': False})
            self.assertEqual(exc.exception.code, 502)
            self.assertTrue(json.loads(exc.exception.read())['error']['native_unconfirmed'])
            self.assertEqual(self.store.get_turn('nonstream')['status'], 'unknown')

    def test_http_stop_and_status_preempt_running_router_lock(self):
        seen, release = threading.Event(), threading.Event()
        self.event_factory = lambda: GateBody(b': keepalive\n\n',
            frame({'event': 'run.cancelled', 'run_id': 'run_abc'}), seen, release)
        self.run_status = {'run_id': 'run_abc', 'status': 'running'}
        with self.gateway():
            result = {}
            worker = threading.Thread(target=lambda: result.setdefault('body', self.chat('stop-http')))
            worker.start()
            try:
                self.assertTrue(seen.wait(2))
                status = self.post('/v1/agent/control', {'session_id': 's_3_900000000101', 'operation': 'status'})
                self.assertIn('running', status)  # actual GET response, not locally guessed
                stopped = self.post('/v1/agent/control', {'session_id': 's_3_900000000101', 'operation': 'stop'})
                self.assertIn('已请求停止', stopped)
                self.assertTrue(worker.is_alive())
                self.assertEqual(self._paths('/stop'), ['/runs/run_abc/stop'])
            finally:
                release.set()
                worker.join(4)
            self.assertNotIn('event: pi.final', result['body'])

    def test_http_approval_consumes_exact_generation_once(self):
        self.adapter.interactive = True
        seen, release = threading.Event(), threading.Event()
        self.event_factory = lambda: GateBody(frame({
            'event': 'approval.request', 'run_id': 'run_abc', 'request_id': 'prompt-id',
            'choices': ['once', 'deny']}), self._success_events('done'), seen, release)
        with self.gateway() as router:
            result = {}
            worker = threading.Thread(target=lambda: result.setdefault('body', self.chat('approve-http')))
            worker.start()
            try:
                self.assertTrue(seen.wait(2))
                backend = router._describe_route('s_3_900000000101')['backend_session_ref']
                token = self.adapter.active[backend]['approval_token']
                value = {'session_id': 's_3_900000000101', 'operation': 'approve', 'argument': token}
                self.assertIn('once', self.post('/v1/agent/control', value))
                with self.assertRaises(HTTPError) as exc:
                    self.post('/v1/agent/control', value)
                self.assertEqual(exc.exception.code, 422)
                posts = [c[1] for c in self.calls if c[0].endswith('/approval')]
                self.assertEqual(posts, [{'choice': 'once', 'request_id': 'prompt-id'}])
            finally:
                release.set()
                worker.join(4)
            self.assertIn('event: pi.final', result['body'])


if __name__ == '__main__':
    unittest.main()
