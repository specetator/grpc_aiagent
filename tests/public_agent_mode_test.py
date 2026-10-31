"""HTTP client contracts with a fixture API; not a real Logic/Runtime acceptance."""
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import sys
import threading
import unittest
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'scripts'))
from verify_public_agent_mode import verify


class ModeTest(unittest.TestCase):
    def setUp(self):
        self.paths = []
        self.task = {'code': 0, 'data': {'cann_enabled': False}}
        self.spaces = [{'room_id': 5}]
        self.redirect = False
        test = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                test.paths.append(self.path)
                if test.redirect:
                    self.send_response(302);self.send_header('Location', test.origin + '/unexpected');self.end_headers();return
                if self.path == '/api/login':
                    test.assertEqual(body, {'account': 'fixture', 'password': 'fixture-password'})
                    response = {'code': 0, 'data': {'token': 'fixture-token'}}
                else:
                    test.assertEqual(self.headers.get('Authorization'), 'Bearer fixture-token')
                    if self.path == '/api/spaces/list': response = {'code': 0, 'data': {'spaces': test.spaces}}
                    else:
                        test.assertEqual(self.path, '/api/chatroom/agent/task')
                        test.assertEqual(body, {'room_id': 5})
                        response = test.task
                data = json.dumps(response).encode()
                self.send_response(200);self.send_header('Content-Length', str(len(data)));self.end_headers();self.wfile.write(data)

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.origin = 'http://127.0.0.1:' + str(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True);self.thread.start()
        self.addCleanup(self.close)

    def close(self):
        self.server.shutdown();self.server.server_close();self.thread.join()

    def check(self):
        return verify(self.origin, {'account': 'fixture', 'password': 'fixture-password'})

    def test_boolean_false_via_normal_login_and_authorized_space(self):
        self.assertEqual(self.check()['cann_enabled'], False)
        self.assertEqual(self.paths, ['/api/login', '/api/spaces/list', '/api/chatroom/agent/task'])

    def test_missing_or_non_false_capability_cannot_enable_old_image(self):
        for value in ({}, {'cann_enabled': True}, {'cann_enabled': 0}, {'cann_enabled': 'false'}, {'cann_enabled': None}):
            with self.subTest(value=value):
                self.task['data'] = value
                with self.assertRaises(ValueError): self.check()

    def test_failed_membership_response_is_not_a_capability(self):
        self.task = {'code': 403, 'data': {'cann_enabled': False}}
        with self.assertRaises(ValueError): self.check()

    def test_no_space_does_not_create_one(self):
        self.spaces = []
        with self.assertRaises(ValueError): self.check()
        self.assertEqual(self.paths, ['/api/login', '/api/spaces/list'])

    def test_credentials_and_token_are_not_redirected(self):
        self.redirect = True
        with self.assertRaises(urllib.error.HTTPError): self.check()
        self.assertEqual(self.paths, ['/api/login'])

    def test_origin_cannot_carry_credentials_or_another_api_path(self):
        for origin in ('http://user:password@127.0.0.1', self.origin + '/other', self.origin + '?key=value'):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError): verify(origin, {'account': 'fixture', 'password': 'fixture-password'})
        self.assertEqual(self.paths, [])


if __name__ == '__main__': unittest.main()
