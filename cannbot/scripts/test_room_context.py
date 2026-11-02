"""Transport/native seam contracts. HTTP authority is a fixture, not Logic or model E2E."""
import asyncio
import copy
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import threading
import subprocess
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

from room_context import RoomContextClient, RoomContextError, TOOLS, hermes_result
from room_workspace import WorkspaceAuthority
from hermes_public_workspace import public_api_adapter, install_todo_history_compat, install_room_media_policy
from hermes_native_adapter import HermesNativeAdapter
from test_hermes_public_workspace import Request, Response

FIXTURE = json.loads((Path(__file__).resolve().parents[2] / 'tests/fixtures/room-context.json').read_text())


class ContextContract(unittest.TestCase):
    def test_private_installer_keeps_scoped_extensions_out_of_autodiscovery(self):
        # Run the real installer only against an owned disposable home.
        with tempfile.TemporaryDirectory(prefix='spark-room-install-contract-') as home:
            command = [sys.executable, str(Path(__file__).with_name('sync_pi_agent.py')),
                       '--pi-home', home]
            installed = subprocess.run([*command,'--install'],capture_output=True,text=True,timeout=15)
            self.assertEqual(installed.returncode,0,installed.stderr)
            for name in ('room-context.ts','public-workspace.ts','room-workspace.ts'):
                self.assertFalse((Path(home)/'extensions'/name).exists())
            self.assertTrue((Path(home)/'extensions/cann-advisor.ts').is_file())
            checked = subprocess.run([*command,'--check'],capture_output=True,text=True,timeout=15)
            self.assertEqual(checked.returncode,0,checked.stderr)

    def setUp(self):
        self.scope = copy.deepcopy(FIXTURE['scope'])
        self.scopes = {self.scope['request_id']: self.scope}
        self.result = copy.deepcopy(FIXTURE['image'])
        self.requests = []
        self.revoke = False
        self.raw = None
        self.code = 0
        self.key = 'test-key-' + 'k' * 40
        test = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args): pass
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                test.requests.append((self.path, body))
                scope = test.scopes.get(body.get('request_id')) if self.headers.get('Authorization') == 'Bearer ' + test.key else None
                if self.path == '/internal/room-agent/authorize':
                    value = {'code': 0 if scope else 403, 'data': {'request_id': body.get('request_id'), 'collaboration': scope}}
                    raw = json.dumps(value).encode()
                else:
                    if test.revoke: test.scopes.clear()
                    result = test.result(body) if callable(test.result) else test.result
                    raw = test.raw if test.raw is not None else json.dumps({'code': test.code, 'data': result}).encode()
                self.send_response(200); self.send_header('Content-Length', str(len(raw))); self.end_headers()
                try: self.wfile.write(raw)
                except (BrokenPipeError, ConnectionResetError): pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=lambda: self.server.serve_forever(poll_interval=.01))
        self.thread.start()
        self.addCleanup(self.close)
        self.authority = WorkspaceAuthority(f'http://127.0.0.1:{self.server.server_port}', self.key)
        self.client = RoomContextClient(self.authority)

    def close(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(3)

    def read_image(self, scope=None):
        return self.client.call(scope or self.scope, 'room_read_media',
            {'asset_id': 'a'*32, 'revision': 'b'*64, 'index': 0})

    def test_image_identity_and_native_content(self):
        result = self.read_image()
        self.assertEqual([r[0].rsplit('/', 1)[1] for r in self.requests], ['authorize', 'media', 'authorize'])
        self.assertEqual(self.requests[1][1]['request_id'], self.scope['request_id'])
        native = hermes_result(result)
        self.assertTrue(native['_multimodal'])
        self.assertTrue(native['content'][1]['image_url']['url'].startswith('data:image/jpeg;base64,'))
        self.assertNotIn(FIXTURE['image']['items'][0]['data'], native['content'][0]['text'])
        self.assertNotIn(FIXTURE['image']['items'][0]['data'], native['text_summary'])
        self.assertIn('unavailable', native['text_summary'])

    def test_forged_parameters_and_types_never_reach_transport(self):
        for arguments in ({'room_id': 2}, {'request_id': self.scope['request_id']}, {'actor_user_id': 3},
                          {'path': '/private'}, {'limit': True}, {'limit': 1.5}, {'limit': -1},
                          {'before_seq': 2**53}, {'query': '中'*81}, {'query': '\0'}, {'type': 'shell'}):
            with self.subTest(arguments=arguments), self.assertRaises(RoomContextError) as error:
                self.client.call(self.scope, 'room_search', arguments)
            self.assertEqual(error.exception.code, 400)
        self.assertEqual(self.requests, [])

    def test_legacy_or_invalid_scope_denied(self):
        for name in ('context_generation', 'workspace_generation', 'project_id', 'source_msg_id'):
            scope = dict(self.scope); del scope[name]
            with self.assertRaises(RoomContextError): self.read_image(scope)
        for value in (None, True, 1.5, -1, 2**53):
            with self.assertRaises(RoomContextError): self.read_image({**self.scope, 'context_generation': value})
        self.assertEqual(self.requests, [])

    def test_revocation_during_fetch_withholds_protected_result(self):
        self.revoke = True
        with self.assertRaises(RoomContextError) as error: self.read_image()
        self.assertEqual(error.exception.code, 403)
        self.assertNotIn(self.key, str(error.exception))

    def test_wrong_room_epoch_request_and_target_rejected(self):
        mutations = [lambda v: v.update(request_id='ra_'+'d'*64), lambda v: v.update(context_generation=True),
                     lambda v: v.update(context_generation=1), lambda v: v['items'][0]['reference'].update(room_id=8),
                     lambda v: v['items'][0]['reference'].update(asset_id='other'),
                     lambda v: v['items'][0]['reference'].update(revision='e'*64),
                     lambda v: v['items'][0]['reference'].update(index=1),
                     lambda v: v['items'][0].update(source_id='unrecorded')]
        for mutate in mutations:
            self.result = copy.deepcopy(FIXTURE['image']); mutate(self.result)
            with self.assertRaises(RoomContextError): self.read_image()

    def test_invalid_image_or_response_cannot_reach_native_model(self):
        for key, value in [('data', 'not base64!'), ('data', '/9j/2Q=='), ('mime', 'image/png')]:
            self.result = copy.deepcopy(FIXTURE['image']); self.result['items'][0][key] = value
            with self.assertRaises(RoomContextError): self.read_image()
        self.result = copy.deepcopy(FIXTURE['image']); self.result['items'][0]['metadata']['sha256'] = '0'*64
        with self.assertRaises(RoomContextError): self.read_image()
        for raw in (b'{bad secret response', b'x'*(2*1024*1024+1)):
            self.raw = raw
            with self.assertRaises(RoomContextError) as error: self.read_image()
            self.assertNotIn('secret', str(error.exception))

    def test_business_error_codes_are_not_false_success(self):
        for code in (403, 409, 413, 429, 503, False, None):
            self.code = code
            with self.assertRaises(RoomContextError) as error: self.read_image()
            self.assertEqual(error.exception.code, code if type(code) is int else 503)

    def test_empty_search_and_unicode_message_range(self):
        self.result = {'request_id': self.scope['request_id'], 'context_generation': 0, 'items': []}
        self.assertEqual(self.client.call(self.scope, 'room_search', {})['items'], [])
        text = 'A中🙂'
        ref = {'kind':'message', 'room_id':7, 'msg_id':'r_7-8', 'msg_seq':8, 'offset':2,
               'length':3, 'sha256':hashlib.sha256(text.encode()).hexdigest()}
        self.result['items'] = [{'source_id':'src_'+'d'*64, 'reference':ref, 'text':text}]
        result = self.client.call(self.scope, 'room_read', {'kind':'message', 'msg_seq':8, 'offset':2, 'limit':3})
        self.assertIn(text, hermes_result(result))
        ref['msg_seq'] = 10; ref['msg_id'] = 'r_7-10'
        with self.assertRaises(RoomContextError): self.client.call(self.scope, 'room_search', {})

    def test_connector_capability_and_toolsets_require_explicit_matching_opt_in(self):
        # Pure protocol double; no Native process or model is started.
        adapter = object.__new__(HermesNativeAdapter)
        adapter.config = {'expected_model':'public', 'room_policy':'read_only', 'room_workspace_contract':1}
        features = {key:True for key in ('run_submission','run_status','run_events_sse','run_stop','run_approval_response','spark_workspace_v1')}
        features['runs_idempotency'] = {'supported':True,'durable':True}
        rows = [{'name':'todo','enabled':True,'tools':['todo_list']},
                {'name':'spark_workspace','enabled':True,'tools':['workspace_read']}]
        adapter._json = lambda route, **kw: {'data':rows} if route == '/toolsets' else {'model':'public','features':features}
        adapter._capabilities() # Existing workspace-only configuration remains valid.
        adapter.config['room_context_contract'] = 1
        with self.assertRaises(ValueError): adapter._capabilities()
        rows.append({'name':'spark_room_context','enabled':True,'tools':sorted(TOOLS)})
        with self.assertRaises(ValueError): adapter._capabilities()
        features['spark_room_context_v1'] = True
        adapter._capabilities()
        rows[-1]['tools'].append('terminal')
        with self.assertRaises(ValueError): adapter._capabilities()
        rows[-1]['tools'].pop()
        del adapter.config['room_context_contract']
        with self.assertRaises(ValueError): adapter._capabilities() # No implicit upgrade.

    def test_hermes_admitted_threads_keep_separate_room_identity(self):
        tools = {}; prompts = []; test = self; action = 'room_search'
        class Registry:
            def register(self, **entry): tools[entry['name']] = entry
            def _normalize_handler_result(self, name, result): return result
        class Base:
            def _check_run_auth(self, request, permission): return None
            async def _handle_runs(self, request):
                agent = self._create_agent(session_id=request.body['session_id'])
                return Response(await asyncio.to_thread(agent.run_conversation))
            def _create_agent(self, session_id=None, **kwargs):
                prompts.append(kwargs['ephemeral_system_prompt'])
                from hermes_public_workspace import _admission
                vision = _admission.get()['scope']['room_id'] == 7
                def run():
                    args = {} if action == 'room_search' else {'asset_id':'a'*32,'revision':'b'*64,'index':0}
                    value = tools[action]['handler'](args)
                    return json.loads(value) if isinstance(value, str) else value
                return types.SimpleNamespace(valid_tool_names={'workspace_read', 'todo_list', *TOOLS},
                    _assistant_has_todo_tool_call=lambda message,call_id:True,
                    _tool_result_content_for_active_model=lambda name,result:result['content'] if vision else result['text_summary'],
                    _try_strip_image_parts_from_tool_messages=lambda messages,remember_model=True:False,
                    run_conversation=run)
        modules = {'aiohttp': types.SimpleNamespace(web=types.SimpleNamespace(json_response=lambda v,status=200:Response(v,status))),
                   'tools.registry': types.SimpleNamespace(registry=Registry()),
                   'tools.file_tools': types.SimpleNamespace(READ_FILE_SCHEMA={'name':'read_file'},read_file_tool=None)}
        def result(body):
            scope = test.scopes[body['request_id']]
            text = 'room-' + str(scope['room_id'])
            return {'request_id':scope['request_id'], 'context_generation':0, 'items':[{
                'source_id':'src_'+'c'*64,'text':text,'reference':{'kind':'message','room_id':scope['room_id'],
                'msg_id':f"r_{scope['room_id']}-1",'msg_seq':1,'offset':0,'length':len(text),'sha256':hashlib.sha256(text.encode()).hexdigest()}}]}
        self.result = result
        first = {**self.scope, 'agent_id':'hermes', 'runtime':'hermes_native'}
        second = {**first, 'room_id':8, 'request_id':'ra_'+'b'*64, 'source_msg_id':'r_8-9'}
        self.scopes = {s['request_id']:s for s in (first, second)}
        with patch.dict('sys.modules', modules):
            api = public_api_adapter(Base, self.authority, None, 'hermes', 'spark-room-test-default', context_tools=True)()
            self.assertEqual(set(tools), {'workspace_read', *TOOLS})
            self.assertEqual(json.loads(tools['room_search']['handler']({}))['code'], 403)
            async def run(): return await asyncio.gather(*(api._handle_runs(Request(s)) for s in (first, second)))
            responses = asyncio.run(run())
            self.assertEqual([json.loads(r.body)['items'][0]['text'] for r in responses], ['room-7','room-8'])
            self.assertTrue(all('room_search' in p and 'PROJECT_FILES.txt' not in p for p in prompts))
            self.assertEqual(json.loads(tools['room_search']['handler']({}))['code'], 403)
            action = 'room_read_media'
            def image_result(body):
                value = copy.deepcopy(FIXTURE['image'])
                value['request_id'] = body['request_id']
                value['items'][0]['reference']['room_id'] = self.scopes[body['request_id']]['room_id']
                return value
            self.result = image_result
            responses = asyncio.run(run())
            self.assertTrue(json.loads(responses[0].body)['_multimodal'])
            self.assertEqual(json.loads(responses[1].body)['code'], 501)
            self.assertEqual(json.loads(tools['room_read_media']['handler']({}))['code'], 403)


class NativeImageDeliveryPolicy(unittest.TestCase):
    class NativeContract:
        """Controlled native projection/recovery seam, not provider E2E."""
        valid_tool_names = {'workspace_read', 'todo_list', *TOOLS}
        def __init__(self): self.learned = False; self.vision = True; self.remembered = []
        def _tool_result_content_for_active_model(self, name, result):
            if not isinstance(result, dict): return result
            return result['content'] if self.vision and not self.learned else result['text_summary']
        def _try_strip_image_parts_from_tool_messages(self, messages, *, remember_model=True):
            self.remembered.append(remember_model)
            if remember_model: self.learned = True
            changed = False
            for message in messages:
                if message.get('role') == 'tool' and isinstance(message.get('content'), list):
                    message['content'] = 'private-summary-canary'
                    changed = True
            return changed

    def assert_unavailable(self, value):
        parsed = json.loads(value)
        self.assertFalse(parsed['success']); self.assertEqual(parsed['code'], 501)
        self.assertNotIn('private-summary-canary', value)
        self.assertNotIn('source_id', value)
        self.assertNotIn(FIXTURE['image']['items'][0]['data'], value)

    def test_native_projection_preserves_real_image_or_returns_error(self):
        agent = self.NativeContract(); install_room_media_policy(agent)
        value = hermes_result(copy.deepcopy(FIXTURE['image'])); before = copy.deepcopy(value)
        self.assertEqual(agent._tool_result_content_for_active_model('room_read_media', value), value['content'])
        self.assertEqual(value, before)
        self.assertEqual(agent._tool_result_content_for_active_model('room_read', 'text'), 'text')
        for mode in ('vision', 'learned'):
            agent.vision = mode != 'vision'; agent.learned = mode == 'learned'
            self.assert_unavailable(agent._tool_result_content_for_active_model('room_read_media', value))

    def test_first_provider_rejection_and_size_recovery_keep_native_state(self):
        value = hermes_result(copy.deepcopy(FIXTURE['image']))
        for remember in (True, False):
            agent = self.NativeContract(); install_room_media_policy(agent)
            messages = [{'role':'user','content':copy.deepcopy(value['content'])},
                        {'role':'tool','name':'room_read_media','tool_call_id':'call-1','content':copy.deepcopy(value['content'])},
                        {'role':'tool','name':'room_read','tool_call_id':'call-2','content':'text'}]
            before = copy.deepcopy(messages)
            self.assertTrue(agent._try_strip_image_parts_from_tool_messages(messages,remember_model=remember))
            self.assertEqual(agent.remembered,[remember]); self.assertEqual(agent.learned,remember)
            self.assertEqual(messages[0],before[0]); self.assertEqual(messages[2],before[2])
            self.assertEqual({k:v for k,v in messages[1].items() if k!='content'},
                             {k:v for k,v in before[1].items() if k!='content'})
            self.assert_unavailable(messages[1]['content'])
            self.assertFalse(agent._try_strip_image_parts_from_tool_messages(messages,remember_model=remember))
            if remember:
                self.assert_unavailable(agent._tool_result_content_for_active_model('room_read_media',value))

    def test_missing_seam_extra_tool_and_wrong_projection_fail_closed(self):
        with self.assertRaises(RuntimeError):install_room_media_policy(types.SimpleNamespace(valid_tool_names=set()))
        for operation in ('missing', 'extra', 'wrong', 'exception'):
            agent = self.NativeContract()
            if operation == 'missing': agent._try_strip_image_parts_from_tool_messages = None
            elif operation == 'extra': agent.valid_tool_names = agent.valid_tool_names | {'terminal'}
            elif operation == 'wrong': agent._tool_result_content_for_active_model = lambda *a:[{'type':'image_url','image_url':{'url':'other'}}]
            else:
                def fail(*args): raise ValueError('private-summary-canary')
                agent._tool_result_content_for_active_model = fail
            if operation in ('missing','extra'):
                with self.assertRaises(RuntimeError): install_room_media_policy(agent)
            else:
                install_room_media_policy(agent)
                self.assert_unavailable(agent._tool_result_content_for_active_model('room_read_media',hermes_result(FIXTURE['image'])))


class TodoHydrationCompatibility(unittest.TestCase):
    class NativeContract:
        """Controlled legacy pairing seam, not a replacement TodoStore or E2E."""
        def __init__(self): self.restored = []; self.received = None
        @staticmethod
        def _assistant_has_todo_tool_call(message, call_id):
            return any(call.get('id') == call_id and call.get('function',{}).get('name') == 'todo'
                       for call in message.get('tool_calls', []))
        def _hydrate_todo_store(self, history):
            self.received = history
            for index in range(len(history)-1, -1, -1):
                message = history[index]
                if message.get('role') != 'tool' or not message.get('tool_call_id'): continue
                for prior in reversed(history[:index]):
                    if prior.get('role') in ('user','system'): break
                    if prior.get('role') == 'assistant':
                        if self._assistant_has_todo_tool_call(prior,message['tool_call_id']):
                            self.restored = json.loads(message['content'])['todos']
                        break
            return 'native-return-value'

    def history(self):
        return [{'role':'assistant','tool_calls':[{'id':'call-1','type':'function',
                    'function':{'name':'todo_list','arguments':'{"todos":[]}'}}]},
                {'role':'tool','tool_call_id':'call-1','content':json.dumps({'todos':[{'id':'1','content':'scoped fixture','status':'pending'}],'revision':1})}]

    def test_wrapped_native_hydration_preserves_input_and_original_checks(self):
        good=self.history()
        invalid=[]
        wrong=copy.deepcopy(good);wrong[1]['tool_call_id']='unpaired';invalid.append(wrong)
        invalid.append([copy.deepcopy(good[1])])
        for role in ('user','system'):
            invalid.append([copy.deepcopy(good[0]),{'role':role,'content':'boundary'},copy.deepcopy(good[1])])
        invalid.append([{'role':'user','content':json.dumps(good[0])},copy.deepcopy(good[1])])
        for history, expected in [(good,True),*[(case,False) for case in invalid]]:
            with self.subTest(history=history):
                agent=self.NativeContract();original=copy.deepcopy(history)
                install_todo_history_compat(agent)
                self.assertEqual(agent._hydrate_todo_store(history),'native-return-value')
                self.assertEqual(bool(agent.restored),expected)
                self.assertEqual(history,original)
                self.assertIsNot(agent.received,history)

    def test_working_native_version_is_untouched_and_unknown_seam_denied(self):
        agent=self.NativeContract();agent._assistant_has_todo_tool_call=lambda message,call_id:True
        original=agent._hydrate_todo_store
        install_todo_history_compat(agent)
        self.assertEqual(agent._hydrate_todo_store,original)
        for unsupported in (types.SimpleNamespace(),types.SimpleNamespace(_assistant_has_todo_tool_call=lambda *a:False)):
            with self.assertRaises(RuntimeError):install_todo_history_compat(unsupported)


if __name__ == '__main__': unittest.main()
