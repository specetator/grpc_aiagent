#!/usr/bin/env python3
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from http.server import ThreadingHTTPServer
from unittest.mock import patch
from agent_router import AgentRouter, AgentStore
from agent_scheduler import SessionScheduler
from pi_gateway import GatewayState, make_handler
from room_agent import RoomPiFactory, run_tests, workspace_manifest


class FakeRoomFactory:
    def __init__(self): self.calls = []
    def for_scope(self, scope): self.scope = dict(scope); return self
    def chat(self, *args, **kwargs):
        self.calls.append((dict(self.scope), args, kwargs))
        return "shared answer", {"workspace_checkpoint":{"files":[],"tests":[],"code_revision":"abc"}}


class RoomTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        entries = [{'id':'pi', 'name':'CANN', 'runtime':'pi_rpc', 'owner_user_ids':[], 'bot_user_id':99},
                   {'id':'private', 'name':'Private', 'runtime':'hermes_http', 'owner_user_ids':[7], 'bot_user_id':199}]
        self.entries = entries
        self.factory = FakeRoomFactory()
        self.router = AgentRouter(entries, {'pi':self.factory,'private':self.factory}, AgentStore(self.root/'state'), bot_user_id=99)
        self.router.room_adapter = self.factory
        self.scope = dict(room_id=1, task_id='a'*32, owner_user_id=7, actor_user_id=7, bot_user_id=99, can_develop=True)
    def tearDown(self): self.temp.cleanup()
    def test_two_users_share_backend_and_keep_actor(self):
        self.router.authorize_room('r_1', self.scope)
        _, first = self.router.chat('A specification',None,2,session_id='r_1',request_id='A')
        second_scope = dict(self.scope, actor_user_id=8)
        self.router.authorize_room('r_1', second_scope)
        _, second = self.router.chat('B takes over',None,2,session_id='r_1',request_id='B')
        self.assertEqual(first['agent_route']['session_key'],second['agent_route']['session_key'])
        self.assertEqual(first['actor_user_id'],7)
        self.assertEqual(second['actor_user_id'],8)
        self.assertEqual(self.factory.calls[0][1][5],self.factory.calls[1][1][5])
        self.assertEqual(len(self.router.store.get('context','r_1')['recent_messages']),4)
    def test_replay_does_not_execute_tools_again(self):
        self.router.authorize_room('r_1',self.scope)
        self.router.chat('implement',None,2,session_id='r_1',request_id='same')
        _, metadata = self.router.chat('implement',None,2,session_id='r_1',request_id='same')
        self.assertEqual(len(self.factory.calls),1)
        self.assertTrue(metadata['turn_replayed'])
        self.router.authorize_room('r_1',dict(self.scope,actor_user_id=8))
        with self.assertRaises(ValueError): self.router.chat('implement',None,2,session_id='r_1',request_id='same')
    def test_room_identity_and_private_acl_fail_closed(self):
        with self.assertRaises(ValueError): self.router.describe_route('r_1')
        for bad in (dict(self.scope,bot_user_id=199),dict(self.scope,room_id=2),
                    dict(self.scope,task_id='../escape'),dict(self.scope,can_develop='true')):
            with self.assertRaises(ValueError): self.router.authorize_room('r_1',bad)
        self.router.authorize_room('r_1',self.scope)
        with self.assertRaises(ValueError): self.router.authorize_room('r_1',dict(self.scope,task_id='b'*32))
        with self.assertRaises(ValueError): self.router.chat('private',None,1,session_id='s_8_199')
    def test_restart_preserves_room_binding_and_completed_turn(self):
        self.router.authorize_room('r_1',self.scope)
        self.router.chat('implement',None,2,session_id='r_1',request_id='restart')
        restarted = AgentRouter(self.entries, {'pi':self.factory,'private':self.factory}, AgentStore(self.root/'state'),bot_user_id=99)
        restarted.room_adapter=self.factory
        restarted.authorize_room('r_1',self.scope)
        _, meta=restarted.chat('implement',None,2,session_id='r_1',request_id='restart')
        self.assertTrue(meta['turn_replayed']);self.assertEqual(len(self.factory.calls),1)
    def test_gateway_requires_bridge_auth_and_room_scope(self):
        server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(GatewayState(self.router,'test-key','pi-agent')))
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        def post(body,key='test-key',path='/v1/chat/completions'):
            req=Request(f'http://127.0.0.1:{server.server_port}'+path,json.dumps(body).encode(),
                        {'Content-Type':'application/json','Authorization':'Bearer '+key})
            try: response=urlopen(req,timeout=5)
            except HTTPError as error: response=error
            with response: return response.status,json.load(response)
        try:
            request={'session_id':'r_1','request_id':'gateway','messages':[{'role':'user','content':'hello'}],
                     'collaboration':self.scope}
            self.assertEqual(post(request,key='bad')[0],401)
            self.assertEqual(post({**request,'collaboration':None})[0],403)
            self.assertEqual(post({**request,'operation':'new'},path='/v1/agent/control')[0],403)
            self.assertEqual(post(request)[0],200)
            self.assertEqual(len(self.factory.calls),1)
        finally: server.shutdown();server.server_close();thread.join()


class ExecutionTests(unittest.TestCase):
    def test_waiter_timeout_cannot_release_running_owner(self):
        scheduler=SessionScheduler(max_inflight=2,max_queue=8,queue_timeout_s=2)
        started,release=threading.Event(),threading.Event()
        worker=threading.Thread(target=lambda:scheduler.run('room','pi',2,None,lambda:(started.set(),release.wait(2))))
        worker.start();self.assertTrue(started.wait(1))
        try:
            with self.assertRaises(TimeoutError): scheduler.run('room','pi',.05,None,lambda:None)
            self.assertIn('room',scheduler.session_running)
            self.assertEqual(scheduler.inflight,1)
            with self.assertRaises(TimeoutError): scheduler.run('room','pi',.05,None,lambda:self.fail('concurrent workspace write'))
        finally: release.set();worker.join()
        self.assertEqual(scheduler.inflight,0);self.assertFalse(scheduler.session_tickets)
    def test_sandbox_receipt_and_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);workspace=root/'workspace';workspace.mkdir();receipts=root/'receipts'
            (workspace/'operator.cpp').write_text('int add(int a,int b){return a+b;}')
            runner=root/'runner.py'
            runner.write_text("import os\nassert not os.path.exists('/home/peco')\nassert os.path.isfile('operator.cpp')\nprint('sandbox verified')\n")
            good=run_tests(workspace,receipts,runner)
            self.assertTrue(good['passed'],good['log']);self.assertIn('sandbox verified',good['log'])
            self.assertEqual(good['code_revision'],workspace_manifest(workspace)['code_revision'])
            runner.write_text("raise RuntimeError('precision failure')\n")
            failed=run_tests(workspace,receipts,runner)
            self.assertFalse(failed['passed']);self.assertNotEqual(failed['exit_code'],0)
            self.assertEqual(len(list(receipts.glob('*.json'))),2)
            with self.assertRaises(ValueError): run_tests(workspace,receipts,workspace/'operator.cpp')
    def test_timeout_is_not_success(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);workspace=root/'workspace';workspace.mkdir()
            runner=root/'runner.py';runner.write_text('import time\ntime.sleep(10)\n')
            receipt=run_tests(workspace,root/'receipts',runner,.1)
            self.assertTrue(receipt['timed_out']);self.assertFalse(receipt['passed'])
    def test_modified_source_cannot_claim_old_revision_passed(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);workspace=root/'workspace';workspace.mkdir()
            (workspace/'operator.cpp').write_text('initial source')
            runner=root/'runner.py';runner.write_text("from pathlib import Path\nPath('operator.cpp').write_text('changed source')\n")
            receipt=run_tests(workspace,root/'receipts',runner)
            self.assertEqual(receipt['exit_code'],0)
            self.assertFalse(receipt['source_unchanged']);self.assertFalse(receipt['passed'])
    def test_manifest_rejects_symlinks(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp);(root/'escape').symlink_to('/etc/passwd')
            with self.assertRaises(ValueError): workspace_manifest(root)


if __name__ == '__main__': unittest.main()
