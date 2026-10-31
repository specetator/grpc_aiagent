#!/usr/bin/env python3
"""Offline protocol tests: real HTTP/subprocesses, no models or credentials."""
import hashlib
import http.client
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from agent_cli_adapter import CliAdapter, CliOutput, ExecutionUnknown, SessionAdapter
from agent_connection_transport import Endpoint
from agent_connector import make_handler, build_adapter, single_writer
from agent_router import AgentStore, AgentRouter, load_router
from user_agent_connections import Connections, UserConnectionAdapter, TYPES
from openclaw_adapter import OpenClawAdapter


class OutputTests(unittest.TestCase):
    def events(self, runtime):
        if runtime=='codex':
            return [{'type':'thread.started','thread_id':'thread-123'},
                {'type':'item.completed','item':{'type':'agent_message','text':'完成'}},
                {'type':'turn.completed'}]
        if runtime=='claude_code':
            return [{'type':'system','session_id':'thread-123'},
                {'type':'result','session_id':'thread-123','is_error':False,'subtype':'success','result':'完成'}]
        return [{'type':'session','sessionId':'thread-123'},
            {'type':'text','text':'部分'},
            {'type':'final','sessionId':'thread-123','text':'完成','reason':{'kind':'completed'}}]

    def test_native_success_required_for_all_three_clis(self):
        for runtime in ['codex','claude_code','dsh']:
            with self.subTest(runtime=runtime):
                parser=CliOutput(runtime)
                for event in self.events(runtime): parser.feed(event)
                self.assertEqual(parser.finish(0),'完成')
                with self.assertRaises(ValueError): parser.finish(1)
                with self.assertRaises(ValueError): parser.feed({'type':'text','text':'late'})
                partial=CliOutput(runtime)
                for event in self.events(runtime)[:-1]: partial.feed(event)
                with self.assertRaises(ValueError): partial.finish(0)

    def test_error_and_conflicting_session_never_become_success(self):
        for runtime in ['codex','claude_code','dsh']:
            parser=CliOutput(runtime)
            parser.feed({'type':'error'})
            for event in self.events(runtime): parser.feed(event)
            with self.assertRaises(ValueError): parser.finish(0)
            parser=CliOutput(runtime,'different-session')
            with self.assertRaises(ValueError): parser.feed(self.events(runtime)[0])
        parser=CliOutput('dsh')
        parser.feed({'type':'final','sessionId':'one','text':'partial','reason':{'kind':'aborted'}})
        with self.assertRaises(ValueError): parser.finish(0)
        parser=CliOutput('claude_code')
        parser.feed({'type':'result','session_id':'one','result':'partial','is_error':True,'subtype':'error_max_turns'})
        with self.assertRaises(ValueError): parser.finish(0)


class FakeRuntime(SessionAdapter):
    def __init__(self, store):
        super().__init__({'runtime':'codex'}, store)
        self.calls=[]
    def is_ready(self): return True
    def chat(self,message,on_delta,timeout_s,**kw):
        self.calls.append((message,kw))
        if on_delta: on_delta('预览')
        return '完成：'+message, {}


class ConnectionTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.store=AgentStore(self.root/'spark')
        self.remote=AgentStore(self.root/'remote')
        self.fake=FakeRuntime(self.remote)
        self.key='test-connector-key-not-a-real-secret'
        self.server=ThreadingHTTPServer(('127.0.0.1',0),make_handler(self.fake,'codex',self.remote,self.key))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
        self.env=patch.dict(os.environ,{'SPARK_PUSH_AGENT_ALLOWED_ORIGINS':self.url}); self.env.start()
        self.connections=Connections(self.store)
        self.adapter=UserConnectionAdapter('codex',self.connections)
        self.sid='s_7_900000000302'
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.env.stop(); self.temp.cleanup()
    def save(self,actor=7,runtime='auto',revision=''):
        return self.connections.manage({'action':'save','actor_user_id':actor,'runtime':runtime,
            'endpoint':self.url,'token':self.key,'revision':revision})
    def router(self):
        return AgentRouter([{'id':'my-codex','name':'我的 Codex','owner_user_ids':[],
            'bot_user_id':TYPES['codex'][1]}], {'my-codex':self.adapter},self.store,default='my-codex')

    def test_detection_redaction_and_owner_isolation(self):
        saved=self.save()
        self.assertEqual(saved['runtime'],'codex')
        self.assertTrue(saved['has_key'])
        listing=self.connections.manage({'action':'list','actor_user_id':7})
        self.assertNotIn(self.key,json.dumps(listing))
        record=self.connections.get(7,'codex')
        self.assertNotIn(self.key,json.dumps(record))
        secret_file=self.connections.credential_path(record['key_ref'])
        self.assertEqual(secret_file.stat().st_mode & 0o777,0o600)
        self.assertEqual(self.connections.vault.stat().st_mode & 0o777,0o700)
        self.assertTrue(self.adapter.visible_to(7)); self.assertFalse(self.adapter.visible_to(8))
        self.assertFalse(any(x['configured'] for x in self.connections.manage({'action':'list','actor_user_id':8})['connections']))
        with self.assertRaises(ValueError): self.save(runtime='dsh')
        with self.assertRaises(ValueError): self.connections.manage({'action':'list','actor_user_id':True})
        with self.assertRaises(ValueError): self.connections.manage({'action':'list','actor_user_id':7,'binary':'sh'})
        with self.assertRaises(ValueError): self.connections.probe(self.url,'wrong-key','auto')
        with self.assertRaises(ValueError): self.router().chat('secret',None,2,session_id='s_8_900000000302',request_id='forged')
        self.assertEqual(self.fake.calls,[])

    def test_gateway_management_requires_internal_bearer(self):
        from pi_gateway import GatewayState, make_handler as gateway_handler
        router=self.router()
        router.manage_connections=self.connections.manage
        server=ThreadingHTTPServer(('127.0.0.1',0),gateway_handler(GatewayState(router,'internal-test-key','pi')))
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            for bearer,expected in [('user-login-token',401),('internal-test-key',200)]:
                conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=2)
                conn.request('POST','/v1/agent/connections',json.dumps({'actor_user_id':7,'action':'list'}),
                    {'Authorization':'Bearer '+bearer,'Content-Type':'application/json'})
                response=conn.getresponse()
                self.assertEqual(response.status,expected)
                data=json.loads(response.read()); conn.close()
                if expected==200: self.assertEqual(len(data['data']['connections']),5)
        finally:
            server.shutdown(); server.server_close(); thread.join()

    def test_connector_unknown_blocks_new_execution_until_new_context(self):
        self.save()
        original=self.fake.chat
        def fail(*args,**kwargs):
            self.fake.calls.append(('failed',kwargs))
            raise RuntimeError('simulated remote execution failure')
        self.fake.chat=fail
        with self.assertRaises(ExecutionUnknown): self.adapter.chat('fail',None,2,session_id=self.sid,request_id='bad')
        self.fake.chat=original
        with self.assertRaises(ExecutionUnknown): self.adapter.chat('next',None,2,session_id=self.sid,request_id='blocked')
        self.assertEqual(len(self.fake.calls),1)
        self.adapter.control({'session_id':self.sid,'operation':'new_session','command_seq':3})
        self.adapter.chat('new context',None,2,session_id=self.sid,request_id='after-new')
        self.assertEqual(len(self.fake.calls),2)

    def test_new_context_atomic_failure_replay_and_stale_command(self):
        self.save()
        _,session=self.adapter.binding(7,self.sid)
        self.remote.put('connector_session',session,{'command_seq':1,'request_id':'unconfirmed-old'})
        original=self.remote.update
        failed=[False]
        def fail_once(namespace,key,mutate,**kwargs):
            if namespace=='connector_session' and not failed[0]:
                failed[0]=True
                raise OSError('injected write failure after native new')
            return original(namespace,key,mutate,**kwargs)
        command={'session_id':self.sid,'operation':'new_session','command_seq':2}
        with patch.object(self.remote,'update',side_effect=fail_once):
            with self.assertRaises(ValueError): self.adapter.control(command)
        self.assertEqual(self.remote.get('connector_session',session),{'command_seq':1,'request_id':'unconfirmed-old'})
        self.adapter.control(command)
        self.assertEqual(self.remote.get('connector_session',session),{'command_seq':2})
        self.adapter.chat('after replay',None,2,session_id=self.sid,request_id='fresh')
        # Replaying the same /new after another uncertain execution must not
        # remove that later execution's admission record.
        self.remote.put('connector_session',session,{'command_seq':2,'request_id':'newer-unknown'})
        self.adapter.control(command)
        self.assertEqual(self.remote.get('connector_session',session)['request_id'],'newer-unknown')
        with self.assertRaises(ExecutionUnknown): self.adapter.chat('blocked',None,2,session_id=self.sid,request_id='blocked-again')

    def test_chat_replay_and_session_isolation(self):
        self.save(); self.save(actor=8)
        router=self.router()
        delta=[]
        one=router.chat('hello',delta.append,3,session_id=self.sid,request_id='one')
        self.assertIn('完成：hello',one[0]); self.assertEqual(delta,['预览'])
        router.chat('hello',None,3,session_id=self.sid,request_id='one')
        self.assertEqual(len(self.fake.calls),1)
        router.chat('other',None,3,session_id='s_8_900000000302',request_id='two')
        self.assertNotEqual(self.fake.calls[0][1]['session_id'],self.fake.calls[1][1]['session_id'])
        # Connector's own durable ledger also prevents repeated side effects.
        self.adapter.chat('hello',None,3,session_id=self.sid,request_id='one')
        self.assertEqual(len(self.fake.calls),2)
        with self.assertRaises(ValueError): router.chat('altered',None,3,session_id=self.sid,request_id='one')

    def test_recovery_queries_result_without_reexecuting(self):
        self.save(); router=self.router()
        router.chat('hello',None,3,session_id=self.sid,request_id='recover')
        with self.store.connect() as db:
            db.execute("UPDATE agent_turns SET status='unknown',result_text=NULL,metadata_json=NULL WHERE request_id='recover'")
        text,meta=router.chat('hello',None,3,session_id=self.sid,request_id='recover')
        self.assertIn('完成：hello',text); self.assertTrue(meta['turn_recovered'])
        self.assertEqual(len(self.fake.calls),1)

    def test_result_survives_connector_and_router_restart(self):
        self.save(); router=self.router()
        router.chat('persistent',None,3,session_id=self.sid,request_id='restart-recover')
        with self.store.connect() as db:
            db.execute("UPDATE agent_turns SET status='unknown',result_text=NULL,metadata_json=NULL WHERE request_id='restart-recover'")
        port=self.server.server_port
        self.server.shutdown(); self.server.server_close(); self.thread.join()
        self.remote=AgentStore(self.root/'remote'); self.remote.reclaim_running_turns(0)
        self.fake=FakeRuntime(self.remote)
        self.server=ThreadingHTTPServer(('127.0.0.1',port),make_handler(self.fake,'codex',self.remote,self.key))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True); self.thread.start()
        self.store=AgentStore(self.root/'spark')
        self.connections=Connections(self.store)
        self.adapter=UserConnectionAdapter('codex',self.connections)
        text,meta=self.router().chat('persistent',None,3,session_id=self.sid,request_id='restart-recover')
        self.assertIn('完成：persistent',text)
        self.assertTrue(meta['turn_recovered'])
        self.assertEqual(self.fake.calls,[])

    def test_config_cas_delete_and_replacement_do_not_rebind_old_turn(self):
        saved=self.save(); router=self.router()
        router.chat('hello',None,3,session_id=self.sid,request_id='old')
        with self.assertRaises(ValueError): self.save(runtime='codex')
        changed=self.save(runtime='codex',revision=saved['revision'])
        with self.assertRaises(ValueError): router.chat('hello',None,3,session_id=self.sid,request_id='old')
        self.connections.manage({'action':'delete','actor_user_id':7,'runtime':'codex','revision':changed['revision']})
        self.assertFalse(self.adapter.visible_to(7))
        self.assertTrue(self.store.get_turn('old')['text'])
        with self.assertRaises(ValueError): router.chat('new',None,3,session_id=self.sid,request_id='new')

    def test_new_is_idempotent_and_unknown_commands_are_not_forwarded(self):
        self.save(); router=self.router()
        for _ in range(2):
            router.control({'session_id':self.sid,'operation':'new_session','command_seq':10})
        _,upstream=self.adapter.binding(7,self.sid)
        self.assertEqual(self.fake.state(upstream)['context_start_seq'],10)
        with self.assertRaises(ValueError): router.control({'session_id':self.sid,'operation':'new_session','command_seq':9})
        self.assertIn('暂不支持',router.control({'session_id':self.sid,'operation':'set_model'})['data']['text'])
        self.assertEqual(self.fake.calls,[])

    def test_private_url_redirect_and_dns_rebinding_boundaries(self):
        with patch.dict(os.environ,{'SPARK_PUSH_AGENT_ALLOWED_ORIGINS':''}):
            with self.assertRaises(ValueError): Endpoint(self.url,self.key).json('/v1/agent/capabilities')
            with patch('socket.getaddrinfo',return_value=[(socket.AF_INET,socket.SOCK_STREAM,6,'',('127.0.0.1',443))]):
                with self.assertRaises(ValueError): Endpoint('https://example.com',self.key).open('/probe')
        for url in ['https://name:password@example.com','http://127.0.0.1/path?key=secret','file:///tmp/key','http://example.com\r\nX:1']:
            with self.assertRaises(ValueError): Endpoint(url,self.key)
        with self.assertRaises(ValueError): Endpoint(self.url,'key\nInjected: header')
        # An allowlisted origin does not implicitly allow another port.
        with self.assertRaises(ValueError): Endpoint('http://127.0.0.1:1',self.key).open('/probe')

    def test_redirect_is_not_followed_and_ip_is_pinned(self):
        class Redirect(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_GET(handler):
                handler.send_response(302)
                handler.send_header('Location',self.url+'/v1/agent/capabilities')
                handler.end_headers()
        redirect=ThreadingHTTPServer(('127.0.0.1',0),Redirect)
        thread=threading.Thread(target=redirect.serve_forever,daemon=True); thread.start()
        origin='http://127.0.0.1:'+str(redirect.server_port)
        try:
            with patch.dict(os.environ,{'SPARK_PUSH_AGENT_ALLOWED_ORIGINS':origin}):
                with self.assertRaisesRegex(RuntimeError,'HTTP 302'):
                    Endpoint(origin,self.key).json('/probe')
        finally:
            redirect.shutdown(); redirect.server_close(); thread.join()
        # Resolve the customer hostname once, then require connecting to that
        # checked numeric IP. Substitute the local fixture for network I/O.
        real_resolve,real_connect=socket.getaddrinfo,socket.create_connection
        checked='93.184.216.34'
        def resolve(host,port,*args,**kwargs):
            if host=='connector.example': return [(socket.AF_INET,socket.SOCK_STREAM,6,'',(checked,port))]
            return real_resolve(host,port,*args,**kwargs)
        def connect(address,**kwargs):
            self.assertEqual(address,(checked,self.server.server_port))
            return real_connect(('127.0.0.1',self.server.server_port),**kwargs)
        with patch('socket.getaddrinfo',side_effect=resolve), patch('socket.create_connection',side_effect=connect) as dial, patch('ssl.create_default_context') as tls:
            tls.return_value.wrap_socket.side_effect=lambda sock,**kwargs:sock
            result=Endpoint(f'https://connector.example:{self.server.server_port}',self.key).json('/v1/agent/capabilities')
            self.assertEqual(result['runtime'],'codex')
            dial.assert_called_once()
            self.assertEqual(tls.return_value.wrap_socket.call_args.kwargs['server_hostname'],'connector.example')

    def test_all_runtime_factories_and_opt_in_registry(self):
        for runtime in ['codex','claude_code','dsh']:
            self.assertIsInstance(build_adapter({'runtime':runtime,'cwd':str(self.root)},self.store),CliAdapter)
        with patch.dict(os.environ,{'OPENCLAW_GATEWAY_TOKEN':self.key}):
            self.assertIsInstance(build_adapter({'runtime':'openclaw','base_url':self.url+'/v1'},self.store),OpenClawAdapter)
        with patch('hermes_adapter.HermesHttpAdapter') as factory:
            build_adapter({'runtime':'hermes'},self.store); factory.assert_called_once()
        path=self.root/'agents.json'
        path.write_text(json.dumps({'schema':1,'agents':[{'id':'pi','name':'Pi','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000001}]}))
        with patch.dict(os.environ,{'SPARK_PUSH_AGENT_SELF_SERVICE':'true','SPARK_PUSH_PI_WORKERS':'1'}):
            router=load_router(path,self.fake,self.root/'registry')
            self.assertEqual(len(router.entries),6)
            self.assertEqual(list(router._visible('s_7_900000000001')),['pi'])
        with patch.dict(os.environ,{'SPARK_PUSH_AGENT_SELF_SERVICE':'false','SPARK_PUSH_PI_WORKERS':'1'}):
            router=load_router(path,self.fake,self.root/'default')
            self.assertEqual(len(router.entries),1)


class CliProcessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.root=Path(self.temp.name)
        self.store=AgentStore(self.root/'state')
    def tearDown(self): self.temp.cleanup()
    def executable(self,source):
        import sys
        binary=self.root/'fake-cli'
        binary.write_text('#!'+sys.executable+'\n'+source)
        binary.chmod(0o700)
        return str(binary)
    def test_prompt_stdin_resume_and_distinct_sessions(self):
        binary=self.executable("""import json,sys
prompt=sys.stdin.read()
args=sys.argv[1:]
sid=args[args.index('resume')+1] if 'resume' in args else 'new-thread'
for e in [{'type':'thread.started','thread_id':sid},{'type':'item.completed','item':{'type':'agent_message','text':json.dumps({'prompt':prompt,'args':args})}},{'type':'turn.completed'}]: print(json.dumps(e),flush=True)
""")
        cli=CliAdapter({'runtime':'codex','cwd':str(self.root),'binary':binary},self.store)
        prompt='$(touch should-not-exist) `touch bad` --yolo 中文'
        text,_=cli.chat(prompt,None,2,session_id='one')
        result=json.loads(text)
        self.assertEqual(result['prompt'],prompt); self.assertNotIn(prompt,result['args'])
        text,_=cli.chat('next',None,2,session_id='one')
        self.assertIn('resume',json.loads(text)['args'])
        self.assertFalse((self.root/'should-not-exist').exists())
        text,_=cli.chat('separate',None,2,session_id='two')
        self.assertNotIn('resume',json.loads(text)['args'])
    def test_timeout_and_truncation_never_produce_final_or_auto_resume(self):
        binary=self.executable("import time,sys\nsys.stdin.read()\nprint('{\"type\":\"thread.started\",\"thread_id\":\"one\"}',flush=True)\ntime.sleep(10)\n")
        cli=CliAdapter({'runtime':'codex','cwd':str(self.root),'binary':binary},self.store)
        start=time.monotonic()
        with self.assertRaises(ExecutionUnknown): cli.chat('hello',None,.2,session_id='one')
        self.assertLess(time.monotonic()-start,2)
        with self.assertRaises(ExecutionUnknown): cli.chat('must not execute',None,2,session_id='one')
        self.assertTrue(cli.state('one')['pending'])
    def test_single_connector_writer_is_enforced(self):
        with single_writer(self.root):
            with self.assertRaises(BlockingIOError):
                with single_writer(self.root):
                    self.fail('second connector acquired same state directory')
        with single_writer(self.root):
            pass
    def test_command_builders_resume_exact_sessions(self):
        for runtime in ['codex','claude_code','dsh']:
            cli=CliAdapter({'runtime':runtime,'cwd':str(self.root)},self.store)
            args=cli.argv({'upstream_session':'owned-thread'})
            self.assertIn('owned-thread',args)
            self.assertNotIn('--last',args); self.assertNotIn('--continue',args)
            self.assertNotIn('--dangerously-skip-permissions',args)


class OpenClawTests(unittest.TestCase):
    def test_terminal_and_session_mapping(self):
        requests=[]
        mode=['success']
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args): pass
            def do_POST(self):
                body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                requests.append(body)
                self.send_response(200); self.send_header('Content-Type','text/event-stream'); self.end_headers()
                packets=[{'choices':[{'index':0,'delta':{'content':'hello'},'finish_reason':None}]}]
                if mode[0]!='truncated': packets.append({'choices':[{'index':0,'delta':{},'finish_reason':'stop' if mode[0]=='success' else 'length'}]})
                for e in packets: self.wfile.write(('data: '+json.dumps(e)+'\n\n').encode())
                if mode[0]!='truncated': self.wfile.write(b'data: [DONE]\n\n')
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
        try:
            with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ,{'OPENCLAW_GATEWAY_TOKEN':'test-token'}):
                adapter=OpenClawAdapter({'runtime':'openclaw','base_url':f'http://127.0.0.1:{server.server_port}/v1'},AgentStore(Path(tmp)))
                self.assertEqual(adapter.chat('first',None,2,session_id='one')[0],'hello')
                adapter.chat('second',None,2,session_id='one')
                adapter.chat('third',None,2,session_id='two')
                self.assertEqual(requests[0]['user'],requests[1]['user'])
                self.assertNotEqual(requests[0]['user'],requests[2]['user'])
                adapter.control({'session_id':'one','operation':'new_session','command_seq':2})
                adapter.chat('new',None,2,session_id='one')
                self.assertNotEqual(requests[0]['user'],requests[3]['user'])
                for outcome in ['truncated','length']:
                    mode[0]=outcome
                    with self.assertRaises(ExecutionUnknown): adapter.chat('fail',None,2,session_id=outcome)
        finally:
            server.shutdown(); server.server_close(); thread.join()

if __name__=='__main__': unittest.main()
