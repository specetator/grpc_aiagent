#!/usr/bin/env python3
import io
import json
from pathlib import Path
import tempfile
import threading
import subprocess
import sys
import time
import unittest
from unittest.mock import patch
from http.server import ThreadingHTTPServer
from urllib.request import Request, urlopen

from agent_router import AgentRouter, AgentStore
from agent_events import agent_event, event_envelope
from hermes_adapter import HermesHttpAdapter
from pi_gateway import GatewayState, make_handler


class FakeAgent:
    def __init__(self):
        self.calls=[]
        self.ready=True
    def is_ready(self): return self.ready
    def chat(self,*args,**kwargs):
        self.calls.append((args,kwargs))
        return args[0],{}
    def control(self,req,timeout):
        self.calls.append(req)
        return agent_event('assistant_final',{'text':'ok'})


class RouterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.store=AgentStore(self.root/'state')
        self.pi,self.hermes=FakeAgent(),FakeAgent()
        self.entries=[{'id':'pi','name':'Pi','owner_user_ids':[]},
                      {'id':'hermes-technical','name':'Hermes','owner_user_ids':[3]}]
        self.adapters={'pi':self.pi,'hermes-technical':self.hermes}
        self.router=AgentRouter(self.entries,self.adapters,self.store)
        self.sid='s_3_900000000001'
    def tearDown(self): self.temp.cleanup()
    def test_context_compaction_keeps_tail_and_revision(self):
        self.store.append_context('ctx', 'user', 'one')
        self.store.append_context('ctx', 'assistant', 'two')
        state=self.store.compact_context('ctx', 'summary', keep_last=1)
        self.assertEqual(state['summary'], 'summary')
        self.assertEqual(state['recent_messages'], [{'role':'assistant','text':'two'}])
        self.assertEqual(state['context_revision'], 3)
    def test_canonical_route_is_stable_and_selection_is_sticky(self):
        first=self.router.describe_route(self.sid)
        self.assertEqual(first['mode'],'sticky')
        self.assertEqual(first['agent_id'],'pi')
        self.assertTrue(first['session_key'].startswith('agent:pi:spark_pc:'))
        self.select('hermes-technical')
        selected=self.router.describe_route(self.sid)
        self.assertEqual(selected['route_key'],first['route_key'])
        self.assertEqual(selected['agent_id'],'hermes-technical')
        self.assertNotEqual(selected['session_key'],first['session_key'])
        again=AgentRouter(self.entries,self.adapters,AgentStore(self.root/'state'))
        self.assertEqual(again.describe_route(self.sid)['agent_id'],'hermes-technical')
        self.assertNotEqual(again.describe_route('s_4_900000000001')['route_key'],first['route_key'])
    def test_first_message_policy_routes_once_then_sticks_until_manual_switch(self):
        entries=[{**self.entries[0], 'routing_keywords':['代码','CANN']},
                 {**self.entries[1], 'routing_keywords':['写作','论文']}]
        router=AgentRouter(entries,self.adapters,self.store)
        _,metadata=router.chat('请帮我写作一段说明',None,1,session_id=self.sid,
                               request_id='policy-turn-1')
        self.assertEqual(len(self.hermes.calls),1)
        self.assertEqual(metadata['agent_route']['selection_source'],'first_message_policy')
        router.chat('接下来分析 CANN 代码',None,1,session_id=self.sid,
                    request_id='policy-turn-2')
        self.assertEqual(len(self.hermes.calls),2)
        self.assertEqual(len(self.pi.calls),0)
        router.control({'session_id':self.sid,'operation':'set_agent',
                        'target_agent':'pi','command_seq':20})
        _,metadata=router.chat('继续',None,1,session_id=self.sid,
                               request_id='policy-turn-3')
        self.assertEqual(len(self.pi.calls),1)
        self.assertEqual(metadata['agent_route']['selection_source'],'manual')

    def test_turn_result_is_durable_and_duplicate_does_not_run_agent(self):
        text,metadata=self.router.chat('execute once',None,1,session_id=self.sid,
                                       request_id='request-once')
        self.assertEqual(len(self.pi.calls),1)
        replay_text,replay_metadata=self.router.chat('execute once',None,1,
            session_id=self.sid,request_id='request-once')
        self.assertEqual(replay_text,text)
        self.assertEqual(len(self.pi.calls),1)
        self.assertTrue(replay_metadata['turn_replayed'])
        self.assertEqual(metadata['agent_route']['route_key'],
                         replay_metadata['agent_route']['route_key'])
        with self.assertRaises(ValueError):
            self.router.chat('different input',None,1,session_id=self.sid,
                             request_id='request-once')
    def test_event_ledger_preserves_sequence_and_terminal_snapshot(self):
        route=self.router.describe_route(self.sid)
        for sequence,(kind,text,terminal) in enumerate([
                ('assistant_progress','working',False),
                ('assistant_delta','中文',False),
                ('assistant_final','中文完成',True)]):
            event=agent_event(kind,{'text':text},event_envelope(
                'request-events',route,sequence,terminal=terminal))
            self.assertTrue(self.router.record_event(event))
        events,truncated=self.router.replay_events('request-events',0)
        self.assertFalse(truncated)
        self.assertEqual([e['envelope']['sequence'] for e in events],[1,2])
        self.assertEqual(events[-1]['type'],'assistant_final')
        self.assertTrue(events[-1]['envelope']['terminal'])
    def select(self,key,seq=10):
        return self.router.control({'session_id':self.sid,'operation':'set_agent','target_agent':key,'command_seq':seq})
    def test_selection_persists_and_does_not_copy_history_or_preferences(self):
        self.router.chat('pi only',None,1,session_id=self.sid)
        self.select('hermes-technical')
        self.router.chat('hermes only',None,1,'pi-provider','pi-model',self.sid,999)
        self.assertEqual(self.hermes.calls[-1][0],('hermes only',None,self.hermes.calls[-1][0][2],None,None,self.sid,0))
        again=AgentRouter(self.entries,self.adapters,AgentStore(self.root/'state'))
        again.chat('resume hermes',None,1,session_id=self.sid)
        self.assertEqual(self.hermes.calls[-1][0][0],'resume hermes')
        self.select('pi',20)
        self.router.chat('resume pi',None,1,session_id=self.sid)
        self.assertEqual(len(self.pi.calls),2)
    def test_owner_catalog_and_authorization_fail_closed(self):
        other='s_4_900000000001'
        event=self.router.control({'session_id':other,'operation':'list_agents'})
        self.assertNotIn('Hermes',json.dumps(event))
        # Newly registered users can have snowflake IDs greater than the bot.
        high=self.router.control({'session_id':'s_900000000001_730000000000000001','operation':'list_agents'})
        self.assertEqual([a['value'] for a in high['data']['presentation']['actions']],['pi'])
        with self.assertRaises(ValueError):
            self.router.control({'session_id':other,'operation':'set_agent','target_agent':'hermes-technical','command_seq':10})
        for bad in ['s_3_4','../s_3_900000000001','s_03_900000000001',None]:
            with self.assertRaises(ValueError): self.router.control({'session_id':bad,'operation':'list_agents'})
    def test_duplicate_stale_and_unavailable_selection(self):
        self.select('hermes-technical')
        self.select('hermes-technical')
        with self.assertRaises(ValueError): self.select('pi',9)
        with self.assertRaises(ValueError): self.select('pi',10)
        self.pi.ready=False
        with self.assertRaises(ValueError): self.select('pi',11)
        self.assertEqual(self.store.get('selection',self.sid)['agent_id'],'hermes-technical')
        with self.assertRaises(ValueError):
            self.router.control({'session_id':self.sid,'operation':'new_session','command_seq':9})
    def test_same_session_busy_but_another_session_can_run(self):
        with self.router.locks.hold(self.sid,1):
            with self.assertRaises(TimeoutError):
                self.router.chat('blocked',None,0.01,session_id=self.sid)
            self.router.chat('other',None,1,session_id='s_4_900000000001')
        self.assertFalse(self.router.locks.entries)
    def test_control_strips_other_runtime_state(self):
        self.select('hermes-technical')
        self.router.control({'session_id':self.sid,'operation':'new_session','command_seq':11,
                             'provider':'pi','model':'wrong','context_start_seq':900})
        request=self.hermes.calls[-1]
        self.assertNotIn('provider',request)
        self.assertNotIn('model',request)
        self.assertEqual(request['context_start_seq'],0)

    def test_independent_contacts_pin_runtime_and_do_not_switch_current_dialog(self):
        entries=[{**entry,'bot_user_id':[900000000001,900000000101][i]} for i,entry in enumerate(self.entries)]
        router=AgentRouter(entries,self.adapters,self.store)
        self.store.put('selection',self.sid,{'agent_id':'hermes-technical','revision':500})
        router.chat('Pi history',None,1,session_id=self.sid)
        self.assertEqual(self.pi.calls[-1][0][0],'Pi history')
        menu=router.control({'session_id':self.sid,'operation':'set_agent','target_agent':'hermes-technical','command_seq':501})
        self.assertEqual(menu['data']['presentation']['actions'],[
            {'id':'agent_open','label':'Hermes','value':'900000000101'}])
        hermes_session='s_3_900000000101'
        router.chat('Hermes history',None,1,session_id=hermes_session)
        self.assertEqual(self.hermes.calls[-1][0][5],hermes_session)
        router.control({'session_id':hermes_session,'operation':'new_session','command_seq':10})
        self.assertEqual(self.hermes.calls[-1]['session_id'],hermes_session)
        self.assertEqual(len(self.pi.calls),1)
        with self.assertRaises(ValueError): router.chat('forbidden',None,1,session_id='s_4_900000000101')
        with self.assertRaises(ValueError): router.chat('bots cannot chat as a user',None,1,session_id='s_900000000001_900000000101')


class GatewayEnvelopeTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.agent=FakeAgent()
        self.router=AgentRouter([{'id':'pi','name':'Pi','owner_user_ids':[]}],
            {'pi':self.agent},AgentStore(Path(self.temp.name)/'state'))
        self.server=ThreadingHTTPServer(('127.0.0.1',0),
            make_handler(GatewayState(self.router,'fixture-key','fixture-model')))
        self.thread=threading.Thread(target=self.server.serve_forever,daemon=True)
        self.thread.start()
        self.url='http://127.0.0.1:'+str(self.server.server_port)
    def tearDown(self):
        self.server.shutdown(); self.server.server_close(); self.thread.join(2)
        self.temp.cleanup()
    def post(self,path,payload):
        request=Request(self.url+path,json.dumps(payload).encode(),headers={
            'Authorization':'Bearer fixture-key','Content-Type':'application/json'})
        with urlopen(request,timeout=5) as response:
            return response.read().decode()
    def post_to(self,port,path,payload):
        request=Request('http://127.0.0.1:'+str(port)+path,
                        json.dumps(payload).encode(),headers={
                            'Authorization':'Bearer fixture-key',
                            'Content-Type':'application/json'})
        with urlopen(request,timeout=5) as response:
            return response.read().decode()
    @staticmethod
    def agent_events(body):
        return [json.loads(line[6:]) for line in body.splitlines()
                if line.startswith('data: {') and
                json.loads(line[6:]).get('schema')=='sparkpush.agent_event.v1']
    def test_stream_envelope_replay_and_turn_idempotency(self):
        payload={'request_id':'gateway-request-1','session_id':'s_3_900000000001',
            'messages':[{'role':'user','content':'hello'}],
            'stream':True,'agent_events':True}
        first=self.agent_events(self.post('/v1/chat/completions',payload))
        self.assertEqual([event['envelope']['sequence'] for event in first],list(range(len(first))))
        self.assertEqual(first[-1]['type'],'assistant_final')
        self.assertTrue(first[-1]['envelope']['terminal'])
        self.assertEqual(first[-1]['envelope']['agent_id'],'pi')
        self.assertEqual(len(self.agent.calls),1)
        replay=json.loads(self.post('/v1/agent/events/replay',{
            'request_id':'gateway-request-1','after_sequence':-1}))
        self.assertEqual(replay['schema'],'sparkpush.agent_replay.v1')
        self.assertEqual([event['envelope']['sequence'] for event in replay['events']],
                         list(range(len(first))))
        second=self.agent_events(self.post('/v1/chat/completions',payload))
        self.assertEqual(len(self.agent.calls),1)
        self.assertTrue(all(event['envelope']['replayed'] for event in second))
        self.assertEqual(second[-1]['data']['text'],first[-1]['data']['text'])

    def test_gateway_resolves_first_message_policy_before_stream_headers(self):
        temp_state=Path(self.temp.name)/'aggregate-state'
        pi,hermes=FakeAgent(),FakeAgent()
        router=AgentRouter([
            {'id':'pi','name':'Pi','owner_user_ids':[], 'routing_keywords':['代码']},
            {'id':'hermes-technical','name':'Hermes','owner_user_ids':[],
             'routing_keywords':['写作']}],
            {'pi':pi,'hermes-technical':hermes},AgentStore(temp_state))
        server=ThreadingHTTPServer(('127.0.0.1',0),
            make_handler(GatewayState(router,'fixture-key','fixture-model')))
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            body=self.post_to(server.server_port,'/v1/chat/completions',{
                'request_id':'gateway-policy-1','session_id':'s_3_900000000001',
                'messages':[{'role':'user','content':'请写作一个短段落'}],
                'stream':True,'agent_events':True})
            events=self.agent_events(body)
            self.assertTrue(events)
            self.assertEqual(events[-1]['envelope']['agent_id'],'hermes-technical')
            self.assertEqual(len(hermes.calls),1)
            self.assertEqual(len(pi.calls),0)
        finally:
            server.shutdown(); server.server_close(); thread.join(2)

    def test_completed_turn_reconstructs_missing_terminal_event(self):
        payload={'request_id':'gateway-request-crash-window',
            'session_id':'s_3_900000000001',
            'messages':[{'role':'user','content':'recover final'}],
            'stream':True,'agent_events':True}
        first=self.agent_events(self.post('/v1/chat/completions',payload))
        self.assertTrue(first[-1]['envelope']['terminal'])
        with self.router.store.connect() as db:
            db.execute("DELETE FROM agent_events WHERE request_id=? AND terminal=1",
                       ('gateway-request-crash-window',))
        replayed=self.agent_events(self.post('/v1/chat/completions',payload))
        self.assertEqual(len(self.agent.calls),1)
        self.assertTrue(replayed[-1]['envelope']['terminal'])
        self.assertTrue(replayed[-1]['envelope']['replayed'])
        self.assertEqual(replayed[-1]['data']['text'],first[-1]['data']['text'])
        stored,_=self.router.replay_events('gateway-request-crash-window',-1)
        self.assertTrue(stored[-1]['envelope']['terminal'])


class Response(io.BytesIO):
    headers={}


class HermesTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        root=Path(self.temp.name)
        (root/'env').write_text('API_SERVER_KEY=test-private-key-not-exported\n')
        self.adapter=HermesHttpAdapter({'id':'hermes-technical','base_url':'http://localhost:8644/v1',
            'credential_env_file':str(root/'env'),'expected_model':'technical','profile':'technical',
            'provider_first':True},AgentStore(root/'state'))
        self.requests=[]
    def tearDown(self): self.temp.cleanup()
    def stream(self,finish='stop',done=True):
        events=[{'choices':[{'delta':{'content':'中文回复'}}]}, {'choices':[{'delta':{},'finish_reason':finish}]}]
        body='event: hermes.tool.progress\ndata: {"tool":"terminal","status":"start"}\n\n'
        body+=''.join('data: '+json.dumps(v,ensure_ascii=False)+'\n\n' for v in events)
        if done: body+='data: [DONE]\n\n'
        def respond(path,payload=None,headers=None,timeout=10):
            self.requests.append((path,payload,headers))
            return Response(body.encode())
        self.adapter._open=respond
    def test_stream_progress_final_and_session_namespace(self):
        self.stream()
        delta,progress=[],[]
        text,_=self.adapter.chat('hello',delta.append,3,session_id='s_3_900000000001',on_progress=progress.append)
        self.assertEqual(text,'中文回复')
        self.assertEqual(delta,['中文回复'])
        self.assertTrue(any('terminal' in item for item in progress))
        self.assertIn('正在连接',progress[0])
        headers=self.requests[-1][2]
        self.assertNotEqual(headers['X-Hermes-Session-Id'],headers['X-Hermes-Session-Key'])
        self.adapter.chat('other',None,3,session_id='s_4_900000000001')
        self.assertNotEqual(headers['X-Hermes-Session-Key'],self.requests[-1][2]['X-Hermes-Session-Key'])
        self.assertEqual(self.requests[-1][1]['messages'],[{'role':'user','content':'other'}])
    def test_partial_or_missing_done_is_not_success(self):
        for finish,done in [('error',True),('length',True),('stop',False)]:
            self.stream(finish,done)
            with self.assertRaises(RuntimeError): self.adapter.chat('x',None,3,session_id='a')
    def test_new_retry_are_scoped_and_idempotent(self):
        self.stream()
        self.adapter.chat('old',None,3,session_id='a')
        first=self.requests[-1][2]
        self.adapter.chat('',None,3,session_id='a',retry=True)
        self.assertEqual(self.requests[-1][1]['messages'][0]['content'],'old')
        new={'operation':'new_session','session_id':'a','command_seq':10}
        self.adapter.control(new)
        with self.assertRaises(ValueError): self.adapter.chat('',None,3,session_id='a',retry=True)
        self.adapter.chat('new',None,3,session_id='a')
        second=self.requests[-1][2]
        self.assertNotEqual(first['X-Hermes-Session-Id'],second['X-Hermes-Session-Id'])
        self.assertEqual(first['X-Hermes-Session-Key'],second['X-Hermes-Session-Key'])
        self.adapter.control(new)
        self.adapter.chat('',None,3,session_id='a',retry=True)
        self.assertEqual(self.requests[-1][1]['messages'][0]['content'],'new')
    def test_bad_model_and_unsupported_reasoning_do_not_mutate_profile(self):
        self.catalog_fixture()
        with self.assertRaises(ValueError):
            self.adapter.control({'session_id':'a','operation':'set_model','target_provider':'pi','target_model':'technical','command_seq':1})
        result=self.adapter.control({'session_id':'a','operation':'set_reasoning','level':'max'})
        self.assertIn('未修改',result['data']['text'])

    def catalog_fixture(self,ack=True):
        def respond(path,payload=None,headers=None,timeout=10):
            self.requests.append((path,payload,headers))
            if path=='/api/model/options':
                result={'provider':'custom:HYX','model':'real-model','providers':[
                    {'slug':'custom:hyx','authenticated':True,'models':['real-model','second-model']},
                    {'slug':'custom:botcf','name':'BotCF','authenticated':True,'models':['bot-model']},
                    {'slug':'locked','authenticated':False,'models':['not-available']} ]}
            elif path.endswith('/model'):
                result={'session_id':path.split('/')[3],
                        'runtime':{'provider':payload['provider'],'model':payload['model'],
                                   'model_lock':'accepted' if ack else 'rejected'}}
            else:
                result={}
            return Response(json.dumps(result).encode())
        self.adapter._open=respond

    def test_model_menu_cache_and_explicit_refresh(self):
        self.catalog_fixture()
        self.adapter.control({'session_id':'a','operation':'list_models'})
        self.adapter.control({'session_id':'a','operation':'list_models'})
        self.assertEqual(sum(p=='/api/model/options' for p,_,_ in self.requests),1)
        self.adapter.control({'session_id':'a','operation':'list_models','refresh':True})
        self.assertEqual(sum(p=='/api/model/options' for p,_,_ in self.requests),2)

    def test_legacy_channel_keeps_full_model_card(self):
        self.catalog_fixture()
        self.adapter.provider_first = False
        card = self.adapter.control({'session_id':'a','operation':'list_models'})['data']['presentation']
        self.assertEqual(len(card['models']), 3)
        self.assertNotIn('selected_provider', card)

    def test_real_catalog_selection_ack_and_new_preserves_model(self):
        self.catalog_fixture()
        event=self.adapter.control({'session_id':'a','operation':'list_models'})
        card=event['data']['presentation']
        self.assertEqual(card['models'],[])
        self.assertEqual(len(card['providers']),2)
        self.assertTrue(any(item['name']=='BotCF' for item in card['providers']))
        self.assertEqual(card['current']['id'],'real-model')
        self.assertNotIn(':',card['current']['provider'])
        event=self.adapter.control({'session_id':'a','operation':'list_models',
                                    'provider_filter':card['current']['provider']})
        card=event['data']['presentation']
        self.assertEqual(len(card['models']),2)
        self.assertEqual(card['selected_provider'],card['current']['provider'])
        model=card['models'][1]
        req={'session_id':'a','operation':'set_model','target_provider':model['provider'],
             'target_model':model['id'],'command_seq':2}
        event=self.adapter.control(req)
        self.assertIn('已切换',event['data']['text'])
        self.assertEqual(self.requests[-1][1]['provider'],'custom:hyx')
        calls=len(self.requests)
        self.adapter.control(req)
        self.assertEqual(len(self.requests),calls,'duplicate set repeated upstream write')
        with self.assertRaises(ValueError):
            self.adapter.control(dict(req,command_seq=1))
        self.adapter.control({'session_id':'a','operation':'new_session','command_seq':3})
        state=self.adapter._state('a')
        self.assertEqual(state['selection']['public']['id'],'second-model')
        self.assertNotIn('upstream_session',state)
        self.assertNotIn('selection',self.adapter._state('b'))
        with self.assertRaises(ValueError): self.adapter.control(req)

    def test_rejected_model_ack_does_not_report_success(self):
        self.catalog_fixture(ack=False)
        model=self.adapter.available_models()[0]
        with self.assertRaises(RuntimeError):
            self.adapter.control({'session_id':'a','operation':'set_model','command_seq':1,
                'target_provider':model['provider'],'target_model':model['id']})
        self.assertNotIn('selection',self.adapter._state('a'))

    def test_restart_preview_replay_and_uncertain_outcome(self):
        self.adapter.is_ready=lambda:True
        with patch('hermes_restart.restart_technical') as restart:
            event=self.adapter.control({'session_id':'a','operation':'restart'})
            self.assertIn('所有渠道',event['data']['text'])
            restart.assert_not_called()
            req={'session_id':'a','operation':'restart_agent','command_seq':4}
            event=self.adapter.control(req)
            self.assertIn('已重启',event['data']['text'])
            self.adapter.control(req)
            restart.assert_called_once()
            restart.side_effect=TimeoutError()
            req['command_seq']=5
            event=self.adapter.control(req)
            self.assertIn('尚未确认',event['data']['text'])
            self.adapter.control(req)
            self.assertEqual(restart.call_count,2)

    def test_restart_waits_for_http_after_process_is_running(self):
        readiness=iter([False,False,True])
        self.adapter.is_ready=lambda:next(readiness)
        with patch('hermes_restart.restart_technical'), patch('hermes_adapter.time.sleep'):
            event=self.adapter.control({'session_id':'a','operation':'restart_agent','command_seq':1})
        self.assertIn('已重启并通过',event['data']['text'])

    def test_selected_model_is_explicit_on_every_turn_and_retry(self):
        self.catalog_fixture()
        model=self.adapter.available_models()[0]
        self.adapter.control({'session_id':'a','operation':'set_model','command_seq':1,
            'target_provider':model['provider'],'target_model':model['id']})
        self.stream()
        self.adapter.chat('test',None,3,session_id='a')
        self.adapter.chat('',None,3,session_id='a',retry=True)
        for path,payload,headers in self.requests[-2:]:
            self.assertEqual(path,'/chat/completions')
            self.assertEqual(payload['provider'],'custom:hyx')
            self.assertEqual(payload['model'],'real-model')
        self.adapter.control({'session_id':'a','operation':'new_session','command_seq':2})
        self.adapter.chat('new',None,3,session_id='a')
        self.assertEqual(self.requests[-1][1]['provider'],'custom:hyx')

    def fake_relay(self):
        code = """
import sys,json,base64,os
for line in sys.stdin:
 req=json.loads(line); body=req.get('payload') or {}; marker=body.get('marker','')
 print(json.dumps({'headers':{'X-Hermes-Session-Id':marker}}),flush=True)
 if body.get('mode')=='error':
  print(json.dumps({'error':'upstream failed','status':503}),flush=True)
  break
 print(json.dumps({'body':base64.b64encode(marker.encode()).decode()}),flush=True)
 if body.get('mode')=='partial':
  print(json.dumps({'body':base64.b64encode(b'late data').decode()}),flush=True)
 print(json.dumps({'eof':True}),flush=True)
"""
        real_popen=subprocess.Popen
        children=[]
        def spawn(argv,**kwargs):
            proc=real_popen([sys.executable,'-u','-c',code],**kwargs)
            children.append(proc)
            return proc
        return patch('windows_agent_transport.subprocess.Popen',side_effect=spawn),children

    def test_pool_reuses_only_complete_responses_and_keeps_sessions_separate(self):
        from windows_agent_transport import WindowsRelayPool,WindowsResponse
        mock,children=self.fake_relay()
        with mock:
            pool=WindowsRelayPool('fake',2)
            try:
                for marker in ['session-a','session-b']:
                    with WindowsResponse('fake','unused',{'marker':marker},{},3,pool=pool) as r:
                        self.assertEqual(r.headers['X-Hermes-Session-Id'],marker)
                        self.assertEqual(r.read(100),marker.encode())
                self.assertEqual(len(children),1)
                self.assertIsNone(children[0].poll())
                with WindowsResponse('fake','unused',{'marker':'partial','mode':'partial'},{},3,pool=pool) as r:
                    self.assertEqual(r.read(100),b'partial')
                self.assertIsNotNone(children[0].poll(),'partially consumed worker was reused')
                with WindowsResponse('fake','unused',{'marker':'fresh'},{},3,pool=pool) as r:
                    self.assertEqual(r.read(100),b'fresh')
                self.assertEqual(len(children),2)
            finally: pool.close()
            self.assertTrue(all(p.poll() is not None for p in children))

    def test_pool_is_bounded_and_does_not_replay_failed_post(self):
        from windows_agent_transport import WindowsRelayPool,WindowsResponse
        mock,children=self.fake_relay()
        with mock:
            pool=WindowsRelayPool('fake',2)
            try:
                first=WindowsResponse('fake','unused',{'marker':'a'},{},3,pool=pool)
                second=WindowsResponse('fake','unused',{'marker':'b'},{},3,pool=pool)
                with self.assertRaises(TimeoutError):
                    WindowsResponse('fake','unused',{'marker':'waiting'},{},0.05,pool=pool)
                self.assertEqual(len(children),2)
                for response,marker in [(second,b'b'),(first,b'a')]:
                    self.assertEqual(response.read(100),marker)
                    response.close()
                with self.assertRaises(RuntimeError):
                    with WindowsResponse('fake','unused',{'mode':'error'},{},3,pool=pool) as r:
                        r.read(100)
                self.assertEqual(len(children),2,'failed POST was automatically replayed')
                self.assertEqual(len(pool.workers),1)
            finally: pool.close()

    def test_windows_relay_blocked_stdin_obeys_deadline_and_reaps_process(self):
        from windows_agent_transport import WindowsResponse
        real_popen=subprocess.Popen
        children=[]
        def hung_relay(argv,**kwargs):
            proc=real_popen([sys.executable,'-c','import time; time.sleep(60)'],**kwargs)
            children.append(proc)
            return proc
        started=time.monotonic()
        with patch('windows_agent_transport.subprocess.Popen',side_effect=hung_relay):
            with self.assertRaises(TimeoutError):
                WindowsResponse('fake','http://127.0.0.1:8644/v1/chat/completions',
                                {'text':'x'*1024*1024},{},0.1)
        self.assertLess(time.monotonic()-started,5)
        self.assertIsNotNone(children[0].poll())


if __name__=='__main__': unittest.main()
