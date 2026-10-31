import hashlib,json,os,tempfile,threading,time,unittest
from pathlib import Path
from unittest.mock import patch
from agent_router import AgentRouter,AgentStore
from room_agent import PublicRoomPiFactory


def scope(room=1,agent='public-pi',actor=7,seq=1):
    rid='ra_'+hashlib.sha256(json.dumps(['room-agent-v1',f'r_{room}-{seq}',agent],separators=(',',':')).encode()).hexdigest()
    return dict(kind='room_agent_v1',room_id=room,agent_id=agent,owner_user_id=7,actor_user_id=actor,
                bot_user_id=101 if agent=='public-pi' else 102,generation=1,policy='read_only',runtime='pi_rpc',
                source_msg_id=f'r_{room}-{seq}',request_id=rid)

class Factory:
    def __init__(self):self.calls=[];self.active=0;self.peak=0;self.sessions={}
    def is_ready(self):return True
    def for_scope(self,scope):
        factory=self
        class Turn:
            def chat(self,message,on_delta,timeout,*args,**kwargs):
                session=args[2];factory.active+=1;factory.peak=max(factory.peak,factory.active)
                factory.calls.append((dict(scope),session))
                if getattr(factory,"barrier",None):factory.barrier.wait(timeout=3)
                time.sleep(.03)
                history=factory.sessions.setdefault(session,[]);history.append(message)
                text='|'.join(history)
                if on_delta:on_delta(text)
                factory.active-=1
                return text,{}
        return Turn()

class GenericRoomTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.root=Path(self.tmp.name);self.factory=Factory()
        self.entries=[dict(id=key,name=name,runtime='pi_rpc',bot_user_id=bot,owner_user_ids=[],room_enabled=True,room_policy='read_only')
            for key,name,bot in [('public-pi','Pi',101),('second-pi','Pi',102)]]
        self.router=self.make_router()
    def make_router(self):
        router=AgentRouter(self.entries,{e['id']:self.factory for e in self.entries},AgentStore(self.root/'state'),default='public-pi',bot_user_id=101)
        router.public_room_adapter=self.factory
        return router
    def call(self,sc,text='hello'):
        self.router.authorize_room(f"r_{sc['room_id']}",sc,sc['request_id'])
        return self.router.chat(text,None,5,session_id=f"r_{sc['room_id']}",request_id=sc['request_id'])
    def tearDown(self):self.tmp.cleanup()
    def test_two_members_share_one_agent_session(self):
        self.call(scope(), 'A-context');text,_=self.call(scope(actor=8,seq=2),'B-context')
        self.assertIn('A-context|B-context',text)
        self.assertEqual({s for _,s in self.factory.calls},{'roomagent_1_public-pi'})
    def test_agents_and_rooms_isolated(self):
        first=self.call(scope(),'secret-room-one')[1]['agent_route']
        second=self.call(scope(agent='second-pi'),'second')[1]['agent_route']
        text,other=self.call(scope(room=2),'other-room')
        self.assertNotIn('secret-room-one',text)
        self.assertEqual(first['conversation_id'],second['conversation_id'])
        self.assertNotEqual(first['route_key'],second['route_key'])
        self.assertEqual(len(self.factory.sessions),3)
    def test_replay_restart_no_execution(self):
        sc=scope();self.call(sc);self.router=self.make_router()
        _,meta=self.call(sc);self.assertTrue(meta['turn_replayed']);self.assertEqual(len(self.factory.calls),1)
    def test_unknown_after_restart_is_not_reexecuted(self):
        sc=scope();self.router.authorize_room('r_1',sc,sc['request_id'])
        route=self.router.describe_route('r_1')
        self.router.store.begin_turn(sc['request_id'],route['route_key'],'unknown-input')
        self.router=self.make_router()
        with self.assertRaises((RuntimeError,ValueError)):self.call(sc)
        self.assertEqual(self.factory.calls,[])
    def test_conflicting_actor_and_prompt_rejected(self):
        sc=scope();self.call(sc)
        with self.assertRaises(ValueError):self.call(dict(sc,actor_user_id=8))
        with self.assertRaises(ValueError):self.call(sc,'different')
    def test_status_and_unavailable_controls(self):
        sc=scope();self.call(sc)
        request=dict(session_id='r_1',request_id=sc['request_id'],collaboration=sc,operation='status')
        self.assertEqual(self.router.room_control(request)['data']['status'],'completed')
        self.assertEqual(self.router.room_control({**request,'operation':'stop'})['code'],501)
    def test_no_private_contact_fallback(self):
        with self.assertRaises(ValueError):self.router.chat('hello',None,1,session_id='s_7_101',request_id='private')
        self.router.public_room_adapter=None
        with self.assertRaises(ValueError):self.router.authorize_room('r_1',scope())
    def test_scope_forgery(self):
        for change in [dict(agent_id='second-pi'),dict(policy='write'),dict(room_id=2),dict(runtime='hermes_native'),dict(generation=0)]:
            with self.assertRaises(ValueError):self.router.authorize_room('r_1',{**scope(),**change})
        with self.assertRaises(ValueError):self.router.authorize_room('r_1',scope(),'wrong-request')
    def test_two_agents_can_run_independently(self):
        self.factory.barrier=threading.Barrier(2)
        errors=[]
        def run(sc):
            try:self.call(sc)
            except Exception as exc:errors.append(exc)
        threads=[threading.Thread(target=run,args=(scope(agent=a),)) for a in ['public-pi','second-pi']]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertFalse(errors);self.assertEqual(self.factory.peak,2)
    def test_public_process_has_no_private_env_or_tools(self):
        made=[]
        class Pi:
            def __init__(self,argv,cwd,env,session_dir):self.argv,self.cwd,self.env,self.session_dir=argv,cwd,env,session_dir;made.append(self)
            def start(self):pass
            def stop(self):pass
            def chat(self,*args,**kwargs):return 'ok',{}
        private=self.root/'private';private.mkdir()
        prototype=Pi(['/fixed/pi','--provider','test','--model','test-model'],private,
                     {'PATH':'/usr/bin','PI_CODING_AGENT_DIR':str(private),'PRIVATE_TOKEN':'secret','NODE_OPTIONS':'malicious'},private/'sessions')
        with patch.dict(os.environ,{},clear=True):factory=PublicRoomPiFactory(prototype,self.root/'public')
        sc=scope();factory.for_scope(sc).chat('hello',None,1,session_id='roomagent_1_public-pi',request_id=sc['request_id'])
        worker=made[-1]
        self.assertNotIn('PRIVATE_TOKEN',worker.env);self.assertNotIn('NODE_OPTIONS',worker.env)
        self.assertIn('--no-tools',worker.argv);self.assertIn('--no-extensions',worker.argv);self.assertIn('--no-skills',worker.argv)
        self.assertNotEqual(worker.cwd,private);self.assertNotEqual(worker.session_dir,prototype.session_dir)
        with self.assertRaises(ValueError):PublicRoomPiFactory(prototype,private/'public')

if __name__=='__main__':unittest.main()
