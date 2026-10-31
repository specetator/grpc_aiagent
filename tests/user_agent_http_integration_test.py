#!/usr/bin/env python3
"""Real Logic HTTP/curl -> Gateway -> Connector. No model, DB server or Kafka."""
from http.server import ThreadingHTTPServer
import http.client
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'cannbot/scripts'))
from agent_cli_adapter import SessionAdapter
from agent_connector import make_handler as connector_handler
from agent_router import AgentStore, load_router
from pi_gateway import GatewayState, make_handler as gateway_handler

class FakeRuntime(SessionAdapter):
    def __init__(self,store):
        super().__init__({'runtime':'codex'},store)
        self.calls=0
    def is_ready(self): return True
    def stop(self): pass
    def chat(self,message,on_delta,timeout_s,**kwargs):
        self.calls+=1
        if on_delta: on_delta('preview')
        return 'offline connector reply',{}

def serve(server):
    thread=threading.Thread(target=server.serve_forever,daemon=True); thread.start()
    return thread

def post(port,path,body,key):
    conn=http.client.HTTPConnection('127.0.0.1',port,timeout=12)
    conn.request('POST',path,json.dumps(body),{'Content-Type':'application/json','Authorization':'Bearer '+key})
    response=conn.getresponse(); status=response.status; raw=response.read(); conn.close()
    return status,raw

with tempfile.TemporaryDirectory() as directory:
    root=Path(directory)
    runtime=FakeRuntime(AgentStore(root/'connector'))
    connector=ThreadingHTTPServer(('127.0.0.1',0),connector_handler(runtime,'codex',runtime.store,'fake-connector-key'))
    connector_thread=serve(connector)
    url='http://127.0.0.1:'+str(connector.server_port)
    registry=root/'registry.json'
    registry.write_text(json.dumps({'schema':1,'agents':[{'id':'pi','name':'Pi','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000001}]}))
    gateway=process=None
    try:
        with patch.dict(os.environ,{'SPARK_PUSH_AGENT_SELF_SERVICE':'true','SPARK_PUSH_PI_WORKERS':'1','SPARK_PUSH_AGENT_ALLOWED_ORIGINS':url}):
            router=load_router(registry,runtime,root/'gateway')
            gateway=ThreadingHTTPServer(('127.0.0.1',0),gateway_handler(GatewayState(router,'test-gateway-key','pi')))
            gateway_thread=serve(gateway)
            with socket.socket() as listener:
                listener.bind(('127.0.0.1',0)); port=listener.getsockname()[1]
            process=subprocess.Popen([sys.argv[1],str(port),str(gateway.server_port)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            deadline=time.monotonic()+10
            while True:
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.2): break
                except OSError:
                    assert process.poll() is None and time.monotonic()<deadline
                    time.sleep(.03)
            def manage(body,key='probe'):
                status,raw=post(port,'/api/agent/connections',body,key)
                return status,json.loads(raw)
            assert manage({'action':'list'},'bad-login')[0]==401
            assert manage({'action':'list','actor_user_id':8})[1]['code']==400
            probe={'action':'probe','runtime':'auto','endpoint':url,'token':'fake-connector-key'}
            result=manage(probe)[1]
            assert result['code']==0 and result['data']['runtime']=='codex',result
            saved=manage({**probe,'action':'save','revision':''})[1]
            assert saved['code']==0,saved
            assert saved['data']['bot_user_id']==900000000302
            listing=manage({'action':'list'})[1]
            assert 'fake-connector-key' not in json.dumps(listing)
            assert next(row for row in listing['data']['connections'] if row['runtime']=='codex')['configured']
            assert runtime.calls==0,'probe or save unexpectedly ran a model'
            # Exercise the actual Bridge-facing protocol, including its final envelope.
            chat={'session_id':'s_7_900000000302','request_id':'offline-http-chain',
                'stream':True,'agent_events':True,'messages':[{'role':'user','content':'offline test'}]}
            status,wire=post(gateway.server_port,'/v1/chat/completions',chat,'test-gateway-key')
            assert status==200 and b'offline connector reply' in wire and b'[DONE]' in wire,wire
            assert runtime.calls==1
            status,replayed=post(gateway.server_port,'/v1/chat/completions',chat,'test-gateway-key')
            assert status==200 and b'[DONE]' in replayed and runtime.calls==1
            events,_=router.replay_events(chat['request_id'],-1)
            assert sum(e['type']=='assistant_final' for e in events)==1
            denied=post(gateway.server_port,'/v1/chat/completions',
                {**chat,'session_id':'s_8_900000000302','request_id':'other-user'},'test-gateway-key')
            assert denied[0]!=200 and runtime.calls==1
            deleted=manage({'action':'delete','runtime':'codex','revision':saved['data']['revision']})[1]
            assert deleted['code']==0 and not deleted['data']['configured']
            assert not router.adapters['customer-codex'].visible_to(7)
            assert router.store.get_turn(chat['request_id'])['status']=='completed'
            print('Real Logic auth/curl -> Gateway -> Connector integration passed; stream/replay/owner/delete verified')
    finally:
        if process:
            process.terminate()
            try: process.wait(timeout=3)
            except subprocess.TimeoutExpired: process.kill(); process.wait()
        if gateway:
            gateway.shutdown(); gateway.server_close(); gateway_thread.join()
        connector.shutdown(); connector.server_close(); connector_thread.join()
