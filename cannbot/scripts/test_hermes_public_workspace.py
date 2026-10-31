"""Contract tests for the Native integration seams; not real Hermes E2E."""
import asyncio
import json
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch

from hermes_public_workspace import public_api_adapter
from room_workspace import ProjectSnapshots, digest, execution_namespace


class Response:
    def __init__(self,value,status=200):self.body=json.dumps(value).encode();self.status=status


class Request:
    def __init__(self,scope,profile='spark-room-test-default'):
        namespace='hermes-native:'+scope['agent_id'];session=execution_namespace(scope)
        native='spark-im-'+digest([namespace,profile,session])
        self.body={'input':'read actual fixture','session_id':native+'-0'}
        self.headers={'X-Spark-Request-ID':scope['request_id'],'X-Hermes-Session-Key':native,
            'Idempotency-Key':'spk-'+digest([namespace,session,scope['request_id'],profile])}
    async def json(self):return self.body


class PublicNativeContract(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        root=Path(self.temp.name);source=root/'source';source.mkdir();(source/'a.cpp').write_text('int scoped() { return 42; }')
        self.snapshots=ProjectSnapshots({'schema':1,'projects':[{'id':'example','name':'Example','root':str(source),'paths':['a.cpp']}]},root/'snapshots')
        self.scopes={};self.reads=[];self.revoke=False;self.count=0;self.tool=None;test=self
        class Authority:
            def authorize(self,request_id,expected=None):
                test.count+=1
                scope=test.scopes.get(request_id)
                if not scope or (expected and scope!=expected):raise PermissionError('revoked')
                return scope
        self.authority=Authority()
        class Registry:
            def register(self,**entry):test.tool=entry['handler']
        def native_read(path,offset,limit,task_id):
            # Tool implementation is a contract double; snapshot I/O is real.
            test.reads.append(path)
            if test.revoke:test.scopes.clear()
            return json.dumps({'content':Path(path).read_text()})
        class Base:
            def _check_run_auth(self,request,permission):return None
            async def _handle_runs(self,request):
                agent=self._create_agent(session_id=request.body['session_id'])
                return Response(await asyncio.to_thread(agent.run_conversation))
            def _create_agent(self,session_id=None,**kwargs):
                return types.SimpleNamespace(valid_tool_names={'workspace_read','todo_list'},
                    run_conversation=lambda:json.loads(test.tool({'path':'a.cpp'})))
        self.patches=patch.dict('sys.modules',{
            'aiohttp':types.SimpleNamespace(web=types.SimpleNamespace(json_response=lambda v,status=200:Response(v,status))),
            'tools.registry':types.SimpleNamespace(registry=Registry()),
            'tools.file_tools':types.SimpleNamespace(READ_FILE_SCHEMA={'name':'read_file'},read_file_tool=native_read)})
        self.patches.start();self.addCleanup(self.patches.stop)
        self.api=public_api_adapter(Base,self.authority,self.snapshots,'hermes','spark-room-test-default')()

    def scope(self,room):
        scope=dict(kind='room_agent_v1',room_id=room,agent_id='hermes',generation=1,workspace_generation=1,
                   project_id='example',request_id='ra_'+digest(room),runtime='hermes_native',actor_user_id=7,policy='read_only')
        self.scopes[scope['request_id']]=scope;self.snapshots.prepare(scope);return scope

    def test_missing_context_denies_without_io(self):
        self.assertIn('error',json.loads(self.tool({'path':'a.cpp'})))
        self.assertEqual(self.reads,[])

    def test_threads_receive_their_own_request_snapshot(self):
        first,second=self.scope(1),self.scope(2)
        async def run():return await asyncio.gather(*(self.api._handle_runs(Request(s)) for s in (first,second)))
        results=asyncio.run(run())
        self.assertTrue(all(json.loads(r.body)['content'].startswith('int scoped') for r in results))
        self.assertEqual({Path(p).parent.parent.name for p in self.reads},{first['request_id'],second['request_id']})
        self.assertIn('error',json.loads(self.tool({'path':'a.cpp'}))) # no leaked context

    def test_scope_session_and_idempotency_are_bound(self):
        scope=self.scope(1)
        for field in ('X-Hermes-Session-Key','Idempotency-Key'):
            request=Request(scope);request.headers[field]='another-session'
            self.assertEqual(asyncio.run(self.api._handle_runs(request)).status,403)
        request=Request(scope);request.body['session_id']='another-room'
        self.assertEqual(asyncio.run(self.api._handle_runs(request)).status,403)
        request=Request(scope);request.body['workspace']='/private'
        self.assertEqual(asyncio.run(self.api._handle_runs(request)).status,403)
        self.assertEqual(self.reads,[])

    def test_revoke_during_read_drops_result(self):
        self.revoke=True;scope=self.scope(1)
        with self.assertRaises(PermissionError):asyncio.run(self.api._handle_runs(Request(scope)))
        self.assertEqual(len(self.reads),1)

if __name__=='__main__':unittest.main()
