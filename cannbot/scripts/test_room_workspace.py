"""Filesystem and authority contract tests; no model calls or claimed E2E."""
import json
import os
from pathlib import Path
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

from room_workspace import ProjectSnapshots, WorkspaceAuthority, execution_namespace, INDEX


def scope(**changes):
    return dict(kind='room_agent_v1',room_id=1,agent_id='public-pi',generation=1,
                workspace_generation=1,project_id='sample',actor_user_id=7,
                policy='read_only',request_id='ra_'+'a'*64,**changes)


class SnapshotsTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.root=Path(self.tmp.name)
        self.source=self.root/'source';self.source.mkdir()
        (self.source/'src').mkdir()
        (self.source/'src/queue.cpp').write_text('int pending() { return 3; }\n')
        self.catalog={'schema':1,'projects':[{'id':'sample','name':'Sample','root':str(self.source),'paths':['src']}]}
        self.store=ProjectSnapshots(self.catalog,self.root/'snapshots')
    def tearDown(self):self.tmp.cleanup()
    def test_real_file_and_manifest(self):
        snapshot=self.store.prepare(scope())
        self.assertEqual(snapshot.file('src/queue.cpp').read_text(),'int pending() { return 3; }\n')
        self.assertIn('src/queue.cpp',snapshot.file(INDEX).read_text())
        self.assertEqual(len(snapshot.manifest['files']),1)
    def test_replay_keeps_original_source_revision(self):
        first=self.store.prepare(scope())
        (self.source/'src/queue.cpp').write_text('changed\n')
        second=self.store.prepare(scope())
        self.assertEqual(first.manifest,second.manifest)
        self.assertIn('pending',second.file('src/queue.cpp').read_text())
        with self.assertRaises(PermissionError):self.store.prepare({**scope(),'workspace_generation':2})
    def test_unapproved_hidden_secret_and_binary_suffix_omitted(self):
        (self.source/'outside.cpp').write_text('NOT_APPROVED')
        for name in ('.env','credentials.json','auth.json','project.local.conf','binary.bin'):
            (self.source/'src'/name).write_text('PRIVATE')
        snapshot=self.store.prepare(scope())
        self.assertEqual([row['path'] for row in snapshot.manifest['files']],['src/queue.cpp'])
    def test_path_escape_and_tamper_refused(self):
        snapshot=self.store.prepare(scope())
        for value in ('../outside.cpp','/etc/passwd','src/../../x','src/.env','src\\queue.cpp','src/C:foo','src//queue.cpp'):
            with self.assertRaises((ValueError,PermissionError)):snapshot.file(value)
        snapshot.file('src/queue.cpp').chmod(0o600)
        (snapshot.root/'src/queue.cpp').write_text('replaced')
        with self.assertRaises(PermissionError):snapshot.file('src/queue.cpp')
    def test_file_and_directory_symlinks_rejected(self):
        for directory in (False,True):
            link=self.source/'src/link'
            target=self.root/'outside';target.mkdir(exist_ok=True)
            (target/'secret.cpp').write_text('PRIVATE')
            link.symlink_to(target if directory else target/'secret.cpp',target_is_directory=directory)
            with self.assertRaises(PermissionError):self.store.prepare(scope())
            link.unlink()
        self.assertEqual(list((self.root/'snapshots').iterdir()),[])
    def test_source_root_symlink_rejected_and_temp_cleaned(self):
        alias=self.root/'alias';alias.symlink_to(self.source,target_is_directory=True)
        self.store.projects['sample']['root']=str(alias)
        with self.assertRaises(OSError):self.store.prepare(scope())
        self.assertEqual(list((self.root/'snapshots').iterdir()),[])
    def test_hardlink_and_fifo_rejected_without_blocking(self):
        link=self.source/'src/link.cpp'
        os.link(self.source/'src/queue.cpp',link)
        with self.assertRaises(PermissionError):self.store.prepare(scope())
        link.unlink();os.mkfifo(link)
        with self.assertRaises(PermissionError):self.store.prepare(scope())
    def test_swap_to_symlink_between_stat_and_open_rejected(self):
        target=self.root/'secret.cpp';target.write_text('PRIVATE')
        original=os.open
        def opening(path,flags,*args,**kwargs):
            if path=='queue.cpp':
                item=self.source/'src/queue.cpp';item.unlink();item.symlink_to(target)
            return original(path,flags,*args,**kwargs)
        with patch('room_workspace.os.open',opening):
            with self.assertRaises(OSError):self.store.prepare(scope())
        self.assertEqual(list((self.root/'snapshots').iterdir()),[])
    def test_limits_fail_without_partial_published_snapshot(self):
        with patch('room_workspace.MAX_FILE_BYTES',4):
            with self.assertRaises(PermissionError):self.store.prepare(scope())
        with patch('room_workspace.MAX_FILES',0):
            with self.assertRaises(ValueError):self.store.prepare(scope())
        self.assertEqual(list((self.root/'snapshots').iterdir()),[])
    def test_concurrent_prepare_keeps_one_immutable_snapshot(self):
        results=[];errors=[]
        def run():
            try:results.append(self.store.prepare(scope()).manifest)
            except Exception as e:errors.append(e)
        threads=[threading.Thread(target=run) for _ in range(3)]
        for t in threads:t.start()
        for t in threads:t.join()
        self.assertFalse(errors)
        self.assertEqual(len(results),3)
        self.assertTrue(all(item==results[0] for item in results))
        self.assertEqual(len(list((self.root/'snapshots').iterdir())),1)
    def test_scope_namespace_isolation_and_legacy_compatibility(self):
        value=scope();first=execution_namespace(value)
        for key,other in [('room_id',2),('agent_id','hermes'),('generation',2),('workspace_generation',2),('project_id','another')]:
            self.assertNotEqual(first,execution_namespace({**value,key:other}))
        del value['workspace_generation']
        self.assertEqual(execution_namespace(value),'roomagent_1_public-pi')


class AuthorityTest(unittest.TestCase):
    def test_trusted_id_only_and_revocation_fail_closed(self):
        requests=[];canonical=scope();revoked=[False]
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_POST(self):
                requests.append((self.path,self.headers.get('Authorization'),json.loads(self.rfile.read(int(self.headers['Content-Length'])))))
                self.send_response(200);self.end_headers()
                self.wfile.write(json.dumps({'code':403 if revoked[0] else 0,'data':{'request_id':canonical['request_id'],'collaboration':canonical}}).encode())
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        try:
            authority=WorkspaceAuthority('http://127.0.0.1:'+str(server.server_port),'k'*48)
            self.assertEqual(authority.authorize(canonical['request_id'],canonical),canonical)
            self.assertEqual(requests[0],('/internal/room-agent/authorize','Bearer '+'k'*48,{'request_id':canonical['request_id']}))
            with self.assertRaises(PermissionError):authority.authorize(canonical['request_id'],{**canonical,'actor_user_id':8})
            revoked[0]=True
            with self.assertRaises(PermissionError):authority.authorize(canonical['request_id'],canonical)
        finally:server.shutdown();server.server_close();thread.join()


if __name__=='__main__':unittest.main()
