"""Public service startup policy tests; these do not call a model."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
import types
import urllib.request
from http.server import BaseHTTPRequestHandler
from unittest.mock import patch

from public_runtime import PublicHome, clean_environment, model_environment, validate_native_policy, restrict_native_dotenv, wait_process_group
from public_agent_gateway import PublicGatewayServer, provision_models, validate_registry


class PublicRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.identity={'runtime':'public_gateway','id':'example'}

    def test_initialize_restart_and_refuse_different_identity(self):
        path=self.root/'public'
        with PublicHome(path,self.identity) as home:
            home.write_new('owned.json','{"preserved":true}')
            with self.assertRaises(FileExistsError):home.write_new('owned.json','overwrite')
        before={p.name:p.read_bytes() for p in path.iterdir()}
        with PublicHome(path,self.identity):pass
        self.assertEqual(before,{p.name:p.read_bytes() for p in path.iterdir()})
        with self.assertRaises(ValueError):
            with PublicHome(path,{**self.identity,'id':'another'}):pass

    def test_refuse_unknown_nonempty_and_symlink_homes(self):
        private=self.root/'private';private.mkdir();(private/'config.yaml').write_text('private: unchanged')
        with self.assertRaises(ValueError):
            with PublicHome(private,self.identity):pass
        self.assertEqual(list(private.iterdir()),[private/'config.yaml'])
        alias=self.root/'alias';alias.symlink_to(private,target_is_directory=True)
        with self.assertRaises(ValueError):PublicHome(alias,self.identity)

    def test_only_one_process_can_own_home(self):
        path=self.root/'service'
        code=('import json,sys\nfrom public_runtime import PublicHome\n'
              'with PublicHome(sys.argv[1],json.loads(sys.argv[2])):pass\n')
        def start():
            return subprocess.run([sys.executable,'-c',code,str(path),json.dumps(self.identity)],
                                  cwd=Path(__file__).parent,capture_output=True)
        with PublicHome(path,self.identity):self.assertNotEqual(start().returncode,0)
        self.assertEqual(start().returncode,0)

    def test_maintenance_lock_does_not_adopt_or_mutate_a_home(self):
        path=self.root/'maintenance'
        with self.assertRaises(ValueError):
            with PublicHome(path,self.identity,create=False):pass
        self.assertFalse(path.exists())
        path.mkdir();(path/'private.json').write_text('do not adopt')
        with self.assertRaises(ValueError):
            with PublicHome(path,self.identity,create=False):pass
        self.assertEqual([p.name for p in path.iterdir()],['private.json'])
        (path/'private.json').unlink()
        with PublicHome(path,self.identity):pass
        before={p.name:p.read_bytes() for p in path.iterdir()}
        with PublicHome(path,self.identity,create=False):
            with self.assertRaises(OSError):
                with PublicHome(path,self.identity,create=False):pass
        self.assertEqual(before,{p.name:p.read_bytes() for p in path.iterdir()})
        (path/'.spark-runtime.lock').unlink()
        with self.assertRaises(ValueError):
            with PublicHome(path,self.identity,create=False):pass
        self.assertFalse((path/'.spark-runtime.lock').exists())

    def test_shutdown_keeps_home_locked_until_http_handler_finishes(self):
        entered,finish,closing,closed=(threading.Event() for _ in range(4))
        path=self.root/'drained-home'
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):
                entered.set();finish.wait(10)
                (path/'settled').write_text('durable result')
                self.send_response(200);self.send_header('Content-Length','2');self.end_headers();self.wfile.write(b'ok')
        home=PublicHome(path,self.identity);home.__enter__()
        server=PublicGatewayServer(('127.0.0.1',0),Handler)
        listener=threading.Thread(target=server.serve_forever);listener.start()
        responses=[]
        def request():
            with urllib.request.urlopen('http://127.0.0.1:'+str(server.server_port),timeout=15) as response:
                responses.append(response.read())
        client=threading.Thread(target=request);client.start()
        def stop():
            server.shutdown();closing.set();server.server_close();home.__exit__();closed.set()
        stopper=None
        try:
            self.assertTrue(entered.wait(5))
            stopper=threading.Thread(target=stop);stopper.start()
            self.assertTrue(closing.wait(5));self.assertFalse(closed.wait(.2))
            self.assertEqual(home.state()['phase'],'running')
            with self.assertRaises(OSError):
                with PublicHome(path,self.identity,create=False):pass
            finish.set();self.assertTrue(closed.wait(5))
            with PublicHome(path,self.identity,create=False):
                self.assertEqual((path/'settled').read_text(),'durable result')
                home.require_clean()
            client.join(5);self.assertEqual(responses,[b'ok'])
        finally:
            finish.set();server.shutdown();server.server_close();home.__exit__()
            listener.join(5);client.join(5)
            if stopper:stopper.join(5)

    def test_unclean_shutdown_stays_latched_after_normal_restart(self):
        path=self.root/'failed-home'
        with self.assertRaises(RuntimeError):
            with PublicHome(path,self.identity):raise RuntimeError('cleanup failed')
        with PublicHome(path,self.identity,create=False) as home:
            with self.assertRaises(ValueError):home.require_clean()
        with PublicHome(path,self.identity):pass
        with PublicHome(path,self.identity,create=False) as home:
            self.assertEqual(home.state()['phase'],'clean')
            self.assertTrue(home.state()['unclean_latched'])
            with self.assertRaises(ValueError):home.require_clean()

    def test_old_home_without_lifecycle_proof_is_not_silently_cleaned(self):
        path=self.root/'old-home'
        with PublicHome(path,self.identity):pass
        (path/'.spark-runtime-state.json').unlink()
        with PublicHome(path,self.identity):pass
        with PublicHome(path,self.identity,create=False) as home:
            with self.assertRaises(ValueError):home.require_clean()

    def test_cold_maintenance_requires_process_exit_not_only_lock_release(self):
        path=self.root/'still-alive'
        with PublicHome(path,self.identity):pass
        with PublicHome(path,self.identity,create=False) as home:
            home.require_clean()
            with self.assertRaisesRegex(ValueError,'not exited'):home.require_stopped()

    def test_reboot_audit_is_explicit_and_does_not_rewrite_task_state(self):
        path=self.root/'audit'
        with self.assertRaises(RuntimeError):
            with PublicHome(path,self.identity) as home:
                home.write_new('task.json','{"status":"unknown","run_id":"keep"}')
                raise RuntimeError('unclean')
        before=(path/'task.json').read_bytes()
        with PublicHome(path,self.identity,create=False) as home:
            with self.assertRaises(ValueError):home.audit_after_reboot()
            # Contract-only boot transition. No actual host reboot is claimed.
            with patch('public_runtime.boot_identity',return_value='linux:later-test-boot'):
                value=home.audit_after_reboot()
                self.assertFalse(value['unclean_latched'])
                self.assertEqual(value['audit_method'],'native_os_reboot')
                home.require_stopped()
        self.assertEqual((path/'task.json').read_bytes(),before)

    def test_new_unclean_run_cannot_be_cleared_by_an_older_boot_transition(self):
        path=self.root/'two-boots'
        with patch('public_runtime.boot_identity',return_value='linux:first'):
            with self.assertRaises(RuntimeError):
                with PublicHome(path,self.identity):raise RuntimeError('first failure')
        with patch('public_runtime.boot_identity',return_value='linux:second'):
            # The second boot killed first-boot writers, but a new failure can
            # leave second-boot writers. Its evidence must replace the old one.
            with self.assertRaises(RuntimeError):
                with PublicHome(path,self.identity):raise RuntimeError('second failure')
            with PublicHome(path,self.identity,create=False) as home:
                self.assertEqual(home.state()['unclean_boot_id'],'linux:second')
                with self.assertRaises(ValueError):home.audit_after_reboot()

    def test_failed_latch_write_is_remembered_until_shutdown(self):
        path=self.root/'disk-failure'
        with PublicHome(path,self.identity) as home:
            with patch.object(home,'write_state',side_effect=OSError('disk unavailable')):
                with self.assertRaises(OSError):home.mark_unclean()
            self.assertFalse(home.state()['unclean_latched'])
        with PublicHome(path,self.identity,create=False) as home:
            self.assertTrue(home.state()['unclean_latched'])
            with self.assertRaises(ValueError):home.require_clean()

    def test_another_machine_cannot_relabel_state_on_start_or_audit(self):
        path=self.root/'machine-bound'
        with PublicHome(path,self.identity):pass
        before=(path/'.spark-runtime-state.json').read_bytes()
        with patch('public_runtime.machine_identity',return_value='different-installation'):
            with self.assertRaises(ValueError):
                with PublicHome(path,self.identity):pass
            with PublicHome(path,self.identity,create=False) as home:
                with self.assertRaises(ValueError):home.audit_after_reboot()
        self.assertEqual((path/'.spark-runtime-state.json').read_bytes(),before)

    def test_another_pid_namespace_cannot_treat_an_owner_as_exited(self):
        path=self.root/'namespace-bound'
        with PublicHome(path,self.identity):pass
        before=(path/'.spark-runtime-state.json').read_bytes()
        with patch('public_runtime.process_namespace',return_value='different-pid-namespace'):
            with self.assertRaises(ValueError):
                with PublicHome(path,self.identity):pass
            with PublicHome(path,self.identity,create=False) as home:
                with self.assertRaises(ValueError):home.require_stopped()
        self.assertEqual((path/'.spark-runtime-state.json').read_bytes(),before)

    def test_group_verification_fails_closed_when_processes_are_unreadable(self):
        with patch('public_runtime.Path.iterdir',side_effect=PermissionError('hidden process state')):
            with self.assertRaises(PermissionError):wait_process_group(12345)

    def test_clean_environment_and_explicit_model_allowlist(self):
        with patch.dict(os.environ,{'PRIVATE_TOKEN':'private','OPENAI_API_KEY':'inherited',
                                    'NODE_OPTIONS':'unsafe','HERMES_HOME':'private','PYTHONPATH':'unsafe'}):
            env=clean_environment(self.root)
        self.assertFalse({'PRIVATE_TOKEN','OPENAI_API_KEY','NODE_OPTIONS','HERMES_HOME','PYTHONPATH'} & env.keys())
        path=self.root/'model.env'
        path.write_text('OPENAI_API_KEY=operator-value\nHERMES_HOME=/private\nHTTP_PROXY=http://proxy\n')
        self.assertEqual(model_environment(path),{'OPENAI_API_KEY':'operator-value'})
        self.assertEqual(model_environment(path,proxy=True)['HTTP_PROXY'],'http://proxy')

    def test_models_select_one_model_without_shell_credentials(self):
        models=self.root/'models.json'
        models.write_text(json.dumps({'providers':{'example':{'apiKey':'OPENAI_API_KEY','models':[{'id':'chosen'},{'id':'other'}]},
                                                   'private-provider':{'apiKey':'private','models':[{'id':'secret'}]}}}))
        pi={'models_file':str(models),'provider':'example','model':'chosen'}
        with PublicHome(self.root/'models-home',self.identity) as home:
            selected=json.loads(provision_models(home,pi).read_text())
            self.assertEqual(set(selected['providers']),{'example'})
            self.assertEqual(selected['providers']['example']['models'],[{'id':'chosen'}])
            changed=json.loads(models.read_text());changed['providers']['example']['apiKey']='!unsafe command'
            models.write_text(json.dumps(changed))
            with self.assertRaises(ValueError):provision_models(home,pi)

    def test_catalog_rejects_private_or_incomplete_native(self):
        entry={'id':'hermes','runtime':'hermes_native','owner_user_ids':[],'room_enabled':True,
               'room_policy':'read_only','room_public_profile':True,'profile':'spark-public-example',
               'room_workspace_contract':1,'native_interactive_approvals':False}
        def validate(value):validate_registry({'schema':1,'agents':[value]})
        validate(entry)
        for change in ({'owner_user_ids':[7]},{'room_policy':'write'},{'profile':'technical'},
                       {'room_public_profile':False},{'room_workspace_contract':0},
                       {'native_interactive_approvals':True},{'room_test_only':True}):
            with self.assertRaises(ValueError):validate({**entry,**change})

    def test_native_policy_refuses_extra_tools_memory_and_approval_bypass(self):
        value={'platform_toolsets':{'api_server':['todo','spark_workspace']},'tools':{'tool_search':{'enabled':'off'}},
               'memory':{'memory_enabled':False,'user_profile_enabled':False},'mcp_servers':{},
               'checkpoints':{'enabled':False},'approvals':{'mode':'manual'}}
        validate_native_policy(value)
        media=copy.deepcopy(value);media['platform_toolsets']['api_server'].append('spark_room_context')
        validate_native_policy(media,context_tools=True)
        with self.assertRaises(ValueError):validate_native_policy(media)
        with self.assertRaises(ValueError):validate_native_policy(value,context_tools=True)
        for key,changed in [('platform_toolsets',{'api_server':['todo','spark_workspace','terminal']}),
                            ('memory',{'memory_enabled':True,'user_profile_enabled':False}),
                            ('mcp_servers',{'private':{}}),('approvals',{'mode':'auto'})]:
            bad=copy.deepcopy(value);bad[key]=changed
            with self.assertRaises(ValueError):validate_native_policy(bad)

    def test_media_upgrade_reports_owned_policy_mismatch_without_rewriting(self):
        import contextlib
        import io
        from hermes_public_gateway import main
        config=self.root/'old-native.json'
        config.write_text(json.dumps({'platform_toolsets':{'api_server':['todo','spark_workspace']}}))
        original=config.read_bytes()
        launcher=self.root/'launcher.json';launcher.write_text(json.dumps({'room_context_contract':1}))
        def attempted_start(settings,**kwargs):
            self.assertEqual(settings['room_context_contract'],1)
            validate_native_policy(json.loads(config.read_text()),context_tools=True)
        output=io.StringIO()
        with patch('hermes_public_gateway.serve',side_effect=attempted_start), patch('sys.argv',['public','--config',str(launcher)]), contextlib.redirect_stdout(output):
            self.assertEqual(main(),1)
        self.assertEqual(config.read_bytes(),original)
        self.assertIn('toolsets mismatch',output.getvalue());self.assertIn('room_context_contract',output.getvalue())
        self.assertNotIn(str(self.root),output.getvalue())

    def test_native_dotenv_cannot_discover_source_or_another_profile(self):
        calls=[]
        loader=types.SimpleNamespace(load_hermes_dotenv=lambda **kw:calls.append(kw) or [])
        restrict_native_dotenv(loader,self.root)
        loader.load_hermes_dotenv(hermes_home=self.root,project_env=self.root/'private-source/.env')
        self.assertEqual(calls,[{'hermes_home':self.root,'project_env':None,'load_external_secrets':False}])
        with self.assertRaises(ValueError):loader.load_hermes_dotenv(hermes_home=self.root/'other')
        self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main()
