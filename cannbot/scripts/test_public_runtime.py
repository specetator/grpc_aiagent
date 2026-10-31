"""Public service startup policy tests; these do not call a model."""
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
import types
from unittest.mock import patch

from public_runtime import PublicHome, clean_environment, model_environment, validate_native_policy, restrict_native_dotenv
from public_agent_gateway import provision_models, validate_registry


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
        for key,changed in [('platform_toolsets',{'api_server':['todo','spark_workspace','terminal']}),
                            ('memory',{'memory_enabled':True,'user_profile_enabled':False}),
                            ('mcp_servers',{'private':{}}),('approvals',{'mode':'auto'})]:
            bad=copy.deepcopy(value);bad[key]=changed
            with self.assertRaises(ValueError):validate_native_policy(bad)

    def test_native_dotenv_cannot_discover_source_or_another_profile(self):
        calls=[]
        loader=types.SimpleNamespace(load_hermes_dotenv=lambda **kw:calls.append(kw) or [])
        restrict_native_dotenv(loader,self.root)
        loader.load_hermes_dotenv(hermes_home=self.root,project_env=self.root/'private-source/.env')
        self.assertEqual(calls,[{'hermes_home':self.root,'project_env':None,'load_external_secrets':False}])
        with self.assertRaises(ValueError):loader.load_hermes_dotenv(hermes_home=self.root/'other')
        self.assertEqual(len(calls),1)


if __name__=='__main__':unittest.main()
