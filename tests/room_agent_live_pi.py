#!/usr/bin/env python3
"""Opt-in REAL installed Pi test. Uses configured model credentials privately.

Two actors share a native Pi session and workspace. The model must actually
write an operator file, then the next actor reads it and invokes the sandbox
runner. This is a host C++ collaboration smoke test, NOT Ascend validation.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
import time
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'cannbot/scripts'))
from agent_router import AgentRouter,AgentStore
from pi_gateway import PiRpcClient
from room_agent import RoomPiFactory

def build_live_factory(root):
    settings=json.loads((Path.home()/'.pi-spark-agent/settings.json').read_text())
    env=dict(os.environ,PI_CODING_AGENT_DIR=str(Path.home()/'.pi-spark-agent'))
    env['PATH']=str(Path.home()/'.local/node/bin')+os.pathsep+env.get('PATH','')
    config=Path(env['PI_CODING_AGENT_DIR'])/'.env'
    if config.is_file():
        for line in config.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith('#') and '=' in line:
                key,value=line.split('=',1);env.setdefault(key.strip(),value.strip().strip('"'))
    prototype=PiRpcClient([str(Path.home()/'.local/node/bin/pi'),'--provider',settings['defaultProvider'],
                          '--model',settings['defaultModel']],ROOT,env,root/'unused')
    return prototype,RoomPiFactory(prototype,root/'shared')

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out',required=True)
    parser.add_argument('--provider')
    parser.add_argument('--model')
    args=parser.parse_args();start=time.monotonic()
    settings=json.loads((Path.home()/'.pi-spark-agent/settings.json').read_text())
    args.provider=args.provider or settings['defaultProvider']
    args.model=args.model or settings['defaultModel']
    result={'schema':'sparkpush.room_agent_live_pi.v1','ok':False,'model':args.model,
            'provider':args.provider,'target':'host C++ smoke; not Ascend NPU'}
    out=Path(args.out).resolve();out.mkdir(parents=True,exist_ok=True)
    env=dict(os.environ,PI_CODING_AGENT_DIR=str(Path.home()/'.pi-spark-agent'))
    env['PATH']=str(Path.home()/'.local/node/bin')+os.pathsep+env.get('PATH','')
    config=Path(env['PI_CODING_AGENT_DIR'])/'.env'
    if config.is_file():
        for line in config.read_text().splitlines():
            if line.strip() and not line.lstrip().startswith('#') and '=' in line:
                key,value=line.split('=',1);env.setdefault(key.strip(),value.strip().strip('"'))
    router=None
    try:
      with tempfile.TemporaryDirectory(prefix='spark-real-room-') as temp:
        root=Path(temp)
        runner=root/'runner.py'
        runner.write_text("import subprocess\nsubprocess.run(['g++','operator.cpp','-o','.operator-test'],check=True)\nsubprocess.run(['./.operator-test'],check=True)\nprint('host precision verified')\n")
        prototype=PiRpcClient([str(Path.home()/'.local/node/bin/pi'),'--provider',args.provider,'--model',args.model],ROOT,env,root/'unused')
        router=AgentRouter([{'id':'pi','name':'CANN','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':99}],
                           {'pi':prototype},AgentStore(root/'state'),bot_user_id=99)
        router.room_adapter=RoomPiFactory(prototype,root/'shared')
        scope=dict(room_id=1,task_id='a'*32,owner_user_id=7,actor_user_id=7,bot_user_id=99,can_develop=True)
        with __import__('unittest.mock',fromlist=['patch']).patch.dict(os.environ,{'SPARK_PUSH_ROOM_TEST_RUNNER':str(runner)}):
            router.authorize_room('r_1',scope)
            _,first=router.chat('本轮仅验证多人协作文件工具，不回答 CANN API 问题。请实际调用 write 工具创建 operator.cpp，内容严格为：int add(int a,int b){return a+b;}\nint main(){return add(2,3)==5?0:1;}\n；然后简短告诉下一位开发者文件已创建，不要调用测试。',
                               None,120,session_id='r_1',request_id='live-A')
            files=first['workspace_checkpoint']['files']
            if not any(f['path']=='operator.cpp' for f in files): raise AssertionError('model did not create requested operator file')
            print('live Pi: actor A created versioned source',flush=True)
            router.authorize_room('r_1',dict(scope,actor_user_id=8))
            _,second=router.chat('我是接手的用户 B。请实际调用 read 工具读取 operator.cpp，再调用 room_run_tests 工具执行管理员配置的测试。不得改文件；只根据真实测试回执简短报告结果。',
                                None,120,session_id='r_1',request_id='live-B')
            receipt=second['workspace_checkpoint']
            if not receipt['tests'] or not all(t['passed'] for t in receipt['tests']):
                raise AssertionError('model did not produce a successful actual test receipt')
            if first['agent_route']['session_key']!=second['agent_route']['session_key']:
                raise AssertionError('actors received different native sessions')
            if first['workspace_checkpoint']['code_revision']!=receipt['code_revision']:
                raise AssertionError('second actor unexpectedly changed source')
            result.update(ok=True,actor_ids=[first['actor_user_id'],second['actor_user_id']],
                          code_revision=receipt['code_revision'],files=receipt['files'],tests=receipt['tests'],shared_session=True)
    except Exception as error:
        result['error_type']=type(error).__name__
        # Classify runtime failures without serializing endpoint/error text.
        message=str(error).lower()
        result['error_categories']=[word for word in ('quota','insufficient','429','403','401','model','provider','tool','timeout','context','permission','connection','network','no api key') if word in message]
        result['error']=str(error) if isinstance(error,AssertionError) else 'real Pi execution failed; no credentials serialized'
    finally:
        if router: router.stop()
        result['duration_s']=time.monotonic()-start
        (out/'live-pi.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
        print(json.dumps({k:v for k,v in result.items() if k not in ('files','tests')},ensure_ascii=False))
    return 0 if result['ok'] else 1
if __name__=='__main__':sys.exit(main())
