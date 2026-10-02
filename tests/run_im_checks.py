"""Run required checks with opt-in isolated SQL/Redis integration tests."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--build',default='/tmp/spark-im-build')
    parser.add_argument('--manifest',required=True)
    parser.add_argument('--output',default='tests/performance/results/checks.txt')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    manifest=json.loads(Path(args.manifest).read_text())
    env=os.environ.copy()
    env.update(SPARK_PUSH_RUN_MYSQL_TESTS='1',SPARK_PUSH_MYSQL_TEST_DB='spark_im_dao_test_20261002',
        SPARK_PUSH_MYSQL_USER=manifest['mysql']['user'],SPARK_PUSH_MYSQL_PASSWORD=manifest['mysql']['password'],
        SPARK_PUSH_RUN_REDIS_TESTS='1')
    out=root/args.output;out.parent.mkdir(parents=True,exist_ok=True)
    commands=[['cmake','--build',args.build,'-j4'],
        ['python3','cannbot/scripts/validate_knowledge.py','--strict'],
        ['python3','cannbot/scripts/test_pi_citations.py']]
    # Validate the distributable skill package in a disposable agent home.
    # The user's installed Pi configuration is never overwritten by this test.
    with tempfile.TemporaryDirectory(prefix='spark-pi-check-') as pi:
        commands.extend([
            ['python3','cannbot/scripts/sync_pi_agent.py','--install','--pi-home',pi],
            ['python3','cannbot/scripts/sync_pi_agent.py','--check','--pi-home',pi],
            ['ctest','--test-dir',args.build,'--output-on-failure']])
        with out.open('w') as log:
            for command in commands:
                log.write('$ '+' '.join(command)+'\n');log.flush()
                result=subprocess.run(command,cwd=root,env=env,stdout=log,stderr=subprocess.STDOUT)
                log.write('exit_code='+str(result.returncode)+'\n');log.flush()
                print(command[0], 'exit_code',result.returncode,flush=True)
                if result.returncode: raise SystemExit(result.returncode)
    print('All required build, knowledge-package and integration checks passed')

if __name__=='__main__': main()
