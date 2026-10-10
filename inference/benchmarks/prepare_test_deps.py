#!/usr/bin/env python3
"""Start isolated local CUDA benchmark dependencies with a private generated secret."""
import argparse
import json
import os
import secrets
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--secret-file', type=Path, required=True)
    parser.add_argument('--stop', action='store_true', help='Stop containers, retain benchmark data')
    args = parser.parse_args()
    path = args.secret_file.resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        with os.fdopen(os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600), 'w') as f:
            json.dump({'mysql_password': secrets.token_hex(24)}, f)
    if path.stat().st_mode & 0o077: parser.error('secret-file must have permissions 0600')
    env = dict(os.environ, SPARK_CUDA_TEST_MYSQL_PASSWORD=json.loads(path.read_text())['mysql_password'])
    cmd = ['docker', 'compose', '-p', 'spark-cuda-bench', '-f',
           str(ROOT/'inference/benchmarks/docker-compose.cuda.yml')]
    # Stop instead of down: preserve benchmark DB and Kafka evidence, no volume deletion.
    subprocess.run(cmd + (['stop'] if args.stop else ['up', '-d']), env=env, check=True)

if __name__ == '__main__': main()
