"""Run Redis fixtures in an isolated owned Docker container; never flush shared Redis."""
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

root = Path(__file__).resolve().parents[1]
build = Path(sys.argv[1] if len(sys.argv) > 1 else root / "build-wsl").resolve()
name = "spark-review-redis-" + secrets.token_hex(6)
def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, **kwargs)
docker("image", "inspect", "redis:7.2-alpine", stdout=subprocess.DEVNULL)
try:
    docker("run", "--detach", "--name", name, "--label", "spark-review-test=true",
           "--publish", "127.0.0.1::6379", "redis:7.2-alpine", stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 20
    while subprocess.run(["docker", "exec", name, "redis-cli", "ping"],
                         stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode:
        if time.monotonic() >= deadline:
            raise RuntimeError("isolated Redis not ready")
        time.sleep(0.1)
    port = docker("port", name, "6379/tcp", capture_output=True, text=True).stdout.strip().rsplit(":", 1)[1]
    env = dict(os.environ, SPARK_PUSH_RUN_REDIS_TESTS="1", SPARK_PUSH_REDIS_HOST="127.0.0.1",
               SPARK_PUSH_REDIS_PORT=port, SPARK_PUSH_REDIS_PASSWORD="")
    subprocess.run(["ctest", "--test-dir", str(build), "-R", "(redis_sequence|route_generation)_integration_test",
                    "--output-on-failure"], env=env, check=True)
finally:
    subprocess.run(["docker", "rm", "--force", name], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
