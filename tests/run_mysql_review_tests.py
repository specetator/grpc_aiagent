"""Run destructive SQL fixtures only in a fresh, owned, ephemeral Docker container."""
import os
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[1]
build = Path(sys.argv[1] if len(sys.argv) > 1 else root / "build-wsl").resolve()
name = "spark-review-mysql-" + secrets.token_hex(6)
password = secrets.token_hex(32)
def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, **kwargs)
docker("image", "inspect", "mysql:8.0", stdout=subprocess.DEVNULL)
with tempfile.TemporaryDirectory(prefix="spark-review-mysql-") as temp:
    envfile = Path(temp) / "environment"
    envfile.write_text(f"MYSQL_ROOT_PASSWORD={password}\nMYSQL_ROOT_HOST=%\nMYSQL_DATABASE=spark_review_test\n")
    envfile.chmod(0o600)
    try:
        docker("run", "--detach", "--name", name, "--label", "spark-review-test=true",
               "--tmpfs", "/var/lib/mysql", "--publish", "127.0.0.1::3306",
               "--env-file", str(envfile), "mysql:8.0", stdout=subprocess.DEVNULL)
        deadline = time.monotonic() + 90
        while True:
            result = subprocess.run(["docker", "exec", name, "sh", "-c",
                'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" mysql -uroot -e "SELECT 1"'],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            if result.returncode == 0:
                break
            if time.monotonic() >= deadline:
                raise RuntimeError("isolated MySQL did not become ready")
            time.sleep(0.5)
        # schema.sql names spark_im explicitly; use only the isolated test database here.
        schema = (root / "sql/schema.sql").read_text()
        schema = schema.replace("spark_push", "spark_review_test")
        docker("exec", "-i", name, "sh", "-c",
               'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot spark_review_test',
               input=schema, text=True, stdout=subprocess.DEVNULL)
        port = docker("port", name, "3306/tcp", capture_output=True, text=True).stdout.strip().rsplit(":", 1)[1]
        env = dict(os.environ, SPARK_PUSH_RUN_MYSQL_TESTS="1", SPARK_TEST_MYSQL_PORT=port,
                   SPARK_TEST_MYSQL_PASSWORD=password)
        subprocess.run(["ctest", "--test-dir", str(build), "-R", "message_reservation_integration_test",
                        "--output-on-failure"], env=env, check=True)
    finally:
        subprocess.run(["docker", "rm", "--force", name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
