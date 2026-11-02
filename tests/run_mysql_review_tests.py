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
worker_only = os.environ.get("SPARK_TEST_MYSQL_WORKER_ONLY") == "1"
if worker_only and os.environ.get("SPARK_TEST_MYSQL_CONTEXT_ONLY") == "1":
    raise ValueError("choose one focused MySQL suite")
name = "spark-review-mysql-" + secrets.token_hex(6)
redis_name = name + "-redis"
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
        # Exercise a real upgrade from the pre-media schema, retaining an old
        # attachment row, rather than only testing CREATE on a new database.
        schema = schema.split("-- Room media originals, resumable uploads and durable processing jobs.")[0]
        schema = schema.replace("`mime` VARCHAR(128)", "`mime` VARCHAR(64)").replace(
            "`bytes` BIGINT UNSIGNED NOT NULL", "`bytes` INT NOT NULL")
        docker("exec", "-i", name, "sh", "-c",
               'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot spark_review_test',
               input=schema, text=True, stdout=subprocess.DEVNULL)
        sql_command = 'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot --batch --skip-column-names spark_review_test'
        legacy_id = 'att_000000000000000000000001'
        docker("exec", "-i", name, "sh", "-c", sql_command,
               input=f"INSERT INTO attachment(id,owner_user_id,session_id,display_name,mime,bytes,sha256,storage_key,status) "
                     f"VALUES('{legacy_id}',1,'r_4242','legacy O''Reilly','image/png',3,REPEAT('a',64),'{legacy_id}','ready');",
               text=True, stdout=subprocess.DEVNULL)
        inspect = f"SELECT id,owner_user_id,session_id,display_name,mime,bytes,sha256,storage_key,status,created_at FROM attachment WHERE id='{legacy_id}';"
        before = docker("exec", "-i", name, "sh", "-c", sql_command,
                        input=inspect, text=True, capture_output=True).stdout
        migration = ((root / "sql/migrations/20261101_room_media.sql").read_text() + "\n" +
                     (root / "sql/migrations/20261101_media_processing.sql").read_text() + "\n" +
                     (root / "sql/migrations/20261101_media_artifacts.sql").read_text() + "\n" +
                     (root / "sql/migrations/20261101_media_publication.sql").read_text() + "\n" +
                     (root / "sql/migrations/20261101_media_context.sql").read_text() + "\n" +
                     (root / "sql/migrations/20261101_media_context_sources.sql").read_text())
        for _ in range(2):
            docker("exec", "-i", name, "sh", "-c", sql_command,
                   input=migration, text=True, stdout=subprocess.DEVNULL)
            if _ == 0:
                docker("exec", "-i", name, "sh", "-c", sql_command,
                       input="INSERT INTO room_context(room_id,generation) VALUES(9007199254740990,17); "
                             "INSERT INTO room_context_source(request_id,source_id,room_id,context_generation,reference) "
                             "VALUES(CONCAT('ra_',REPEAT('0',64)),CONCAT('src_',REPEAT('0',64)),9007199254740990,17,JSON_OBJECT('migration_probe',true));",
                       text=True, stdout=subprocess.DEVNULL)
        epoch = docker("exec", "-i", name, "sh", "-c", sql_command,
                       input="SELECT generation FROM room_context WHERE room_id=9007199254740990;",
                       text=True, capture_output=True).stdout.strip()
        if epoch != "17":
            raise RuntimeError("repeated context migration reset the existing epoch")
        print("Real MySQL: repeated context migration preserved the existing epoch", flush=True)
        preserved_source = docker("exec", "-i", name, "sh", "-c", sql_command,
                       input="SELECT COUNT(*) FROM room_context_source WHERE room_id=9007199254740990 "
                             "AND context_generation=17 AND JSON_EXTRACT(reference,'$.migration_probe')=true;",
                       text=True, capture_output=True).stdout.strip()
        if preserved_source != "1":
            raise RuntimeError("repeated source migration changed an existing receipt")
        print("Real MySQL: repeated source migration preserved the existing receipt", flush=True)
        after = docker("exec", "-i", name, "sh", "-c", sql_command,
                       input=inspect, text=True, capture_output=True).stdout
        if not before or before != after:
            raise RuntimeError("room media migration changed the legacy attachment record")
        print("Real MySQL: pre-media upgrade and repeated migration preserved the old attachment row", flush=True)
        port = docker("port", name, "3306/tcp", capture_output=True, text=True).stdout.strip().rsplit(":", 1)[1]
        env = dict(os.environ, SPARK_PUSH_RUN_MYSQL_TESTS="1", SPARK_TEST_MYSQL_PORT=port,
                   SPARK_TEST_MYSQL_PASSWORD=password, SPARK_TEST_MYSQL_CONTAINER=name)
        docker("run", "--detach", "--name", redis_name, "--label", "spark-review-test=true",
               "--publish", "127.0.0.1::6379", "redis:7.2-alpine", stdout=subprocess.DEVNULL)
        redis_port = docker("port", redis_name, "6379/tcp", capture_output=True, text=True).stdout.strip().rsplit(":", 1)[1]
        env["SPARK_TEST_REDIS_PORT"] = redis_port
        # Run on the fresh database: the following SQL fixtures intentionally use
        # different temporary blob roots and do not share an ingestion queue.
        if not worker_only:
            subprocess.run(["ctest", "--test-dir", str(build), "-R", "^media_context_http_test$",
                            "--output-on-failure", "--verbose"], env=env, check=True)
        if env.get("SPARK_TEST_MYSQL_CONTEXT_ONLY") != "1":
            if env.get("SPARK_TEST_MEDIA_WORKER_TEST") or worker_only:
                env["SPARK_TEST_MEDIA_WORKER_TEST"] = "1"
                subprocess.run(["ctest", "--test-dir", str(build), "-R", "^media_worker_http_test$",
                                "--output-on-failure"], env=env, check=True)
            elif env.get("SPARK_TEST_MEDIA_IMAGE"):
                subprocess.run(["ctest", "--test-dir", str(build), "-R", "^media_pipeline_http_test$",
                                "--output-on-failure"], env=env, check=True)
            if not worker_only:
                subprocess.run(["ctest", "--test-dir", str(build), "-R", "^media_processing_http_test$",
                                "--output-on-failure"], env=env, check=True)
                subprocess.run(["ctest", "--test-dir", str(build), "-R", "message_reservation_integration_test|space_mysql_integration_test|media_mysql_integration_test|media_artifact_mysql_test",
                                "--output-on-failure", "--verbose"], env=env, check=True)
    finally:
        subprocess.run(["docker", "rm", "--force", redis_name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
        subprocess.run(["docker", "rm", "--force", name], stdout=subprocess.DEVNULL,
                       stderr=subprocess.DEVNULL)
