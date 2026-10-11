#!/usr/bin/env python3
"""Benchmark current IM binaries against fresh owned Docker dependencies.

Uses only the Python standard library. Never starts/stops the Compose project,
reads personal credential files, or writes the existing application database.
Credentials/configs/service logs stay in a private temporary directory.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import secrets
import socket
import statistics
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def write_config(private, service, updates):
    lines = []
    updates = dict(updates)
    for line in (ROOT / "conf" / (service + ".conf")).read_text().splitlines():
        key = line.split("=", 1)[0] if "=" in line and not line.startswith("#") else None
        if key in updates:
            line = f"{key}={updates.pop(key)}"
        lines.append(line)
    lines.extend(f"{key}={value}" for key, value in updates.items())
    path = private / (service + ".conf")
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)
    return path


def request(base, path, body=None, token=None):
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(base + path, data, headers)
    with urllib.request.urlopen(req, timeout=5) as response:
        return response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", default="build-wsl")
    parser.add_argument("--out", required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--messages-per-conn", type=int, default=20)
    parser.add_argument("--allow-incomplete", action="store_true", help="Record E2E rejections/timeouts and continue diagnostic measurements")
    args = parser.parse_args()
    if not 1 <= args.repeats <= 10:
        parser.error("repeats must be between 1 and 10")
    if not 1 <= args.messages_per_conn <= 100:
        parser.error("messages-per-conn must be between 1 and 100")
    expected_messages = 8 * args.messages_per_conn
    build = Path(args.build).resolve()
    out = Path(args.out).resolve()
    out.mkdir(parents=True, exist_ok=True)
    binaries = {name: build / "load_test" / name for name in
                ("e2e_bench", "connection_bench", "history_bench", "kafka_delivery_bench")}
    for path in list(binaries.values()) + [build / x / (x + "_server") for x in ("logic", "comet", "job")]:
        if not path.is_file():
            parser.error("missing build binary: " + str(path))
    run_id = "spark-current-" + secrets.token_hex(6)
    names = {x: run_id + "-" + x for x in ("mysql", "redis", "kafka")}
    ports = {x: free_port() for x in ("mysql", "redis", "kafka", "logic", "http", "ws", "comet", "jobm", "cometm")}
    owned, processes, logs = [], [], []
    report = {"schema": "sparkpush.current_benchmark.v1", "ok": False,
              "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "runner_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              "binary_sha256": {n: hashlib.sha256(p.read_bytes()).hexdigest() for n, p in binaries.items()},
              "environment": {"kernel": platform.release(), "python": platform.python_version(),
                              "cpu_count": os.cpu_count(), "docker_images": ["mysql:8.0", "redis:7.2-alpine", "apache/kafka:3.7.0"],
                              "mem_total_kib": int(re.search(r"MemTotal:\s+(\d+)", Path("/proc/meminfo").read_text())[1]),
                              "placement": "clients, Logic, Comet, Job and Docker dependencies on the same host; other host workloads not stopped"},
              "profile": {"repeats": args.repeats, "mysql_storage": "ephemeral container writable layer, no tmpfs",
                          "kafka_replication_factor": 1, "topic_partitions": 1, "hermes_enabled": False,
                          "logic_http_threads": 4, "comet_io_threads": 8, "grpc_stream_count": 8,
                          "e2e": f"8 sending connections x {args.messages_per_conn} small text messages; 1 independent receiver; fresh pair each trial",
                          "e2e_default_ack_deadline_ms": 8000, "allow_incomplete": args.allow_incomplete,
                          "connections": "200 authenticated connections, 8 threads, 1 second hold; fresh no-history user",
                          "history": f"8 threads x 50 queries, limit 50; up to {expected_messages} stored messages per trial",
                          "kafka_delivery": "1000 tasks directly into push_single; bypasses Logic acceptance and history persistence"},
              "warmup": [], "trials": [], "checks": []}
    phase = "prepare"

    def docker(*parts, **kw):
        return subprocess.run(["docker", *parts], check=True, timeout=120, **kw)

    def sql(query, database="spark_push"):
        return docker("exec", "-i", names["mysql"], "sh", "-c",
                      'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -N -B ' + database,
                      input=query, text=True, capture_output=True).stdout.strip()

    def wait_for(predicate, seconds=90):
        end = time.monotonic() + seconds
        while time.monotonic() < end:
            if processes and any(p.poll() is not None for p in processes):
                raise RuntimeError("application exited during " + phase)
            try:
                if predicate():
                    return
            except (subprocess.CalledProcessError, urllib.error.URLError, TimeoutError):
                pass
            time.sleep(.2)
        raise RuntimeError("readiness timeout during " + phase)

    def account(base, label):
        credentials = {"account": run_id + "-" + label, "password": secrets.token_hex(24), "name": label}
        result = json.loads(request(base, "/api/register", credentials))
        if result.get("code") != 0:
            raise RuntimeError("registration failed")
        return credentials, result["data"]["user_id"]

    def benchmark(private, kind, trial, params, expected):
        command = [str(binaries[kind]), *map(str, params)]
        result = subprocess.run(command, cwd=private, env=env, capture_output=True, text=True, timeout=90)
        markers = [line for line in result.stdout.splitlines() if line.startswith(expected)]
        record = {"kind": kind, "trial": trial, "exit_code": result.returncode, "raw_result_lines": markers}
        for line in markers:
            fields = {}
            for key, value in re.findall(r"([a-zA-Z_][a-zA-Z0-9_]*)=([^ ]+)", line):
                try:
                    fields[key] = float(value) if "." in value or "e" in value.lower() else int(value)
                except ValueError:
                    fields[key] = value
            record["metrics"] = fields
        errors = [line for line in result.stdout.splitlines() if line.startswith("E2E_ACK_ERROR")]
        if errors:
            record["ack_error_counts"] = {int(re.search(r"code=(\d+)", line)[1]): int(re.search(r"count=(\d+)", line)[1]) for line in errors}
        # Server counters help distinguish pending ACK timeouts from ingress rejections.
        if args.allow_incomplete and kind == "e2e_bench":
            record["server_metrics"] = {}
            for name, metrics_base in (("logic", base), ("comet", comet_base), ("job", job_base)):
                raw = request(metrics_base, "/metrics").decode()
                record["server_metrics"][name] = {key: float(value) for key, value in
                    re.findall(r"^(spark_push_[A-Za-z0-9_]+) ([0-9.eE+-]+)$", raw, re.MULTILINE)}
        report["warmup" if trial == 0 else "trials"].append(record)
        print(json.dumps(record), flush=True)
        (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        if not markers or (result.returncode != 0 and not (args.allow_incomplete and kind == "e2e_bench")):
            raise RuntimeError("benchmark failed: " + kind)
        return record

    temporary = tempfile.TemporaryDirectory(prefix=run_id + "-")
    try:
        temp = temporary.name
        private = Path(temp)
        private.chmod(0o700)
        password = secrets.token_hex(32)
        env = dict(os.environ, SPARK_PUSH_MYSQL_PASSWORD=password, SPARK_PUSH_REDIS_PASSWORD="",
                   SPARK_PUSH_HERMES_ENABLED="false", SPARK_PUSH_ADMIN_ACCOUNT="", SPARK_PUSH_ADMIN_PASSWORD="",
                   SPARK_PUSH_ROOM_AGENT_ENABLED="false", SPARK_LOG_CONSOLE="0")
        for role in ("COMET", "JOB", "ADMIN"):
            env["SPARK_RPC_" + role + "_KEY"] = secrets.token_hex(32)
        for variable in ("SPARK_RPC_TLS_CERT", "SPARK_RPC_TLS_KEY", "SPARK_RPC_TLS_CA"):
            env.pop(variable, None)
        mysql_env = private / "mysql.env"
        mysql_env.write_text("MYSQL_ROOT_PASSWORD=" + password + "\nMYSQL_ROOT_HOST=%\n")
        mysql_env.chmod(0o600)
        kafka_env = private / "kafka.env"
        import base64
        cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")
        kafka_env.write_text("\n".join([
            "CLUSTER_ID=" + cluster, "KAFKA_NODE_ID=1", "KAFKA_PROCESS_ROLES=broker,controller",
            "KAFKA_LISTENERS=INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093",
            f"KAFKA_ADVERTISED_LISTENERS=INTERNAL://127.0.0.1:29092,EXTERNAL://127.0.0.1:{ports['kafka']}",
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
            "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
            "KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
            "KAFKA_LOG_DIRS=/var/lib/kafka/data"]) + "\n")
        kafka_env.chmod(0o600)
        for name, image, inner, extra in [
                ("mysql", "mysql:8.0", 3306, ["--env-file", str(mysql_env)]),
                ("redis", "redis:7.2-alpine", 6379, []),
                ("kafka", "apache/kafka:3.7.0", 9092, ["--env-file", str(kafka_env)])]:
            owned.append(names[name])
            docker("run", "--detach", "--name", names[name], "--label", "spark-current-test=true",
                   "--publish", f"127.0.0.1:{ports[name]}:{inner}", *extra, image, stdout=subprocess.DEVNULL)
        phase = "dependencies"
        wait_for(lambda: sql("SELECT 1", "mysql") == "1")
        sql((ROOT / "sql/schema.sql").read_text(), "mysql")
        wait_for(lambda: docker("exec", names["redis"], "redis-cli", "ping", capture_output=True, text=True).stdout.strip() == "PONG")
        kafka_base = ["exec", names["kafka"], "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "127.0.0.1:29092"]
        wait_for(lambda: docker(*kafka_base, "--list", stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0)
        topics = {scene: "current_" + scene for scene in ("single", "group", "broadcast", "persist")}
        for topic in list(topics.values()) + [topics["persist"] + ".dlq"]:
            docker(*kafka_base, "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1", stdout=subprocess.DEVNULL)
        common = {"kafka_brokers": f"127.0.0.1:{ports['kafka']}", "kafka_consumer_group": run_id,
                  "mysql_host": "127.0.0.1", "mysql_port": ports["mysql"], "mysql_user": "root",
                  "mysql_db": "spark_push", "mysql_password": "${SPARK_PUSH_MYSQL_PASSWORD}"}
        common.update({"kafka_" + scene + "_topic": topic for scene, topic in topics.items()})
        configs = {
            "logic": dict(common, listen_addr="127.0.0.1", internal_grpc_listen_addr="127.0.0.1",
                          listen_port=ports["logic"], http_port=ports["http"], logic_http_threads=4,
                          redis_port=ports["redis"], redis_db=0, hermes_enabled="false", agent_bot_users="",
                          attachment_dir=str(private / "attachments")),
            "comet": {"listen_addr": "127.0.0.1", "internal_grpc_listen_addr": "127.0.0.1",
                      "listen_port": ports["ws"], "comet_grpc_port": ports["comet"],
                      "metrics_port": ports["cometm"], "logic_grpc_target": f"127.0.0.1:{ports['logic']}"},
            "job": dict(common, comet_targets=f"comet-1=127.0.0.1:{ports['comet']}", metrics_port=ports["jobm"])}
        config_paths = {name: write_config(private, name, updates) for name, updates in configs.items()}
        phase = "applications"
        for name, config in config_paths.items():
            log = open(private / (name + ".out"), "w")
            logs.append(log)
            processes.append(subprocess.Popen([str(build / name / (name + "_server")), "--config", str(config)],
                                              env=env, cwd=private, stdout=log, stderr=subprocess.STDOUT))
            time.sleep(.4)
        base = f"http://127.0.0.1:{ports['http']}"
        job_base = f"http://127.0.0.1:{ports['jobm']}"
        comet_base = f"http://127.0.0.1:{ports['cometm']}"
        wait_for(lambda: request(base, "/ready") and request(job_base, "/ready"))
        wait_for(lambda: all(key + " 1" in request(comet_base, "/metrics").decode() for key in
                            ("spark_push_comet_logic_stream_ready", "spark_push_comet_push_stream_ready")))
        time.sleep(2)
        endpoint = ["--logic-port", ports["http"], "--comet-port", ports["ws"]]
        # A separately registered pair warms Kafka/SQL paths without contaminating timed history.
        phase = "warmup"
        sender, _ = account(base, "warm-s")
        receiver, _ = account(base, "warm-r")
        def credentials(sender, receiver):
            return ["--sender-account", sender["account"], "--sender-password", sender["password"],
                    "--receiver-account", receiver["account"], "--receiver-password", receiver["password"]]
        benchmark(private, "e2e_bench", 0, endpoint + credentials(sender, receiver) +
                  ["--connections", 1, "--messages-per-conn", 20, "--timeout-ms", 30000], "E2E_RESULT")
        for trial in range(1, args.repeats + 1):
            phase = "trial-" + str(trial)
            sender, sid = account(base, f"s-{trial}")
            receiver, rid = account(base, f"r-{trial}")
            session = f"s_{min(sid, rid)}_{max(sid, rid)}"
            benchmark(private, "e2e_bench", trial, endpoint + credentials(sender, receiver) +
                      ["--connections", 8, "--messages-per-conn", args.messages_per_conn, "--timeout-ms", 30000], "E2E_RESULT")
            count_sql = f"SELECT COUNT(*),COUNT(DISTINCT msg_seq) FROM message WHERE session_id='{session}';"
            if args.allow_incomplete:
                time.sleep(3)
            else:
                wait_for(lambda: sql(count_sql) == f"{expected_messages}\t{expected_messages}", 30)
            stored, unique = map(int, sql(count_sql).split())
            report["checks"].append({"trial": trial, "stored_messages": stored, "distinct_sequences": unique,
                                     "expected_messages": expected_messages})
            benchmark(private, "history_bench", trial, ["--logic-port", ports["http"],
                      "--account", sender["account"], "--password", sender["password"], "--session-id", session,
                      "--threads", 8, "--requests-per-thread", 50, "--limit", 50], "HISTORY_RESULT")
            empty, _ = account(base, f"empty-{trial}")
            benchmark(private, "connection_bench", trial, endpoint + ["--account", empty["account"],
                      "--password", empty["password"], "--connections", 200, "--threads", 8,
                      "--hold-seconds", 1, "--rounds", 1], "CONNECTION_ROUND")
            time.sleep(1)
            benchmark(private, "kafka_delivery_bench", trial, endpoint + ["--account", receiver["account"],
                      "--password", receiver["password"], "--config", config_paths["job"],
                      "--messages", 1000, "--timeout-ms", 30000], "KAFKA_DELIVERY_RESULT")
            time.sleep(1)
        phase = "complete"
        report["summary"] = {}
        for kind, field in [("e2e_bench", "delivery_qps"), ("history_bench", "qps"),
                            ("connection_bench", "connect_rate"), ("kafka_delivery_bench", "delivery_qps")]:
            trials = [r["metrics"] for r in report["trials"] if r["kind"] == kind]
            values = [r[field] for r in trials]
            summary = {"throughput_field": field, "throughput_median": statistics.median(values),
                       "throughput_min": min(values), "throughput_max": max(values)}
            if "latency_p99_ms" in trials[0]:
                summary["trial_p99_median_ms"] = statistics.median(r["latency_p99_ms"] for r in trials)
            report["summary"][kind] = summary
        report["measurements_completed"] = True
        report["ok"] = all(r["exit_code"] == 0 for r in report["trials"])
    except Exception as error:
        report["error"] = {"phase": phase, "type": type(error).__name__}
        print("Benchmark failed during " + phase + " (" + type(error).__name__ + ")", flush=True)
    finally:
        for p in reversed(processes):
            if p.poll() is None:
                p.terminate()
        for p in reversed(processes):
            try:
                p.wait(timeout=10)
            except subprocess.TimeoutExpired:
                p.kill()
                p.wait()
        for log in logs:
            log.close()
        cleanup = True
        for name in reversed(owned):
            result = subprocess.run(["docker", "rm", "--force", "--volumes", name],
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=45)
            cleanup = cleanup and result.returncode == 0
        temporary.cleanup()
        report["cleanup_succeeded"] = cleanup
        report["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        (out / "results.json").write_text(json.dumps(report, indent=2) + "\n")
        print(json.dumps({"ok": report["ok"], "cleanup_succeeded": cleanup, "summary": report.get("summary", {})}), flush=True)
    return 0 if report["ok"] and cleanup else 1


if __name__ == "__main__":
    raise SystemExit(main())
