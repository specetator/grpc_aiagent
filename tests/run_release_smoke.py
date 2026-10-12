#!/usr/bin/env python3
"""Build a fresh local clone and smoke-test the documented IM startup path.

Requires Docker/Compose and the build dependencies listed in README.
Tests committed Git HEAD; uncommitted edits are not part of the cloned fixture.
Uses fresh credentials and owned volumes; never reads personal .env files.
Standard Spark ports/container names must be free. AI and CANN are disabled.
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
import subprocess
import tempfile
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
CONTAINERS = ("spark-redis", "spark-mysql", "spark-kafka")
PORTS = (6379, 3307, 9092, 9000, 9010, 9100, 9101, 9105, 9202, 9203)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="Public JSON receipt path")
    parser.add_argument("--jobs", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.jobs <= 16:
        parser.error("jobs must be between 1 and 16")
    output = Path(args.out).resolve()
    project = "spark-release-" + secrets.token_hex(6)
    receipt = {"schema": "sparkpush.release_smoke.v1", "ok": False,
               "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
               "environment": {"kernel": platform.release(), "python": platform.python_version()},
               "profile": {"fresh_clone": True, "build_jobs": args.jobs,
                           "agent_enabled": False, "cann_installed_in_fixture": False,
                           "mysql_storage": "fresh named Docker volume", "e2e_messages": 80},
               "checks": []}
    phase = "preflight"
    workspace = None
    clone = None
    env = None
    startup_attempted = False

    def check_command(command, label, timeout=120, expected=0, command_env=None):
        print(label, flush=True)
        started = time.monotonic()
        result = subprocess.run(command, cwd=clone, env=command_env or env,
                                capture_output=True, text=True, timeout=timeout)
        receipt["checks"].append({"name": label, "exit_code": result.returncode,
                                  "duration_s": round(time.monotonic() - started, 3)})
        if workspace:
            # Private diagnostic output is removed with the fixture, never published.
            (Path(workspace.name) / (re.sub(r"\W+", "_", label) + ".log")).write_text(
                result.stdout + result.stderr)
        if result.returncode != expected:
            raise RuntimeError("check failed: " + label)
        return result.stdout

    def http(path, data=None):
        payload = None if data is None else json.dumps(data).encode()
        req = urllib.request.Request("http://127.0.0.1:9101" + path, payload,
                                     {"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=5) as response:
            return json.loads(response.read())

    try:
        for port in PORTS:
            with socket.socket() as listener:
                listener.bind(("127.0.0.1", port))
        for name in CONTAINERS:
            if subprocess.run(["docker", "inspect", name], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL).returncode == 0:
                raise RuntimeError("existing Spark container; use a separate host")
        receipt["source_commit"] = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        workspace = tempfile.TemporaryDirectory(prefix=project + "-")
        private = Path(workspace.name)
        private.chmod(0o700)
        clone = private / "repo"
        subprocess.run(["git", "clone", "--quiet", "--no-hardlinks", str(ROOT), str(clone)],
                       check=True, timeout=120, capture_output=True)
        for path in ("02_auth_subset", "03_msg_subset", "04_room_subset", "05_persist_subset",
                     "5.1_room_subset", "06", "课件", "muduo/lib", "cmake/FindgRPC-bk.cmake",
                     "web_demo/static/demo.mp4", "build", ".env.local", ".env.rpc"):
            if (clone / path).exists():
                raise RuntimeError("unexpected distributed artifact: " + path)
        password = secrets.token_hex(32)
        local_env = clone / ".env.local"
        local_env.write_text("SPARK_PUSH_MYSQL_PASSWORD=" + password + "\n"
                             "SPARK_PUSH_HERMES_ENABLED=false\nSPARK_PUSH_ROOM_AGENT_ENABLED=false\n")
        local_env.chmod(0o600)
        env = dict(os.environ, COMPOSE_PROJECT_NAME=project,
                   SPARK_PUSH_ENV_FILE=str(local_env), SPARK_PUSH_BUILD_DIR=str(clone / "build"),
                   SPARK_RPC_ENV_FILE=str(clone / ".env.rpc"), SPARK_PUSH_BUILD_JOBS=str(args.jobs),
                   SPARK_PUSH_MYSQL_PASSWORD=password, SPARK_PUSH_REDIS_PASSWORD="",
                   SPARK_PUSH_HERMES_ENABLED="false", SPARK_PUSH_ROOM_AGENT_ENABLED="false",
                   SPARK_PUSH_ADMIN_ACCOUNT="", SPARK_PUSH_ADMIN_PASSWORD="",
                   CANN_KNOWLEDGE_ROOT=str(private / "absent-cann-workspace"))
        for key in ("SPARK_RPC_COMET_KEY", "SPARK_RPC_JOB_KEY", "SPARK_RPC_ADMIN_KEY",
                    "SPARK_RPC_TLS_CERT", "SPARK_RPC_TLS_KEY", "SPARK_RPC_TLS_CA"):
            env.pop(key, None)
        phase = "doctor"
        text = check_command(["bash", "scripts/sparkctl.sh", "doctor"], "doctor_without_agent_or_cann")
        if "CANN knowledge checks skipped" not in text:
            raise RuntimeError("optional CANN preflight was not skipped")
        phase = "configure"
        check_command(["cmake", "-S", ".", "-B", "build", "-DCMAKE_BUILD_TYPE=Release",
                       "-DBUILD_TESTING=ON"], "fresh_cmake_configure")
        phase = "build"
        check_command(["cmake", "--build", "build", "-j" + str(args.jobs)], "fresh_source_build", 900)
        receipt["service_binary_sha256"] = {
            name: hashlib.sha256((clone / "build" / name / (name + "_server")).read_bytes()).hexdigest()
            for name in ("logic", "comet", "job")}
        phase = "ctest"
        text = check_command(["ctest", "--test-dir", "build", "--output-on-failure", "-j2"],
                             "fresh_ctest", 180)
        receipt["ctest"] = {"passed": len(re.findall(r"Test\s+#\d+:.*?\bPassed\b", text)),
                            "skipped": len(re.findall(r"Test\s+#\d+:.*?\*\*\*Skipped", text))}
        phase = "startup"
        startup_attempted = True
        check_command(["bash", "scripts/sparkctl.sh", "up"], "documented_startup", 420)
        phase = "health"
        check_command(["bash", "scripts/sparkctl.sh", "health"], "full_health_check")
        with urllib.request.urlopen("http://127.0.0.1:9010/danmaku.html", timeout=5) as response:
            if 'id="video-file"' not in response.read().decode():
                raise RuntimeError("local-video page was not served")
        receipt["local_video_page_served"] = True
        phase = "e2e"
        users = []
        for name in ("sender", "receiver"):
            user = {"account": project + "-" + name, "password": secrets.token_hex(24), "name": name}
            registered = http("/api/register", user)
            if registered.get("code") != 0:
                raise RuntimeError("registration failed")
            user["user_id"] = int(registered["data"]["user_id"])
            users.append(user)
        sender, receiver = users
        text = check_command(["build/load_test/e2e_bench", "--sender-account", sender["account"],
                              "--sender-password", sender["password"], "--receiver-account", receiver["account"],
                              "--receiver-password", receiver["password"], "--connections", "4",
                              "--messages-per-conn", "20", "--timeout-ms", "30000"], "actual_message_e2e", 60)
        lines = [line for line in text.splitlines() if line.startswith("E2E_RESULT")]
        if len(lines) != 1:
            raise RuntimeError("missing E2E result")
        receipt["e2e_raw_result"] = lines[0]
        phase = "sql"
        low, high = sorted((sender["user_id"], receiver["user_id"]))
        query = f"SELECT COUNT(*),COUNT(DISTINCT msg_seq) FROM message WHERE session_id='s_{low}_{high}';"
        for _ in range(100):
            result = subprocess.run(["docker", "exec", "-i", "spark-mysql", "sh", "-c",
                                     'MYSQL_PWD="$MYSQL_ROOT_PASSWORD" exec mysql -uroot -N -B spark_push'],
                                    input=query, text=True, capture_output=True, timeout=10)
            if result.returncode == 0 and result.stdout.strip() == "80\t80":
                break
            time.sleep(.2)
        else:
            raise RuntimeError("SQL history did not match accepted messages")
        receipt["sql"] = {"stored_messages": 80, "distinct_sequences": 80}
        receipt["ok"] = True
    except Exception as error:
        receipt["error"] = {"phase": phase, "type": type(error).__name__}
        print("Release check failed during " + phase + " (" + type(error).__name__ + ")", flush=True)
    finally:
        cleanup = True
        if startup_attempted:
            print("Stopping owned services and removing fixture volumes", flush=True)
            try:
                result = subprocess.run(["bash", "scripts/stop_demo.sh"], cwd=clone, env=env,
                                        capture_output=True, text=True, timeout=170)
                cleanup = result.returncode == 0
                # Only remove dependency containers if they still belong to this fixture.
                for name in CONTAINERS:
                    inspect = subprocess.run(["docker", "inspect", "-f",
                                              '{{index .Config.Labels "com.docker.compose.project"}}', name],
                                             text=True, capture_output=True, timeout=10)
                    if inspect.returncode == 0 and inspect.stdout.strip() != project:
                        raise RuntimeError("container ownership changed")
                result = subprocess.run(["docker", "compose", "--env-file", str(clone / ".env.local"),
                                         "down", "--volumes"], cwd=clone, env=env,
                                        capture_output=True, text=True, timeout=120)
                cleanup = cleanup and result.returncode == 0
            except Exception:
                cleanup = False
        if workspace and cleanup and receipt["ok"]:
            workspace.cleanup()
        elif workspace:
            # Preserve diagnostics after failure; never expose private log contents.
            workspace._finalizer.detach()
            print("Private failure diagnostics preserved at " + workspace.name, flush=True)
        receipt["cleanup_succeeded"] = cleanup
        receipt["finished_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt), flush=True)
    return 0 if receipt["ok"] and cleanup else 1


if __name__ == "__main__":
    raise SystemExit(main())
