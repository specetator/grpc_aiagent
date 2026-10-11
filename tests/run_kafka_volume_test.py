"""Verify Kafka record/offset retention across container replacement in a fresh owned volume.
Never use the compose project or names/volumes of running application dependencies.
"""
import base64
import json
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[1]
build = Path(sys.argv[1] if len(sys.argv) > 1 else root / "build-wsl").resolve()
name = "spark-review-kafka-" + secrets.token_hex(6)
volume = name + "-data"
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
cluster = base64.urlsafe_b64encode(secrets.token_bytes(16)).decode().rstrip("=")

def docker(*args, **kwargs):
    return subprocess.run(["docker", *args], check=True, **kwargs)
def kafka(tool, *args, **kwargs):
    return docker("exec", "-i", name, "/opt/kafka/bin/" + tool + ".sh",
                  "--bootstrap-server", "127.0.0.1:29092", *args, **kwargs)
def start():
    environment = {
        "CLUSTER_ID": cluster, "KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
        "KAFKA_LISTENERS": "INTERNAL://:29092,EXTERNAL://:9092,CONTROLLER://:9093",
        "KAFKA_ADVERTISED_LISTENERS": f"INTERNAL://127.0.0.1:29092,EXTERNAL://127.0.0.1:{port}",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "KAFKA_INTER_BROKER_LISTENER_NAME": "INTERNAL", "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
        "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@127.0.0.1:9093",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0",
        "KAFKA_LOG_DIRS": "/var/lib/kafka/data",
    }
    args = ["run", "--detach", "--name", name, "--label", "spark-review-test=true",
            "--publish", f"127.0.0.1:{port}:9092", "--volume", volume + ":/var/lib/kafka/data"]
    for key, value in environment.items():
        args.extend(["--env", key + "=" + value])
    docker(*args, "apache/kafka:3.7.0", stdout=subprocess.DEVNULL)
    deadline = time.monotonic() + 90
    while True:
        ready = subprocess.run(["docker", "exec", name, "/opt/kafka/bin/kafka-topics.sh",
                                "--bootstrap-server", "127.0.0.1:29092", "--list"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20)
        if ready.returncode == 0:
            break
        if time.monotonic() >= deadline:
            raise RuntimeError("isolated Kafka not ready")
        time.sleep(0.5)

docker("image", "inspect", "apache/kafka:3.7.0", stdout=subprocess.DEVNULL)
docker("volume", "create", volume, stdout=subprocess.DEVNULL)
try:
    start()
    kafka("kafka-topics", "--create", "--topic", "persist_message", "--partitions", "1",
          "--replication-factor", "1", stdout=subprocess.DEVNULL)
    with tempfile.TemporaryDirectory(prefix="spark-kafka-record-") as directory:
        path = Path(directory) / "dlq.json"
        path.write_text(json.dumps({"schema": "sparkpush.dead_letter.v1", "topic": "persist_message",
                                   "key_hex": b"session-test".hex(), "payload_hex": b"retained-record".hex(),
                                   "key_is_null": False, "payload_is_null": False}))
        subprocess.run([str(build / "tools/dlq_replay"), "--file", str(path), "--topic", "persist_message",
                        "--apply", "--brokers", f"127.0.0.1:{port}"], check=True)
    kafka("kafka-consumer-groups", "--group", "volume-proof", "--topic", "persist_message",
          "--reset-offsets", "--to-offset", "1", "--execute", stdout=subprocess.DEVNULL)
    docker("stop", "--time", "20", name, stdout=subprocess.DEVNULL)
    docker("rm", "--volumes", name, stdout=subprocess.DEVNULL)
    start()
    record = kafka("kafka-console-consumer", "--topic", "persist_message", "--partition", "0",
                   "--offset", "0", "--max-messages", "1", "--timeout-ms", "10000",
                   capture_output=True, text=True)
    assert record.stdout.strip() == "retained-record", "record lost after container replacement"
    offsets = kafka("kafka-consumer-groups", "--group", "volume-proof", "--describe",
                    capture_output=True, text=True).stdout
    rows = [line.split() for line in offsets.splitlines() if "persist_message" in line]
    assert rows and rows[0][3] == "1", "committed offset lost after container replacement"
    print("Kafka binary replay confirmation, named-volume record and committed offset retention passed")
finally:
    subprocess.run(["docker", "rm", "--force", "--volumes", name], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
    subprocess.run(["docker", "volume", "rm", volume], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL)
