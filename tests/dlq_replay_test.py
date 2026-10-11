import json
from pathlib import Path
import subprocess
import sys
import tempfile

record = {"schema": "sparkpush.dead_letter.v1", "topic": "persist_message",
          "key_hex": "6b6579", "payload_hex": "ff0061", "key_is_null": False,
          "payload_is_null": False}
with tempfile.TemporaryDirectory(prefix="spark-dlq-test-") as directory:
    path = Path(directory) / "record.json"
    def run(value, topic="persist_message", extra=()):
        path.write_text(json.dumps(value))
        return subprocess.run([sys.argv[1], "--file", str(path), "--topic", topic, *extra],
                              capture_output=True, text=True)
    result = run(record)
    assert result.returncode == 0 and "payload_bytes=3" in result.stdout and "dry-run" in result.stdout
    for field, value in (("payload_hex", "0"), ("payload_hex", "xx"), ("schema", "wrong"),
                         ("key_is_null", True), ("payload_hex", 123)):
        assert run(dict(record, **{field: value})).returncode != 0
    assert run(record, "another_topic").returncode != 0
    assert run(record, extra=("--apply",)).returncode != 0
    recovered = dict(record, recovery={"topic": "ai_reply", "key_hex": "61", "payload_hex": "6263"})
    assert run(recovered, "ai_reply", ("--recovery",)).returncode == 0
    path.write_text(json.dumps(record) + json.dumps(record))
    assert subprocess.run([sys.argv[1], "--file", str(path), "--topic", "persist_message"],
                          capture_output=True).returncode != 0
