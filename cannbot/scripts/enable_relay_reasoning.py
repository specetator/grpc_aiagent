#!/usr/bin/env python3
"""Enable verified CLI Relay GPT-5.6 Luna effort levels, preserving private config.

Capability evidence: local CliRelay internal/registry/codex_model_capabilities.go.
Pi 0.84.4 uses model.reasoning plus model.thinkingLevelMap for RPC and wire values.
"""
import argparse
import json
import os
from pathlib import Path
import tempfile
import time


def patch(path: Path) -> bool:
    if path.is_symlink():
        raise ValueError("models config must not be a symlink")
    raw = path.read_bytes()
    config = json.loads(raw)
    model = next(m for m in config["providers"]["cli-relay"]["models"] if m["id"] == "gpt-5.6-luna")
    model["reasoning"] = True
    model["thinkingLevelMap"] = {"off": None, "minimal": None,
                                 **{level: level for level in ("low", "medium", "high", "xhigh", "max")}}
    model.setdefault("compat", {})["supportsReasoningEffort"] = True
    if config == json.loads(raw):
        return False
    backup = path.with_name(path.name + ".before-reasoning-" + str(time.time_ns()))
    fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as output:
        output.write(raw)
        output.flush()
        os.fsync(output.fileno())
    temporary = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent,
                                         prefix=".models-", delete=False) as output:
            temporary = Path(output.name)
            json.dump(config, output, ensure_ascii=False, indent=2)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--models", type=Path, default=Path.home() / ".pi-spark-agent/models.json")
    args = parser.parse_args()
    changed = patch(args.models)
    print("Updated GPT-5.6 Luna reasoning metadata; private backup saved; restart Pi gateway."
          if changed else "GPT-5.6 Luna reasoning metadata is already current.")


if __name__ == "__main__":
    main()
