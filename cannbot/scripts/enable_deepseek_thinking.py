#!/usr/bin/env python3
"""Apply DeepSeek official thinking/effort/context metadata to models.json.

Docs: https://api-docs.deepseek.com/zh-cn/guides/thinking_mode
Pi openai-completions uses compat.thinkingFormat=deepseek:
  thinking on  -> extra_body thinking.type=enabled + mapped reasoning_effort
  thinking off -> thinking.type=disabled (map off to "none", not null)

Official effort map: minimal->low, medium/xhigh->high, max/ultra->max.
Context 1M, max output 384K. Default effort is high.
"""
import argparse
import json
import os
from pathlib import Path
import tempfile
import time

DEEPSEEK_THINKING = {
    "off": "none",
    "minimal": "low",
    "low": "low",
    "medium": "high",
    "high": "high",
    "xhigh": "high",
    "max": "max",
}


def patch_model(model):
    model["contextWindow"] = 1048576
    model["maxTokens"] = 393216
    model["reasoning"] = True
    model["thinkingLevelMap"] = dict(DEEPSEEK_THINKING)
    model.setdefault("compat", {})
    model["compat"]["thinkingFormat"] = "deepseek"
    model["compat"]["supportsReasoningEffort"] = True


def patch(path: Path) -> bool:
    if path.is_symlink():
        raise ValueError("models config must not be a symlink")
    raw = path.read_bytes()
    config = json.loads(raw)
    provider = config.get("providers", {}).get("deepseek")
    if not isinstance(provider, dict):
        raise ValueError("deepseek provider is missing")
    provider.setdefault("compat", {})
    provider["compat"]["thinkingFormat"] = "deepseek"
    provider["compat"]["supportsReasoningEffort"] = True
    for model in provider.get("models") or []:
        if model.get("id") in {"deepseek-flash", "deepseek-v4-pro"}:
            patch_model(model)
    if config == json.loads(raw):
        return False
    backup = path.with_name(path.name + ".before-deepseek-thinking-" + str(time.time_ns()))
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
    print("Updated DeepSeek thinking metadata; private backup saved; restart Pi gateway."
          if changed else "DeepSeek thinking metadata is already current.")


if __name__ == "__main__":
    main()
