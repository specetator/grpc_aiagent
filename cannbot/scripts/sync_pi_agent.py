#!/usr/bin/env python3
"""Install Spark Push Skills and the CANN advisor extension into a Pi agent home."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[1]


def load_hermes_sync():
    spec = importlib.util.spec_from_file_location(
        "sync_hermes_skills", SCRIPT_DIR / "sync_hermes_skills.py"
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load sync_hermes_skills.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument("--check", action="store_true")
    action.add_argument("--install", action="store_true")
    parser.add_argument("--replace", action="store_true")
    parser.add_argument(
        "--pi-home",
        type=Path,
        help="Pi config directory; defaults to PI_CODING_AGENT_DIR or ~/.pi-spark-agent",
    )
    return parser.parse_args()


def default_pi_home() -> Path:
    configured = os.environ.get("PI_CODING_AGENT_DIR", "").strip()
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".pi-spark-agent"


def main() -> int:
    args = parse_args()
    hermes_sync = load_hermes_sync()
    pi_home = (args.pi_home or default_pi_home()).expanduser().resolve()
    try:
        packages = hermes_sync.build_skill_packages(REPO_ROOT)
        extension = (REPO_ROOT / "cannbot/pi/extensions/cann-advisor.ts").read_bytes()
        settings = (REPO_ROOT / "cannbot/pi/settings.json").read_bytes()
        models_example = (REPO_ROOT / "cannbot/pi/models.json.example").read_bytes()
    except (OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    skill_targets = {name: pi_home / "skills" / name for name in packages}
    extension_target = pi_home / "extensions" / "cann-advisor.ts"
    settings_target = pi_home / "settings.json"
    models_target = pi_home / "models.json"
    models_example_target = pi_home / "models.json.example"

    if args.check:
        ok = True
        for name, target in skill_targets.items():
            state = hermes_sync.package_state(target, packages[name])
            print(f"{name}: {state} ({target})")
            ok = ok and state == "current"
        state = hermes_sync.file_state(extension_target, extension)
        print(f"cann-advisor: {state} ({extension_target})")
        ok = ok and state == "current"
        print(f"settings: {hermes_sync.file_state(settings_target, settings)} ({settings_target})")
        print(f"models: {'present' if models_target.is_file() else 'missing'} ({models_target})")
        return 0 if ok else 1

    try:
        for name, target in skill_targets.items():
            result = hermes_sync.install_directory(target, packages[name], args.replace)
            print(f"{name}: {result} ({target})")
        result = hermes_sync.install_file(extension_target, extension, args.replace)
        print(f"cann-advisor: {result} ({extension_target})")
        if not settings_target.exists():
            result = hermes_sync.install_file(settings_target, settings, replace=False)
            print(f"settings: {result} ({settings_target})")
        else:
            print(f"settings: kept ({settings_target})")
        hermes_sync.install_file(models_example_target, models_example, replace=True)
        if not models_target.exists():
            result = hermes_sync.install_file(models_target, models_example, replace=False)
            print(f"models: {result} ({models_target})")
        else:
            print(f"models: kept ({models_target})")
    except (OSError, RuntimeError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2

    print("Pi Agent 同步完成。新会话或重启 Pi gateway 后生效。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
