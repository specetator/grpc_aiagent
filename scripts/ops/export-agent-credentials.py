#!/usr/bin/env python3
"""Export only the two Connector keys from the running Logic container.

Usage: export-agent-credentials.py OUTPUT [-- docker compose options...]
The output is a JSON file, never shell code. Existing files are not overwritten.
"""
import json
import os
from pathlib import Path
import subprocess
import sys

def main():
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    target = Path(sys.argv[1]).expanduser()
    options = sys.argv[2:]
    if options[:1] == ['--']:
        options = options[1:]
    # Capture, validate and write without logging secrets or exporting RPC/DB keys.
    result = subprocess.run(['docker', 'compose', *options, 'exec', '-T', 'logic',
        'python3', '/app/deploy/export_agent_credentials.py'], capture_output=True, check=False)
    if result.returncode:
        raise SystemExit('Cannot export Connector keys; ensure Logic is running and credentials are valid.')
    values = json.loads(result.stdout)
    if set(values) != {'SPARK_PUSH_HERMES_API_KEY', 'SPARK_PUSH_WORKSPACE_AUTH_KEY'}:
        raise SystemExit('Unexpected credential fields; no file written.')
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as output:
        json.dump(values, output)
        output.write('\n')
    print('Connector credentials saved; keep this file private.')

if __name__ == '__main__':
    main()
