#!/usr/bin/env python3
"""Native-OS freeze lease for an explicitly configured public Runtime.

This process never starts a model, adopts a directory, or changes task/session
data. A freeze is held until its coordinator releases the matching batch.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys

from public_runtime import PublicHome, read_json, validate_native_policy
from public_agent_gateway import validate_registry


def registered_home(config_path, runtime):
    config = read_json(config_path)
    if config.get('schema') != 1:
        raise ValueError('unsupported public service configuration')
    if runtime == 'public_gateway':
        if sys.platform != 'linux':raise ValueError('public Pi Gateway requires Linux')
        if not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', config.get('id', '')):
            raise ValueError('invalid public Gateway identity')
        validate_registry(read_json(config['registry']))
        identity = {'runtime': runtime, 'id': config['id']}
    else:
        if (not re.fullmatch(r'spark-public-[a-z0-9_-]{1,34}', config.get('profile', '')) or
                not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', config.get('agent_id', ''))):
            raise ValueError('a dedicated public Native profile is required')
        identity = {'runtime': runtime, 'agent_id': config['agent_id'], 'profile': config['profile']}
    home = PublicHome(config['home'], identity, create=False)
    if runtime == 'hermes_native':
        import yaml
        path = home.root / 'config.yaml'
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
            raise ValueError('public Native policy is unavailable')
        validate_native_policy(yaml.safe_load(path.read_text(encoding='utf-8')))
    return home


def emit(value):
    print(json.dumps(value, separators=(',', ':')), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operation', choices=('check', 'freeze', 'audit-after-reboot'))
    parser.add_argument('--runtime', required=True, choices=('public_gateway', 'hermes_native'))
    parser.add_argument('--config', required=True)
    parser.add_argument('--batch-id')
    args = parser.parse_args()
    try:
        if args.operation == 'freeze' and not re.fullmatch(r'[0-9a-f]{32}', args.batch_id or ''):
            raise ValueError('freeze requires a batch ID')
        with registered_home(args.config, args.runtime) as home:
            if args.operation == 'audit-after-reboot':
                state = home.audit_after_reboot()
                emit({'ok': True, 'state': 'audited', 'platform': os.name,
                      'method': state['audit_method']})
                return 0
            state = home.require_stopped()
            evidence = {'ok': True, 'platform': os.name, 'identity': home.identity,
                        'home': str(home.root), 'lifecycle': state,
                        'config_sha256': hashlib.sha256(Path(args.config).read_bytes()).hexdigest()}
            if args.operation == 'check':
                emit({**evidence, 'state': 'stopped'})
                return 0
            emit({**evidence, 'state': 'frozen', 'batch_id': args.batch_id})
            # EOF, invalid commands and a missing helper are failures to the
            # coordinator. A one-shot check is never a substitute for a lease.
            line = sys.stdin.readline(4097)
            if len(line) > 4096 or json.loads(line) != {'operation': 'release', 'batch_id': args.batch_id}:
                raise ValueError('invalid freeze release')
        emit({'ok': True, 'state': 'released', 'batch_id': args.batch_id})
        return 0
    except Exception as error:
        # Do not echo operator paths, credentials or provider configuration.
        emit({'ok': False, 'state': 'refused', 'error': type(error).__name__})
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
