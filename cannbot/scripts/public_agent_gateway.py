#!/usr/bin/env python3
"""Operator-owned public Gateway; reuse the existing Router, Store and Pi RPC."""
import argparse
import json
import os
from pathlib import Path
import signal
import threading
from http.server import ThreadingHTTPServer

from public_runtime import PublicHome, clean_environment, key, model_environment, read_json
from room_workspace import SLUG, WorkspaceAuthority


def validate_registry(registry):
    if registry.get('schema') != 1 or not isinstance(registry.get('agents'), list) or not 1 <= len(registry['agents']) <= 12:
        raise ValueError('public Gateway requires an explicit Agent catalog')
    for entry in registry['agents']:
        if (entry.get('owner_user_ids') != [] or type(entry.get('room_enabled')) is not bool or
            entry.get('room_policy') != 'read_only' or entry.get('runtime') not in {'pi_rpc', 'hermes_native'}):
            raise ValueError('public Gateway cannot load private or legacy Agent definitions')
        if entry['runtime'] == 'hermes_native' and (entry.get('room_public_profile') is not True or
            entry.get('room_test_only') is True or
            not str(entry.get('profile', '')).startswith('spark-public-') or
            entry.get('room_workspace_contract') != 1 or entry.get('native_interactive_approvals') is not False):
            raise ValueError('Hermes requires the formal isolated public profile contract')


def provision_models(home, pi):
    models = read_json(pi['models_file'])
    provider = models.get('providers', {}).get(pi['provider'])
    if not isinstance(provider, dict):
        raise ValueError('selected public provider is missing from operator models file')
    selected = [m for m in provider.get('models', []) if m.get('id') == pi['model']]
    if len(selected) != 1 or str(provider.get('apiKey', '')).startswith('!'):
        raise ValueError('public provider needs one explicit model and no credential shell command')
    value = {'providers': {pi['provider']: {**provider, 'models': selected}}}
    path = home.root / 'public-models.json'
    if path.exists():
        if read_json(path) != value:
            raise ValueError('public model configuration changed; update the owned model file explicitly before restart')
    else:
        home.write_new(path.name, json.dumps(value))
    return path


def serve(config):
    if config.get('schema') != 1 or not SLUG.fullmatch(config.get('id', '')):
        raise ValueError('invalid public Gateway configuration identity')
    registry = read_json(config['registry'])
    validate_registry(registry)
    credentials = read_json(config['keys_file'])
    gateway_key = key(credentials.get('SPARK_PUSH_HERMES_API_KEY'))
    authority_key = key(credentials.get('SPARK_PUSH_WORKSPACE_AUTH_KEY'))
    WorkspaceAuthority(config['authority_origin'], authority_key)
    pi = config['pi']
    command = Path(pi['command'])
    if not command.is_absolute() or not command.is_file():
        raise ValueError('public Pi command must be an explicit executable path')
    with PublicHome(config['home'], {'runtime': 'public_gateway', 'id': config['id']}) as home:
        prototype_root = home.directory('prototype')
        prototype_home = home.directory('prototype/home')
        agent_home = home.directory('prototype/agent-home')
        sessions = home.directory('prototype/sessions')
        models = provision_models(home, pi)
        model_values = model_environment(pi.get('model_env_file'))
        env = clean_environment(prototype_home)
        env['PATH'] = str(command.parent) + os.pathsep + env['PATH']
        env.update(model_values)
        env.update(PI_CODING_AGENT_DIR=str(agent_home))
        target = agent_home / 'models.json'
        if target.exists():
            if read_json(target) != read_json(models):
                raise ValueError('public prototype model file changed')
        else:
            home.write_new('prototype/agent-home/models.json', models.read_text(encoding='utf-8'))
        # All globals used by load_router now come from this operator config.
        gateway_env = clean_environment(prototype_home)
        os.environ.clear(); os.environ.update(gateway_env)
        os.environ.update(SPARK_PUSH_PI_WORKERS='1', SPARK_PUSH_ROOM_HERMES_PUBLIC='true',
            SPARK_PUSH_PUBLIC_PI_ROOT=str(home.directory('rooms')),
            SPARK_PUSH_PUBLIC_PI_MODELS_FILE=str(models),
            SPARK_PUSH_PUBLIC_PI_ENV_FILE=pi.get('model_env_file', ''),
            SPARK_PUSH_ROOM_PROJECT_CATALOG=config['projects'],
            SPARK_PUSH_PUBLIC_WORKSPACE_ROOT=config['snapshots'],
            SPARK_WORKSPACE_AUTH_ORIGIN=config['authority_origin'],
            SPARK_PUSH_WORKSPACE_AUTH_KEY=authority_key)
        from pi_gateway import PiRpcClient, GatewayState, make_handler
        from agent_router import load_router
        argv = [str(command), '--mode', 'rpc', '--session-dir', str(sessions), '--provider', pi['provider'],
                '--model', pi['model'], '--no-tools', '--no-extensions', '--no-skills',
                '--no-prompt-templates', '--no-themes', '--system-prompt', 'Public runtime prototype. No tools.']
        prototype = PiRpcClient(argv, prototype_root, env, sessions)
        router = server = None
        try:
            prototype.start()
            router = load_router(Path(config['registry']), prototype, home.directory('router'))
            server = ThreadingHTTPServer((config.get('host', '127.0.0.1'), config.get('port', 8643)),
                                        make_handler(GatewayState(router, gateway_key, 'public-room')))
            def stop(*_):
                threading.Thread(target=server.shutdown, daemon=True).start()
            for sig in (signal.SIGINT, signal.SIGTERM):
                signal.signal(sig, stop)
            print('Public Agent Gateway ready', flush=True)
            server.serve_forever()
        finally:
            if server: server.server_close()
            if router: router.stop()
            prototype.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    try:
        serve(read_json(args.config))
    except Exception as error:
        # No parser values, paths, provider responses or credentials in logs.
        print('Public Gateway unavailable: ' + type(error).__name__ + '; check operator configuration', flush=True)
        return 1
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
