#!/usr/bin/env python3
"""Launch Hermes' existing Runs API in an explicitly owned public service home."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import signal
import sys

from public_runtime import PublicHome, clean_environment, key, model_environment, read_json, validate_native_policy, restrict_native_dotenv


async def run(config, home):
    from gateway.config import PlatformConfig
    from gateway.platforms.api_server import APIServerAdapter
    from hermes_public_workspace import public_api_adapter
    from room_workspace import ProjectSnapshots, WorkspaceAuthority
    credentials = read_json(config['keys_file'])
    authority = WorkspaceAuthority(config['authority_origin'], key(credentials.get('SPARK_PUSH_WORKSPACE_AUTH_KEY')))
    snapshots = ProjectSnapshots({'schema': 1, 'projects': []}, Path(config['snapshots']))
    api = public_api_adapter(APIServerAdapter, authority, snapshots, config['agent_id'], config['profile'], public_profile=True)
    adapter = api(PlatformConfig(enabled=True, extra={'host': config.get('host', '127.0.0.1'),
        'port': config.get('port', 18644), 'key': key(Path(config['api_key_file']).read_text().strip()),
        'model_name': config['profile']}))
    if not await adapter.connect():
        raise RuntimeError('public Native startup failed')
    try:
        adapter.verify_public_routes()
        # Native discovery must show the actual tools before declaring ready.
        from hermes_cli.config import load_config
        validate_native_policy(load_config())
        from toolsets import resolve_toolset
        if set(resolve_toolset('spark_workspace')) != {'workspace_read'}:
            raise RuntimeError('public workspace tool discovery failed')
        # Native tool discovery is lazy and can exceed the Bridge readiness
        # timeout on a cold process. Exercise the actual authenticated surface
        # before declaring ready, not just the listening socket.
        import aiohttp
        async with aiohttp.ClientSession() as client:
            async with client.get(f"http://127.0.0.1:{config.get('port', 18644)}/v1/toolsets",
                headers={'Authorization': 'Bearer ' + key(Path(config['api_key_file']).read_text().strip())},
                timeout=aiohttp.ClientTimeout(total=60)) as response:
                if response.status != 200:
                    raise RuntimeError('public Native tool policy preflight failed')
                catalog = await response.json()
                if {r['name'] for r in catalog['data'] if r['enabled']} != {'todo', 'spark_workspace'}:
                    raise RuntimeError('unexpected public Native tool surface')
        print('Public Hermes Native ready', flush=True)
        stopped = asyncio.Event()
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, lambda *_: loop.call_soon_threadsafe(stopped.set))
        await stopped.wait()
    finally:
        await adapter.disconnect()


def serve(config):
    import re
    if (config.get('schema') != 1 or not re.fullmatch(r'spark-public-[a-z0-9_-]{1,34}', config.get('profile', '')) or
        not re.fullmatch(r'[a-z][a-z0-9_-]{0,47}', config.get('agent_id', ''))):
        raise ValueError('explicit public profile and Agent identity required')
    source = Path(config['source'])
    if not source.is_absolute() or not (source/'gateway/platforms/api_server.py').is_file():
        raise ValueError('operator must provide the installed Hermes source')
    with PublicHome(config['home'], {'runtime': 'hermes_native', 'agent_id': config['agent_id'], 'profile': config['profile']}) as home:
        user_home, workspace = home.directory('home'), home.directory('workspace')
        env = clean_environment(user_home)
        env.update(model_environment(config.get('model_env_file'), proxy=True))
        env.update(HERMES_HOME=str(home.root), HERMES_PROFILE=config['profile'], HERMES_QUIET='1',
                   TERMINAL_CWD=str(workspace), HERMES_EXEC_ASK='1')
        if config.get('git_bash'):
            bash = Path(config['git_bash'])
            if not bash.is_absolute() or not bash.is_file():
                raise ValueError('invalid operator Git Bash executable')
            env['HERMES_GIT_BASH_PATH'] = str(bash)
        os.environ.clear(); os.environ.update(env)
        os.chdir(workspace)
        sys.path.insert(0, str(source))
        import yaml
        expected = {'model': config['model'], 'agent': {'max_turns': 8},
            'platform_toolsets': {'api_server': ['todo', 'spark_workspace']},
            'tools': {'tool_search': {'enabled': 'off'}},
            'memory': {'memory_enabled': False, 'user_profile_enabled': False},
            'mcp_servers': {}, 'checkpoints': {'enabled': False}, 'approvals': {'mode': 'manual'},
            'terminal': {'cwd': str(workspace)}, 'gateway': {'api_server': {'max_concurrent_runs': 2}}}
        if (not isinstance(config['model'], dict) or set(config['model']) - {'default', 'provider', 'base_url'} or
            not all(isinstance(config['model'].get(k), str) and config['model'][k].strip() for k in ('default', 'provider'))):
            raise ValueError('public model configuration accepts only provider, default and base_url')
        path = home.root/'config.yaml'
        if path.exists():
            if path.is_symlink() or path.stat().st_size > 65536:
                raise ValueError('invalid public Native config file')
            current = yaml.safe_load(path.read_text(encoding='utf-8'))
            validate_native_policy(current)
            if current.get('model') != expected['model']:
                raise ValueError('public Native model changed; update owned config explicitly')
        else:
            home.write_new('config.yaml', yaml.safe_dump(expected))
        # Optional native provider login state must be explicitly provisioned by
        # the operator. Never discover/copy a default or personal auth.json.
        if not (home.root/'auth.json').exists() and config.get('model_auth_file'):
            auth = read_json(config['model_auth_file'])
            home.write_new('auth.json', json.dumps(auth))
        if (home.root/'auth.json').is_symlink():
            raise ValueError('public Native credentials cannot be a symlink')
        # Native's run_agent imports its source-tree .env as a development
        # fallback even when HERMES_HOME is set. Disable that discovery before
        # importing any Agent/API modules; do not modify the installed source.
        from hermes_constants import get_hermes_home, get_default_hermes_root
        if get_hermes_home().resolve() != home.root or get_default_hermes_root().resolve() != home.root:
            raise ValueError('public Native home cannot inherit a global profile root')
        for name in ('.env', '.op.env'):
            if (home.root/name).exists() or (home.root/name).is_symlink():
                raise ValueError('use explicit public model_env_file instead of implicit profile environment files')
        from hermes_cli import env_loader
        restrict_native_dotenv(env_loader, home.root)
        asyncio.run(run(config, home))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    try:
        serve(read_json(args.config))
    except Exception as error:
        print('Public Hermes unavailable: ' + type(error).__name__ + '; check operator configuration', flush=True)
        return 1
    return 0

if __name__ == '__main__':
    raise SystemExit(main())
