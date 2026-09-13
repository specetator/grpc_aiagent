"""Fixed local lifecycle operation. Never accepts a shell command from chat."""
import subprocess


WINDOWS_RESTART = r'''
import os, pathlib, subprocess, sys, time
root = pathlib.Path(r'F:\hermes\hermes-agent')
profile = pathlib.Path(r'F:\hermes\profiles\technical')
os.environ.update(HERMES_HOME=str(profile), HERMES_GATEWAY_DETACHED='1', PYTHONIOENCODING='utf-8')
os.chdir(root)
sys.path.insert(0,str(root))
from gateway.status import get_running_pid
before = get_running_pid(cleanup_stale=False)
deadline = time.monotonic() + 75
env = dict(os.environ, HERMES_HOME=r'F:\hermes')
result = subprocess.run([sys.executable, '-m', 'hermes_cli.main', '--profile', 'technical', 'gateway', 'restart'],
    cwd=str(root), env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=70)
# The existing Windows supervisor may finish relaunching after the CLI
# returns. Verify observed process replacement, not the CLI exit code alone.
after = get_running_pid(cleanup_stale=False)
while (not after or after == before) and time.monotonic() < deadline:
    time.sleep(0.5)
    after = get_running_pid(cleanup_stale=False)
if not after or (before and after == before):
    sys.exit(1)
print('restarted',flush=True)
'''


def restart_technical(config, timeout=80):
    # A per-user registry cannot turn this port into an arbitrary process runner.
    if (config.get('transport') != 'windows_stdio' or config.get('profile') != 'technical' or
        config.get('base_url') != 'http://127.0.0.1:8644/v1' or
        config.get('windows_python') != '/mnt/f/hermes/hermes-agent/venv/Scripts/python.exe'):
        raise ValueError('此 Agent 未配置 technical 本机重启能力')
    result = subprocess.run([config['windows_python'], '-u', '-c', WINDOWS_RESTART],
        stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
        timeout=max(1,timeout))
    if result.returncode or result.stdout.strip() != b'restarted':
        raise RuntimeError('Hermes restart was not confirmed')
