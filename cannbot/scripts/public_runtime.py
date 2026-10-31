"""Startup isolation for operator-owned public Agent services, not an Agent loop."""
import json
import os
from pathlib import Path

MODEL_KEYS = frozenset({'OPENAI_API_KEY', 'ANTHROPIC_API_KEY', 'GEMINI_API_KEY', 'GOOGLE_API_KEY',
    'DEEPSEEK_API_KEY', 'CLI_RELAY_API_KEY', 'OPENROUTER_API_KEY', 'AZURE_OPENAI_API_KEY'})


def read_json(path, limit=65536):
    path = Path(path)
    if path.is_symlink() or not path.is_file() or path.stat().st_size > limit:
        raise ValueError('invalid operator JSON file')
    value = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(value, dict):
        raise ValueError('operator configuration must be an object')
    return value


def key(value):
    if not isinstance(value, str) or not 32 <= len(value) <= 256 or any(not 33 <= ord(c) <= 126 for c in value):
        raise ValueError('operator key must contain 32..256 printable ASCII characters')
    return value


def model_environment(path, *, proxy=False):
    values = {}
    if path:
        source = Path(path)
        if source.is_symlink() or not source.is_file() or source.stat().st_size > 65536:
            raise ValueError('invalid operator model environment file')
        allowed = MODEL_KEYS | ({'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY'} if proxy else set())
        for line in source.read_text(encoding='utf-8').splitlines():
            if not line.strip() or line.lstrip().startswith('#') or '=' not in line:
                continue
            name, value = line.split('=', 1)
            if name.strip() in allowed:
                values[name.strip()] = value.strip().strip('"').strip("'")
    return values


def clean_environment(home):
    # Do not inherit provider keys, NODE_OPTIONS, PYTHONPATH, HERMES_* or PI_*.
    keep = {'SYSTEMROOT', 'WINDIR', 'PATHEXT', 'COMSPEC', 'TEMP', 'TMP'}
    values = {k: v for k, v in os.environ.items() if k.upper() in keep}
    values.update(HOME=str(home), USERPROFILE=str(home), LANG='C.UTF-8', PYTHONUTF8='1',
                  PATH=os.environ.get('PATH', '') if os.name == 'nt' else '/usr/local/bin:/usr/bin:/bin')
    return values


class PublicHome:
    """Refuse unknown directories; hold a process lock for the service lifetime."""
    def __init__(self, path, identity):
        self.root = Path(path)
        if not self.root.is_absolute() or self.root.is_symlink() or self.root.resolve() != self.root:
            raise ValueError('public home must be an absolute directory without symlinks')
        self.identity = {'schema': 1, **identity}
        self.lock = None

    def __enter__(self):
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        marker = self.root / '.spark-public-runtime.json'
        if not marker.exists() and any(self.root.iterdir()):
            raise ValueError('refusing to adopt an existing nonempty public home')
        if marker.exists() and read_json(marker) != self.identity:
            raise ValueError('public home belongs to a different service identity')
        lock_path = self.root / '.spark-runtime.lock'
        if lock_path.is_symlink():
            raise ValueError('public runtime lock cannot be a symlink')
        self.lock = lock_path.open('a+b')
        self.lock.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                if not lock_path.stat().st_size:
                    self.lock.write(b'0'); self.lock.flush(); self.lock.seek(0)
                msvcrt.locking(self.lock.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if marker.exists():
                if read_json(marker) != self.identity:
                    raise ValueError('public home identity changed')
            else:
                self.write_new('.spark-public-runtime.json', json.dumps(self.identity))
            return self
        except BaseException:
            self.lock.close(); self.lock = None
            raise

    def directory(self, name):
        path = self.root / name
        if path.is_symlink() or path.resolve() != path or not path.is_relative_to(self.root):
            raise ValueError('public service directory escaped its home')
        path.mkdir(parents=True, exist_ok=True, mode=0o700)
        return path

    def write_new(self, name, value):
        path = self.root / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, 'O_NOFOLLOW', 0)
        with os.fdopen(os.open(path, flags, 0o600), 'w', encoding='utf-8') as output:
            output.write(value)

    def __exit__(self, *args):
        if self.lock:
            self.lock.close(); self.lock = None


def validate_native_policy(config):
    if (not isinstance(config, dict) or
        config.get('platform_toolsets', {}).get('api_server') != ['todo', 'spark_workspace'] or
        config.get('tools', {}).get('tool_search', {}).get('enabled') != 'off' or
        config.get('memory', {}).get('memory_enabled') is not False or
        config.get('memory', {}).get('user_profile_enabled') is not False or
        config.get('mcp_servers') != {} or config.get('checkpoints', {}).get('enabled') is not False or
        config.get('approvals', {}).get('mode') != 'manual'):
        raise ValueError('public Native policy must disable memory, MCP, extra tools, tool search and checkpoints')


def restrict_native_dotenv(loader, home):
    """Retain Native's loader, but never discover the source tree's .env."""
    root = Path(home).resolve()
    original = loader.load_hermes_dotenv
    def owned_environment(*, hermes_home=None, project_env=None, load_external_secrets=True):
        if Path(hermes_home or os.environ['HERMES_HOME']).resolve() != root:
            raise ValueError('public Native cannot load another profile environment')
        return original(hermes_home=root, project_env=None, load_external_secrets=False)
    loader.load_hermes_dotenv = owned_environment
