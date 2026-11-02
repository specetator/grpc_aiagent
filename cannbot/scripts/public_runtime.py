"""Startup isolation for operator-owned public Agent services, not an Agent loop."""
import json
import hashlib
import os
from pathlib import Path
import subprocess
import signal
import sys
import tempfile
import threading
import time

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


def runtime_lock(path, *, create=False, exclusive=False):
    """Use the same native lock for a launcher, freeze or fresh restore."""
    path = Path(path)
    if path.is_symlink() or (path.exists() and (not path.is_file() or path.stat().st_nlink != 1)):
        raise ValueError('invalid public runtime lock')
    stream = path.open('x+b' if exclusive else 'a+b' if create else 'r+b')
    try:
        stream.seek(0)
        if os.name == 'nt':
            import msvcrt
            if not path.stat().st_size:
                if not (create or exclusive):raise ValueError('maintenance cannot initialize a Windows runtime lock')
                stream.write(b'0');stream.flush();stream.seek(0)
            msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return stream
    except BaseException:
        stream.close()
        raise


class PublicHome:
    """Refuse unknown directories; hold a process lock for the service lifetime."""
    def __init__(self, path, identity, *, create=True):
        self.root = Path(path)
        if not self.root.is_absolute() or self.root.is_symlink() or self.root.resolve() != self.root:
            raise ValueError('public home must be an absolute directory without symlinks')
        self.identity = {'schema': 1, **identity}
        self.create = create
        self.lock = None
        self.unsafe = False
        self.state_path = self.root / '.spark-runtime-state.json'

    def state(self):
        value = read_json(self.state_path)
        if (value.get('schema') != 1 or value.get('phase') not in {'running', 'clean'} or
                type(value.get('unclean_latched')) is not bool or value.get('platform') != os.name or
                type(value.get('pid')) is not int or value['pid'] <= 0 or
                not isinstance(value.get('process_identity'), str) or not value['process_identity'].isdigit() or
                not isinstance(value.get('boot_id'), str) or not value['boot_id'] or
                value.get('machine_id') != machine_identity() or
                (value['unclean_latched'] and not isinstance(value.get('unclean_boot_id'), str))):
            raise ValueError('invalid public runtime lifecycle state')
        if (not isinstance(value.get('pid_namespace'), str) or
                (value['boot_id'] == boot_identity() and value['pid_namespace'] != process_namespace())):
            raise ValueError('Runtime maintenance must use the original native process namespace')
        return value

    def write_state(self, value):
        if self.state_path.is_symlink():
            raise ValueError('public lifecycle state cannot be a symlink')
        fd, filename = tempfile.mkstemp(prefix='.spark-state-', dir=self.root)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as output:
                json.dump(value, output, sort_keys=True)
                output.flush();os.fsync(output.fileno())
            os.replace(filename, self.state_path)
            if os.name != 'nt':
                directory = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
                try:os.fsync(directory)
                finally:os.close(directory)
        finally:
            if os.path.exists(filename):os.unlink(filename)

    def require_clean(self):
        value = self.state()
        if value['phase'] != 'clean' or value['unclean_latched']:
            raise ValueError('public Runtime shutdown is unproven; cold backup refused')
        return value

    def require_stopped(self):
        value = self.require_clean()
        if value['boot_id'] == boot_identity() and process_identity(value['pid']) == value['process_identity']:
            raise ValueError('public Runtime process has not exited yet')
        return value

    def audit_after_reboot(self):
        """Explicit native-OS audit; no task/session changes or unsafe override."""
        value = self.state()
        current = boot_identity()
        unsafe_boot = value['boot_id'] if value['phase'] == 'running' else value.get('unclean_boot_id')
        if ((value['phase'] == 'clean' and not value['unclean_latched']) or
                not unsafe_boot or unsafe_boot == current):
            raise ValueError('audit requires proof of a later native OS boot')
        if value['boot_id'] == current and process_identity(value['pid']) == value['process_identity']:
            raise ValueError('public Runtime process has not exited yet')
        value.update(phase='clean', unclean_latched=False,
                     audit_method='native_os_reboot', audited_boot_id=current)
        self.write_state(value)
        return value

    def mark_unclean(self):
        # A failed write can be swallowed by an HTTP error response. Never let
        # a later successful close forget that an owned process was unproven.
        self.unsafe = True
        value = self.state()
        value['unclean_latched'] = True
        value['unclean_boot_id'] = value['boot_id']
        self.write_state(value)

    def __enter__(self):
        if (self.root / '.spark-restore-incomplete').exists():
            raise ValueError('public Runtime restore is incomplete')
        if self.create:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        elif not self.root.is_dir():
            raise ValueError('public home must already exist for maintenance')
        marker = self.root / '.spark-public-runtime.json'
        new_home = not marker.exists()
        if not self.create and not marker.is_file():
            raise ValueError('maintenance cannot initialize an unregistered public home')
        if not marker.exists() and any(self.root.iterdir()):
            raise ValueError('refusing to adopt an existing nonempty public home')
        if marker.exists() and read_json(marker) != self.identity:
            raise ValueError('public home belongs to a different service identity')
        lock_path = self.root / '.spark-runtime.lock'
        if lock_path.is_symlink():
            raise ValueError('public runtime lock cannot be a symlink')
        if not self.create and (not lock_path.is_file() or lock_path.stat().st_nlink != 1):
            raise ValueError('maintenance requires the existing public runtime lock')
        self.lock = runtime_lock(lock_path, create=self.create)
        try:
            if marker.exists():
                if read_json(marker) != self.identity:
                    raise ValueError('public home identity changed')
            else:
                self.write_new('.spark-public-runtime.json', json.dumps(self.identity))
            if self.create:
                previous = self.state() if self.state_path.exists() else None
                boot = boot_identity()
                latched = (not new_home and previous is None) or bool(previous and (
                    previous['phase'] == 'running' or previous['unclean_latched']))
                value = {'schema': 1, 'platform': os.name, 'phase': 'running',
                         'unclean_latched': latched, 'boot_id': boot,
                         'machine_id': machine_identity(),
                         'pid_namespace': process_namespace(),
                         'pid': os.getpid(), 'process_identity': process_identity(os.getpid())}
                if latched:
                    value['unclean_boot_id'] = (previous['boot_id'] if previous and previous['phase'] == 'running'
                        else (previous or {}).get('unclean_boot_id', (previous or {}).get('boot_id', boot)))
                self.write_state(value)
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

    def __exit__(self, exc_type=None, *_):
        if self.lock:
            try:
                if self.create:
                    if exc_type is not None or self.unsafe:
                        self.mark_unclean()
                    else:
                        value = self.state()
                        value['phase'] = 'clean'
                        self.write_state(value)
            finally:
                self.lock.close(); self.lock = None


def boot_identity():
    """Native OS boot evidence; a WSL boot is never used for Windows state."""
    if os.name == 'nt':
        powershell = Path(os.environ.get('SystemRoot', r'C:\Windows')) / 'System32/WindowsPowerShell/v1.0/powershell.exe'
        result = subprocess.run([str(powershell), '-NoProfile', '-NonInteractive', '-Command',
            '(Get-CimInstance Win32_OperatingSystem).LastBootUpTime.ToUniversalTime().Ticks'],
            capture_output=True, text=True, timeout=20, check=True)
        value = result.stdout.strip()
        if not value.isdigit():raise ValueError('Windows boot identity unavailable')
        return 'windows:' + value
    value = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    if not value or len(value) > 64:raise ValueError('Linux boot identity unavailable')
    return 'linux:' + value


def machine_identity():
    if os.name == 'nt':
        import winreg
        with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r'SOFTWARE\Microsoft\Cryptography',
                            0, winreg.KEY_READ | winreg.KEY_WOW64_64KEY) as key:
            value, _ = winreg.QueryValueEx(key, 'MachineGuid')
    else:
        value = Path('/etc/machine-id').read_text().strip()
    if not isinstance(value, str) or not value.strip():raise ValueError('native machine identity unavailable')
    return hashlib.sha256((os.name + ':' + value.strip().lower()).encode()).hexdigest()


def process_namespace():
    if os.name == 'nt':return 'native-windows'
    # A host /proc mounted into another PID namespace cannot identify this
    # process by os.getpid(); refuse that unsupported control environment.
    if int(Path('/proc/self/stat').read_text().split(' ',1)[0]) != os.getpid():
        raise ValueError('procfs does not describe this process namespace')
    return os.readlink('/proc/self/ns/pid')


def wait_process_group(pgid, timeout=5):
    """Linux public Pi: prove that only exited/zombie group members remain."""
    if sys.platform != 'linux':raise RuntimeError('public Pi requires Linux process-group verification')
    deadline = time.monotonic() + timeout
    while True:
        found = live = False
        # Do not ignore unreadable process state: uncertainty is not a clean
        # shutdown. Only a process disappearing between list/read is harmless.
        for path in Path('/proc').iterdir():
            if not path.name.isdigit():continue
            try:fields=(path/'stat').read_text().rsplit(')',1)[1].split()
            except FileNotFoundError:continue
            if int(fields[2]) == pgid:
                found = True
                if fields[0] not in {'Z','X'}:live = True
        if not live:
            if found:return
            try:os.killpg(pgid, 0)
            except ProcessLookupError:return
            raise RuntimeError('remaining public Pi process group cannot be observed')
        if time.monotonic() >= deadline:raise RuntimeError('public Pi process group did not exit')
        time.sleep(.02)


def process_identity(pid):
    """Native process creation identity, without reading arguments or secrets."""
    if type(pid) is not int or pid <= 0:raise ValueError('invalid runtime process ID')
    if os.name == 'nt':
        import ctypes
        from ctypes import wintypes
        kernel = ctypes.WinDLL('kernel32', use_last_error=True)
        kernel.OpenProcess.argtypes = [wintypes.DWORD, wintypes.BOOL, wintypes.DWORD]
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.CloseHandle.argtypes = [wintypes.HANDLE]
        kernel.GetProcessTimes.argtypes = [wintypes.HANDLE, *([ctypes.POINTER(wintypes.FILETIME)] * 4)]
        kernel.GetExitCodeProcess.argtypes = [wintypes.HANDLE, ctypes.POINTER(wintypes.DWORD)]
        handle = kernel.OpenProcess(0x1000, False, pid)
        if not handle:
            error = ctypes.get_last_error()
            if error in {87, 1168}:return None
            raise OSError(error, 'cannot verify native Runtime process state')
        try:
            code = wintypes.DWORD()
            if not kernel.GetExitCodeProcess(handle, ctypes.byref(code)):
                raise OSError('cannot inspect native Runtime process')
            if code.value != 259:return None
            values = [wintypes.FILETIME() for _ in range(4)]
            if not kernel.GetProcessTimes(handle, *(ctypes.byref(value) for value in values)):
                raise OSError('cannot identify native Runtime process')
            return str((values[0].dwHighDateTime << 32) | values[0].dwLowDateTime)
        finally:kernel.CloseHandle(handle)
    try:
        fields = Path('/proc/' + str(pid) + '/stat').read_text().rsplit(')', 1)[1].split()
        return None if fields[0] == 'Z' else fields[19]
    except FileNotFoundError:return None


def install_shutdown(callback, *, control_stdin=False):
    """Operator process control only; never exposed as a chat or HTTP action."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: callback())
    if control_stdin:
        def control():
            for line in sys.stdin:
                if line.strip() == 'shutdown':break
            callback()  # Closing the owning process's pipe also drains safely.
        threading.Thread(target=control, daemon=True).start()


class PublicNativePolicyMismatch(ValueError):
    def __init__(self):
        super().__init__('Public Native toolsets mismatch: make the owned launcher room_context_contract and '
                         'owned config.yaml api_server toolsets agree; no configuration files were rewritten.')


def validate_native_policy(config, *, context_tools=False):
    tools = ['todo', 'spark_workspace'] + (['spark_room_context'] if context_tools else [])
    if isinstance(config, dict) and config.get('platform_toolsets', {}).get('api_server') != tools:
        raise PublicNativePolicyMismatch()
    if (not isinstance(config, dict) or
        config.get('platform_toolsets', {}).get('api_server') != tools or
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
