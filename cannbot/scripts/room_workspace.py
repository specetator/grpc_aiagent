"""Trusted project snapshots and live authorization, shared by public runtimes.

No model, IM body, or tool argument can choose a project root or an identity.
The operator catalog selects source paths; Logic selects the canonical grant.
"""
from __future__ import annotations

import hashlib
import errno
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import tempfile
import urllib.parse
import urllib.request

SLUG = re.compile(r"[a-z][a-z0-9_-]{0,47}")
REQUEST = re.compile(r"ra_[a-f0-9]{64}")
MAX_FILES, MAX_BYTES, MAX_FILE_BYTES = 4096, 32 * 1024 * 1024, 1024 * 1024
SOURCE_SUFFIXES = frozenset((
    '.c', '.cc', '.cpp', '.cxx', '.h', '.hpp', '.hxx', '.py', '.js', '.cjs', '.mjs',
    '.ts', '.tsx', '.jsx', '.rs', '.go', '.java', '.kt', '.kts', '.md', '.txt',
    '.cmake', '.proto', '.sql', '.json', '.yaml', '.yml', '.toml', '.html', '.css',
    '.sh', '.bash', '.xml', '.gradle', '.properties', '.conf', '.ini'))
EXCLUDED_DIRS = {'node_modules', 'build', 'dist', 'venv', 'sessions', 'memory', 'backups', '__pycache__'}
SECRET = re.compile(r'(^|[._-])(auth|credentials?|secrets?|tokens?|private|local)([._-]|$)', re.I)
INDEX = 'PROJECT_FILES.txt'


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     separators=(',', ':')).encode()).hexdigest()


def execution_namespace(scope):
    base = f"roomagent_{scope['room_id']}_{scope['agent_id']}"
    # Old persisted requests retain their old tool-free native session.
    if 'workspace_generation' not in scope:
        return base
    return base + '_w' + digest([scope['generation'],scope['workspace_generation'],scope.get('project_id','')])[:24]


def relative_path(value):
    if not isinstance(value, str) or not value or len(value) > 512 or '\\' in value or '\0' in value:
        raise ValueError('invalid project-relative path')
    parts = value.split('/')
    if any(not part or part.startswith('.') or ':' in part for part in parts):
        raise ValueError('hidden or escaping project path')
    return PurePosixPath(value)


def allowed_file(name):
    return (not SECRET.search(name) and not name.startswith('.') and
            (Path(name).suffix.lower() in SOURCE_SUFFIXES or name in {'Dockerfile', 'Makefile', 'LICENSE'}))


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise PermissionError('workspace authority redirect refused')


class WorkspaceAuthority:
    def __init__(self, origin, key):
        url = urllib.parse.urlsplit(origin)
        if (url.scheme not in {'http', 'https'} or not url.hostname or url.username or
                url.password or url.query or url.fragment or url.path not in {'', '/'} or
                not isinstance(key, str) or not 32 <= len(key) <= 256 or '\n' in key or '\r' in key):
            raise ValueError('invalid operator workspace authority configuration')
        self.url = origin.rstrip('/') + '/internal/room-agent/authorize'
        self.key = key
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), _NoRedirect())

    def authorize(self, request_id, expected=None):
        if not isinstance(request_id, str) or not REQUEST.fullmatch(request_id):
            raise PermissionError('invalid workspace request identity')
        request = urllib.request.Request(self.url, json.dumps({'request_id': request_id}).encode(),
            {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.key})
        try:
            with self.opener.open(request, timeout=3) as response:
                raw = response.read(65537)
            if len(raw) > 65536:
                raise ValueError('oversized authority response')
            value = json.loads(raw)
            if value.get('code') != 0:
                raise PermissionError('workspace access revoked or unavailable')
            result = value['data']
            scope = result['collaboration']
            if (result['request_id'] != request_id or scope['request_id'] != request_id or
                    scope.get('kind') != 'room_agent_v1' or scope.get('policy') != 'read_only' or
                    (expected is not None and scope != expected)):
                raise PermissionError('workspace scope does not match the stored request')
            return scope
        except Exception:
            # No endpoint, response text, credentials or private paths in model errors.
            raise PermissionError('workspace authorization denied or temporarily unavailable') from None


def _open_directory(path):
    """Traverse from / using directory descriptors; no resolve/open race."""
    if os.name != 'posix' or not hasattr(os, 'O_NOFOLLOW'):
        raise RuntimeError('snapshot creation requires Linux directory-descriptor isolation')
    path = Path(path)
    if not path.is_absolute() or '..' in path.parts:
        raise ValueError('operator project root must be absolute')
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open('/', flags)
    try:
        for part in path.parts[1:]:
            new = os.open(part, flags, dir_fd=fd)
            os.close(fd)
            fd = new
        return fd
    except BaseException:
        os.close(fd)
        raise


class ProjectSnapshots:
    def __init__(self, catalog, storage):
        if (not isinstance(catalog, dict) or catalog.get('schema') != 1 or
                not isinstance(catalog.get('projects'), list) or len(catalog['projects']) > 32):
            raise ValueError('invalid operator project catalog')
        self.projects = {}
        for entry in catalog['projects']:
            key = entry.get('id')
            paths = entry.get('paths')
            if (not isinstance(key, str) or not SLUG.fullmatch(key) or key in self.projects or
                    not isinstance(paths, list) or not 1 <= len(paths) <= 32 or
                    not isinstance(entry.get('root'), str) or not Path(entry['root']).is_absolute()):
                raise ValueError('project requires an ID, absolute root and explicit source paths')
            for path in paths:
                relative_path(path)
            if len(set(paths)) != len(paths):
                raise ValueError('duplicate source paths')
            self.projects[key] = {'root': entry['root'], 'paths': paths}
        self.root = Path(storage)
        if not self.root.is_absolute() or self.root.is_symlink() or self.root.resolve() != self.root:
            raise ValueError('snapshot storage must be an absolute non-symlink directory')
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def prepare(self, scope):
        request_id = scope.get('request_id', '')
        if not REQUEST.fullmatch(request_id) or scope.get('policy') != 'read_only':
            raise PermissionError('invalid snapshot scope')
        project = self.projects.get(scope.get('project_id'))
        if project is None:
            raise PermissionError('project is not available on this runtime')
        destination = self.root / request_id
        if destination.exists() or destination.is_symlink():
            return self.load(scope)
        temporary = Path(tempfile.mkdtemp(prefix='.snapshot-', dir=self.root))
        files = []
        total = 0
        visited = 0
        workspace = temporary / 'files'
        workspace.mkdir(mode=0o700)
        source_fd = None
        try:
            source_fd = _open_directory(project['root'])
            def visit(parent, name, relative):
                nonlocal total, visited
                visited += 1
                if visited > 16384:
                    raise ValueError('project traversal exceeds its entry limit')
                relative_path(relative)
                if name.startswith('.') or name in EXCLUDED_DIRS or name.startswith('build-'):
                    return
                info = os.stat(name, dir_fd=parent, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise PermissionError('project contains a symbolic link')
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                    try:
                        for item in sorted(os.listdir(child)):
                            if not item.startswith('.'):
                                visit(child, item, relative + '/' + item)
                    finally:
                        os.close(child)
                    return
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise PermissionError('project contains a non-regular or multiply-linked file')
                if not allowed_file(name):
                    return
                fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                try:
                    before = os.fstat(fd)
                    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or
                            before.st_size > MAX_FILE_BYTES or before.st_ino != info.st_ino or before.st_dev != info.st_dev):
                        raise PermissionError('project file is unsafe, too large or changed during snapshot')
                    with os.fdopen(fd, 'rb', closefd=False) as source:
                        content = source.read(MAX_FILE_BYTES + 1)
                    after = os.fstat(fd)
                    identity = lambda value: (value.st_dev,value.st_ino,value.st_size,value.st_mtime_ns,value.st_ctime_ns)
                    if identity(before) != identity(after) or len(content) != before.st_size:
                        raise PermissionError('project file changed during snapshot')
                finally:
                    os.close(fd)
                if b'\0' in content:
                    raise ValueError('project source file contains binary content')
                content.decode('utf-8')
                total += len(content)
                if total > MAX_BYTES or len(files) >= MAX_FILES:
                    raise ValueError('project snapshot exceeds its file or byte limit')
                if any(row['path'] == relative for row in files):
                    raise ValueError('overlapping operator source paths')
                if relative == INDEX:
                    raise ValueError('reserved project index filename')
                target = workspace / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(content)
                target.chmod(0o444)
                files.append({'path':relative,'size':len(content),'sha256':hashlib.sha256(content).hexdigest()})

            for approved in project['paths']:
                parts = relative_path(approved).parts
                parent = os.dup(source_fd)
                try:
                    for part in parts[:-1]:
                        child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent)
                        os.close(parent)
                        parent = child
                    visit(parent, parts[-1], approved)
                finally:
                    os.close(parent)
            files.sort(key=lambda row: row['path'])
            revision = digest(files)
            index = ('Read-only source snapshot ' + revision + '\n' +
                     '\n'.join(row['path'] for row in files) + '\n').encode()
            if len(index) > MAX_FILE_BYTES:
                raise ValueError('project index exceeds its byte limit')
            (workspace / INDEX).write_bytes(index)
            (workspace / INDEX).chmod(0o444)
            manifest = {'schema':1,'scope_sha256':digest(scope),'request_id':request_id,
                        'project_id':scope['project_id'],'revision':revision,'files':files,
                        'index_sha256':hashlib.sha256(index).hexdigest()}
            (temporary / 'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False))
            (temporary / 'manifest.json').chmod(0o400)
            # A duplicate request may race; it must load the winner, never replace it.
            try:
                temporary.rename(destination)
            except OSError as error:
                if error.errno not in {errno.EEXIST, errno.ENOTEMPTY}:
                    raise
                return self.load(scope)
            return self.load(scope)
        finally:
            if source_fd is not None:
                os.close(source_fd)
            if temporary.exists():
                shutil.rmtree(temporary)

    def load(self, scope):
        request_id = scope.get('request_id', '')
        if not REQUEST.fullmatch(request_id):
            raise PermissionError('invalid snapshot request')
        root = self.root / request_id
        if root.is_symlink() or (root/'manifest.json').is_symlink() or (root/'files').is_symlink():
            raise PermissionError('snapshot storage was replaced')
        manifest = json.loads((root/'manifest.json').read_text())
        if (manifest.get('scope_sha256') != digest(scope) or manifest.get('request_id') != request_id or
                manifest.get('revision') != digest(manifest.get('files'))):
            raise PermissionError('snapshot identity or manifest changed')
        return Snapshot(root/'files', manifest)


class Snapshot:
    def __init__(self, root, manifest):
        self.root, self.manifest = root, manifest

    def file(self, relative):
        relative_path(relative)
        wanted = (self.manifest['index_sha256'] if relative == INDEX else
                  next((row['sha256'] for row in self.manifest['files'] if row['path'] == relative), None))
        if wanted is None:
            raise PermissionError('file is outside the approved snapshot manifest')
        path = self.root
        for part in PurePosixPath(relative).parts:
            path /= part
            if path.is_symlink():
                raise PermissionError('snapshot symbolic link refused')
        info = path.stat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size > MAX_FILE_BYTES:
            raise PermissionError('snapshot file is not safe to read')
        if hashlib.sha256(path.read_bytes()).hexdigest() != wanted:
            raise PermissionError('snapshot content changed')
        return path
