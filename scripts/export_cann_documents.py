#!/usr/bin/env python3
"""Export explicitly public CANN documents for the unprivileged Logic container.

The output is a data directory, not a knowledge root. Source permissions and
bytes are never changed. No index, code, model configuration or profile is copied.
Run during a maintenance window without editing the published generation.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import stat
import tempfile

GENERATION = re.compile(r'[A-Za-z0-9-]{16,80}\Z')
DOC = re.compile(r'doc_[0-9a-f]{24}\Z')
CHUNK = re.compile(r'chk_[0-9a-f]{24}\Z')
DOCUMENT_LIMIT = 8 * 1024 * 1024


def generation_from_pointer(raw):
    value = json.loads(raw)
    generation = value.get('generation') if isinstance(value, dict) else None
    if not isinstance(generation, str) or not GENERATION.fullmatch(generation):
        raise ValueError('invalid knowledge generation')
    return generation


def validate_document(raw, generation, filename):
    path = Path(filename)
    doc_id = path.stem
    if path.name != filename or path.suffix != '.json' or not DOC.fullmatch(doc_id):
        raise ValueError('unexpected file in published documents')
    if len(raw) > DOCUMENT_LIMIT:
        raise ValueError('document exceeds size limit')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('document must be an object')
    document, chunks = value.get('document'), value.get('chunks')
    if (value.get('generation') != generation or not isinstance(document, dict) or
            document.get('doc_id') != doc_id or not isinstance(chunks, list)):
        raise ValueError('document does not match its generation or ID')
    if any(not isinstance(chunk, dict) or chunk.get('doc_id') != doc_id or
           not isinstance(chunk.get('chunk_id'), str) or not CHUNK.fullmatch(chunk['chunk_id']) for chunk in chunks):
        raise ValueError('chunk does not match its document')
    return hashlib.sha256(raw).hexdigest()


def descriptor(pointer, generation, hashes):
    if not hashes:
        raise ValueError('generation has no documents')
    return {'generation': generation, 'documents': len(hashes),
            'current_sha256': hashlib.sha256(pointer).hexdigest(),
            'documents_sha256': hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest()}


def directory(path):
    for item in (path, *path.parents):
        if not stat.S_ISDIR(item.lstat().st_mode):
            raise ValueError('directory is missing, linked or not a directory')


def identity(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_nlink,
            info.st_size, info.st_mtime_ns, info.st_ctime_ns)


def read_stable(path, limit):
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        before = os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size > limit:
            raise ValueError('document must be a bounded regular file without links')
        content = stream.read(limit + 1)
        if (len(content) > limit or identity(before) != identity(os.fstat(stream.fileno())) or
                identity(before) != identity(path.lstat())):
            raise ValueError('source changed during export')
    return content, identity(before)


def exported_snapshot(source):
    """Strict public export only, plus identities for detecting changes during backup."""
    source = Path(source).absolute()
    if '..' in source.parts:
        raise ValueError('parent path aliases are not supported')
    directory(source)
    states = {}

    def folder(path, expected=None):
        directory(path)
        info = path.lstat()
        if stat.S_IMODE(info.st_mode) != 0o755:
            raise ValueError('public document directories must have mode 0755')
        states[path] = identity(info)
        paths = sorted(path.iterdir())
        if expected is not None and {p.name for p in paths} != expected:
            raise ValueError('unexpected public document directory contents')
        return paths

    def read(path, limit):
        raw, states[path] = read_stable(path, limit)
        if stat.S_IMODE(states[path][2]) != 0o644:
            raise ValueError('public documents must have mode 0644')
        return raw

    folder(source, {'current.json', 'generations'})
    pointer = read(source / 'current.json', 4096)
    generation = generation_from_pointer(pointer)
    folder(source / 'generations', {generation})
    base = source / 'generations' / generation
    folder(base, {'documents'})
    paths = folder(base / 'documents')
    hashes = {path.name: validate_document(read(path, DOCUMENT_LIMIT), generation, path.name) for path in paths}
    if any(identity(path.lstat()) != state for path, state in states.items()):
        raise ValueError('public documents changed during scan')
    return descriptor(pointer, generation, hashes), states


def exported_archive(archive, members):
    """Validate an already link/path-checked tar using the same document rules."""
    by_name = {member.name: member for member in members}

    def read(name, limit):
        member = by_name.get(name)
        if not member or not member.isfile() or member.mode != 0o644 or not 0 <= member.size <= limit:
            raise ValueError('invalid public document archive file')
        with archive.extractfile(member) as stream:
            raw = stream.read(limit + 1)
        if len(raw) != member.size:
            raise ValueError('truncated public document archive')
        return raw

    pointer = read('content/current.json', 4096)
    generation = generation_from_pointer(pointer)
    base = 'content/generations/' + generation
    directories = {'content', 'content/generations', base, base + '/documents'}
    files = {'content/current.json'}
    hashes = {}
    for name, member in by_name.items():
        if name in directories:
            if not member.isdir() or member.mode != 0o755:
                raise ValueError('invalid public document archive directory')
        elif name != 'content/current.json':
            if not name.startswith(base + '/documents/'):
                raise ValueError('unexpected public document archive member')
            filename = name[len(base + '/documents/'):]
            hashes[filename] = validate_document(read(name, DOCUMENT_LIMIT), generation, filename)
            files.add(name)
    if set(by_name) != directories | files:
        raise ValueError('incomplete public document archive')
    return descriptor(pointer, generation, hashes)


def export_documents(knowledge_root, output):
    source = Path(knowledge_root).expanduser().absolute() / 'data'
    output = Path(output).expanduser().absolute()
    if '..' in source.parts or '..' in output.parts:
        raise ValueError('parent path aliases are not supported')
    directory(source)
    directory(output.parent)
    if output.exists() or output.is_symlink():
        raise ValueError('output must not exist')
    if output == source or source in output.parents:
        raise ValueError('output must be outside the source data')
    pointer, pointer_state = read_stable(source / 'current.json', 4096)
    generation = generation_from_pointer(pointer)
    documents = source / 'generations' / generation / 'documents'
    directory(documents)
    paths = sorted(documents.iterdir())
    if not paths:
        raise ValueError('generation has no documents')
    staging = Path(tempfile.mkdtemp(prefix='.spark-cann-export-', dir=output.parent))
    claimed = False
    try:
        target = staging / 'generations' / generation / 'documents'
        target.mkdir(parents=True)
        states, hashes = {}, {}
        for path in paths:
            raw, states[path] = read_stable(path, DOCUMENT_LIMIT)
            hashes[path.name] = validate_document(raw, generation, path.name)
            (target / path.name).write_bytes(raw)
            (target / path.name).chmod(0o644)
        # Check the same generation and complete source set after the copy.
        if (read_stable(source / 'current.json', 4096) != (pointer, pointer_state) or
                sorted(documents.iterdir()) != paths or
                any(identity(path.lstat()) != state for path, state in states.items())):
            raise ValueError('source changed during export')
        (staging / 'current.json').write_bytes(pointer)
        (staging / 'current.json').chmod(0o644)
        for path in (target, target.parent, target.parent.parent):
            path.chmod(0o755)
        # Exclusive claim prevents replacing an existing directory, even if
        # another exporter creates it while this export is being prepared.
        output.mkdir(mode=0o700)
        claimed = True
        for path in staging.iterdir():
            path.rename(output / path.name)
        output.chmod(0o755)
        return {'generation': generation, 'documents': len(paths),
                'documents_sha256': hashlib.sha256(json.dumps(hashes, sort_keys=True).encode()).hexdigest(),
                'source_bytes_and_permissions_unchanged': True}
    except BaseException:
        if claimed:
            shutil.rmtree(output)
        raise
    finally:
        shutil.rmtree(staging)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--knowledge-root', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    try:
        print(json.dumps(export_documents(args.knowledge_root, args.output)))
    except (OSError, ValueError, TypeError, AttributeError) as error:
        parser.exit(1, 'CANN document export failed: ' + str(error) + '\n')


if __name__ == '__main__':
    main()
