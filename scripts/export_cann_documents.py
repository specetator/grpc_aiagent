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
    generation = json.loads(pointer).get('generation', '')
    if not isinstance(generation, str) or not GENERATION.fullmatch(generation):
        raise ValueError('invalid knowledge generation')
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
            doc_id = path.stem
            if path.suffix != '.json' or not DOC.fullmatch(doc_id):
                raise ValueError('unexpected file in published documents')
            raw, states[path] = read_stable(path, 8 * 1024 * 1024)
            value = json.loads(raw)
            document = value.get('document', {})
            chunks = value.get('chunks')
            if (value.get('generation') != generation or not isinstance(document, dict) or
                    document.get('doc_id') != doc_id or not isinstance(chunks, list)):
                raise ValueError('document does not match its generation or ID')
            if any(not isinstance(chunk, dict) or chunk.get('doc_id') != doc_id or
                   not isinstance(chunk.get('chunk_id'), str) or not CHUNK.fullmatch(chunk['chunk_id']) for chunk in chunks):
                raise ValueError('chunk does not match its document')
            (target / path.name).write_bytes(raw)
            (target / path.name).chmod(0o644)
            hashes[path.name] = hashlib.sha256(raw).hexdigest()
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
