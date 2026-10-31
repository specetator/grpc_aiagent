"""Use the same scenarios from a verified distribution or a Git checkout."""
from pathlib import Path
import subprocess
import sys


def source_commit(root):
    if (root / 'release.json').exists():
        sys.path.insert(0, str(root / 'scripts'))
        from verify_release import verify_files
        return verify_files(root)['revision']
    return subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=root, text=True).strip()


def running_images(root, image, project):
    if not (root / 'release.json').exists():
        return None
    sys.path.insert(0, str(root / 'scripts'))
    from verify_release import manifest, verify_running
    value = manifest(root)
    if image != value['application']['reference']:
        raise ValueError('release tests require the exact application digest')
    return verify_running(value, project)
