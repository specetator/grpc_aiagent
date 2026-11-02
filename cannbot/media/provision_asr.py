#!/usr/bin/env python3
"""Operator-only public ASR model provisioning; never called during parsing.

Pinned public model, no token, private cache or user-selectable Runtime URL.
Verify Git/LFS identities before publishing each immutable model file. Rerunning
verifies and reuses complete files; incomplete downloads cannot be approved.
"""
import argparse
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import stat
import re
import urllib.request
import uuid

# Operator aliases only: neither chat nor this CLI accepts a model URL/revision.
# Keep small as the default for existing installations. Upgrade into a separate
# directory, then explicitly approve its new pipeline/model fingerprint.
MODELS = {
    "small": {
        "repository": "Systran/faster-whisper-small",
        "revision": "536b0662742c02347bc0e980a01041f333bce120",
        "files": {"config.json", "model.bin", "tokenizer.json", "vocabulary.txt"},
        "weights_limit": 512 * 1024 ** 2,
    },
    "large-v3-turbo": {
        "repository": "dropbox-dash/faster-whisper-large-v3-turbo",
        "revision": "0a363e9161cbc7ed1431c9597a8ceaf0c4f78fcf",
        "files": {"config.json", "model.bin", "tokenizer.json", "vocabulary.json", "preprocessor_config.json"},
        "weights_limit": 2 * 1024 ** 3,
    },
}


def download(url, stream, size):
    """Bounded range reads; resume a dropped response within this invocation.

    Published files are still verified in full. A wrong/ignored nonzero range
    must never append duplicate bytes. Process restart retains the existing
    policy of discarding private partial files, not trusting them as a model.
    """
    failures = 0
    while stream.tell() < size:
        start = stream.tell()
        end = min(start + 32 * 1024 ** 2, size) - 1
        request = urllib.request.Request(url, headers={"Range": f"bytes={start}-{end}"})
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                if response.status == 206:
                    if response.headers.get("Content-Range") != f"bytes {start}-{end}/{size}":
                        raise ValueError("model download range mismatch")
                elif response.status == 200 and start == 0:
                    end = size - 1  # small Git objects may ignore Range
                else:
                    raise ValueError("model download range ignored")
                while stream.tell() <= end:
                    chunk = response.read(min(1024 ** 2, end + 1 - stream.tell()))
                    if not chunk:
                        raise OSError("incomplete model response")
                    stream.write(chunk)
                if response.read(1):
                    raise ValueError("model download limit")
        except (OSError, http.client.HTTPException):
            failures += 1
            if failures >= 3:
                raise
        else:
            failures = 0


def verify(path, entry):
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_size != entry["size"]:
        raise ValueError("invalid model file")
    sha256, git = hashlib.sha256(), hashlib.sha1()
    git.update(f"blob {info.st_size}\0".encode())
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 ** 2), b""):
            sha256.update(chunk); git.update(chunk)
    if "lfs" in entry:
        if sha256.hexdigest() != entry["lfs"]["sha256"]: raise ValueError("model LFS hash mismatch")
    elif git.hexdigest() != entry["blobId"]: raise ValueError("model Git blob hash mismatch")
    return {"file": path.name, "bytes": info.st_size, "sha256": sha256.hexdigest()}


def _provision(directory, staging, model):
    profile = MODELS[model]
    repository, revision, files = profile["repository"], profile["revision"], profile["files"]
    directory.mkdir(mode=0o755, parents=True, exist_ok=True)
    info = directory.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o022:
        raise ValueError("model directory must be operator-owned and not writable by others")
    api = f"https://huggingface.co/api/models/{repository}/revision/{revision}?blobs=true"
    with urllib.request.urlopen(api, timeout=60) as response:
        raw = response.read(1024 ** 2 + 1)
    if len(raw) > 1024 ** 2: raise ValueError("model metadata limit")
    metadata = json.loads(raw)
    if metadata["sha"] != revision: raise ValueError("model revision mismatch")
    entries = {entry["rfilename"]: entry for entry in metadata["siblings"] if entry["rfilename"] in files}
    if set(entries) != files: raise ValueError("incomplete model metadata")
    inventory = []
    for name, entry in sorted(entries.items()):
        limit = profile["weights_limit"] if name == "model.bin" else 16 * 1024 ** 2
        if type(entry["size"]) is not int or not 0 < entry["size"] <= limit:
            raise ValueError("model size outside provisioning limit")
        target = directory / name
        if target.exists() or target.is_symlink():
            inventory.append(verify(target, entry)); continue
        temporary = staging / ("." + name + "." + uuid.uuid4().hex + ".partial")
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise ValueError("invalid partial model file")
            os.ftruncate(fd, 0)
            with os.fdopen(fd, "wb", closefd=False) as stream:
                download(f"https://huggingface.co/{repository}/resolve/{revision}/{name}", stream, entry["size"])
                stream.flush(); os.fsync(fd)
            item = verify(temporary, entry); item["file"] = name
            os.chmod(temporary, 0o444)
            try:
                os.link(temporary, target)  # no replacement if another provisioner won
            except FileExistsError:
                item = verify(target, entry)
            temporary.unlink(); inventory.append(item)
        finally:
            os.close(fd)
            temporary.unlink(missing_ok=True)
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)
    canonical = json.dumps(inventory, sort_keys=True, separators=(",", ":")).encode()
    return {"model": model, "repository": repository, "revision": revision, "files": inventory,
            "model_sha256": hashlib.sha256(canonical).hexdigest()}


def provision(directory, model="small"):
    if model not in MODELS:
        raise ValueError("unsupported public ASR model")
    directory = directory.absolute()
    directory.parent.mkdir(parents=True, exist_ok=True)
    staging = directory.parent / ("." + directory.name + ".provision")
    staging.mkdir(mode=0o700, exist_ok=True)
    info = staging.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise ValueError("invalid model staging directory")
    fd = os.open(staging / "lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("invalid model provisioning lock")
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for path in staging.iterdir():
            if re.fullmatch(r"\.(config\.json|model\.bin|tokenizer\.json|vocabulary\.(txt|json)|preprocessor_config\.json)\.[0-9a-f]{32}\.partial", path.name):
                info = path.lstat()
                if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
                    raise ValueError("invalid interrupted model staging file")
                path.unlink()  # private namespace, under stable lock; never remove the published target
        return _provision(directory, staging, model)
    finally:
        os.close(fd)  # stable lock inode is not removed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--directory", type=Path, required=True)
    parser.add_argument("--model", choices=sorted(MODELS), default="small")
    args = parser.parse_args()
    print(json.dumps(provision(args.directory, args.model), indent=2))
