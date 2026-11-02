#!/usr/bin/env python3
"""Durable, local UTF-8 ingestion; no model, shell, private profile or Agent loop.

This is the first pipeline of the multimodal worker protocol. It deliberately
does not claim PDF/image/audio/video jobs until their real stages are installed.
"""
import argparse
import base64
import fcntl
import hashlib
import http.client
import json
import os
from pathlib import Path
import secrets
import stat
import sys
import time
from urllib.parse import urlsplit

MAX_TEXT = 16 * 1024 * 1024
PART_BYTES = 16 * 1024
MAX_PARTS = 2048


class BackendError(Exception):
    def __init__(self, code):
        super().__init__(f"media backend error {code}")
        self.code = code


class InvalidText(Exception):
    pass


def fingerprint():
    return hashlib.sha256(Path(__file__).read_bytes()).hexdigest()


def fragments(raw):
    if len(raw) > MAX_TEXT:
        raise InvalidText("text_too_large")
    try:
        text = raw.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise InvalidText("invalid_utf8") from exc
    if not text or "\0" in text:
        raise InvalidText("invalid_utf8")
    result, current, size, first, line_number = [], [], 0, 1, 0
    # Only LF is the source line delimiter. Preserve CR, BOM, Unicode and final
    # newline exactly so Logic can verify reconstructed bytes against the source.
    lines = text.split("\n")
    for index, part in enumerate(lines):
        line = part + ("\n" if index + 1 < len(lines) else "")
        if not line:
            continue
        length = len(line.encode("utf-8"))
        if length > PART_BYTES:
            raise InvalidText("line_too_long")
        if current and size + length > PART_BYTES:
            result.append({"kind": "text", "text": "".join(current),
                           "anchor": {"line_start": first, "line_end": line_number}})
            current, size, first = [], 0, line_number + 1
        current.append(line)
        size += length
        line_number += 1
    result.append({"kind": "text", "text": "".join(current),
                   "anchor": {"line_start": first, "line_end": line_number}})
    if len(result) > MAX_PARTS:
        raise InvalidText("too_many_fragments")
    return result


def private_read(path, maximum):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or
                info.st_uid != os.getuid() or info.st_mode & 0o077 or info.st_size > maximum):
            raise ValueError("worker state and credential files must be private regular files")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            return stream.read(maximum + 1)
    finally:
        os.close(fd)


class Client:
    def __init__(self, origin, key):
        url = urlsplit(origin)
        if (url.scheme not in ("http", "https") or not url.hostname or url.username or
                url.password or url.path not in ("", "/") or url.query or url.fragment):
            raise ValueError("operator origin must be a fixed HTTP(S) origin")
        self.host, self.port, self.scheme, self.key = url.hostname, url.port, url.scheme, key

    def call(self, operation, payload):
        # http.client does not honor proxy variables or follow redirects. Neither
        # source text nor API responses can supply a new destination.
        connection_type = http.client.HTTPSConnection if self.scheme == "https" else http.client.HTTPConnection
        for attempt in range(3):
            conn = connection_type(self.host, self.port, timeout=10)
            try:
                conn.request("POST", "/internal/media/" + operation,
                             json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode(),
                             {"Authorization": "Bearer " + self.key, "Content-Type": "application/json"})
                response = conn.getresponse()
                if response.status != 200:
                    raise BackendError(response.status)
                raw = response.read(2 * 1024 * 1024 + 1)
                if len(raw) > 2 * 1024 * 1024:
                    raise BackendError(502)
                envelope = json.loads(raw)
                if envelope["code"]:
                    raise BackendError(envelope["code"])
                return envelope["data"]
            except (OSError, http.client.HTTPException, BackendError) as exc:
                if isinstance(exc, BackendError) and exc.code not in (500, 502, 503, 504):
                    raise
                if attempt == 2:
                    raise BackendError(503) from None
                time.sleep(0.2 * (attempt + 1))
            finally:
                conn.close()


class State:
    def __init__(self, directory):
        self.root = Path(directory)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.root.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError("worker state directory must be private and owned by the worker")
        self.lock = os.open(self.root / "worker.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
        info = os.fstat(self.lock)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid() or info.st_mode & 0o077:
            os.close(self.lock)
            raise ValueError("invalid worker lock file")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            os.close(self.lock)
            raise

    def sync(self):
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def load(self):
        try:
            return json.loads(private_read(self.root / "claim.json", 16384))
        except FileNotFoundError:
            return None

    def save(self, value):
        path = self.root / ("claim.partial." + secrets.token_hex(12))
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        try:
            with os.fdopen(fd, "w", closefd=False) as stream:
                json.dump(value, stream)
                stream.flush()
                os.fsync(fd)
            os.replace(path, self.root / "claim.json")
            self.sync()
        finally:
            os.close(fd)
            path.unlink(missing_ok=True)

    def clear(self):
        (self.root / "claim.json").unlink(missing_ok=True)
        self.sync()

    def close(self):
        os.close(self.lock)


def once(client, state):
    pending = state.load()
    if pending is None:
        pending = {"claim_id": f"{int(time.time())}-{secrets.token_hex(16)}", "pipeline_sha": fingerprint()}
        state.save(pending)  # Durable before the first allocating request.
    if (not isinstance(pending, dict) or set(pending) != {"claim_id", "pipeline_sha"} or
            pending["pipeline_sha"] != fingerprint()):
        raise ValueError("pending assignment belongs to a different worker build; preserve it for operator recovery")
    try:
        assignment = client.call("claim", pending)
    except BackendError as exc:
        if exc.code != 410:
            raise
        # Only this explicit read-only expiry answer permits abandoning an old
        # unresolvable claim. A timeout or a generic 409 never grants permission.
        state.clear()
        return {"state": "expired_claim"}
    if assignment.get("idle"):
        state.clear()
        return {"state": "idle"}
    if not assignment["live"]:
        state.clear()
        return {"state": assignment["state"] if assignment["state"] != "running" else "expired",
                "id": assignment["id"]}
    lease = {field: assignment[field] for field in ("id", "attempt", "lease_token", "revision")}
    last_heartbeat = time.monotonic()

    def heartbeat():
        nonlocal last_heartbeat
        if time.monotonic() - last_heartbeat > 25:
            client.call("heartbeat", lease)
            last_heartbeat = time.monotonic()

    try:
        if assignment["bytes"] > MAX_TEXT:
            raise InvalidText("text_too_large")
        raw = bytearray()
        while len(raw) < assignment["bytes"]:
            heartbeat()
            part = client.call("source", dict(lease, offset=len(raw), length=1024 * 1024))
            data = base64.b64decode(part["data"], validate=True)
            if (part["id"] != lease["id"] or part["revision"] != lease["revision"] or
                    part["offset"] != len(raw) or part["total_bytes"] != assignment["bytes"] or
                    part["sha256"] != assignment["sha256"] or not data or len(data) != part["bytes"] or
                    len(data) > min(1024 * 1024, assignment["bytes"] - len(raw))):
                raise InvalidText("source_integrity")
            raw.extend(data)
        if hashlib.sha256(raw).hexdigest() != assignment["sha256"]:
            raise InvalidText("source_integrity")
        parts, digests = fragments(bytes(raw)), []
        for index, part in enumerate(parts):
            heartbeat()
            accepted = client.call("stage", dict(lease, index=index, fragment=part))
            digests.append(accepted["sha256"])
        manifest = hashlib.sha256(json.dumps(digests, separators=(",", ":")).encode()).hexdigest()
        result = client.call("complete", dict(lease, fragment_count=len(parts), manifest_sha256=manifest))
        state.clear()
        return result
    except InvalidText as exc:
        client.call("fail", dict(lease, error_code=str(exc)))
        state.clear()
        return {"state": "failed", "id": lease["id"], "error_code": str(exc)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fingerprint", action="store_true")
    parser.add_argument("--origin")
    parser.add_argument("--key-file", type=Path)
    parser.add_argument("--state-dir", type=Path)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()
    if args.fingerprint:
        print(fingerprint())
        return 0
    if not args.origin or not args.key_file or not args.state_dir:
        parser.error("--origin, --key-file and --state-dir are required")
    key = private_read(args.key_file, 257).decode().strip()
    if not 32 <= len(key) <= 256 or any(c in key for c in "\r\n"):
        raise ValueError("invalid operator worker credential")
    state = State(args.state_dir)
    try:
        client = Client(args.origin, key)
        while True:
            try:
                result = once(client, state)
                print(json.dumps(result, ensure_ascii=False), flush=True)
                if args.once:
                    return 0 if result["state"] in ("ready", "idle") else 1
            except BackendError as exc:
                # Keep the pending claim on every transport/lease error. The next
                # iteration recovers its assignment, never allocates by guessing.
                print(str(exc), file=sys.stderr, flush=True)
                if args.once:
                    return 1
            time.sleep(3)
    finally:
        state.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (ValueError, OSError, json.JSONDecodeError):
        print("media worker configuration or durable state is invalid", file=sys.stderr)
        sys.exit(1)
