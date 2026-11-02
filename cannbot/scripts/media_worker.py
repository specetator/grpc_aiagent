#!/usr/bin/env python3
"""Durable room media processing. Operator configuration only; no Agent loop.

One private state directory owns one claim and one parser execution at a time.
All room access, publication and quota decisions remain in Logic.
"""
import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import stat
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from cannbot.media import sandbox
from cannbot.media.parser import canonical
from media_text_worker import BackendError, Client, State as TextState, private_read

LEASE_FIELDS = ("id", "revision", "attempt", "lease_token")
IDENTITY_FIELDS = LEASE_FIELDS + ("pipeline_sha", "pipeline_kind", "parser_sha256", "mime", "bytes", "sha256")
INPUT_ERRORS = {"invalid_media_timing", "ambiguous_audio_timing", "invalid_source", "unsupported_format",
                "invalid_extracted_text", "text_limit", "fragment_limit", "artifact_limit", "image_pixel_limit",
                "animated_image_unsupported", "mime_mismatch", "image_byte_limit", "artifact_total_limit",
                "encrypted_document", "document_page_limit", "document_entry_limit", "duplicate_document_entry",
                "unsafe_document_entry", "document_expansion_limit", "invalid_docx", "embedded_object_unsupported",
                "external_document_reference", "media_duration_limit", "media_stream_limit", "missing_media_frames",
                "invalid_decoded_audio", "invalid_transcript_timing", "incomplete_audio_decode", "missing_media_stream",
                "multiple_media_tracks_unsupported", "invalid_frame_manifest", "video_sampling_gap",
                "incomplete_video_decode", "manifest_limit"}


class WorkerError(Exception):
    pass


class State(TextState):
    def save(self, value):
        check(len(json.dumps(value).encode()) <= 128 * 1024, "state_size_limit")
        super().save(value)

    def load(self):
        # A bounded DOCX coverage summary can exceed text-v1's 16 KiB state.
        try:return json.loads(private_read(self.root / "claim.json", 128 * 1024))
        except FileNotFoundError:return None


def emit(event, **values):
    # Never print credentials, lease tokens, extracted text or arbitrary errors.
    print(json.dumps(dict(event=event, **values), ensure_ascii=False), flush=True)


def check(value, code):
    if not value:
        raise WorkerError(code)


def sync_directory(path):
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def private_directory(path):
    path.mkdir(mode=0o700, exist_ok=True)
    info = path.lstat()
    check(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid() and not info.st_mode & 0o077,
          "invalid_execution_directory")


def operator_config(path):
    """Private, explicit deployment config; no environment/URL discovery."""
    value = json.loads(private_read(path, 16 * 1024))
    fields = {"schema", "image_id", "model_dir", "origin", "key_file", "state_dir",
              "pipeline_sha256", "parser_sha256"}
    check(isinstance(value, dict) and set(value) == fields and
          type(value["schema"]) is int and value["schema"] == 1, "invalid_worker_config")
    check(all(isinstance(value[k], str) and value[k] and "\0" not in value[k]
              for k in fields - {"schema"}), "invalid_worker_config")
    for name in ("model_dir", "key_file", "state_dir"):
        check(Path(value[name]).is_absolute(), "absolute_worker_path_required")
    check(all(sandbox.HEX.fullmatch(value[k]) for k in ("pipeline_sha256", "parser_sha256")),
          "invalid_worker_fingerprint")
    Client(value["origin"], "")  # validate fixed origin, without making a request
    return value


class Configuration:
    def __init__(self, image, model):
        check(isinstance(image, str) and sandbox.IMAGE.fullmatch(image), "exact_image_id_required")
        self.image = image
        self.model = Path(model).resolve(strict=True)
        self.model_sha = sandbox.model_fingerprint(self.model)
        self.parser_sha = sandbox.hash_file(ROOT / "cannbot/media/parser.py")
        files = [Path(__file__), ROOT / "cannbot/scripts/media_text_worker.py",
                 ROOT / "cannbot/media/sandbox.py", ROOT / "cannbot/media/parser.py"]
        self.pipeline = hashlib.sha256(canonical({"schema": "room-media-worker-v1", "image": image,
            "model": self.model_sha, "sources": {p.relative_to(ROOT).as_posix(): sandbox.hash_file(p) for p in files}})).hexdigest()

    def description(self):
        return {"pipeline_sha256": self.pipeline, "parser_sha256": self.parser_sha,
                "image_id": self.image, "model_sha256": self.model_sha}


class LeaseKeeper:
    """A successful response never grants time measured from response arrival."""
    def __init__(self, client, assignment, claim_sent, stop, clock=time.monotonic):
        self.client, self.clock, self.stop = client, clock, stop
        self.lease = {k: assignment[k] for k in LEASE_FIELDS}
        self.hard = claim_sent + assignment["remaining_seconds"]
        self.until = 0.0
        self.done, self.lost = threading.Event(), threading.Event()
        self.count = 0
        self.thread = None

    def renew(self):
        sent = self.clock()
        check(not self.stop.is_set() and sent < self.hard, "lease_budget_exhausted")
        self.client.call("heartbeat", self.lease)
        self.until = min(self.hard, sent + 120)
        self.count += 1
        emit("heartbeat", id=self.lease["id"], count=self.count)

    def cancelled(self):
        return self.stop.is_set() or self.lost.is_set() or self.clock() >= min(self.hard, self.until) - 1

    def guard(self):
        check(not self.cancelled(), "lease_unavailable")

    def start(self):
        self.renew()  # A recovered live lease can have only milliseconds left.
        self.guard()
        self.thread = threading.Thread(target=self._loop, name="media-lease")
        self.thread.start()
        return self

    def _loop(self):
        while not self.done.wait(20):
            try:
                if self.cancelled():
                    self.lost.set(); return
                self.renew()
            except BackendError as exc:
                if exc.code not in (500, 502, 503, 504) or self.cancelled():
                    self.lost.set(); return
            except Exception:
                self.lost.set(); return

    def close(self):
        self.done.set()
        if self.thread:
            self.thread.join()  # Client calls are bounded; no old lease thread survives.


class Worker:
    def __init__(self, client, state, config, stop=None):
        self.client, self.state, self.config = client, state, config
        self.stop = stop or threading.Event()
        self.gc_after = ""
        self.executions = state.root.resolve() / "executions"
        private_directory(self.executions)

    def collect(self, keeper=None):
        check(not self.stop.is_set(), "worker_stopping")
        if keeper: keeper.guard()
        page = self.client.call("reclaim_candidates", {"after": self.gc_after, "limit": 5})
        items = page["items"]
        check(type(items) is list and len(items) <= 5 and all(isinstance(x, str) and sandbox.HEX.fullmatch(x) for x in items),
              "invalid_gc_response")
        check(page["next_after"] == (items[-1] if items else ""), "invalid_gc_response")
        self.gc_after = page["next_after"]
        for revision in items:
            check(not self.stop.is_set(), "worker_stopping")
            if keeper: keeper.guard()
            try:
                self.client.call("reclaim", {"revision": revision})
                emit("reclaimed", revision=revision)
            except BackendError as exc:
                if exc.code not in (404, 409, 500, 502, 503, 504): raise
                emit("reclaim_deferred", revision=revision, code=exc.code)

    def load(self):
        pending = self.state.load()
        if pending is None:
            pending = {"claim_id": f"{int(time.time())}-{secrets.token_hex(16)}",
                       "pipeline_sha": self.config.pipeline, "execution_id": secrets.token_hex(16)}
            self.state.save(pending)
        check(type(pending) is dict and not (set(pending) - {"claim_id", "pipeline_sha", "execution_id", "assignment", "summary", "failure"}) and
              re.fullmatch(r"[0-9]{10}-[a-f0-9]{32}", pending.get("claim_id", "")) and
              re.fullmatch(r"[a-f0-9]{32}", pending.get("execution_id", "")) and
              pending.get("pipeline_sha") == self.config.pipeline, "pending_pipeline_or_state_mismatch")
        return pending

    def execution(self, pending):
        return self.executions / pending["execution_id"]

    def stop_container(self, pending):
        reclaimed = sandbox.cleanup_execution(pending["execution_id"], self.execution(pending), self.config.image, self.config.model)
        if reclaimed: emit("parser_recovered", execution_id=pending["execution_id"])

    def cleanup(self, pending, assignment=None):
        self.stop_container(pending)
        directory = self.execution(pending)
        if directory.exists() or directory.is_symlink():
            private_directory(directory)
            shutil.rmtree(directory)
            sync_directory(self.executions)
        if assignment and assignment["state"] != "ready":
            try:
                self.client.call("reclaim", {"revision": assignment["revision"]})
            except BackendError as exc:
                if exc.code != 404: raise  # No reservation is a legitimate no-op.
        self.state.clear()  # Only after known terminal/expiry and owned cleanup.

    def download(self, assignment, directory, keeper):
        path = directory / "original"
        if path.exists() or path.is_symlink():
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == assignment["bytes"], "source_integrity")
                digest, total = hashlib.sha256(), 0
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    for data in iter(lambda: stream.read(1024 * 1024), b""):
                        keeper.guard();total += len(data)
                        check(total <= assignment["bytes"], "source_integrity")
                        digest.update(data)
                check(digest.hexdigest() == assignment["sha256"], "source_integrity")
            finally:
                os.close(fd)
            keeper.guard(); return path
        partial = directory / "download.partial"
        partial.unlink(missing_ok=True)
        digest, offset = hashlib.sha256(), 0
        fd = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            while offset < assignment["bytes"]:
                keeper.guard()
                part = self.client.call("source", dict(keeper.lease, offset=offset, length=1024 * 1024))
                data = base64.b64decode(part["data"], validate=True)
                check(part["id"] == assignment["id"] and part["revision"] == assignment["revision"] and
                      part["offset"] == offset and part["total_bytes"] == assignment["bytes"] and
                      part["sha256"] == assignment["sha256"] and len(data) == part["bytes"] and
                      0 < len(data) <= min(1024 * 1024, assignment["bytes"] - offset), "source_integrity")
                stream.write(data);digest.update(data);offset += len(data)
            stream.flush();os.fsync(stream.fileno())
        check(digest.hexdigest() == assignment["sha256"], "source_integrity")
        keeper.guard()
        os.rename(partial, path);sync_directory(directory)
        emit("source_ready", id=assignment["id"])
        return path

    def finish(self, pending, keeper):
        keeper.guard()
        receipt = self.client.call("complete", dict(keeper.lease, **pending["summary"]))
        check(receipt["id"] == keeper.lease["id"] and receipt["revision"] == keeper.lease["revision"] and
              receipt["state"] == "ready" and receipt["manifest_sha256"] == pending["summary"]["manifest_sha256"],
              "invalid_publication_receipt")
        # Do not call keeper.guard here: ready can concurrently fence heartbeat.
        emit("published", id=receipt["id"], revision=receipt["revision"], heartbeats=keeper.count)
        return receipt

    def once(self):
        pending = self.load()
        request = {k: pending[k] for k in ("claim_id", "pipeline_sha")}
        sent = time.monotonic()
        try:
            assignment = self.client.call("claim_media", request)
        except BackendError as exc:
            if exc.code != 410: raise
            # Explicit old identity lookup found nothing; never guess on a 409.
            self.cleanup(pending);return {"state": "expired_claim"}
        if assignment.get("idle") is True:
            check("assignment" not in pending, "allocated_claim_became_idle")
            self.state.clear();self.collect();return {"state": "idle"}
        identity = {k: assignment[k] for k in IDENTITY_FIELDS}
        check(identity["pipeline_kind"] == "media-parser-v1" and identity["pipeline_sha"] == self.config.pipeline and
              identity["parser_sha256"] == self.config.parser_sha and identity["mime"] in sandbox.MIMES and
              type(identity["bytes"]) is int and 0 < identity["bytes"] <= sandbox.MAX_SOURCE and
              type(assignment["remaining_seconds"]) is int and 0 <= assignment["remaining_seconds"] <= 3600 and
              type(assignment["live"]) is bool and assignment["state"] in {"running", "ready", "failed", "expired"} and
              re.fullmatch(r"att_[a-f0-9]{24,64}", identity["id"]) and type(identity["attempt"]) is int and identity["attempt"] > 0 and
              all(isinstance(identity[k], str) and sandbox.HEX.fullmatch(identity[k]) for k in ("revision", "lease_token", "sha256")),
              "invalid_assignment")
        check(pending.get("assignment", identity) == identity, "assignment_identity_changed")
        pending["assignment"] = identity;self.state.save(pending)
        emit("assigned", id=identity["id"], execution_id=pending["execution_id"], state=assignment["state"])
        if not assignment["live"]:
            if assignment["state"] == "ready" and "summary" in pending:
                receipt = self.client.call("complete", dict({k: identity[k] for k in LEASE_FIELDS}, **pending["summary"]))
                check(receipt["id"] == identity["id"] and receipt["revision"] == identity["revision"] and
                      receipt["state"] == "ready" and receipt["manifest_sha256"] == pending["summary"]["manifest_sha256"],
                      "invalid_publication_receipt")
            self.cleanup(pending, assignment)
            return {"id": identity["id"], "state": "expired" if assignment["state"] == "running" else assignment["state"]}
        keeper = LeaseKeeper(self.client, assignment, sent, self.stop).start()
        result = None
        try:
            self.stop_container(pending)  # Before touching any old parser files.
            if "summary" in pending:
                result = self.finish(pending, keeper)
            elif "failure" in pending:
                keeper.guard();self.client.call("fail", dict(keeper.lease, error_code=pending["failure"]))
                result = {"state": "failed", "id": identity["id"]}
            else:
                while True:
                    keeper.guard()
                    try:
                        self.client.call("reserve", keeper.lease);break
                    except BackendError as exc:
                        if exc.code != 413: raise
                        emit("waiting_capacity", id=identity["id"])
                        self.collect(keeper)
                        self.stop.wait(3)
                directory = self.execution(pending);private_directory(directory)
                # Only this execution's scratch is removed, after exact cleanup.
                for entry in directory.iterdir():
                    if re.fullmatch(r"spark-media-[a-z0-9_]+", entry.name):
                        check(entry.is_dir() and not entry.is_symlink(), "unexpected_execution_file")
                        shutil.rmtree(entry)
                output = directory / "output"
                source = {k: identity[k] for k in ("mime", "bytes", "sha256")}
                if output.exists() or output.is_symlink():
                    manifest = sandbox.validate_directory(output, source, self.config.parser_sha, keeper.cancelled)
                    emit("bundle_recovered", id=identity["id"])
                else:
                    original = self.download(assignment, directory, keeper)
                    keeper.guard()
                    emit("parser_start", id=identity["id"], execution_id=pending["execution_id"])
                    try:
                        manifest = sandbox.run(original, identity["mime"], output, image_id=self.config.image,
                            parser_sha=self.config.parser_sha, source_sha=identity["sha256"], model_root=self.config.model,
                            model_sha=self.config.model_sha, execution_id=pending["execution_id"], cancelled=keeper.cancelled,
                            timeout=max(1, math.floor(keeper.hard - time.monotonic())),
                            on_stage=lambda stage: emit("parser_stage", id=identity["id"], stage=stage))
                    except sandbox.SandboxError as exc:
                        if str(exc) not in INPUT_ERRORS: raise
                        keeper.guard()
                        pending["failure"] = str(exc) if str(exc) in {"unsupported_format", "encrypted_document", "invalid_media_timing"} else "parser_error"
                        self.state.save(pending)
                        self.client.call("fail", dict(keeper.lease, error_code=pending["failure"]))
                        result = {"state": "failed", "id": identity["id"]}
                if result is None:
                    emit("bundle_ready", id=identity["id"])
                    fragments = [{k: v for k, v in f.items() if k != "index"} for f in manifest["fragments"]]
                    artifacts = [{k: v for k, v in a.items() if k not in ("index", "file")} for a in manifest["artifacts"]]
                    hashes = lambda items: [hashlib.sha256(canonical(item)).hexdigest() for item in items]
                    for index, fragment in enumerate(fragments):
                        keeper.guard();accepted = self.client.call("stage", dict(keeper.lease, index=index, fragment=fragment))
                        check(accepted["sha256"] == hashlib.sha256(canonical(fragment)).hexdigest(), "invalid_stage_receipt")
                    for index, metadata in enumerate(artifacts):
                        keeper.guard()
                        parent = os.open(output, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                        try:
                            fd = os.open(f"image-{index:06d}.jpg", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                            try:
                                info = os.fstat(fd)
                                check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and info.st_size == metadata["bytes"], "bundle_changed")
                                with os.fdopen(fd, "rb", closefd=False) as stream:data = stream.read(sandbox.MAX_ARTIFACT + 1)
                            finally:os.close(fd)
                        finally:os.close(parent)
                        check(len(data) == metadata["bytes"] and hashlib.sha256(data).hexdigest() == metadata["sha256"], "bundle_changed")
                        accepted = self.client.call("stage_artifact", dict(keeper.lease, index=index, metadata=metadata,
                            data=base64.b64encode(data).decode()))
                        check(accepted["sha256"] == metadata["sha256"], "invalid_stage_receipt")
                        emit("artifact_staged", id=identity["id"], index=index)
                    summary = {"fragment_count": len(fragments), "artifact_count": len(artifacts), "coverage": manifest["coverage"]}
                    summary["manifest_sha256"] = hashlib.sha256(canonical({"schema": "room-media-publication-v1",
                        "source": source, "parser_sha256": self.config.parser_sha, "pipeline_sha256": self.config.pipeline,
                        "coverage": summary["coverage"], "fragments": hashes(fragments), "artifacts": hashes(artifacts)})).hexdigest()
                    pending["summary"] = summary;self.state.save(pending)
                    result = self.finish(pending, keeper)
        finally:
            keeper.close()
        self.cleanup(pending, dict(assignment, state=result["state"]))
        return result


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--config", type=Path, help="private operator JSON with approved fingerprints")
    cli.add_argument("--image-id")
    cli.add_argument("--model-dir", type=Path)
    cli.add_argument("--describe", action="store_true")
    cli.add_argument("--origin")
    cli.add_argument("--key-file", type=Path)
    cli.add_argument("--state-dir", type=Path)
    cli.add_argument("--once", action="store_true")
    args = cli.parse_args()
    approved = None
    if args.config:
        if any((args.image_id, args.model_dir, args.origin, args.key_file, args.state_dir)):
            cli.error("--config cannot be mixed with individual deployment settings")
        approved = operator_config(args.config)
        for name in ("image_id", "model_dir", "origin", "key_file", "state_dir"):
            setattr(args, name, approved[name])
    if not args.image_id or not args.model_dir:
        cli.error("--image-id and --model-dir, or --config, are required")
    config = Configuration(args.image_id, args.model_dir)
    if approved:
        check(config.pipeline == approved["pipeline_sha256"] and config.parser_sha == approved["parser_sha256"],
              "worker_configuration_changed")
    if args.describe:
        print(json.dumps(config.description()));return 0
    if not args.origin or not args.key_file or not args.state_dir:
        cli.error("--origin, --key-file and --state-dir are required")
    key = private_read(args.key_file, 257).decode().strip()
    check(32 <= len(key) <= 256 and not any(c in key for c in "\r\n"), "invalid_worker_credential")
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    state = State(args.state_dir)
    try:
        worker = Worker(Client(args.origin, key), state, config, stop)
        while not stop.is_set():
            try:
                result = worker.once();emit("result", **result)
                if args.once:return 0 if result["state"] in {"ready", "idle"} else 1
            except (BackendError, WorkerError, sandbox.SandboxError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
                reason = str(exc) if isinstance(exc, (WorkerError, sandbox.SandboxError)) and re.fullmatch(r"[a-z_]{1,64}", str(exc)) else "state_preserved"
                emit("deferred", error_type=type(exc).__name__, code=exc.code if isinstance(exc, BackendError) else reason)
                if args.once:return 1
            stop.wait(3)
        return 1
    finally:
        state.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (WorkerError, sandbox.SandboxError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
        emit("configuration_error", error_type=type(exc).__name__)
        sys.exit(1)
