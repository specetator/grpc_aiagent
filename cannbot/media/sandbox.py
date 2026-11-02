#!/usr/bin/env python3
"""Operator-owned Docker supervisor for the local media parser.

The caller keeps its Logic key and lease heartbeat OUTSIDE this process's
container. A cancellation callback can fence work on lease loss. This module
does not publish a revision or assert room access; those remain Logic's job.
"""
import hashlib
import ctypes
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time
import uuid

from .parser import (MAX_ARTIFACT, MAX_ARTIFACT_BYTES, MAX_ARTIFACTS, MAX_FRAGMENT,
                     MAX_FRAGMENTS, MAX_SOURCE, MAX_TEXT, MIMES, canonical)

MAX_MANIFEST = 20 * 1024 ** 2
MAX_BUNDLE = 152 * 1024 ** 2
HEX = re.compile(r"[0-9a-f]{64}")
IMAGE = re.compile(r"sha256:[0-9a-f]{64}")


class SandboxError(Exception):
    pass


def check(ok, code="invalid_parser_bundle"):
    if not ok:
        raise SandboxError(code)


def integer(value, minimum, maximum):
    return type(value) is int and minimum <= value <= maximum


def hash_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for part in iter(lambda: stream.read(1024 ** 2), b""):
            h.update(part)
    return h.hexdigest()


def model_fingerprint(root):
    """Hash an operator-provisioned model tree; no symlinks, credentials or cache."""
    root = Path(root)
    check(root.is_dir() and not root.is_symlink(), "invalid_model_directory")
    inventory = []
    for path in sorted(root.rglob("*")):
        info = path.lstat()
        check(not stat.S_ISLNK(info.st_mode), "invalid_model_directory")
        if path.is_dir(): continue
        check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, "invalid_model_directory")
        relative = path.relative_to(root).as_posix()
        check(relative in {"config.json", "model.bin", "tokenizer.json", "vocabulary.json",
                           "vocabulary.txt", "preprocessor_config.json"}, "unexpected_model_file")
        inventory.append({"file": relative, "bytes": info.st_size, "sha256": hash_file(path)})
    check(any(x["file"] == "model.bin" for x in inventory), "asr_model_unavailable")
    return hashlib.sha256(canonical(inventory)).hexdigest()


def snapshot(source, destination, expected_sha):
    check(isinstance(expected_sha, str) and HEX.fullmatch(expected_sha), "invalid_source_hash")
    fd = os.open(source, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= MAX_SOURCE,
              "invalid_source")
        h, size = hashlib.sha256(), 0
        with os.fdopen(fd, "rb", closefd=False) as src, open(destination, "xb") as dst:
            for part in iter(lambda: src.read(1024 ** 2), b""):
                size += len(part); check(size <= MAX_SOURCE, "source_limit")
                h.update(part); dst.write(part)
            dst.flush(); os.fsync(dst.fileno())
        check(size == info.st_size and h.hexdigest() == expected_sha, "source_hash_mismatch")
        os.chmod(destination, 0o444)
        return size
    finally:
        os.close(fd)


def anchor(value, artifact_count):
    check(type(value) is dict and bool(value))
    allowed = {"page", "section", "paragraph", "part", "artifact", "image",
               "timestamp_ms", "start_ms", "end_ms"}
    check(not value.keys() - allowed)
    for key, item in value.items():
        if key == "section": check(isinstance(item, str) and re.fullmatch(r"document|header\d+|footer\d+|footnotes|endnotes", item))
        else: check(integer(item, 0 if key in {"part", "artifact", "timestamp_ms", "start_ms", "end_ms"} else 1, 3_601_000))
    if "artifact" in value: check(value["artifact"] < artifact_count)
    if "start_ms" in value or "end_ms" in value:
        check("start_ms" in value and "end_ms" in value and value["start_ms"] <= value["end_ms"])


def jpeg_size(data):
    """Bounded header check, not a host-side image decode.

    Our encoder emits one 8-bit RGB baseline scan. Reject DNL, extra frames/scans,
    or forged SOF dimensions before native runtimes ever see the derived image.
    """
    check(4 <= len(data) <= MAX_ARTIFACT and data[:2] == b"\xff\xd8" and data[-2:] == b"\xff\xd9")
    position, dimensions = 2, None
    while position < len(data):
        check(data[position] == 0xFF)
        while position < len(data) and data[position] == 0xFF: position += 1
        check(position < len(data))
        marker = data[position]; position += 1
        check(marker in {0xC0, 0xC4, 0xDB, 0xDD, 0xFE, 0xDA} or 0xE0 <= marker <= 0xEF)
        check(position + 2 <= len(data))
        length = int.from_bytes(data[position:position + 2], "big")
        check(length >= 2 and position + length <= len(data))
        payload = data[position + 2:position + length]; position += length
        if marker == 0xC0:
            check(dimensions is None and len(payload) == 15 and payload[0] == 8 and payload[5] == 3)
            height, width = int.from_bytes(payload[1:3], "big"), int.from_bytes(payload[3:5], "big")
            check(1 <= width <= 1600 and 1 <= height <= 1600)
            check([payload[6], payload[9], payload[12]] == [1, 2, 3])
            dimensions = (width, height)
        elif marker == 0xDA:
            check(dimensions is not None and len(payload) == 10 and payload[0] == 3 and
                  [payload[1], payload[3], payload[5]] == [1, 2, 3] and payload[-3:] == b"\x00\x3f\x00")
            while position < len(data):
                position = data.find(b"\xff", position)
                check(position >= 0)
                while position < len(data) and data[position] == 0xFF: position += 1
                check(position < len(data))
                marker = data[position]; position += 1
                if marker == 0xD9:
                    check(position == len(data)); return dimensions
                check(marker == 0 or 0xD0 <= marker <= 0xD7)
            check(False)
    check(False)


def validate_coverage(manifest):
    value, artifacts, fragments = manifest["coverage"], manifest["artifacts"], manifest["fragments"]
    check(type(value) is dict)
    kind = value.get("kind")
    mime = manifest["source"]["mime"]
    if kind == "image":
        check(mime.startswith("image/") and value == {"kind": "image", "images": 1})
        check(len(artifacts) == 1 and artifacts[0]["anchor"] == {"image": 1})
    elif kind == "pdf":
        check(mime == "application/pdf" and set(value) == {"kind", "page_count", "rendered_pages"})
        count = value["page_count"]; check(integer(count, 1, 200) and value["rendered_pages"] == count)
        check(len(artifacts) == count and [a["anchor"] for a in artifacts] == [{"page": i + 1} for i in range(count)])
        check(all(integer(f["anchor"].get("page"), 1, count) for f in fragments))
    elif kind == "docx":
        check(mime.endswith("wordprocessingml.document") and set(value) == {"kind", "sections", "paragraphs"})
        sections = value["sections"]
        check(type(sections) is list and 0 < len(sections) <= 2000 and "document" in sections)
        check(all(isinstance(s, str) and re.fullmatch(r"document|header\d+|footer\d+|footnotes|endnotes", s) for s in sections))
        check(len(set(sections)) == len(sections) and integer(value["paragraphs"], 0, 100000))
        check(all(x["anchor"].get("section") in sections and
                  integer(x["anchor"].get("paragraph"), 1, value["paragraphs"]) for x in artifacts + fragments))
    elif kind in {"audio", "video"}:
        check(mime.startswith(kind + "/") and set(value) == ({"kind", "duration_ms", "container_duration_ms", "source_origin_ms", "audio"} |
                                                              ({"video"} if kind == "video" else set())))
        duration = value["duration_ms"]; check(integer(duration, 1, 3_600_000))
        check(integer(value["source_origin_ms"], -9_000_000_000_000, 9_000_000_000_000))
        check(integer(value["container_duration_ms"], 1, 9_000_000_000_000))
        audio = value["audio"]
        check(audio is not None or kind == "video")
        if audio is not None:
            check(type(audio) is dict and set(audio) == {"decoded_samples", "sample_rate", "duration_ms", "offset_ms",
                                                        "language", "segments", "speaker_labels", "track_end_ms"})
            check(audio["sample_rate"] == 16000 and integer(audio["decoded_samples"], 1, 57_616_000))
            check(integer(audio["duration_ms"], 1, 3_601_000) and
                  abs(audio["decoded_samples"] / 16 - audio["duration_ms"]) <= 1)
            check(integer(audio["offset_ms"], 0, duration) and audio["offset_ms"] + audio["duration_ms"] <= duration + 1000)
            check(integer(audio["track_end_ms"], 1, duration + 1000) and
                  abs(audio["track_end_ms"] - audio["offset_ms"] - audio["duration_ms"]) <= 1000)
            check(integer(audio["segments"], 0, MAX_FRAGMENTS) and audio["speaker_labels"] is False and
                  isinstance(audio["language"], str) and re.fullmatch(r"[a-z]{2,3}", audio["language"]))
            check(all(audio["offset_ms"] <= f["anchor"].get("start_ms", -1) <=
                      f["anchor"].get("end_ms", -1) <= audio["offset_ms"] + audio["duration_ms"] + 500
                      for f in fragments if f["kind"] == "transcript"))
        if kind == "audio": check(not artifacts and all(f["kind"] == "transcript" for f in fragments))
        else:
            video = value["video"]
            check(type(video) is dict and set(video) == {"frames", "sample_interval_ms", "first_frame_ms", "last_frame_ms", "track_end_ms"})
            stamps = [a["anchor"].get("timestamp_ms") for a in artifacts]
            check(stamps and all(integer(t, 0, duration + 1000) for t in stamps))
            check(video["frames"] == len(stamps) and video["sample_interval_ms"] == 5000 and
                  video["first_frame_ms"] == stamps[0] and video["last_frame_ms"] == stamps[-1])
            check(integer(video["track_end_ms"], 1, duration + 1000))
            check(all(0 < b - a <= 6000 for a, b in zip(stamps, stamps[1:])) and stamps[-1] >= video["track_end_ms"] - 6000)
    else: check(False)


def validate_directory(destination, source, parser_sha, cancelled=None):
    """Validate an installed execution bundle again after worker restart.

    No parser rerun and no second archive. Every read is relative to one open
    directory descriptor and rejects links, devices, growth and extra entries.
    """
    directory = os.open(destination, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        seen = set(os.listdir(directory))
        check(0 < len(seen) <= MAX_ARTIFACTS + 1)
        def read(name, maximum):
            check(not cancelled or not cancelled(), "parser_cancelled")
            fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
            try:
                info = os.fstat(fd)
                check(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= maximum)
                with os.fdopen(fd, "rb", closefd=False) as stream:
                    data = stream.read(maximum + 1)
                check(len(data) == info.st_size)
                return data
            finally:
                os.close(fd)
        check("manifest.json" in seen)
        def pairs(items):
            result = {}
            for key, value in items:
                check(key not in result); result[key] = value
            return result
        manifest = json.loads(read("manifest.json", MAX_MANIFEST), object_pairs_hook=pairs,
                              parse_constant=lambda _: check(False))
        check(type(manifest) is dict and set(manifest) == {"schema", "source", "parser_sha256", "coverage",
                                                          "fragments", "artifacts", "text_bytes", "artifact_bytes"})
        check(manifest["schema"] == "room-media-v1" and manifest["source"] == source and
              manifest["parser_sha256"] == parser_sha)
        artifacts, fragments = manifest["artifacts"], manifest["fragments"]
        check(type(artifacts) is list and len(artifacts) <= MAX_ARTIFACTS)
        check(type(fragments) is list and len(fragments) <= MAX_FRAGMENTS)
        names, size, text_size = {"manifest.json"}, 0, 0
        for index, artifact in enumerate(artifacts):
            check(type(artifact) is dict and set(artifact) == {"index", "file", "mime", "bytes", "sha256",
                                                              "width", "height", "anchor"})
            name = f"image-{index:06d}.jpg"
            check(type(artifact["index"]) is int and artifact["index"] == index and artifact["file"] == name)
            check(artifact["mime"] == "image/jpeg" and integer(artifact["bytes"], 1, MAX_ARTIFACT) and
                  integer(artifact["width"], 1, 1600) and integer(artifact["height"], 1, 1600))
            check(name in seen)
            data = read(name, MAX_ARTIFACT)
            check(len(data) == artifact["bytes"] and hashlib.sha256(data).hexdigest() == artifact["sha256"])
            check(jpeg_size(data) == (artifact["width"], artifact["height"]))
            anchor(artifact["anchor"], len(artifacts))
            names.add(name); size += len(data)
        for index, fragment in enumerate(fragments):
            check(type(fragment) is dict and set(fragment) == {"index", "kind", "text", "anchor"})
            check(type(fragment["index"]) is int and fragment["index"] == index and
                  fragment["kind"] in {"text", "ocr", "transcript"} and isinstance(fragment["text"], str))
            raw = fragment["text"].encode("utf-8")
            check(0 < len(raw) <= MAX_FRAGMENT and b"\x00" not in raw)
            text_size += len(raw); anchor(fragment["anchor"], len(artifacts))
        check(names == seen and size == manifest["artifact_bytes"] and size <= MAX_ARTIFACT_BYTES and
              text_size == manifest["text_bytes"] and text_size <= MAX_TEXT)
        validate_coverage(manifest)
        # Logic must revalidate these independently before making a publication.
        return manifest
    finally:
        os.close(directory)


def validate_bundle(bundle, destination, source, parser_sha):
    """Never extractall: parser-controlled paths, links and extras are rejected."""
    destination = Path(destination)
    destination.mkdir(mode=0o700, exist_ok=False)
    try:
        seen = set()
        with tarfile.open(bundle, mode="r|") as archive:
            for entry in archive:
                check(entry.name not in seen and entry.isfile() and not entry.pax_headers)
                check(entry.name == "manifest.json" or re.fullmatch(r"image-\d{6}\.jpg", entry.name))
                check(integer(entry.size, 1, MAX_MANIFEST if entry.name == "manifest.json" else MAX_ARTIFACT))
                seen.add(entry.name); check(len(seen) <= MAX_ARTIFACTS + 1)
                stream = archive.extractfile(entry); check(stream is not None)
                data = stream.read(entry.size + 1); check(len(data) == entry.size)
                with open(destination / entry.name, "xb") as target:
                    target.write(data)
                    target.flush(); os.fsync(target.fileno())
        return validate_directory(destination, source, parser_sha)
    except Exception:
        shutil.rmtree(destination)
        raise


def cleanup_execution(execution_id, directory, image_id, model_root):
    """Recover only the one execution recorded in this worker's private state.

    Name alone never grants cleanup. A daemon failure is not absence. After
    verifying the full identity/mount set, removal uses the exact inspected ID.
    """
    check(isinstance(execution_id, str) and re.fullmatch(r"[0-9a-f]{32}", execution_id), "invalid_execution_id")
    check(IMAGE.fullmatch(image_id), "unapproved_image_reference")
    name = "spark-media-" + execution_id
    result = subprocess.run(["docker", "inspect", "--type", "container", name], capture_output=True, timeout=15)
    if result.returncode:
        # Docker explicitly reports this container missing; connection errors
        # and arbitrary stderr are not permission to discard execution state.
        expected = ("Error: No such container: " + name).encode()
        expected_object = ("Error: No such object: " + name).encode()
        daemon_missing = ("Error response from daemon: No such container: " + name).encode()
        check(result.stderr.strip() in (expected, expected_object, daemon_missing), "parser_cleanup_unavailable")
        return False
    values = json.loads(result.stdout)
    check(type(values) is list and len(values) == 1, "parser_identity_mismatch")
    item = values[0]; config = item["Config"]; host = item["HostConfig"]
    labels = config.get("Labels") or {}
    check(item["Name"] == "/" + name and item["Image"] == image_id and HEX.fullmatch(item["Id"]) and
          labels.get("spark.media.parser") == "1" and labels.get("spark.media.execution") == execution_id and
          config["User"] == "65534:65534" and host["NetworkMode"] == "none" and host["ReadonlyRootfs"] is True,
          "parser_identity_mismatch")
    mounts = item["Mounts"]
    expected_targets = {"/input/source"} | ({"/model"} if model_root is not None else set())
    # tmpfs is represented by HostConfig.Tmpfs on Docker versions used here;
    # when present in Mounts, it still must be one of the exact scratch targets.
    bindings = [m for m in mounts if m["Type"] != "tmpfs"]
    check(len(bindings) == len(expected_targets) and {m["Destination"] for m in bindings} == expected_targets,
          "parser_identity_mismatch")
    check(all(m["Destination"] in {"/tmp", "/output"} for m in mounts if m["Type"] == "tmpfs"),
          "parser_identity_mismatch")
    directory = Path(directory).absolute()
    for mount in bindings:
        check(mount["Type"] == "bind" and mount["RW"] is False, "parser_identity_mismatch")
        source = Path(mount["Source"])
        if mount["Destination"] == "/model":
            check(source == Path(model_root).resolve(), "parser_identity_mismatch")
        else:
            check(source.name == "source" and source.parent.parent == directory and
                  re.fullmatch(r"spark-media-[a-z0-9_]+", source.parent.name), "parser_identity_mismatch")
    removed = subprocess.run(["docker", "rm", "-f", "-v", item["Id"]], capture_output=True, timeout=20)
    check(removed.returncode == 0 or b"No such container" in removed.stderr, "parser_cleanup_failed")
    return True


def run(source, mime, output, *, image_id, parser_sha, source_sha, model_root=None,
        model_sha=None, cancelled=None, timeout=3600, execution_id=None, on_stage=None):
    """Parse a verified snapshot. Return only after process success and validation.

    image/model config is fixed by the operator, never supplied by a room member.
    Caller must keep the approved model tree immutable for the process lifetime.
    """
    check(mime in MIMES, "unsupported_format")
    check(isinstance(image_id, str) and IMAGE.fullmatch(image_id), "unapproved_image_reference")
    check(isinstance(parser_sha, str) and HEX.fullmatch(parser_sha), "invalid_parser_hash")
    check(integer(timeout, 1, 7200), "invalid_timeout")
    if model_root is not None:
        model_root = Path(model_root).resolve(strict=True)
        check(model_fingerprint(model_root) == model_sha, "model_hash_mismatch")
    if mime.startswith("audio/"):
        check(model_root is not None, "asr_model_unavailable")
    execution_id = execution_id or uuid.uuid4().hex
    check(isinstance(execution_id, str) and re.fullmatch(r"[0-9a-f]{32}", execution_id), "invalid_execution_id")
    name = "spark-media-" + execution_id
    output = Path(output).absolute()
    parent = output.parent.lstat()
    check(stat.S_ISDIR(parent.st_mode) and parent.st_uid == os.getuid() and not parent.st_mode & 0o022,
          "unsafe_output_parent")
    with tempfile.TemporaryDirectory(prefix="spark-media-", dir=output.parent) as temporary:
        temporary = Path(temporary)
        original = temporary / "source"
        source_size = snapshot(source, original, source_sha)
        check("," not in str(original) and (model_root is None or "," not in str(model_root)), "invalid_mount_path")
        check(not cancelled or not cancelled(), "parser_cancelled")
        argv = ["docker", "create", "--pull", "never", "--name", name, "--network", "none", "--read-only",
                "--label", "spark.media.parser=1", "--label", "spark.media.execution=" + execution_id,
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--pids-limit", "64",
                "--memory", "4g", "--memory-swap", "4g", "--cpus", "2", "--user", "65534:65534",
                "--log-driver", "none", "--init", "--tmpfs", "/tmp:rw,noexec,nosuid,size=384m,mode=1777",
                "--tmpfs", "/output:rw,noexec,nosuid,size=160m,mode=1777",
                "--mount", f"type=bind,source={original},target=/input/source,readonly"]
        if model_root is not None:
            argv += ["--mount", f"type=bind,source={model_root},target=/model,readonly"]
        argv += [image_id, "--mime", mime, "--deadline-seconds", str(timeout)]
        # Establish the container handle before attaching/running, so cancellation
        # cannot race `docker run` creating a container after cleanup saw no handle.
        process, created_id = None, None
        start, totals, stderr, progress = time.monotonic(), [0, 0], bytearray(), bytearray()
        selector = selectors.DefaultSelector()
        bundle = temporary / "bundle.tar"
        try:
            created = subprocess.run(argv, capture_output=True, timeout=30)
            check(created.returncode == 0, "parser_setup_failed")
            created_id = created.stdout.decode().strip()
            check(re.fullmatch(r"[0-9a-f]{64}", created_id), "parser_setup_failed")
            check(not cancelled or not cancelled(), "parser_cancelled")
            process = subprocess.Popen(["docker", "start", "--attach", created_id], stdin=subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            selector.register(process.stdout, selectors.EVENT_READ, 0)
            selector.register(process.stderr, selectors.EVENT_READ, 1)
            with bundle.open("xb") as stream:
                while selector.get_map():
                    check(time.monotonic() - start < timeout, "parser_timeout")
                    check(not cancelled or not cancelled(), "parser_cancelled")
                    for key, _ in selector.select(0.25):
                        data = os.read(key.fileobj.fileno(), 65536)
                        if not data:
                            selector.unregister(key.fileobj); continue
                        totals[key.data] += len(data)
                        check(totals[key.data] <= (MAX_BUNDLE if key.data == 0 else 65536), "parser_output_limit")
                        if key.data == 0: stream.write(data)
                        else:
                            stderr.extend(data); progress.extend(data)
                            while b"\n" in progress:
                                line, _, rest = progress.partition(b"\n"); progress = bytearray(rest)
                                if line == b'{"stage":"asr_running"}' and on_stage:
                                    on_stage("asr_running")  # progress only; never grants authority/completion
            code = process.wait(timeout=max(1, timeout - (time.monotonic() - start)))
            if code:
                allowed = {"decoder_timeout", "decoder_failed", "decoder_output_limit", "invalid_media_timing",
                           "ambiguous_audio_timing", "invalid_source", "unsupported_format", "invalid_extracted_text", "text_limit",
                           "fragment_limit", "artifact_limit", "image_pixel_limit", "animated_image_unsupported",
                           "mime_mismatch", "image_byte_limit", "artifact_total_limit", "encrypted_document", "document_page_limit",
                           "document_entry_limit", "duplicate_document_entry", "unsafe_document_entry",
                           "document_expansion_limit", "invalid_docx", "embedded_object_unsupported",
                           "external_document_reference", "media_duration_limit", "media_stream_limit",
                           "missing_media_frames", "asr_model_unavailable", "invalid_decoded_audio",
                           "invalid_transcript_timing", "incomplete_audio_decode", "missing_media_stream",
                           "multiple_media_tracks_unsupported", "invalid_frame_manifest", "video_sampling_gap",
                           "incomplete_video_decode", "output_not_empty", "manifest_limit", "malformed_media"}
                try: error = json.loads(stderr.decode().splitlines()[-1])["error"]
                except (ValueError, IndexError, KeyError, TypeError): error = None
                raise SandboxError(error if error in allowed else "parser_failed")
            check(not cancelled or not cancelled(), "parser_cancelled")
            validated = temporary / "validated"
            result = validate_bundle(bundle, validated, {"mime": mime, "bytes": source_size, "sha256": source_sha}, parser_sha)
        finally:
            selector.close()
            cleanup = None
            try:
                if created_id is not None:
                    cleanup = subprocess.run(["docker", "rm", "-f", "-v", created_id], capture_output=True, timeout=20)
            except (subprocess.TimeoutExpired, OSError):
                raise SandboxError("parser_cleanup_failed") from None
            finally:
                if process is not None:
                    try:
                        if process.poll() is None: process.kill()
                        process.wait(timeout=10)
                    finally:
                        process.stdout.close(); process.stderr.close()
            check(cleanup is None or cleanup.returncode == 0 or b"No such container" in cleanup.stderr, "parser_cleanup_failed")
        check(not cancelled or not cancelled(), "parser_cancelled")
        directory = os.open(validated, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
        # Same-filesystem atomic installation, no clobber of an existing attempt.
        libc = ctypes.CDLL(None, use_errno=True)
        result_code = libc.renameat2(-100, os.fsencode(validated), -100, os.fsencode(output), 1)
        if result_code:
            raise OSError(ctypes.get_errno(), "cannot install parser output")
        directory = os.open(output.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(directory)
        finally: os.close(directory)
        return result
