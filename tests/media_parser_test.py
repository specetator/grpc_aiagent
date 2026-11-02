#!/usr/bin/env python3
"""Bundle boundary tests need stdlib only. Real decoder cases are opt-in."""
import io
import hashlib
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
import unittest
from unittest import mock
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cannbot.media import sandbox
from cannbot.media.parser import canonical
from cannbot.media import parser as media_parser
from cannbot.media import provision_asr


class BundleTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.source = {"mime": "image/png", "bytes": 20, "sha256": "a" * 64}
        self.manifest = {"schema": "room-media-v1", "source": self.source, "parser_sha256": "b" * 64,
                         "coverage": {"kind": "image"}, "fragments": [], "artifacts": [],
                         "text_bytes": 0, "artifact_bytes": 0}

    def tearDown(self): self.temporary.cleanup()

    def bundle(self, entries):
        path = self.root / "bundle.tar"
        with tarfile.open(path, "w", format=tarfile.USTAR_FORMAT) as archive:
            for name, data, kind in entries:
                info = tarfile.TarInfo(name); info.size = len(data); info.type = kind
                if kind == tarfile.SYMTYPE: info.linkname = "/etc/passwd"
                archive.addfile(info, io.BytesIO(data))
        return path

    def validate(self, entries):
        return sandbox.validate_bundle(self.bundle(entries), self.root / "output", self.source, "b" * 64)

    def test_traversal_links_duplicate_and_unlisted_rejected(self):
        for name, kind in [("../outside", tarfile.REGTYPE), ("/outside", tarfile.REGTYPE),
                           ("image-000000.jpg", tarfile.SYMTYPE), ("extra.txt", tarfile.REGTYPE),
                           ("manifest.json", tarfile.REGTYPE)]:
            with self.subTest(name=name, kind=kind), self.assertRaises(sandbox.SandboxError):
                self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE), (name, b"x", kind)])
            self.assertFalse((self.root / "output").exists())

    def test_source_and_code_identity(self):
        for field, value in [("source", dict(self.source, sha256="c" * 64)), ("parser_sha256", "c" * 64)]:
            broken = dict(self.manifest, **{field: value})
            with self.assertRaises(sandbox.SandboxError):
                self.validate([("manifest.json", canonical(broken), tarfile.REGTYPE)])

    def test_no_model_permissions_in_payload(self):
        self.manifest["actor_user_id"] = 1
        with self.assertRaises(sandbox.SandboxError):
            self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE)])

    def test_source_symlink_hardlink_hash(self):
        source = self.root / "input"; source.write_bytes(b"original")
        link = self.root / "link"; link.symlink_to(source)
        with self.assertRaises(OSError): sandbox.snapshot(link, self.root / "copy", sandbox.hash_file(source))
        link.unlink(); os.link(source, link)
        with self.assertRaises(sandbox.SandboxError): sandbox.snapshot(source, self.root / "copy", sandbox.hash_file(source))
        link.unlink()
        with self.assertRaises(sandbox.SandboxError): sandbox.snapshot(source, self.root / "copy", "0" * 64)

    def test_model_tree_rejects_links_and_extra_files(self):
        model = self.root / "model"; model.mkdir(); (model / "model.bin").write_bytes(b"weights")
        before = sandbox.model_fingerprint(model)
        (model / "model.bin").write_bytes(b"different")
        self.assertNotEqual(before, sandbox.model_fingerprint(model))
        (model / "credentials.json").write_text("not-real")
        with self.assertRaises(sandbox.SandboxError): sandbox.model_fingerprint(model)

    def test_missing_rendered_page_rejected(self):
        source = dict(self.source, mime="application/pdf")
        self.source = source; self.manifest["source"] = source
        self.manifest["coverage"] = {"kind": "pdf", "page_count": 2, "rendered_pages": 2}
        with self.assertRaises(sandbox.SandboxError):
            self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE)])

    def test_valid_text_bundle(self):
        self.source["mime"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        self.manifest["coverage"] = {"kind": "docx", "sections": ["document"], "paragraphs": 1}
        self.manifest["fragments"] = [{"index": 0, "kind": "text", "text": "hello",
                                       "anchor": {"section": "document", "paragraph": 1, "part": 0}}]
        self.manifest["text_bytes"] = 5
        actual = self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE)])
        self.assertEqual(actual, self.manifest)
        self.assertEqual(sandbox.validate_directory(self.root / "output", self.source, "b" * 64), self.manifest)

    def test_restored_directory_rejects_links_extra_and_changed_source(self):
        self.test_valid_text_bundle()
        out = self.root / "output"
        with self.assertRaises(sandbox.SandboxError):
            sandbox.validate_directory(out, dict(self.source, sha256="c" * 64), "b" * 64)
        (out / "extra").write_text("unlisted")
        with self.assertRaises(sandbox.SandboxError):sandbox.validate_directory(out, self.source, "b" * 64)
        (out / "extra").unlink()
        raw = (out / "manifest.json").read_bytes()
        target = self.root / "target";target.write_bytes(raw)
        (out / "manifest.json").unlink();(out / "manifest.json").symlink_to(target)
        with self.assertRaises(OSError):sandbox.validate_directory(out, self.source, "b" * 64)
        (out / "manifest.json").unlink();os.link(target, out / "manifest.json")
        with self.assertRaises(sandbox.SandboxError):sandbox.validate_directory(out, self.source, "b" * 64)
        (out / "manifest.json").unlink();(out / "manifest.json").mkdir()
        with self.assertRaises(sandbox.SandboxError):sandbox.validate_directory(out, self.source, "b" * 64)

    def test_provisioning_lock_and_stale_private_staging(self):
        directory = self.root / "weights"
        staging = self.root / ".weights.provision"; staging.mkdir(mode=0o700)
        stale = staging / (".model.bin." + "a" * 32 + ".partial"); stale.write_bytes(b"interrupted")
        entered, release = threading.Event(), threading.Event()
        failures = []
        def download(*_):
            self.assertFalse(stale.exists())
            entered.set(); self.assertTrue(release.wait(5)); return {}
        def first():
            try: provision_asr.provision(directory)
            except Exception as exc: failures.append(exc)
        with mock.patch.object(provision_asr, "_provision", download):
            worker = threading.Thread(target=first); worker.start()
            try:
                self.assertTrue(entered.wait(5))
                inode = (staging / "lock").stat().st_ino
                with self.assertRaises(BlockingIOError): provision_asr.provision(directory)
                self.assertEqual((staging / "lock").stat().st_ino, inode)
            finally: release.set(); worker.join(5)
        self.assertFalse(failures)
        self.assertTrue((staging / "lock").is_file())

    def test_model_download_resumes_truncated_response_and_rejects_wrong_range(self):
        def response(body, content_range):
            value = io.BytesIO(body)
            value.status = 206
            value.headers = {"Content-Range": content_range}
            return value
        output = io.BytesIO()
        with mock.patch.object(provision_asr.urllib.request, "urlopen", side_effect=[
                response(b"abc", "bytes 0-5/6"), response(b"def", "bytes 3-5/6")]) as opened:
            provision_asr.download("https://example.invalid/fixed-model", output, 6)
        self.assertEqual(output.getvalue(), b"abcdef")
        self.assertEqual(opened.call_args_list[1].args[0].get_header("Range"), "bytes=3-5")
        output = io.BytesIO()
        with mock.patch.object(provision_asr.urllib.request, "urlopen", side_effect=[
                response(b"abc", "bytes 0-5/6"), response(b"abcdef", "bytes 0-5/6")]):
            with self.assertRaisesRegex(ValueError, "range mismatch"):
                provision_asr.download("https://example.invalid/fixed-model", output, 6)
        self.assertEqual(output.getvalue(), b"abc")

    def test_audio_overlapping_packets_rejected_video_reorder_allowed(self):
        parser = media_parser.Parser.__new__(media_parser.Parser)
        parser.source = Path("/input/source")
        for packets in (b"0.0,0.02\n0.0,0.02\n", b"0.04,0.02\n0.02,0.02\n", b"0.0,0.0\n"):
            with mock.patch.object(media_parser, "command", return_value=(packets, b"")):
                with self.assertRaisesRegex(media_parser.ParseError, "ambiguous_audio_timing"):
                    parser.track_end("a:0")
                self.assertGreaterEqual(parser.track_end("0"), 0)  # B frame packet PTS can reorder

    def test_jpeg_actual_dimensions_truncated_and_additional_markers(self):
        # Header-boundary fixture, not a claim that this synthetic entropy decodes.
        def header(width, height):
            sof = b"\x08" + height.to_bytes(2, "big") + width.to_bytes(2, "big") + b"\x03\x01\x22\x00\x02\x11\x01\x03\x11\x01"
            sos = b"\x03\x01\x00\x02\x11\x03\x11\x00\x3f\x00"
            return b"\xff\xd8\xff\xc0\x00\x11" + sof + b"\xff\xda\x00\x0c" + sos + b"\x01\xff\x00\x02\xff\xd9"
        valid = header(640, 480)
        self.assertEqual(sandbox.jpeg_size(valid), (640, 480))
        self.manifest["coverage"] = {"kind": "image", "images": 1}
        self.manifest["artifacts"] = [{"index": 0, "file": "image-000000.jpg", "mime": "image/jpeg",
                                       "bytes": len(valid), "sha256": hashlib.sha256(valid).hexdigest(),
                                       "width": 1, "height": 1, "anchor": {"image": 1}}]
        self.manifest["artifact_bytes"] = len(valid)
        with self.assertRaises(sandbox.SandboxError):
            self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE),
                           ("image-000000.jpg", valid, tarfile.REGTYPE)])
        for data in (header(64000, 480), valid[:6] + b"\xff\xd9", valid[:-2],
                     valid[:-2] + b"\xff\xdc\x00\x04\xff\xff\xff\xd9", b"GIF89a\xff\xd9"):
            with self.assertRaises(sandbox.SandboxError): sandbox.jpeg_size(data)

    def test_duplicate_json_and_unlisted_artifact(self):
        self.source["mime"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        self.manifest["coverage"] = {"kind": "docx", "sections": ["document"], "paragraphs": 0}
        raw = canonical(self.manifest)[:-1] + b',"schema":"room-media-v1"}'
        with self.assertRaises(sandbox.SandboxError):
            self.validate([("manifest.json", raw, tarfile.REGTYPE)])
        with self.assertRaises(sandbox.SandboxError):
            self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE),
                           ("image-000000.jpg", b"unlisted", tarfile.REGTYPE)])

    def test_negative_source_clock_is_independent_of_clip_duration(self):
        # Coverage contract only; the real regression fixture is rejected, and
        # must not be reported as a successful valid-negative-PTS decoder case.
        self.manifest["source"]["mime"] = "audio/wav"
        self.manifest["coverage"] = {"kind": "audio", "duration_ms": 2000, "container_duration_ms": 2000,
            "source_origin_ms": -5000, "audio": {"decoded_samples": 32000, "sample_rate": 16000,
            "duration_ms": 2000, "offset_ms": 0, "track_end_ms": 2000, "language": "en",
            "segments": 0, "speaker_labels": False}}
        self.assertEqual(self.validate([("manifest.json", canonical(self.manifest), tarfile.REGTYPE)]), self.manifest)


if __name__ == "__main__": unittest.main()
