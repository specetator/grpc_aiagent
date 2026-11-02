#!/usr/bin/env python3
"""Actual isolated decoders/ASR, never a mock model or room/browser E2E.

Generate fixtures with media_parser_fixtures.py in the fixtures Docker target.
Use --long-audio to transcribe the entire synthetic 20 minute meeting.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import uuid
import wave

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from cannbot.media import sandbox
from cannbot.media.parser import DOCX


def main():
    cli = argparse.ArgumentParser()
    cli.add_argument("--image", required=True)
    cli.add_argument("--fixtures", type=Path, required=True)
    cli.add_argument("--model", type=Path)
    cli.add_argument("--long-audio", action="store_true")
    cli.add_argument("--out", type=Path, required=True)
    opts = cli.parse_args()
    root = Path(__file__).resolve().parents[1]
    image_id = subprocess.check_output(["docker", "image", "inspect", opts.image, "--format", "{{.Id}}"], text=True).strip()
    parser_sha = sandbox.hash_file(root / "cannbot/media/parser.py")
    model_sha = sandbox.model_fingerprint(opts.model) if opts.model else None
    report = {"scope": "real isolated local decoders and CPU ASR; synthetic fixtures; no Logic/native Agent/Web integration",
              "image_id": image_id, "parser_sha256": parser_sha, "model_sha256": model_sha,
              "source_sha256": {path: sandbox.hash_file(root / path) for path in
                                ("cannbot/media/parser.py", "cannbot/media/sandbox.py", "cannbot/media/provision_asr.py",
                                 "cannbot/media/requirements.txt", "cannbot/media/Dockerfile",
                                 "tests/media_parser_e2e.py", "tests/media_parser_fixtures.py", "tests/media_parser_test.py")},
              "cases": [], "status": "running"}
    opts.out.parent.mkdir(parents=True, exist_ok=True)
    def record(): opts.out.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    try:
        with tempfile.TemporaryDirectory(prefix="spark-parser-e2e-") as temporary:
            temporary = Path(temporary)
            outputs = {}
            def parse(name, mime, expected_error=None, cancel=None, timeout=3600, stage=None, execution=None):
                source = opts.fixtures / name
                output = temporary / ("case-" + str(len(report["cases"]))); outputs[name] = output
                start = time.monotonic()
                row = {"file": name, "source_sha256": sandbox.hash_file(source), "bytes": source.stat().st_size}
                print("Parsing " + name, flush=True)
                try:
                    manifest = sandbox.run(source, mime, output, image_id=image_id, parser_sha=parser_sha,
                                           source_sha=row["source_sha256"], model_root=opts.model, model_sha=model_sha,
                                           cancelled=cancel, timeout=timeout, on_stage=stage, execution_id=execution)
                except sandbox.SandboxError as exc:
                    if str(exc) != expected_error: raise
                    if output.exists(): raise AssertionError("failed parser installed output")
                    row.update({"expected_rejection": str(exc), "passed": True, "seconds": round(time.monotonic() - start, 3)})
                    report["cases"].append(row); record(); return None
                if expected_error: raise AssertionError("malformed input unexpectedly accepted")
                row.update({"coverage": manifest["coverage"], "fragments": len(manifest["fragments"]),
                            "artifacts": len(manifest["artifacts"]), "manifest_sha256": sandbox.hash_file(output / "manifest.json"),
                            "seconds": round(time.monotonic() - start, 3)})
                report["cases"].append(row); record()
                return manifest, row

            manifest, row = parse("diagram.png", "image/png")
            assert len(manifest["artifacts"]) == 1 and "ORANGE QUEUE" in " ".join(f["text"] for f in manifest["fragments"])
            row["passed"] = True
            parse("diagram.png", "image/jpeg", "mime_mismatch")
            manifest, row = parse("design.pdf", "application/pdf")
            assert manifest["coverage"]["page_count"] == 3
            assert any("Kafka delivery" in f["text"] and f["anchor"]["page"] == 1 for f in manifest["fragments"])
            assert any("ORANGE QUEUE" in f["text"] and f["anchor"]["page"] == 3 for f in manifest["fragments"])
            assert [a["anchor"]["page"] for a in manifest["artifacts"]] == [1, 2, 3]
            row["passed"] = True
            manifest, row = parse("notes.docx", DOCX)
            assert len(manifest["artifacts"]) == 1 and any("负责人" in f["text"] for f in manifest["fragments"])
            assert manifest["coverage"]["paragraphs"] == 2
            row["passed"] = True
            for name, mime, error in [("encrypted.pdf", "application/pdf", "encrypted_document"),
                                      ("broken.pdf", "application/pdf", "malformed_media"),
                                      ("unsafe.docx", DOCX, "unsafe_document_entry"),
                                      ("external.docx", DOCX, "external_document_reference"),
                                      ("entity.docx", DOCX, "malformed_media"),
                                      ("animated.gif", "image/gif", "animated_image_unsupported")]:
                parse(name, mime, error)
            # Cancellation uses a real created/attached container; no output survives.
            parse("speech.wav", "audio/wav", "parser_cancelled" if opts.model else "asr_model_unavailable", cancel=lambda: True)
            if opts.model:
                manifest, row = parse("speech.wav", "audio/wav")
                transcript = " ".join(f["text"] for f in manifest["fragments"]).lower()
                assert "red" in transcript and "green" in transcript
                row["transcript"] = transcript; row["passed"] = True
                manifest, row = parse("incident.mp4", "video/mp4")
                assert len(manifest["artifacts"]) >= 2
                assert 1800 <= manifest["coverage"]["audio"]["offset_ms"] <= 2200
                assert manifest["coverage"]["video"]["first_frame_ms"] == 0
                assert manifest["coverage"]["video"]["last_frame_ms"] >= 5000
                assert any(f["kind"] == "transcript" and "red" in f["text"].lower() and
                           f["anchor"]["start_ms"] >= 1800 for f in manifest["fragments"])
                # Independently decode the actual derived frames in a bounded
                # container: timestamps/counts alone cannot prove distinct frames.
                pixels = json.loads(subprocess.check_output([
                    "docker", "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
                    "--security-opt", "no-new-privileges", "--memory", "256m", "--pids-limit", "32",
                    "--user", f"{os.getuid()}:{os.getgid()}", "--mount",
                    f"type=bind,source={outputs['incident.mp4']},target=/frames,readonly", "--entrypoint", "python3", image_id,
                    "-c", "import json;from PIL import Image;from pathlib import Path;"
                    "images=[Image.open(p).convert('RGB') for p in sorted(Path('/frames').glob('image-*.jpg'))];"
                    "print(json.dumps([im.getpixel((im.width//2,im.height//2)) for im in images]))"], text=True))
                assert pixels[0][0] > 200 and pixels[0][1] < 20
                assert pixels[-1][1] > 80 and pixels[-1][0] < 20
                row["frame_center_rgb"] = pixels
                row["passed"] = True
                for name, mime in [("audio-longer.mp4", "video/mp4"), ("audio-shorter.webm", "video/webm"),
                                   ("audio-gap.m4a", "audio/mp4"), ("large-pts.m4a", "audio/mp4")]:
                    manifest, row = parse(name, mime)
                    if name == "audio-longer.mp4":
                        assert manifest["coverage"]["audio"]["track_end_ms"] > manifest["coverage"]["video"]["track_end_ms"]
                    if name == "audio-shorter.webm":
                        assert manifest["coverage"]["audio"]["track_end_ms"] < manifest["coverage"]["video"]["track_end_ms"]
                    if name == "audio-gap.m4a":
                        expected = json.loads((opts.fixtures / "gap-timing.json").read_text())
                        decoded = manifest["coverage"]["audio"]
                        assert abs(decoded["decoded_samples"] / 16000 - expected["total_seconds"]) < 0.2
                        after = [f for f in manifest["fragments"] if "silver" in f["text"].lower()]
                        row["expected_timing"] = expected; row["after_gap_transcript"] = after; record()
                        assert after and all(f["anchor"]["start_ms"] >=
                                             (expected["prefix_seconds"] + expected["gap_seconds"] - 1) * 1000 for f in after)
                    if name == "large-pts.m4a":
                        assert manifest["coverage"]["source_origin_ms"] > 89_000_000
                        assert manifest["coverage"]["audio"]["offset_ms"] == 0
                    row["passed"] = True
                parse("regressing-pts.webm", "audio/webm", "ambiguous_audio_timing")
                if opts.long_audio:
                    source = opts.fixtures / "meeting-20min.wav"
                    with wave.open(str(source)) as wav:
                        seconds = wav.getnframes() / wav.getframerate()
                    assert 1199 <= seconds <= 1201, seconds
                    stages = []
                    parse(source.name, "audio/wav", "parser_cancelled", cancel=lambda: bool(stages), stage=stages.append)
                    assert stages == ["asr_running"]
                    report["cases"][-1]["cancelled_during"] = stages[0]
                    # An occupied execution ID belongs to its existing container.
                    conflict = uuid.uuid4().hex
                    existing = subprocess.check_output(["docker", "create", "--network", "none", "--name",
                        "spark-media-" + conflict, image_id, "--mime", "audio/wav", "--deadline-seconds", "8"], text=True).strip()
                    try:
                        parse(source.name, "audio/wav", "parser_setup_failed", execution=conflict)
                        state = json.loads(subprocess.check_output(["docker", "inspect", existing], text=True))[0]
                        assert state["State"]["Status"] == "created"
                        report["cases"][-1]["existing_container_preserved"] = True
                    finally: subprocess.run(["docker", "rm", "-v", existing], check=True, capture_output=True)
                    # Kill the actual supervisor after its actual container starts.
                    execution = uuid.uuid4().hex; name = "spark-media-" + execution
                    output = temporary / "interrupted-output"
                    config = dict(source=str(source), mime="audio/wav", output=str(output), image_id=image_id,
                                  parser_sha=parser_sha, source_sha=sandbox.hash_file(source),
                                  model_root=str(opts.model), model_sha=model_sha, timeout=8, execution_id=execution)
                    with tempfile.TemporaryFile() as log:
                        child = subprocess.Popen([sys.executable, "-c",
                            "import json,sys;from cannbot.media.sandbox import run;run(**json.loads(sys.argv[1]))",
                            json.dumps(config)], cwd=root, stdout=log, stderr=log)
                        try:
                            deadline = time.monotonic() + 30
                            container = None
                            while time.monotonic() < deadline:
                                inspect = subprocess.run(["docker", "inspect", name], capture_output=True, text=True)
                                if inspect.returncode == 0:
                                    container = json.loads(inspect.stdout)[0]
                                    if container["State"]["Running"]: break
                                if child.poll() is not None: raise AssertionError("supervisor ended before SIGKILL barrier")
                                time.sleep(0.1)
                            else: raise AssertionError("actual parser did not start")
                            host = container["HostConfig"]
                            assert host["NetworkMode"] == "none" and host["ReadonlyRootfs"]
                            assert host["CapDrop"] == ["ALL"] and host["PidsLimit"] == 64
                            assert host["Memory"] == 4 * 1024 ** 3
                            mounts = [m for m in container["Mounts"] if m["Type"] == "bind"]
                            assert {m["Destination"] for m in mounts} == {"/input/source", "/model"}
                            assert all(not m["RW"] for m in mounts)
                            child.kill(); child.wait(timeout=5)
                            deadline = time.monotonic() + 20
                            while time.monotonic() < deadline:
                                container = json.loads(subprocess.check_output(["docker", "inspect", name], text=True))[0]
                                if not container["State"]["Running"]: break
                                time.sleep(0.2)
                            else: raise AssertionError("container outlived internal hard deadline")
                            assert container["State"]["ExitCode"] == 142, container["State"]
                            assert not output.exists()
                            report["cases"].append({"case": "supervisor_SIGKILL", "passed": True,
                                                     "container_exit_code": 142, "internal_deadline_seconds": 8,
                                                     "container_started_at": container["State"]["StartedAt"],
                                                     "container_finished_at": container["State"]["FinishedAt"],
                                                     "isolation_inspected": True, "output_installed": False})
                            record()
                        finally:
                            if child.poll() is None: child.kill(); child.wait(timeout=5)
                            subprocess.run(["docker", "rm", "-f", "-v", name], capture_output=True, check=False)
                    manifest, row = parse(source.name, "audio/wav", timeout=7200)
                    assert abs(manifest["coverage"]["audio"]["duration_ms"] / 1000 - seconds) < 0.01
                    tail = [f for f in manifest["fragments"] if f["anchor"]["start_ms"] > 1150000]
                    text = " ".join(f["text"] for f in tail).lower()
                    assert "magenta" in text and "harbor" in text and "paused" in text, text
                    row["tail_transcript"] = tail
                    row["fixture_type"] = "continuous synthetic technical speech, time-normalized to 1200s; not human/noisy meeting benchmark"
                    row["passed"] = True
            elif opts.long_audio:
                raise ValueError("--long-audio requires --model")
            report["status"] = "passed"; record()
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = type(exc).__name__ + ": " + str(exc)
        record(); raise
    print(json.dumps({"status": report["status"], "cases": len(report["cases"])}))


if __name__ == "__main__": main()
