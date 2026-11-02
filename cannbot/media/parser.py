#!/usr/bin/env python3
"""Local, bounded media extraction. Run ONLY inside media_sandbox's container.

No credentials, HTTP, Agent loop, identity inference or model downloads. Success
emits an untrusted tar bundle, validated again by the supervising parent. Parsed
content is evidence for native media tools, not a claim of Agent understanding.
"""
import argparse
import contextlib
import hashlib
import io
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import resource
import signal
import stat
import subprocess
import sys
import tarfile
import tempfile
import warnings
import wave
import zipfile

MAX_SOURCE = 1024 ** 3
MAX_TEXT = 16 * 1024 ** 2
MAX_FRAGMENT = 16 * 1024
MAX_FRAGMENTS = 4096
MAX_ARTIFACTS = 1024
MAX_ARTIFACT = 1024 ** 2
MAX_ARTIFACT_BYTES = 128 * 1024 ** 2
MAX_DURATION = 3600
MAX_PAGES = 200
MAX_PIXELS = 40_000_000
FRAME_INTERVAL = 5
DOCX = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
MIMES = {"application/pdf", DOCX, "image/png", "image/jpeg", "image/webp", "image/gif",
         "audio/wav", "audio/mpeg", "audio/flac", "audio/ogg", "audio/mp4", "audio/webm",
         "video/mp4", "video/webm", "video/ogg"}
FORMATS = "wav,mp3,flac,ogg,mov,matroska,webm"


class ParseError(Exception):
    """Only finite public codes leave the process; never document/parser text."""


def require(condition, code):
    if not condition:
        raise ParseError(code)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode()


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1024 ** 2), b""):
            h.update(chunk)
    return h.hexdigest()


def command(argv, timeout=120, limit=4 * 1024 ** 2):
    # Temp files and per-file limits prevent unbounded subprocess PIPE capture.
    with tempfile.TemporaryFile() as out, tempfile.TemporaryFile() as err:
        try:
            result = subprocess.run(argv, stdin=subprocess.DEVNULL, stdout=out,
                                    stderr=err, timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            raise ParseError("decoder_timeout") from None
        require(result.returncode == 0, "decoder_failed")
        require(out.tell() <= limit and err.tell() <= limit, "decoder_output_limit")
        out.seek(0); err.seek(0)
        return out.read(), err.read()


def number(value):
    try:
        result = float(value)
    except (TypeError, ValueError):
        raise ParseError("invalid_media_timing") from None
    require(math.isfinite(result), "invalid_media_timing")
    return result


class Parser:
    def __init__(self, source, output, mime, model=Path("/model")):
        self.source, self.output, self.mime, self.model = Path(source), Path(output), mime, Path(model)
        info = self.source.lstat()
        require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1 and 0 < info.st_size <= MAX_SOURCE,
                "invalid_source")
        require(mime in MIMES, "unsupported_format")
        self.fragments, self.artifacts = [], []
        self.text_bytes = self.artifact_bytes = 0
        self.manifest = {"schema": "room-media-v1", "source": {"mime": mime,
                         "bytes": info.st_size, "sha256": digest(self.source)},
                         "parser_sha256": digest(Path(__file__)), "coverage": {},
                         "fragments": self.fragments, "artifacts": self.artifacts}

    def text(self, kind, text, anchor):
        require(isinstance(text, str) and "\x00" not in text, "invalid_extracted_text")
        text = text.strip()
        if not text:
            return
        raw = text.encode("utf-8")
        require(self.text_bytes + len(raw) <= MAX_TEXT, "text_limit")
        self.text_bytes += len(raw)
        # Split only at UTF-8 boundaries; never silently discard a long paragraph.
        part = 0
        while raw:
            end = min(len(raw), MAX_FRAGMENT)
            while end < len(raw) and raw[end] & 0xC0 == 0x80:
                end -= 1
            require(len(self.fragments) < MAX_FRAGMENTS, "fragment_limit")
            self.fragments.append({"index": len(self.fragments), "kind": kind,
                                   "text": raw[:end].decode(), "anchor": dict(anchor, part=part)})
            raw = raw[end:]; part += 1

    def picture(self, source, anchor, ocr=True):
        from PIL import Image, ImageOps
        Image.MAX_IMAGE_PIXELS = MAX_PIXELS
        warnings.simplefilter("error", Image.DecompressionBombWarning)
        require(len(self.artifacts) < MAX_ARTIFACTS, "artifact_limit")
        with Image.open(source) as opened:
            if self.mime.startswith("image/"):
                require(opened.format == {"image/png": "PNG", "image/jpeg": "JPEG", "image/webp": "WEBP", "image/gif": "GIF"}[self.mime],
                        "mime_mismatch")
            require(opened.width * opened.height <= MAX_PIXELS, "image_pixel_limit")
            require(getattr(opened, "n_frames", 1) == 1, "animated_image_unsupported")
            opened.load()
            image = ImageOps.exif_transpose(opened)
            # Flatten alpha on white and strip EXIF/ICC/other input metadata.
            image = image.convert("RGBA")
            background = Image.new("RGBA", image.size, (255, 255, 255, 255))
            background.alpha_composite(image)
            image = background.convert("RGB")
            image.thumbnail((1600, 1600))
            data = None
            for quality in (85, 70, 55):
                buffer = io.BytesIO(); image.save(buffer, format="JPEG", quality=quality)
                data = buffer.getvalue()
                if len(data) <= MAX_ARTIFACT:
                    break
            require(len(data) <= MAX_ARTIFACT, "image_byte_limit")
            require(self.artifact_bytes + len(data) <= MAX_ARTIFACT_BYTES, "artifact_total_limit")
            index = len(self.artifacts); name = f"image-{index:06d}.jpg"
            destination = self.output / name
            with destination.open("xb") as stream:
                stream.write(data)
            self.artifact_bytes += len(data)
            artifact = {"index": index, "file": name, "mime": "image/jpeg", "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(), "width": image.width,
                        "height": image.height, "anchor": anchor}
            self.artifacts.append(artifact)
        if ocr:
            raw, _ = command(["tesseract", str(destination), "stdout", "-l", "eng+chi_sim", "--psm", "3"])
            self.text("ocr", raw.decode("utf-8", "strict"), dict(anchor, artifact=index))
        return index

    def pdf(self):
        from pypdf import PdfReader
        reader = PdfReader(str(self.source), strict=True)
        require(not reader.is_encrypted, "encrypted_document")
        require(0 < len(reader.pages) <= MAX_PAGES, "document_page_limit")
        for index, page in enumerate(reader.pages):
            anchor = {"page": index + 1}
            text = page.extract_text() or ""
            self.text("text", text, anchor)
            # Render every page, including born-digital pages: charts and diagrams
            # must remain available to native vision, regardless of text extraction.
            with tempfile.TemporaryDirectory() as temporary:
                prefix = str(Path(temporary) / "page")
                command(["pdftoppm", "-f", str(index + 1), "-l", str(index + 1),
                         "-singlefile", "-scale-to", "1600", "-png", str(self.source), prefix])
                self.picture(prefix + ".png", anchor, ocr=not text.strip())
        self.manifest["coverage"] = {"kind": "pdf", "page_count": len(reader.pages),
                                     "rendered_pages": len(reader.pages)}

    def docx(self):
        from defusedxml.ElementTree import fromstring
        ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
              "a": "http://schemas.openxmlformats.org/drawingml/2006/main",
              "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
        with zipfile.ZipFile(self.source) as archive:
            entries = archive.infolist()
            require(len(entries) <= 2000, "document_entry_limit")
            names = [entry.filename for entry in entries]
            require(len(set(names)) == len(names), "duplicate_document_entry")
            total = 0
            for entry in entries:
                path = PurePosixPath(entry.filename)
                require(not path.is_absolute() and ".." not in path.parts and "\\" not in entry.filename,
                        "unsafe_document_entry")
                require(not entry.flag_bits & 1 and not stat.S_ISLNK(entry.external_attr >> 16),
                        "unsafe_document_entry")
                total += entry.file_size
                require(entry.file_size <= 32 * 1024 ** 2 and total <= 64 * 1024 ** 2 and
                        entry.file_size <= max(1, entry.compress_size) * 1000, "document_expansion_limit")
            require("word/document.xml" in names, "invalid_docx")
            parts = sorted(name for name in names if re.fullmatch(
                r"word/(document|header\d+|footer\d+|footnotes|endnotes)\.xml", name))
            paragraphs = 0
            for part in parts:
                root = fromstring(archive.read(part))
                stem = PurePosixPath(part).stem
                tag = "hdr" if stem.startswith("header") else "ftr" if stem.startswith("footer") else stem
                require(root.tag == "{" + ns["w"] + "}" + tag, "invalid_docx")
                rel_name = "word/_rels/" + PurePosixPath(part).name + ".rels"
                relationships = {}
                if rel_name in names:
                    relationships = {node.attrib.get("Id"): node.attrib for node in
                                     fromstring(archive.read(rel_name))}
                # Unsupported opaque objects must not silently disappear.
                require(not root.findall(".//w:object", ns), "embedded_object_unsupported")
                require(not root.findall(".//w:pict", ns) and all(
                    drawing.findall(".//a:blip", ns) for drawing in root.findall(".//w:drawing", ns)),
                    "embedded_object_unsupported")
                for paragraph, node in enumerate(root.findall(".//w:p", ns), 1):
                    paragraphs += 1
                    anchor = {"section": PurePosixPath(part).stem, "paragraph": paragraph}
                    value = []
                    for item in node.iter():
                        if item.tag == "{" + ns["w"] + "}t": value.append(item.text or "")
                        elif item.tag == "{" + ns["w"] + "}tab": value.append("\t")
                        elif item.tag in ("{" + ns["w"] + "}br", "{" + ns["w"] + "}cr"): value.append("\n")
                    self.text("text", "".join(value), anchor)
                    for blip in node.findall(".//a:blip", ns):
                        require("{" + ns["r"] + "}link" not in blip.attrib, "external_document_reference")
                        rel = relationships.get(blip.attrib.get("{" + ns["r"] + "}embed"), {})
                        target = rel.get("Target", "")
                        require(rel.get("TargetMode", "Internal") == "Internal" and
                                re.fullmatch(r"media/[A-Za-z0-9_.-]+", target), "external_document_reference")
                        self.picture(io.BytesIO(archive.read("word/" + target)), anchor)
            self.manifest["coverage"] = {"kind": "docx", "sections": [PurePosixPath(p).stem for p in parts],
                                         "paragraphs": paragraphs}

    def probe(self):
        raw, _ = command(["ffprobe", "-v", "error", "-protocol_whitelist", "file,pipe",
                          "-format_whitelist", FORMATS, "-show_streams", "-show_format",
                          "-of", "json", str(self.source)])
        data = json.loads(raw)
        duration = number(data.get("format", {}).get("duration"))
        require(0 < duration <= 9_000_000_000, "invalid_media_timing")
        streams = data.get("streams", [])
        require(0 < len(streams) <= 16, "media_stream_limit")
        return data, duration

    def first_pts(self, selector):
        raw, _ = command(["ffprobe", "-v", "error", "-protocol_whitelist", "file,pipe",
                          "-format_whitelist", FORMATS, "-select_streams", selector,
                          "-read_intervals", "%+#64", "-show_frames", "-show_entries",
                          "frame=best_effort_timestamp_time", "-of", "json", str(self.source)])
        frames = json.loads(raw).get("frames", [])
        require(frames, "missing_media_frames")
        return number(frames[0].get("best_effort_timestamp_time"))

    def track_end(self, selector):
        # Container duration is NOT each track's end. Inspect packet presentation
        # times (including reordered video packets) without another full decode.
        raw, _ = command(["ffprobe", "-v", "error", "-protocol_whitelist", "file,pipe",
                          "-format_whitelist", FORMATS, "-select_streams", selector,
                          "-show_packets", "-show_entries", "packet=pts_time,duration_time",
                          "-of", "csv=p=0", str(self.source)], limit=16 * 1024 ** 2)
        end, count, previous_start = None, 0, None
        for line in raw.splitlines():
            if not line.strip(): continue
            fields = line.split(b",")
            require(len(fields) >= 2, "invalid_media_timing")
            start, duration = number(fields[0]), number(fields[1])
            require(0 <= duration <= MAX_DURATION, "invalid_media_timing")
            if selector == "a:0": require(duration > 0, "ambiguous_audio_timing")
            if selector == "a:0" and end is not None:
                # Audio packets are in decode/presentation order. Permit only
                # ffprobe's microsecond formatting roundoff, not duplicate speech
                # intervals that async resampling could otherwise silently discard.
                require(start >= previous_start and start + 0.000002 >= end, "ambiguous_audio_timing")
            end = max(end if end is not None else start, start + duration)
            previous_start = start
            count += 1
        require(end is not None and count <= 500000, "invalid_media_timing")
        return end

    def transcribe(self, origin, audio_pts):
        require(self.model.is_dir() and (self.model / "model.bin").is_file(), "asr_model_unavailable")
        with tempfile.TemporaryDirectory() as temporary:
            wav = Path(temporary) / "audio.wav"
            # Do not pass -t: decode the full selected audio stream or fail. The
            # input protocol/demuxer whitelist excludes playlists and network IO.
            command(["ffmpeg", "-v", "error", "-xerror", "-nostdin", "-protocol_whitelist", "file,pipe",
                     "-format_whitelist", FORMATS, "-i", str(self.source), "-map", "0:a:0", "-vn",
                     "-af", "asetpts=PTS-STARTPTS,aresample=16000:async=1:first_pts=0",
                     "-ac", "1", "-ar", "16000", "-c:a", "pcm_s16le",
                     str(wav)], timeout=600)
            with wave.open(str(wav), "rb") as audio:
                seconds = audio.getnframes() / audio.getframerate()
                require(audio.getnchannels() == 1 and audio.getsampwidth() == 2 and
                        0 < seconds <= MAX_DURATION + 1, "invalid_decoded_audio")
                pcm = audio.readframes(audio.getnframes())
                require(len(pcm) == audio.getnframes() * 2, "incomplete_audio_decode")
            import numpy as np
            samples = np.frombuffer(pcm, dtype="<i2").astype(np.float32) / 32768.0
            del pcm
            from faster_whisper import WhisperModel
            model = WhisperModel(str(self.model), device="cpu", compute_type="int8", cpu_threads=2,
                                 num_workers=1, local_files_only=True)
            segments, info = model.transcribe(samples, beam_size=5, vad_filter=False,
                                              condition_on_previous_text=False, word_timestamps=True)
            print('{"stage":"asr_running"}', file=sys.stderr, flush=True)
            count, previous = 0, 0.0
            for segment in segments:
                start, end = number(segment.start), number(segment.end)
                require(0 <= start <= end <= seconds + 0.5 and start >= previous - 0.1,
                        "invalid_transcript_timing")
                previous = start
                offset = audio_pts - origin
                require(offset >= 0, "invalid_media_timing")
                self.text("transcript", segment.text,
                          {"start_ms": max(0, round((offset + start) * 1000)),
                           "end_ms": max(0, round((offset + end) * 1000))})
                count += 1
            require(abs(number(info.duration) - seconds) <= 1, "incomplete_audio_decode")
            return {"decoded_samples": round(seconds * 16000), "sample_rate": 16000,
                    "duration_ms": round(seconds * 1000), "offset_ms": round((audio_pts - origin) * 1000),
                    "language": info.language, "segments": count, "speaker_labels": False}

    def audiovisual(self):
        data, container_duration = self.probe()
        audio = [s for s in data["streams"] if s.get("codec_type") == "audio"]
        video = [s for s in data["streams"] if s.get("codec_type") == "video" and
                 not s.get("disposition", {}).get("attached_pic")]
        is_video = self.mime.startswith("video/")
        require(bool(video) if is_video else bool(audio), "missing_media_stream")
        # Explicitly reject ambiguity rather than silently selecting among tracks.
        require(len(audio) <= 1 and (len(video) == 1 if is_video else len(video) == 0),
                "multiple_media_tracks_unsupported")
        audio_pts = self.first_pts("a:0") if audio else None
        video_pts = self.first_pts(str(video[0]["index"])) if is_video else None
        audio_end = self.track_end("a:0") if audio else None
        video_end = self.track_end(str(video[0]["index"])) if is_video else None
        # One explicit timeline: zero = earliest selected decoded media sample.
        # Preserve the source origin separately (it may be negative/large). Some
        # containers report duration including a timestamp offset, not clip length.
        origin = min(x for x in (audio_pts, video_pts) if x is not None)
        require(abs(origin) <= 9_000_000_000, "invalid_media_timing")
        duration = max(x for x in (audio_end, video_end) if x is not None) - origin
        require(0 < duration <= MAX_DURATION, "media_duration_limit")
        coverage = {"kind": "video" if is_video else "audio", "duration_ms": round(duration * 1000),
                    "container_duration_ms": round(container_duration * 1000),
                    "source_origin_ms": round(origin * 1000), "audio": None}
        if is_video:
            stream = video[0]
            require(0 < int(stream.get("width", 0)) * int(stream.get("height", 0)) <= MAX_PIXELS,
                    "image_pixel_limit")
            with tempfile.TemporaryDirectory() as temporary:
                pattern = str(Path(temporary) / "frame-%06d.jpg")
                filters = (f"select=isnan(prev_selected_t)+gte(t-prev_selected_t\\,{FRAME_INTERVAL}),"
                           "scale=1600:1600:force_original_aspect_ratio=decrease,showinfo")
                _, logs = command(["ffmpeg", "-v", "info", "-xerror", "-nostdin", "-copyts",
                                   "-protocol_whitelist", "file,pipe", "-format_whitelist", FORMATS,
                                   "-i", str(self.source), "-map", f"0:{stream['index']}", "-an", "-vf", filters,
                                   "-vsync", "0", "-q:v", "3", pattern], timeout=600)
                stamps = [number(x) for x in re.findall(rb"\bn:\s*\d+\s+pts:\s*\S+\s+pts_time:(\S+)", logs)]
                files = sorted(Path(temporary).glob("frame-*.jpg"))
                require(0 < len(files) == len(stamps) <= MAX_ARTIFACTS, "invalid_frame_manifest")
                require(abs(stamps[0] - video_pts) < 0.2 and
                        all(b > a and b - a <= FRAME_INTERVAL + 1 for a, b in zip(stamps, stamps[1:])),
                        "video_sampling_gap")
                require(stamps[-1] >= video_end - FRAME_INTERVAL - 1, "incomplete_video_decode")
                for path, stamp in zip(files, stamps):
                    require(stamp >= origin - 0.1, "invalid_media_timing")
                    self.picture(path, {"timestamp_ms": max(0, round((stamp - origin) * 1000))})
                coverage["video"] = {"frames": len(files), "sample_interval_ms": FRAME_INTERVAL * 1000,
                                     "first_frame_ms": round((stamps[0] - origin) * 1000),
                                     "last_frame_ms": round((stamps[-1] - origin) * 1000),
                                     "track_end_ms": round((video_end - origin) * 1000)}
        if audio:
            coverage["audio"] = self.transcribe(origin, audio_pts)
            # Account for source track offset; no silent prefix-only ASR success.
            decoded_end = (coverage["audio"]["offset_ms"] + coverage["audio"]["duration_ms"]) / 1000
            expected = audio_end - audio_pts
            require(abs(coverage["audio"]["duration_ms"] / 1000 - expected) <= 1.0 and
                    decoded_end <= duration + 1.0, "incomplete_audio_decode")
            coverage["audio"]["track_end_ms"] = round((audio_end - origin) * 1000)
        self.manifest["coverage"] = coverage

    def parse(self):
        self.output.mkdir(parents=True, exist_ok=True)
        require(not any(self.output.iterdir()), "output_not_empty")
        if self.mime == "application/pdf": self.pdf()
        elif self.mime == DOCX: self.docx()
        elif self.mime.startswith("image/"):
            self.picture(self.source, {"image": 1})
            self.manifest["coverage"] = {"kind": "image", "images": 1}
        else: self.audiovisual()
        self.manifest["text_bytes"] = self.text_bytes
        self.manifest["artifact_bytes"] = self.artifact_bytes
        raw = canonical(self.manifest)
        require(len(raw) <= 20 * 1024 ** 2, "manifest_limit")
        (self.output / "manifest.json").write_bytes(raw)
        return self.manifest


def main():
    args = argparse.ArgumentParser()
    args.add_argument("--mime", required=True, choices=sorted(MIMES))
    args.add_argument("--deadline-seconds", type=int, required=True)
    opts = args.parse_args()
    if not 1 <= opts.deadline_seconds <= 7200: return 2
    # Default SIGALRM disposition terminates even while a native decoder blocks
    # Python signal delivery. The container's init then exits and its PID namespace
    # kills remaining descendants, independently of a crashed parent supervisor.
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.alarm(opts.deadline_seconds)
    resource.setrlimit(resource.RLIMIT_CPU, (opts.deadline_seconds * 2 + 1, opts.deadline_seconds * 2 + 1))
    # Parent imposes stronger cgroup, tmpfs, wall-time and stdout limits. This
    # bound also applies to ffmpeg/poppler/OCR output files inherited by children.
    resource.setrlimit(resource.RLIMIT_FSIZE, (192 * 1024 ** 2, 192 * 1024 ** 2))
    resource.setrlimit(resource.RLIMIT_NOFILE, (128, 128))
    os.umask(0o077)
    try:
        with contextlib.redirect_stdout(sys.stderr):
            parser = Parser(Path("/input/source"), Path("/output"), opts.mime)
            manifest = parser.parse()
        # Only regular, generated names. The parent still distrusts this archive.
        with tarfile.open(fileobj=sys.stdout.buffer, mode="w|", format=tarfile.USTAR_FORMAT) as bundle:
            bundle.add("/output/manifest.json", arcname="manifest.json", recursive=False)
            for artifact in manifest["artifacts"]:
                bundle.add(Path("/output") / artifact["file"], arcname=artifact["file"], recursive=False)
        return 0
    except ParseError as exc:
        print(json.dumps({"error": str(exc)}), file=sys.stderr)
        return 2
    except Exception:
        print('{"error":"malformed_media"}', file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
