#!/usr/bin/env python3
"""Generate synthetic, non-private inputs inside the separate fixtures image."""
import argparse
import io
import json
import secrets
from pathlib import Path
import subprocess
import wave
import zipfile

from PIL import Image, ImageDraw, ImageFont
from pypdf import PdfWriter, PdfReader
from pypdf.generic import DictionaryObject, NameObject, DecodedStreamObject


def cmd(*args):
    subprocess.run(args, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)


def make(root, long_audio, collaboration=False):
    root.mkdir(parents=True, exist_ok=True)
    nonce = secrets.token_hex(12) if collaboration else None
    font = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 32)
    image = Image.new("RGB", (640, 400), "white")
    draw = ImageDraw.Draw(image)
    draw.rectangle((30, 80, 180, 240), fill="orange")
    draw.ellipse((380, 90, 550, 260), fill="blue")
    draw.text((30, 300), "ORANGE QUEUE", fill="black", font=font)
    image.save(root / "diagram.png")
    writer = PdfWriter()
    for text in (("Design nonce: " + nonce) if nonce else "Kafka delivery proposal", "Persist final message only"):
        page = writer.add_blank_page(width=600, height=800)
        face = DictionaryObject({NameObject("/Type"): NameObject("/Font"),
                                 NameObject("/Subtype"): NameObject("/Type1"),
                                 NameObject("/BaseFont"): NameObject("/Helvetica")})
        page[NameObject("/Resources")] = DictionaryObject({NameObject("/Font"):
            DictionaryObject({NameObject("/F1"): writer._add_object(face)})})
        stream = DecodedStreamObject()
        stream.set_data(("BT /F1 24 Tf 30 700 Td (" + text + ") Tj ET\n"
                         "1 0 0 rg 30 300 100 100 re f\n").encode())
        page[NameObject("/Contents")] = writer._add_object(stream)
    scanned = io.BytesIO(); image.save(scanned, format="PDF")
    writer.add_page(PdfReader(io.BytesIO(scanned.getvalue())).pages[0])
    with (root / "design.pdf").open("wb") as out: writer.write(out)
    writer.encrypt("not-a-real-secret")
    with (root / "encrypted.pdf").open("wb") as out: writer.write(out)
    (root / "broken.pdf").write_bytes(b"%PDF-1.7\ninvalid")
    document = '''<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"
        xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main"
        xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><w:body>
        <w:p><w:r><w:t>Release decision: keep the rollout paused.</w:t></w:r></w:p>
        <w:p><w:r><w:t>负责人需要核对 Kafka 回执。</w:t></w:r><w:r><w:drawing>
        <a:blip r:embed="rId1"/></w:drawing></w:r></w:p></w:body></w:document>'''
    relationship = '''<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
        <Relationship Id="rId1" Target="media/diagram.png" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image"/>
        </Relationships>'''
    with zipfile.ZipFile(root / "notes.docx", "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("word/document.xml", document)
        archive.writestr("word/_rels/document.xml.rels", relationship)
        archive.write(root / "diagram.png", "word/media/diagram.png")
    with zipfile.ZipFile(root / "unsafe.docx", "w") as archive:
        archive.writestr("../escape", "rejected")
        archive.writestr("word/document.xml", document)
    with zipfile.ZipFile(root / "external.docx", "w") as archive:
        archive.writestr("word/document.xml", document)
        archive.writestr("word/_rels/document.xml.rels", relationship.replace(
            'Target="media/diagram.png"', 'Target="file:///etc/passwd" TargetMode="External"'))
    with zipfile.ZipFile(root / "entity.docx", "w") as archive:
        archive.writestr("word/document.xml", '<!DOCTYPE doc [<!ENTITY xxe SYSTEM "file:///etc/passwd">]><doc>&xxe;</doc>')
    image.save(root / "animated.gif", save_all=True, append_images=[Image.new("RGB", image.size, "red")], duration=100)
    cmd("espeak-ng", "-s", "170", "-w", str(root / "speech.wav"),
        "The red panel shows a connection failure. The green panel means recovery.")
    video_inputs = ("-f", "lavfi", "-i", "color=c=red:s=320x240:r=10:d=5",
                    "-f", "lavfi", "-i", "color=c=green:s=320x240:r=10:d=5")
    if collaboration:
        before = Image.new("RGB", (320, 240), "red")
        ImageDraw.Draw(before).ellipse((40, 60, 140, 160), fill="white")
        after = Image.new("RGB", (320, 240), "green")
        ImageDraw.Draw(after).polygon(((200, 50), (150, 160), (250, 160)), fill="yellow")
        before.save(root / "panel-before.png"); after.save(root / "panel-after.png")
        video_inputs = ("-loop", "1", "-framerate", "10", "-t", "5", "-i", str(root / "panel-before.png"),
                        "-loop", "1", "-framerate", "10", "-t", "5", "-i", str(root / "panel-after.png"))
    cmd("ffmpeg", "-v", "error", *video_inputs, "-itsoffset", "2",
        "-i", str(root / "speech.wav"), "-filter_complex", "[0:v][1:v]concat=n=2:v=1:a=0[v]",
        "-map", "[v]", "-map", "2:a", "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac",
        str(root / "incident.mp4"))
    cmd("ffmpeg", "-v", "error", "-f", "lavfi", "-i", "color=c=red:s=320x240:r=10:d=2",
        "-itsoffset", "2", "-i", str(root / "speech.wav"), "-map", "0:v", "-map", "1:a",
        "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", str(root / "audio-longer.mp4"))
    cmd("ffmpeg", "-v", "error", "-copyts", "-i", str(root / "incident.mp4"),
        "-c:v", "libvpx-vp9", "-c:a", "libopus", str(root / "audio-shorter.webm"))
    cmd("espeak-ng", "-s", "150", "-w", str(root / "gap-before.wav"), "Checkpoint alpha. The deployment is paused.")
    cmd("espeak-ng", "-s", "150", "-w", str(root / "gap-after.wav"), "The recovery code is silver lantern. Bob will check the database.")
    with wave.open(str(root / "gap-before.wav")) as audio: prefix = audio.getnframes() / audio.getframerate()
    with wave.open(str(root / "gap-after.wav")) as audio: suffix = audio.getnframes() / audio.getframerate()
    filters = (f"[0:a]apad=pad_dur=35[a0];[a0][1:a]concat=n=2:v=0:a=1,"
               f"aselect=lt(t\\,{prefix})+gte(t\\,{prefix+35})[a]")
    cmd("ffmpeg", "-v", "error", "-i", str(root / "gap-before.wav"), "-i", str(root / "gap-after.wav"),
        "-filter_complex", filters, "-map", "[a]", "-c:a", "aac", str(root / "audio-gap.m4a"))
    (root / "gap-timing.json").write_text(json.dumps({"prefix_seconds": prefix, "gap_seconds": 35,
                                                     "suffix_seconds": suffix, "total_seconds": prefix + 35 + suffix}))
    cmd("ffmpeg", "-v", "error", "-i", str(root / "speech.wav"), "-c:a", "aac",
        "-output_ts_offset", "90000", str(root / "large-pts.m4a"))
    cmd("ffmpeg", "-v", "error", "-copyts", "-itsoffset", "-2", "-i", str(root / "speech.wav"),
        "-c:a", "libopus", "-avoid_negative_ts", "disabled", str(root / "regressing-pts.webm"))
    # This muxer produces an actual backwards packet jump for this offset. Verify
    # the fault rather than assuming a negative offset implies a negative timeline.
    packets = subprocess.check_output(["ffprobe", "-v", "error", "-select_streams", "a:0", "-show_packets",
        "-show_entries", "packet=pts_time,duration_time", "-of", "csv=p=0", str(root / "regressing-pts.webm")], text=True)
    values = [tuple(map(float, line.split(",")[:2])) for line in packets.splitlines() if line.strip()]
    if not any(b[0] + 0.000002 < a[0] + a[1] for a, b in zip(values, values[1:])):
        raise RuntimeError("timestamp regression fixture was not reproduced by this ffmpeg build")
    if long_audio:
        discussion = []
        for index in range(60):
            if collaboration and index in (0, 30):
                discussion.append(
                    "Decision checkpoint one. The code name is silver lantern. Alice owns queue backpressure review."
                    if index == 0 else
                    "Decision checkpoint two. The code name is cobalt bridge. Bob owns database recovery verification.")
            discussion.append(
                f"This is discussion item {index + 1}. We are reviewing the messaging service and its failure recovery. "
                "A broker acknowledgment does not mean the browser has displayed the message. "
                "Keep the original request identifier when retrying. Check the database history and the worker logs. "
                "The release owner will collect the test evidence before making a decision. "
                "We must keep the private room isolated from other rooms.")
        discussion.append(("Decision checkpoint three. " if collaboration else "") +
                          "The final decision uses the code name magenta harbor. Keep the rollout paused. "
                          "Alice will investigate the duplicate delivery. Bob will verify the recovery test tomorrow. "
                          "Do not enable automatic tool retries when the execution result is unknown.")
        text = root / "meeting-script.txt"; text.write_text("\n".join(discussion))
        raw = root / "meeting-source.wav"
        cmd("espeak-ng", "-s", "190", "-w", str(raw), "-f", str(text))
        with wave.open(str(raw)) as audio: seconds = audio.getnframes() / audio.getframerate()
        cmd("ffmpeg", "-v", "error", "-i", str(raw), "-af", f"atempo={seconds / 1200}",
            "-ar", "16000", "-ac", "1", str(root / "meeting-20min.wav"))
        raw.unlink()
    if collaboration:
        if not long_audio:
            raise ValueError("collaboration fixtures require --long-audio")
        (root / "collaboration-expectations.json").write_text(json.dumps({
            "pdf_nonce": nonce, "audio_codes": ["silver lantern", "cobalt bridge", "magenta harbor"],
            "audio_decisions": [
                {"code": "silver lantern", "actions": [{"owner": "Alice", "terms": ["queue", "backpressure"]}]},
                {"code": "cobalt bridge", "actions": [{"owner": "Bob", "terms": ["database", "recovery"]}]},
                {"code": "magenta harbor", "actions": [{"owner": "Alice", "terms": ["duplicate", "delivery"]},
                                                      {"owner": "Bob", "terms": ["recovery", "test"]}]}],
            "audio_kind": "continuous synthetic speech, decision checkpoints near start/middle/end",
            "video": {"before": "red", "after": "green", "transition_seconds": 5,
                      "markers": [{"panel_color": "red", "marker_color": "white", "marker_shape": "circle"},
                                  {"panel_color": "green", "marker_color": "yellow", "marker_shape": "triangle"}]}}))


if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("directory", type=Path)
    p.add_argument("--long-audio", action="store_true")
    p.add_argument("--collaboration", action="store_true")
    args = p.parse_args(); make(args.directory, args.long_audio, args.collaboration)
