"""Real HTTP media suite with a large derived JPEG, in a fresh owned database.

Run manually with the same SPARK_TEST_MEDIA_IMAGE/FIXTURES/MODEL variables as
the publication E2E. This reuses its authority checks and does not mock a parser.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    cli = argparse.ArgumentParser(description=__doc__)
    cli.add_argument("--build", default=str(ROOT / "build-wsl"))
    cli.add_argument("--out", type=Path, required=True)
    args = cli.parse_args()
    image = os.environ["SPARK_TEST_MEDIA_IMAGE"]
    original = Path(os.environ["SPARK_TEST_MEDIA_FIXTURES"])
    with tempfile.TemporaryDirectory(prefix="spark-media-large-") as temporary:
        root = Path(temporary)
        for name in ("design.pdf", "notes.docx", "incident.mp4", "speech.wav"):
            shutil.copyfile(original / name, root / name)
        # Fixed nonprivate random pixels produce a genuinely large encoded image.
        raw = subprocess.check_output(["docker", "run", "--rm", "--network", "none", "--read-only", "--cap-drop", "ALL",
            "--security-opt", "no-new-privileges", "--entrypoint", "python3", image, "-c",
            "from PIL import Image; import random,sys; Image.frombytes('RGB',(768,512),random.Random(20261101).randbytes(768*512*3)).save(sys.stdout.buffer,format='PNG')"])
        (root / "diagram.png").write_bytes(raw)
        report = root / "report.json"
        env = dict(os.environ, SPARK_TEST_MEDIA_FIXTURES=str(root), SPARK_TEST_MEDIA_LONG_AUDIO="0", SPARK_TEST_MEDIA_REPORT=str(report))
        subprocess.run([sys.executable, str(ROOT / "tests/run_mysql_review_tests.py"), args.build], env=env, check=True)
        result = json.loads(report.read_text()); assert result["status"] == "passed"
        case = next(c for c in result["cases"] if c["file"] == "diagram.png")
        assert case["artifact_count"] == 1
        # charged = actual JPEG bytes + actual UTF-8 text bytes. Each fragment
        # is capped at 16 KiB, so this lower bound cannot overstate JPEG size.
        jpeg_lower_bound = case["charged_bytes"] - 16384 * case["fragment_count"]
        base64_lower_bound = 4 * ((jpeg_lower_bound + 2) // 3)
        assert base64_lower_bound > 128 * 1024, "test did not exceed the old HTTP limit"
        actual_request_bytes = result["largest_successful_artifact_request_bytes"]
        assert 128 * 1024 < actual_request_bytes < 2 * 1024 * 1024
        assert result["oversized_artifact_rejected_without_write"] and result["normal_internal_body_limit_preserved"]
        result["large_payload_boundary"] = {"jpeg_bytes_lower_bound": jpeg_lower_bound,
            "base64_bytes_lower_bound": base64_lower_bound, "previous_body_limit": 128 * 1024,
            "current_artifact_body_limit": 2 * 1024 * 1024,
            "largest_successful_http_body_bytes": actual_request_bytes, "passed": True}
        args.out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
        print(json.dumps(result["large_payload_boundary"]), flush=True)


if __name__ == "__main__":
    main()
