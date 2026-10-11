"""Narrow, reproducible logger cost comparison; no production throughput claim."""
from pathlib import Path
import hashlib
import json
import os
import platform
import subprocess
import sys
import tempfile
import time

root = Path(__file__).resolve().parents[1]
build = Path(sys.argv[1] if len(sys.argv) > 1 else root / "build-wsl").resolve()
base = "2f285f89146db4a7b9c65cdbc263c4a521973ee2"
with tempfile.TemporaryDirectory(prefix="spark-log-bench-") as directory:
    temp = Path(directory)
    for name in ("baseline", "current"):
        (temp / name).mkdir()
    source = '#include <logging.h>\nint main() { sparkpush::InitLogging("bench"); for (int i=0;i<100000;++i) LOG_INFO << std::string(128,\'x\'); sparkpush::ShutdownLogging(); }\n'
    (temp / "main.cpp").write_text(source)
    (temp / "baseline.cpp").write_text(subprocess.check_output(["git", "show", base + ":common/logging.cpp"], cwd=root, text=True))
    (temp / "logging.h").write_text(subprocess.check_output(["git", "show", base + ":common/logging.h"], cwd=root, text=True))
    result = {"baseline": base, "candidate": "working tree 2026-10-11", "platform": platform.platform(),
              "source_sha256": {name: hashlib.sha256((root / name).read_bytes()).hexdigest()
                                for name in ("common/logging.cpp", "common/logging.h")},
              "messages": 100000, "payload_bytes": 128,
              "scope": "single thread; local buffered filesystem; stdout redirected to a regular file; no E2E throughput claim",
              "runs": {}}
    environment = dict(os.environ)
    environment.pop("SPARK_LOG_CONSOLE", None)
    for name, cpp, include in (("baseline", temp / "baseline.cpp", temp),
                               ("current", root / "common/logging.cpp", root / "common")):
        executable = temp / name / "benchmark"
        subprocess.run(["c++", "-O2", "-std=c++17", str(temp / "main.cpp"), str(cpp),
                        "-I" + str(include), "-I" + str(root), str(build / "muduo/base/libmuduo_base.a"),
                        "-pthread", "-o", str(executable)], check=True)
        timings = []
        for _ in range(3):
            for path in (temp / name).glob("logs/*"):
                path.unlink()
            with (temp / name / "stdout").open("wb") as out:
                start = time.perf_counter()
                subprocess.run([str(executable)], cwd=temp / name, stdout=out, env=environment, check=True)
                timings.append(round((time.perf_counter() - start) * 1000, 2))
        result["runs"][name] = {"wall_ms": timings, "stdout_bytes": (temp / name / "stdout").stat().st_size,
                                "log_bytes": (temp / name / "logs/bench.log").stat().st_size}
    target = root / "docs/benchmarks/2026-10-11-review"
    target.mkdir(parents=True, exist_ok=True)
    (target / "logging.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))
