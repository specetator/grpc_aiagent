"""Runs the real HTTP/SSE adapter against a local deterministic model fixture."""

import http.server
import json
import socket
import subprocess
import sys
import tempfile
import threading
import time


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class ModelHandler(http.server.BaseHTTPRequestHandler):
    calls = []

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            self.send_error(404)
            return
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.calls.append(body)
        if body["messages"][-1]["content"] == "fail":
            self.send_error(503)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        if body["messages"][-1]["content"] == "stall":
            time.sleep(1)
        deltas = ("Hi", " there") if body["messages"][-1]["content"] != "cancel" \
            else ("one", "two", "three", "four", "five")
        for delta in deltas:
            event = {"choices": [{"delta": {"content": delta}, "finish_reason": None}]}
            try:
                self.wfile.write(("data: " + json.dumps(event) + "\n\n").encode())
                self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                return
            time.sleep(0.08 if len(deltas) > 2 else 0.03)
        event = {"choices": [{"delta": {}, "finish_reason": "stop"}]}
        try:
            self.wfile.write(("data: " + json.dumps(event) + "\n\n" +
                              "data: [DONE]\n\n").encode())
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_args):
        pass


def wait_port(port):
    for _ in range(100):
        try:
            with socket.create_connection(("127.0.0.1", port), 0.1):
                return
        except OSError:
            time.sleep(0.02)
    raise AssertionError(f"port {port} did not open")


def main(gateway_bin, worker_bin, cli_bin):
    model_port, gateway_port, worker_port, metrics_port = [free_port() for _ in range(4)]
    model = http.server.ThreadingHTTPServer(("127.0.0.1", model_port), ModelHandler)
    server_thread = threading.Thread(target=model.serve_forever, daemon=True)
    server_thread.start()
    processes = []
    with tempfile.TemporaryFile(mode="w+t") as gateway_log, \
         tempfile.TemporaryFile(mode="w+t") as worker_log:
        try:
            processes.append(subprocess.Popen([
                gateway_bin, "--listen", f"127.0.0.1:{gateway_port}",
                "--metrics-port", str(metrics_port)],
                stdout=gateway_log, stderr=subprocess.STDOUT))
            wait_port(gateway_port)
            processes.append(subprocess.Popen([
                worker_bin, "--worker-id", "llama-test", "--listen",
                f"127.0.0.1:{worker_port}", "--gateway",
                f"127.0.0.1:{gateway_port}", "--model", "real-test",
                "--backend", "llamacpp", "--model-endpoint",
                f"http://127.0.0.1:{model_port}"],
                stdout=worker_log, stderr=subprocess.STDOUT))
            wait_port(worker_port)
            # Allow the first registration RPC to complete.
            time.sleep(0.2)
            cmd = [cli_bin, "--gateway", f"127.0.0.1:{gateway_port}",
                   "--model", "real-test"]
            result = subprocess.run(cmd + ["--prompt", "hello"],
                                    capture_output=True, text=True, timeout=5)
            assert result.returncode == 0, result.stderr
            assert "[0] Hi\n[1]  there\nfinished: stop" in result.stdout, result.stdout
            assert ModelHandler.calls[-1]["stream"] is True
            assert ModelHandler.calls[-1]["messages"][-1] == {
                "role": "user", "content": "hello"}
            failed = subprocess.run(cmd + ["--prompt", "fail"],
                                    capture_output=True, text=True, timeout=5)
            assert failed.returncode != 0 and "generation backend failed" in failed.stderr, failed.stderr
            cancelled = subprocess.run(cmd + ["--prompt", "cancel",
                                          "--cancel-after-chunks", "1"],
                                       capture_output=True, text=True, timeout=5)
            assert cancelled.returncode != 0 and "[0] one" in cancelled.stdout
            assert "finished:" not in cancelled.stdout, cancelled.stdout
            prefill_cancel = subprocess.run(cmd + ["--prompt", "stall",
                                               "--cancel-after-ms", "100"],
                                            capture_output=True, text=True, timeout=3)
            assert prefill_cancel.returncode != 0
            assert "finished:" not in prefill_cancel.stdout
            print("llama.cpp adapter streaming and failure test passed")
        except Exception:
            gateway_log.seek(0)
            worker_log.seek(0)
            print("gateway log:\n" + gateway_log.read(), file=sys.stderr)
            print("worker log:\n" + worker_log.read(), file=sys.stderr)
            raise
        finally:
            for process in processes:
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            model.shutdown()
            model.server_close()
            server_thread.join(timeout=3)


if __name__ == "__main__":
    main(*sys.argv[1:])
