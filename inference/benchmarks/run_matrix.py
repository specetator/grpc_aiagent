#!/usr/bin/env python3
"""Run controlled CPU/HIP comparisons and serving optimizations on real models."""
import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def wait_health(url, process):
    for _ in range(600):
        if process.poll() is not None: raise RuntimeError(f'runtime exited with {process.returncode}')
        try:
            with urllib.request.urlopen(url+'/health', timeout=.5) as r:
                if r.status == 200: return
        except Exception: pass
        time.sleep(.1)
    raise RuntimeError('runtime health timeout')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cpu-bin', default='/home/peco/tools/llama.cpp/build/bin/llama-server')
    parser.add_argument('--rocm-bin', default='/home/peco/tools/llama.cpp/build-rocm/bin/llama-server')
    parser.add_argument('--model', default='/home/peco/models/Qwen3-0.6B-Q8_0.gguf')
    parser.add_argument('--out', required=True)
    parser.add_argument('--only', nargs='*')
    parser.add_argument('--requests', type=int, default=12)
    parser.add_argument('--repeats', type=int, default=3)
    args = parser.parse_args()
    out = Path(args.out).resolve(); out.mkdir(parents=True, exist_ok=True)
    profiles = [
        dict(name='cpu_q8_p1', gpu=False, slots=1, conc=[1,4]),
        dict(name='rocm_q8_p1', gpu=True, slots=1, conc=[1,4]),
        dict(name='cpu_q8_p4', gpu=False, slots=4, conc=[4]),
        dict(name='rocm_q8_p4', gpu=True, slots=4, conc=[1,4,8]),
        dict(name='rocm_batch_on', gpu=True, slots=4, conc=[4], mixed=True),
        dict(name='rocm_batch_off', gpu=True, slots=4, conc=[4], mixed=True, cb=False),
        dict(name='rocm_prefix_off', gpu=True, slots=1, conc=[1], prefix=True),
        dict(name='rocm_prefix_on', gpu=True, slots=1, conc=[1], prefix=True, cache=True),
        dict(name='rocm_flash_f16', gpu=True, slots=4, conc=[4], fa='on'),
        dict(name='rocm_flash_kv_q8', gpu=True, slots=4, conc=[4], fa='on', kv='q8_0'),
        dict(name='grpc_cpu_p4', gpu=False, slots=4, conc=[4], grpc=True),
        dict(name='grpc_rocm_p4', gpu=True, slots=4, conc=[4], grpc=True),
        dict(name='grpc_rocm_replicas2', gpu=True, slots=2, conc=[4], grpc=True, replicas=2),
        dict(name='cpu_flash_f16', gpu=False, slots=4, conc=[4], fa='on'),
        dict(name='rocm_flash_p1', gpu=True, slots=1, conc=[1], fa='on'),
        dict(name='grpc_cpu_flash_f16', gpu=False, slots=4, conc=[4], grpc=True, fa='on'),
        dict(name='grpc_rocm_flash_f16', gpu=True, slots=4, conc=[4], grpc=True, fa='on'),
        dict(name='grpc_rocm_shared2', gpu=True, slots=4, conc=[4], grpc=True, fa='on', workers=2),
    ]
    env = dict(os.environ, HIP_VISIBLE_DEVICES='0')
    env['LD_LIBRARY_PATH'] = '/opt/rocm/lib:' + env.get('LD_LIBRARY_PATH', '')
    failed = False
    for profile in profiles:
        name = profile['name']
        if args.only and name not in args.only: continue
        processes, files = [], []
        manifest = {'profile': profile, 'model': args.model, 'runtime_commands': []}
        try:
            urls = []
            for replica in range(profile.get('replicas', 1)):
                port = free_port(); url = f'http://127.0.0.1:{port}'; urls.append(url)
                cmd = [args.rocm_bin if profile['gpu'] else args.cpu_bin,
                       '-m', args.model, '--alias', 'qwen3-0.6b', '--host', '127.0.0.1',
                       '--port', str(port), '-np', str(profile['slots']),
                       '-c', str(profile['slots']*2048), '-t', '6', '-tb', '6',
                       '-b', '512', '-ub', '128', '--metrics', '-lv', '4',
                       '--chat-template-kwargs', '{"enable_thinking":false}',
                       '-fa', profile.get('fa', 'off'),
                       '-cb' if profile.get('cb', True) else '-nocb']
                if profile['gpu']: cmd += ['-ngl', '99']
                else: cmd += ['-ngl', '0', '--device', 'none', '--no-kv-offload', '--no-op-offload']
                if profile.get('cache'): cmd += ['--cache-prompt']
                else: cmd += ['--no-cache-prompt', '--cache-ram', '0']
                if profile.get('kv'): cmd += ['-ctk', profile['kv'], '-ctv', profile['kv']]
                manifest['runtime_commands'].append(cmd)
                log = open(out/f'{name}-runtime-{replica}.log', 'w'); files.append(log)
                proc = subprocess.Popen(cmd, env=env, stdout=log, stderr=subprocess.STDOUT)
                processes.append(proc); wait_health(url, proc)
            transport = 'http'; gateway = None
            if profile.get('grpc'):
                transport = 'grpc'; gp, mp = free_port(), free_port(); gateway = f'127.0.0.1:{gp}'
                log = open(out/f'{name}-gateway.log', 'w'); files.append(log)
                processes.append(subprocess.Popen([str(ROOT/'build-wsl/inference/inference_gateway'),
                    '--listen', gateway, '--metrics-port', str(mp), '--max-queue', '64'],
                    stdout=log, stderr=subprocess.STDOUT, env=env))
                time.sleep(.2)
                worker_endpoints = []
                for i in range(profile.get('workers', len(urls))):
                    url = urls[i % len(urls)]
                    wp = free_port(); worker_endpoints.append(f'127.0.0.1:{wp}')
                    log = open(out/f'{name}-worker-{i}.log', 'w'); files.append(log)
                    processes.append(subprocess.Popen([str(ROOT/'build-wsl/inference/inference_worker'),
                        '--worker-id', f'worker-{i}', '--listen', worker_endpoints[-1],
                        '--gateway', gateway, '--model', 'qwen3-0.6b', '--backend', 'llamacpp',
                        '--model-endpoint', url, '--max-concurrent',
                        str(profile['slots']//profile.get('workers', 1)),
                        '--device-type', 'rocm' if profile['gpu'] else 'cpu'],
                        stdout=log, stderr=subprocess.STDOUT, env=env))
                time.sleep(1.5)
            for concurrency in profile['conc']:
                cmd = [sys.executable, str(ROOT/'inference/benchmarks/bench_serving.py'),
                    '--transport', transport, '--url', urls[0], '--label', name,
                    '--concurrency', str(concurrency), '--requests', str(args.requests),
                    '--repeats', str(args.repeats), '--out', str(out/f'{name}-c{concurrency}.json')]
                if gateway: cmd += ['--gateway', gateway, '--proto-dir', str(ROOT/'build-wsl/python-proto')]
                if profile.get('mixed'): cmd += ['--mixed-lengths']
                if profile.get('prefix'):
                    prompt = out/'prefix-prompt.txt'
                    prompt.write_text('Reference design. '+
                        ('Each request has independent admission, generation, streaming, cancellation and persistence state.\n'*60)+
                        'Describe the design and its tradeoffs.')
                    cmd += ['--prompt-file', str(prompt), '--max-tokens', '32']
                if profile.get('cache'): cmd += ['--cache-prompt']
                subprocess.run(cmd, check=True, env=env)
            for i, url in enumerate(urls):
                (out/f'{name}-metrics-{i}.txt').write_bytes(urllib.request.urlopen(url+'/metrics').read())
            if gateway:
                (out/f'{name}-gateway-metrics.txt').write_bytes(
                    urllib.request.urlopen(f'http://127.0.0.1:{mp}/metrics').read())
                sys.path.insert(0, str(ROOT/'build-wsl/python-proto'))
                import grpc, inference_pb2 as pb, inference_pb2_grpc as rpc
                from google.protobuf.json_format import MessageToDict
                manifest['worker_status'] = []
                for endpoint in worker_endpoints:
                    with grpc.insecure_channel(endpoint) as channel:
                        status = rpc.InferenceWorkerStub(channel).GetStatus(pb.GetWorkerStatusRequest(), timeout=3)
                        manifest['worker_status'].append(MessageToDict(status, preserving_proto_field_name=True))
            manifest['ok'] = True
        except Exception as error:
            failed = True
            manifest['ok'] = False; manifest['error'] = str(error)
            print(json.dumps({'profile': name, 'error': str(error)}), flush=True)
        finally:
            for process in reversed(processes):
                if process.poll() is None: process.terminate()
            for process in reversed(processes):
                try: process.wait(timeout=5)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
            for file in files: file.close()
            (out/f'{name}-manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return int(failed)


if __name__ == '__main__': raise SystemExit(main())
