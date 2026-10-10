#!/usr/bin/env python3
"""Opt-in real CUDA model test: ordered stream, usage, cancel, deadline, TTL recovery.

Not a mock/CPU fallback test; requires prepared model/runtime and Python gRPC bindings.
"""
import argparse
import csv
import json
import os
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'benchmarks'))
from run_matrix import ROOT, free_port, wait_health

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', required=True)
    parser.add_argument('--model', required=True)
    parser.add_argument('--out', type=Path, required=True)
    parser.add_argument('--build-dir', type=Path, default=ROOT/'build-wsl')
    parser.add_argument('--gateway-managed-only', action='store_true')
    args = parser.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    sys.path.insert(0, str(args.build_dir/'python-proto'))
    import grpc
    import inference_pb2 as pb
    import inference_pb2_grpc as rpc
    from google.protobuf.json_format import MessageToDict
    rp, gp, wp, mp = [free_port() for _ in range(4)]
    gateway, endpoint = f'127.0.0.1:{gp}', f'127.0.0.1:{wp}'
    processes, files = [], []
    result = {'schema':'sparkpush.real_cuda_test.v1', 'gateway_managed_only':args.gateway_managed_only,'checks':{}}
    def launch(cmd, name):
        log = open(args.out/f'real-{name}.log', 'w'); files.append(log)
        proc = subprocess.Popen(cmd, env=os.environ, stdout=log, stderr=subprocess.STDOUT)
        processes.append(proc); return proc
    def require(ok, name):
        result['checks'][name] = bool(ok)
        if not ok: raise RuntimeError(name)
    def request(name, tokens=1024):
        return pb.GenerateRequest(request_id=name, session_id=name, model='qwen3-0.6b',
            prompt='Explain inference serving, batching, cancellation and KV cache in detail.',
            max_tokens=tokens, temperature=0,
            metadata={'ignore_eos':'true', 'enable_thinking':'false', 'cache_prompt':'false'})
    worker_cmd = [str(args.build_dir/'inference/inference_worker'), '--listen',endpoint,
        '--gateway',gateway,'--worker-id','cuda-test','--model','qwen3-0.6b',
        '--backend','llamacpp','--model-endpoint',f'http://127.0.0.1:{rp}',
        '--max-concurrent','1','--device-type','cuda']
    try:
        runtime = launch([args.runtime,'-m',args.model,'--alias','qwen3-0.6b',
            '--host','127.0.0.1','--port',str(rp),'-ngl','99','--device','CUDA0',
            '-np','1','-c','2048','-fa','on','--no-cache-prompt','--cache-ram','0',
            '--metrics','-lv','4','--chat-template-kwargs','{"enable_thinking":false}'], 'runtime')
        wait_health(f'http://127.0.0.1:{rp}',runtime)
        require('offloaded 29/29 layers to GPU' in (args.out/'real-runtime.log').read_text(),
                'full_gpu_offload')
        gateway_cmd = [str(args.build_dir/'inference/inference_gateway'),'--listen',gateway,
                '--metrics-port',str(mp),'--max-queue','1']
        if args.gateway_managed_only: gateway_cmd.append('--gateway-managed-only')
        launch(gateway_cmd,'gateway')
        time.sleep(.3)
        worker = launch(worker_cmd, 'worker'); time.sleep(1.5)
        with grpc.insecure_channel(gateway) as channel, grpc.insecure_channel(endpoint) as wc:
            stub = rpc.InferenceGatewayStub(channel); ws = rpc.InferenceWorkerStub(wc)
            status = ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=5)
            result['worker_status'] = MessageToDict(status,preserving_proto_field_name=True)
            require(status.device_type == 'cuda' and status.total_memory_bytes > 11*2**30
                    and 0 < status.free_memory_bytes <= status.total_memory_bytes,'real_memory_sample')
            require(status.memory_probe_source == 'nvml', 'nvml_device_wide_probe')
            smi = subprocess.check_output(['nvidia-smi', '--query-gpu=pci.bus_id,memory.total,memory.used,memory.free',
                '--format=csv,noheader,nounits'], text=True)
            result['nvidia_smi_memory'] = [dict(zip(['pci_bus_id','total_mib','used_mib','free_mib'],
                [value.strip() for value in row])) for row in csv.reader(smi.splitlines())]
            require(any(abs(int(row['total_mib'])*2**20 - status.total_memory_bytes) < 2**20
                for row in result['nvidia_smi_memory']), 'nvml_total_matches_nvidia_smi')
            chunks = list(stub.Generate(request('complete',128),timeout=30))
            require(all(c.sequence == i and c.request_id == 'complete' for i,c in enumerate(chunks)),
                    'ordered_request_identified_stream')
            require(sum(c.finished for c in chunks) == 1 and chunks[-1].finished
                    and chunks[-1].completion_tokens == 128 and chunks[-1].prompt_tokens > 0,
                    'single_terminal_and_exact_usage')
            require(bool(''.join(c.text_delta for c in chunks)), 'nonempty_output')
            stream = stub.Generate(request('cancel'),timeout=30); first = next(stream)
            require(bool(first.text_delta),'delta_before_cancel')
            require(stub.Cancel(pb.CancelRequest(request_id='cancel'),timeout=3).cancelled,'cancel_rpc')
            try:
                list(stream); require(False,'cancelled_status')
            except grpc.RpcError as error:
                require(error.code() == grpc.StatusCode.CANCELLED,'cancelled_status')
            for _ in range(100):
                if ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=3).running_requests == 0: break
                time.sleep(.05)
            require(ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=3).running_requests == 0,
                    'capacity_released_after_cancel')
            try:
                list(stub.Generate(request('deadline'),timeout=.03)); require(False,'deadline_status')
            except grpc.RpcError as error:
                require(error.code() == grpc.StatusCode.DEADLINE_EXCEEDED,'deadline_status')
            time.sleep(.3)
            require(ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=3).running_requests == 0,
                    'capacity_released_after_deadline')
            owner = stub.Generate(request('capacity-owner'),timeout=30); next(owner)
            queued = stub.Generate(request('capacity-queued'),timeout=30)
            waiting = False
            for _ in range(100):
                metrics = urllib.request.urlopen(f'http://127.0.0.1:{mp}/metrics',timeout=2).read().decode()
                if 'inference_requests_waiting 1' in metrics:
                    waiting = True; break
                time.sleep(.02)
            require(waiting,'real_request_queued')
            try:
                list(stub.Generate(request('capacity-overflow'),timeout=3))
                require(False,'bounded_queue_rejects_overflow')
            except grpc.RpcError as error:
                require(error.code() == grpc.StatusCode.RESOURCE_EXHAUSTED,'bounded_queue_rejects_overflow')
            require(stub.Cancel(pb.CancelRequest(request_id='capacity-queued'),timeout=3).cancelled,
                    'queued_cancel_rpc')
            stub.Cancel(pb.CancelRequest(request_id='capacity-owner'),timeout=3)
            for name, stream in [('queued_cancel_status',queued),('owner_cancel_status',owner)]:
                try:
                    list(stream); require(False,name)
                except grpc.RpcError as error: require(error.code() == grpc.StatusCode.CANCELLED,name)
            for _ in range(100):
                if ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=3).running_requests == 0: break
                time.sleep(.05)
            require(ws.GetStatus(pb.GetWorkerStatusRequest(),timeout=3).running_requests == 0,
                    'capacity_released_after_queue_test')
            worker.terminate(); worker.wait(timeout=5)
            time.sleep(6)  # Registry TTL is 5s.
            try:
                list(stub.Generate(request('no-worker',16),timeout=3)); require(False,'stale_worker_removed')
            except grpc.RpcError as error:
                require(error.code() == grpc.StatusCode.UNAVAILABLE,'stale_worker_removed')
            launch(worker_cmd,'worker-restarted'); time.sleep(1.5)
            recovered = list(stub.Generate(request('recovered',16),timeout=10))
            require(recovered[-1].finished and recovered[-1].completion_tokens == 16,
                    'worker_reregistration_recovers_service')
        result['ok'] = True
    except Exception as error:
        result['ok'] = False; result['error'] = str(error)
    finally:
        for proc in reversed(processes):
            if proc.poll() is None: proc.terminate()
        for proc in reversed(processes):
            try: proc.wait(timeout=5)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait()
        for log in files: log.close()
        (args.out/'real-cuda-test.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result),flush=True)
    return 0 if result['ok'] else 1

if __name__ == '__main__': raise SystemExit(main())
