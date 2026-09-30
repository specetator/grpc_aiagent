#!/usr/bin/env python3
"""Real HTTP or gRPC serving benchmark. Counts runtime tokens, never chunks.

gRPC requires grpcio plus generated modules in --proto-dir. Generate those with
grpc_tools.protoc into an ignored build directory, not the source tree.
"""
import argparse
import concurrent.futures
import hashlib
import json
import math
import platform
import statistics
import sys
import time
import urllib.request
from pathlib import Path


def percentile(values, p):
    values = sorted(values)
    rank = (len(values) - 1) * p
    lo, hi = math.floor(rank), math.ceil(rank)
    return values[lo] + (values[hi] - values[lo]) * (rank - lo)


def run_request(args, index):
    prompt = args.prompt + f'\nQuestion {index % 12}: explain the tradeoffs in detail.'
    start = time.perf_counter()
    first = None
    text = ''
    usage, timings = {}, {}
    worker_id = None
    finish = None
    expected_tokens = args.max_tokens
    if args.mixed_lengths:
        expected_tokens = [32, 64, 128, 256][index % 4]
    try:
        if args.transport == 'http':
            body = {'model': args.model, 'messages': [{'role': 'user', 'content': prompt}],
                    'stream': True, 'stream_options': {'include_usage': True},
                    'max_tokens': expected_tokens, 'temperature': 0, 'seed': 42,
                    'ignore_eos': True, 'cache_prompt': args.cache_prompt,
                    'chat_template_kwargs': {'enable_thinking': False}}
            req = urllib.request.Request(args.url.rstrip('/') + '/v1/chat/completions',
                                         json.dumps(body).encode(),
                                         {'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=120) as response:
                for line in response:
                    if not line.startswith(b'data:'): continue
                    data = line[5:].strip()
                    if data == b'[DONE]': break
                    event = json.loads(data)
                    if event.get('usage'): usage = event['usage']
                    if event.get('timings'): timings = event['timings']
                    for choice in event.get('choices', []):
                        delta = choice.get('delta', {}).get('content', '')
                        if delta:
                            if first is None: first = time.perf_counter()
                            text += delta
                        if choice.get('finish_reason'): finish = choice['finish_reason']
        else:
            import grpc
            import inference_pb2 as pb
            import inference_pb2_grpc as rpc
            request = pb.GenerateRequest(request_id=f'bench-{time.time_ns()}-{index}',
                session_id=f'bench-session-{index % args.concurrency}', model=args.model,
                prompt=prompt, max_tokens=expected_tokens, temperature=0,
                metadata={'seed': '42', 'ignore_eos': 'true',
                          'cache_prompt': str(args.cache_prompt).lower()})
            with grpc.insecure_channel(args.gateway) as channel:
                stream = rpc.InferenceGatewayStub(channel).Generate(request, timeout=120)
                sequence = 0
                for chunk in stream:
                    if chunk.sequence != sequence or chunk.request_id != request.request_id:
                        raise RuntimeError('invalid stream sequence/request id')
                    sequence += 1
                    if chunk.text_delta:
                        if first is None: first = time.perf_counter()
                        text += chunk.text_delta
                    if chunk.finished:
                        worker_id = chunk.worker_id
                        finish = chunk.finish_reason
                        usage = {'prompt_tokens': chunk.prompt_tokens,
                                 'completion_tokens': chunk.completion_tokens,
                                 'prompt_tokens_details': {'cached_tokens': chunk.cached_prompt_tokens}}
                        timings = {'prompt_ms': chunk.runtime_prefill_ms,
                                   'predicted_ms': chunk.runtime_decode_ms}
        end = time.perf_counter()
        tokens = usage.get('completion_tokens', 0)
        if finish is None or first is None or tokens != expected_tokens:
            raise RuntimeError(f'incomplete generation: finish={finish}, tokens={tokens}')
        return {'index': index, 'ok': True, 'completion_tokens': tokens, 'worker_id': worker_id,
                'prompt_tokens': usage.get('prompt_tokens', 0),
                'cached_prompt_tokens': usage.get('prompt_tokens_details', {}).get('cached_tokens', 0),
                'ttft_ms': (first-start)*1000, 'e2e_ms': (end-start)*1000,
                'tpot_ms': (end-first)*1000/(tokens-1) if tokens > 1 else 0,
                'runtime_prefill_ms': timings.get('prompt_ms', 0),
                'runtime_decode_ms': timings.get('predicted_ms', 0),
                'finish_reason': finish, 'output_sha256': hashlib.sha256(text.encode()).hexdigest(),
                'output_preview': text[:180]}
    except Exception as error:
        return {'index': index, 'ok': False, 'error': str(error),
                'e2e_ms': (time.perf_counter()-start)*1000}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--transport', choices=['http', 'grpc'], default='http')
    p.add_argument('--url', default='http://127.0.0.1:8080')
    p.add_argument('--gateway', default='127.0.0.1:9300')
    p.add_argument('--proto-dir')
    p.add_argument('--model', default='qwen3-0.6b')
    p.add_argument('--label', required=True)
    p.add_argument('--concurrency', type=int, default=1)
    p.add_argument('--requests', type=int, default=12)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--warmup', type=int, default=2)
    p.add_argument('--max-tokens', type=int, default=128)
    p.add_argument('--cache-prompt', action='store_true')
    p.add_argument('--mixed-lengths', action='store_true')
    p.add_argument('--prompt-file')
    p.add_argument('--out', required=True)
    args = p.parse_args()
    if min(args.concurrency, args.requests, args.repeats) < 1: p.error('positive counts required')
    if args.proto_dir: sys.path.insert(0, args.proto_dir)
    args.prompt = Path(args.prompt_file).read_text() if args.prompt_file else (
        'You are explaining an inference serving system to a software engineer. '
        'Describe request admission, queueing, cancellation, streaming, batching, '
        'KV cache reuse, and the difference between latency and throughput. '
        'Use concrete examples and write a long, detailed answer.')
    warmup = [run_request(args, -i-1) for i in range(args.warmup)]
    if not all(row['ok'] for row in warmup): raise RuntimeError(f'warmup failed: {warmup}')
    trials = []
    for repeat in range(args.repeats):
        start = time.perf_counter()
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            records = list(pool.map(lambda i: run_request(args, i), range(args.requests)))
        duration = time.perf_counter()-start
        good = [row for row in records if row['ok']]
        summary = {'duration_s': duration, 'successful': len(good),
                   'failed': len(records)-len(good),
                   'requests_per_s': len(good)/duration,
                   'output_tokens_per_s': sum(r['completion_tokens'] for r in good)/duration}
        if good:
            for field in ['ttft_ms', 'e2e_ms', 'tpot_ms', 'runtime_prefill_ms', 'runtime_decode_ms']:
                values = [r[field] for r in good]
                summary[field] = {'mean': statistics.mean(values), 'p50': percentile(values, .5),
                                  'p95': percentile(values, .95), 'p99': percentile(values, .99)}
            summary['cached_prompt_tokens'] = sum(r['cached_prompt_tokens'] for r in good)
            summary['prompt_tokens'] = sum(r['prompt_tokens'] for r in good)
        trials.append({'repeat': repeat, 'summary': summary, 'requests': records})
    payload = {'schema': 'sparkpush.serving_benchmark.v1',
               'client_platform': platform.platform(),
               'configuration': {k:v for k,v in vars(args).items() if k != 'prompt'},
               'prompt_sha256': hashlib.sha256(args.prompt.encode()).hexdigest(),
               'warmup': warmup, 'trials': trials}
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(payload, indent=2, ensure_ascii=False)+'\n')
    print(json.dumps({'label': args.label, 'concurrency': args.concurrency,
          'median_output_tokens_per_s': statistics.median(t['summary']['output_tokens_per_s'] for t in trials),
          'median_request_ttft_ms': statistics.median(t['summary'].get('ttft_ms', {}).get('p50', 0) for t in trials),
          'failed': sum(t['summary']['failed'] for t in trials)}, ensure_ascii=False), flush=True)
    return int(any(t['summary']['failed'] for t in trials))


if __name__ == '__main__':
    sys.exit(main())
