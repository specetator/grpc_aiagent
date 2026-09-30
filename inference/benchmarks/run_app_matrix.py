#!/usr/bin/env python3
"""Real IM benchmark: WS -> Comet/Logic/Kafka/Bridge -> gRPC -> runtime -> durable reply.

Requires a dedicated initialized MySQL database, Redis DB and Kafka broker.
Credentials are read from an external JSON file; tokens/passwords are never results.
"""
import argparse
import asyncio
import hashlib
import json
import os
import secrets
import socket
import statistics
import subprocess
import time
import urllib.request
from pathlib import Path

import websockets
from bench_serving import percentile
from run_matrix import ROOT, free_port, wait_health

BOT = 900000000001
PROMPT = ('Explain an inference serving architecture to a software engineer. '
          'Discuss admission, queueing, cancellation, streaming, batching, KV cache reuse, '
          'and latency versus throughput. Write a detailed answer with examples.')


def post(base, path, body, token=None):
    headers = {'Content-Type': 'application/json'}
    if token: headers['Authorization'] = 'Bearer ' + token
    request = urllib.request.Request(base+path, json.dumps(body).encode(), headers)
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    if result.get('code') != 0: raise RuntimeError(f'HTTP business error: {result.get("code")}')
    return result['data']


def register(base):
    return post(base, '/api/register', {'account': 'bench_'+secrets.token_hex(10),
                 'password': secrets.token_hex(16), 'name': 'Serving benchmark'})


def history(base, user):
    uid = user['user_id']
    return post(base, '/api/session/history', {'session_id': f's_{min(uid,BOT)}_{max(uid,BOT)}',
                'anchor_seq': 0, 'limit': 50}, user['token'])['messages']


async def connect(ws_base, user):
    return await websockets.connect(ws_base+'/ws?token='+user['token'],
                                    open_timeout=10, max_size=2**20)


async def request(ws, index):
    client_id = 'bench-'+secrets.token_hex(12)
    body = {'type': 'single_chat', 'to_user_id': BOT, 'client_msg_id': client_id,
            'content': {'text': PROMPT+f'\nQuestion {index % 12}: explain in detail.'}}
    start = time.perf_counter(); first = None; accepted = None
    try:
        await ws.send(json.dumps(body))
        async with asyncio.timeout(120):
            while True:
                event = json.loads(await ws.recv())
                if event.get('type') == 'accepted_ack' and event.get('client_msg_id') == client_id:
                    accepted = (time.perf_counter()-start)*1000
                if event.get('type') == 'hermes_delta' and event.get('delta') and first is None:
                    first = time.perf_counter()
                if event.get('type') != 'single_chat' or event.get('from_user_id') != BOT: continue
                content = event.get('content', {})
                usage = content.get('inference_usage', {})
                tokens = usage.get('completion_tokens', 0)
                if tokens <= 0 or first is None or accepted is None:
                    raise RuntimeError('missing exact runtime usage, delta or accepted_ack')
                end = time.perf_counter(); text = content.get('text', '')
                return {'index': index, 'ok': True, 'completion_tokens': tokens,
                        'prompt_tokens': usage.get('prompt_tokens', 0),
                        'cached_prompt_tokens': usage.get('cached_prompt_tokens', 0),
                        'accepted_ack_ms': accepted, 'ttft_ms': (first-start)*1000,
                        'e2e_ms': (end-start)*1000,
                        'tpot_ms': (end-first)*1000/(tokens-1) if tokens > 1 else 0,
                        'worker_id': content.get('inference_worker_id'),
                        'final_msg_id': event.get('msg_id'),
                        'output_sha256': hashlib.sha256(text.encode()).hexdigest(),
                        'output_preview': text[:180]}
    except Exception as error:
        return {'index': index, 'ok': False, 'error': str(error),
                'e2e_ms': (time.perf_counter()-start)*1000}


async def trial(base, ws_base, count, concurrency):
    # Registration and connection are outside measured serving time. Fresh
    # users keep the conversation context equal across profiles and repeats.
    users = [await asyncio.to_thread(register, base) for _ in range(count)]
    sockets = [await connect(ws_base, user) for user in users]
    sem = asyncio.Semaphore(concurrency)
    async def one(i):
        async with sem: return await request(sockets[i], i)
    start = time.perf_counter()
    records = await asyncio.gather(*(one(i) for i in range(count)))
    duration = time.perf_counter()-start
    await asyncio.gather(*(ws.close() for ws in sockets))
    good = [r for r in records if r['ok']]
    summary = {'duration_s': duration, 'successful': len(good), 'failed': count-len(good),
               'requests_per_s': len(good)/duration,
               'output_tokens_per_s': sum(r['completion_tokens'] for r in good)/duration}
    for field in ['accepted_ack_ms', 'ttft_ms', 'e2e_ms', 'tpot_ms']:
        if good:
            values = [r[field] for r in good]
            summary[field] = {'mean': statistics.mean(values), 'p50': percentile(values,.5),
                              'p95': percentile(values,.95), 'p99': percentile(values,.99)}
    return {'summary': summary, 'requests': records}


async def recovery(base, ws_base):
    user = await asyncio.to_thread(register, base)
    ws = await connect(ws_base, user)
    client_id = 'recovery-'+secrets.token_hex(12)
    body = {'type':'single_chat', 'to_user_id':BOT, 'client_msg_id':client_id,
            'content':{'text':PROMPT}}
    await ws.send(json.dumps(body))
    async with asyncio.timeout(120):
        while True:
            event = json.loads(await ws.recv())
            if event.get('type') == 'hermes_delta': break
    await ws.close()  # Deltas are ephemeral; the completed reply remains durable.
    rows = []
    for _ in range(120):
        rows = await asyncio.to_thread(history, base, user)
        if any(r['sender_id'] == BOT for r in rows): break
        await asyncio.sleep(.2)
    finals = [r for r in rows if r['sender_id'] == BOT]
    if len(finals) != 1: raise RuntimeError('offline final not recovered from history')
    ws = await connect(ws_base, user)
    await ws.send(json.dumps(body))  # Replay the identical client message id.
    await asyncio.sleep(3)
    await ws.close()
    after = await asyncio.to_thread(history, base, user)
    if len(after) != 2 or len([r for r in after if r['sender_id'] == BOT]) != 1:
        raise RuntimeError('duplicate replay created additional persisted messages')
    return {'disconnected_after_first_delta': True, 'history_messages': len(after),
            'durable_final_count': 1, 'duplicate_client_msg_id_replayed': True}


def write_config(out, service, updates):
    path = out/f'{service}.conf'
    lines = (ROOT/f'conf/{service}.conf').read_text().splitlines()
    result = []
    for line in lines:
        key = line.split('=',1)[0] if '=' in line and not line.startswith('#') else None
        if key in updates: line = f'{key}={updates.pop(key)}'
        result.append(line)
    result += [f'{k}={v}' for k,v in updates.items()]
    path.write_text('\n'.join(result)+'\n'); path.chmod(0o600)
    return path


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--out', required=True)
    p.add_argument('--cpu-bin', default='/home/peco/tools/llama.cpp/build/bin/llama-server')
    p.add_argument('--rocm-bin', default='/home/peco/tools/llama.cpp/build-rocm/bin/llama-server')
    p.add_argument('--model', default='/home/peco/models/Qwen3-0.6B-Q8_0.gguf')
    p.add_argument('--secret-file', required=True, help='External JSON with mysql_password')
    p.add_argument('--brokers', default='127.0.0.1:29092')
    p.add_argument('--mysql-port', type=int, default=3306)
    p.add_argument('--mysql-user', default='serving_bench')
    p.add_argument('--mysql-db', default='spark_push_benchmark')
    p.add_argument('--redis-db', type=int, default=11)
    p.add_argument('--requests', type=int, default=12)
    p.add_argument('--repeats', type=int, default=3)
    p.add_argument('--only', nargs='*')
    args = p.parse_args()
    out = Path(args.out).resolve(); out.mkdir(parents=True,exist_ok=True)
    private = Path(args.secret_file).resolve().parent/'app-configs'; private.mkdir(exist_ok=True)
    env = dict(os.environ, HIP_VISIBLE_DEVICES='0', SPARK_PUSH_HERMES_ENABLED='true',
               SPARK_PUSH_HERMES_STREAMING='true', SPARK_PUSH_INFERENCE_BACKEND='grpc',
               SPARK_PUSH_MYSQL_PASSWORD=json.loads(Path(args.secret_file).read_text())['mysql_password'])
    env['LD_LIBRARY_PATH'] = '/opt/rocm/lib:'+env.get('LD_LIBRARY_PATH','')
    profiles = [('app_cpu_bridge1',False,1,False), ('app_rocm_bridge1',True,1,False),
                ('app_rocm_bridge4',True,4,False), ('app_rocm_flash_bridge4',True,4,True),
                ('app_cpu_flash_bridge4',False,4,True)]
    failed = False
    for label, gpu, workers, flash in profiles:
        if args.only and label not in args.only: continue
        processes, logs = [], []; manifest = {'label':label, 'gpu':gpu,
            'bridge_workers':workers, 'flash_attention':flash}
        def launch(cmd, name):
            log = open(out/f'{label}-{name}.log','w'); logs.append(log)
            process = subprocess.Popen(cmd,env=env,cwd=ROOT,stdout=log,stderr=subprocess.STDOUT)
            processes.append(process); return process
        try:
            ports = {k:free_port() for k in ['runtime','gateway','gm','worker','logic','http','ws','comet','jobm','cometm']}
            prefix = 'bench_'+secrets.token_hex(6)+'_'
            topics = {k:prefix+k for k in ['single','group','broadcast','persist','ai_request','ai_delta','ai_reply']}
            common = {'kafka_brokers':args.brokers, 'kafka_single_topic':topics['single'],
                      'kafka_group_topic':topics['group'], 'kafka_broadcast_topic':topics['broadcast'],
                      'kafka_persist_topic':topics['persist'], 'kafka_consumer_group':prefix+'group',
                      'mysql_port':args.mysql_port, 'mysql_user':args.mysql_user, 'mysql_db':args.mysql_db,
                      'mysql_password':'${SPARK_PUSH_MYSQL_PASSWORD}'}
            configs = {
                'logic':dict(common, listen_addr='127.0.0.1',listen_port=ports['logic'],http_port=ports['http'],
                    redis_db=args.redis_db, hermes_enabled='true', agent_bot_users='',
                    kafka_ai_request_topic=topics['ai_request'],kafka_ai_delta_topic=topics['ai_delta'],
                    kafka_ai_reply_topic=topics['ai_reply']),
                'comet':dict(listen_addr='127.0.0.1',listen_port=ports['ws'],comet_grpc_port=ports['comet'],
                    metrics_port=ports['cometm'],logic_grpc_target=f'127.0.0.1:{ports["logic"]}'),
                'job':dict(common,comet_targets=f'comet-1=127.0.0.1:{ports["comet"]}',metrics_port=ports['jobm']),
                'hermes_bridge':dict(kafka_brokers=args.brokers,kafka_request_topic=topics['ai_request'],
                    kafka_delta_topic=topics['ai_delta'],kafka_reply_topic=topics['ai_reply'],
                    kafka_consumer_group=prefix+'bridge',inference_backend='grpc',
                    inference_gateway=f'127.0.0.1:{ports["gateway"]}',inference_model='qwen3-0.6b',
                    processing_workers=workers,max_batch_records=16,hermes_streaming='true',
                    inference_temperature_milli=0,inference_max_tokens=128,inference_cache_prompt='false')}
            runtime = [args.rocm_bin if gpu else args.cpu_bin,
                       '-m',args.model,'--alias','qwen3-0.6b',
                       '--host','127.0.0.1','--port',str(ports['runtime']),'-np','4','-c','8192',
                       '-t','6','-tb','6','-b','512','-ub','128','--metrics','-fa','on' if flash else 'off','-cb',
                       '--no-cache-prompt','--cache-ram','0','--chat-template-kwargs','{"enable_thinking":false}']
            runtime += ['-ngl','99'] if gpu else ['-ngl','0','--device','none','--no-kv-offload','--no-op-offload']
            manifest['runtime_command'] = runtime
            proc = launch(runtime,'runtime'); wait_health(f'http://127.0.0.1:{ports["runtime"]}',proc)
            launch([str(ROOT/'build-wsl/inference/inference_gateway'),'--listen',f'127.0.0.1:{ports["gateway"]}',
                    '--metrics-port',str(ports['gm'])],'gateway'); time.sleep(.2)
            launch([str(ROOT/'build-wsl/inference/inference_worker'),'--listen',f'127.0.0.1:{ports["worker"]}',
                    '--gateway',f'127.0.0.1:{ports["gateway"]}','--worker-id','app-worker',
                    '--model','qwen3-0.6b','--backend','llamacpp','--model-endpoint',f'http://127.0.0.1:{ports["runtime"]}',
                    '--max-concurrent','4','--device-type','rocm' if gpu else 'cpu'],'worker')
            for service in ['logic','comet','job','hermes_bridge']:
                config = write_config(private,service,configs[service])
                binary = 'hermes_bridge' if service == 'hermes_bridge' else service+'_server'
                launch([str(ROOT/f'build-wsl/{service}/{binary}'),'--config',str(config)],service)
                time.sleep(.5)
            time.sleep(5)
            if any(process.poll() is not None for process in processes): raise RuntimeError('service exited at startup')
            base, ws_base = f'http://127.0.0.1:{ports["http"]}', f'ws://127.0.0.1:{ports["ws"]}'
            warmup = asyncio.run(trial(base,ws_base,2,1))
            if warmup['summary']['failed']: raise RuntimeError(f'warmup failed: {warmup}')
            trials = [asyncio.run(trial(base,ws_base,args.requests,4)) for _ in range(args.repeats)]
            payload = {'schema':'sparkpush.app_benchmark.v1', 'configuration':vars(args)|{'label':label,
                       'concurrency':4,'bridge_workers':workers,'max_tokens':128,'temperature':0,'cache_prompt':False},
                       'prompt_sha256':hashlib.sha256(PROMPT.encode()).hexdigest(),'warmup':warmup,'trials':trials}
            # Paths are fine to share; credential contents and client tokens never enter payload.
            (out/f'{label}-c4.json').write_text(json.dumps(payload,indent=2,ensure_ascii=False)+'\n')
            manifest['recovery'] = asyncio.run(recovery(base,ws_base))
            for name, port in [('gateway',ports['gm']),('job',ports['jobm']),('comet',ports['cometm'])]:
                (out/f'{label}-{name}-metrics.txt').write_bytes(urllib.request.urlopen(f'http://127.0.0.1:{port}/metrics').read())
            manifest['ok'] = not any(t['summary']['failed'] for t in trials)
            failed |= not manifest['ok']
            print(json.dumps({'label':label,'median_output_tokens_per_s':statistics.median(
                t['summary']['output_tokens_per_s'] for t in trials),'recovery':manifest['recovery']}),flush=True)
        except Exception as error:
            failed = True; manifest['ok'] = False; manifest['error'] = str(error)
            print(json.dumps({'label':label,'error':str(error)}),flush=True)
        finally:
            for process in reversed(processes):
                if process.poll() is None: process.terminate()
            for process in reversed(processes):
                try: process.wait(timeout=10)
                except subprocess.TimeoutExpired: process.kill(); process.wait()
            for log in logs: log.close()
            (out/f'{label}-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
    return int(failed)


if __name__ == '__main__': raise SystemExit(main())
