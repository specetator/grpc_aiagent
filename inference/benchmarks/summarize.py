#!/usr/bin/env python3
"""Summarize raw benchmark trials without dropping failures or selecting best runs."""
import argparse
import json
import statistics
from pathlib import Path
from bench_serving import percentile


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('directory')
    p.add_argument('--plot', action='store_true')
    args = p.parse_args(); directory = Path(args.directory)
    results = {}
    for path in sorted(directory.glob('*-c*.json')):
        data = json.loads(path.read_text(encoding='utf-8-sig'))
        if 'trials' not in data: continue
        trials = data['trials']; records = [r for t in trials for r in t['requests']]
        good = [r for r in records if r['ok']]
        throughputs = [t['summary']['output_tokens_per_s'] for t in trials]
        row = {'file':path.name,'successful':len(good),'failed':len(records)-len(good),
               'throughput_median':statistics.median(throughputs),
               'throughput_min':min(throughputs),'throughput_max':max(throughputs),
               'requests_per_s_median':statistics.median(t['summary']['requests_per_s'] for t in trials),
               'completion_tokens':sum(r['completion_tokens'] for r in good),
               'prompt_tokens':sum(r.get('prompt_tokens',0) for r in good),
               'cached_prompt_tokens':sum(r.get('cached_prompt_tokens',0) for r in good),
               'worker_counts':{worker:sum(r.get('worker_id')==worker for r in good)
                                for worker in sorted({r.get('worker_id') for r in good if r.get('worker_id')})}}
        for field in ['ttft_ms','e2e_ms','tpot_ms','accepted_ack_ms']:
            values = [r[field] for r in good if field in r]
            if values: row[field] = {'p50':percentile(values,.5),'p95':percentile(values,.95),
                                    'mean':statistics.mean(values)}
        if row.get('tpot_ms',{}).get('p50',0) > 0:
            row['single_request_decode_tokens_per_s'] = 1000/row['tpot_ms']['p50']
        results[path.stem] = row
    manifests = {p.stem:json.loads(p.read_text()) for p in directory.glob('*-manifest.json')}
    payload = {'definition':'Median total output tokens / measured trial duration; all trial failures retained.',
               'results':results,'failed_profiles':{k:v for k,v in manifests.items() if not v.get('ok')},
               'startup_failure_attempts':{p.stem:json.loads(p.read_text())
                                            for p in directory.glob('*-startup-failure.json')}}
    (directory/'summary.json').write_text(json.dumps(payload,indent=2)+'\n')
    lines = ['| Profile | token/s (median; min–max) | TTFT p50 / p95 ms | E2E p50 ms | Failures |',
             '|---|---:|---:|---:|---:|']
    for key,r in results.items():
        lines.append(f'| {key} | {r["throughput_median"]:.2f}; {r["throughput_min"]:.2f}–{r["throughput_max"]:.2f} '
                     f'| {r.get("ttft_ms",{}).get("p50",0):.1f} / {r.get("ttft_ms",{}).get("p95",0):.1f} '
                     f'| {r.get("e2e_ms",{}).get("p50",0):.1f} | {r["failed"]}/{r["successful"]+r["failed"]} |')
    (directory/'tables.md').write_text('\n'.join(lines)+'\n')
    if args.plot:
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        labels = [('cpu_q8_p4-c4','CPU, FA off'),('rocm_q8_p4-c4','ROCm, FA off'),
                  ('cpu_flash_f16-c4','CPU, FA on'),('rocm_flash_f16-c4','ROCm, FA on'),
                  ('grpc_cpu_flash_f16-c4','gRPC CPU, FA on'),('grpc_rocm_flash_f16-c4','gRPC ROCm, FA on'),
                  ('app_cpu_flash_bridge4-c4','IM CPU, FA on'),('app_rocm_flash_bridge4-c4','IM ROCm, FA on')]
        labels = [(key,label) for key,label in labels if key in results]
        fig,ax = plt.subplots(figsize=(9,5.4),layout='constrained')
        values = [results[k]['throughput_median'] for k,_ in labels]
        bars = ax.barh([v for _,v in labels],values,color=['#68788f' if 'CPU' in v else '#237eae' for _,v in labels])
        ax.errorbar(values,range(len(values)),xerr=[
            [results[k]['throughput_median']-results[k]['throughput_min'] for k,_ in labels],
            [results[k]['throughput_max']-results[k]['throughput_median'] for k,_ in labels]],
            fmt='none',ecolor='#25334b',capsize=4)
        for i,(key,_) in enumerate(labels):
            ax.annotate(f'{values[i]:.1f}',(results[key]['throughput_max'],i),
                        xytext=(6,0),textcoords='offset points',va='center')
        ax.invert_yaxis(); ax.set_xlim(0,max(values)*1.18)
        ax.set_xlabel('Aggregate output tokens / second (median of 3 trials)')
        ax.set_title('Qwen3-0.6B Q8_0 — Ryzen 9600X vs RX 9070 XT / ROCm 7.2')
        ax.grid(axis='x',alpha=.2); ax.set_axisbelow(True)
        fig.savefig(directory/'throughput.png',dpi=180)
        fig.savefig(directory/'throughput.svg')
    print('\n'.join(lines))


if __name__ == '__main__': main()
