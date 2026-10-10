#!/usr/bin/env python3
"""Selected real CUDA operator correctness cases against upstream CPU reference.

Not full backend coverage or model-quality evaluation. Fails on zero executed cases.
"""
import argparse
import json
import re
import subprocess
import time
from pathlib import Path

CASES = [
    ('matmul_q8_f16','MUL_MAT',r'type_a=(q8_0|f16),type_b=f32,m=16,'),
    ('rms_norm','RMS_NORM',r'type=f32,ne=\[(64|128|1024|4096),'),
    ('rope_head128','ROPE',r'type=f32,ne_a=\[128,'),
    ('flash_f16','FLASH_ATTN_EXT',r'hsk=128,hsv=128,nh=(8|1),nr23=\[(4|8),1\],kv=(1024|2048|4096),nb=(1|32|64),.*type_K=f16,type_V=f16,'),
    ('flash_q8','FLASH_ATTN_EXT',r'hsk=128,hsv=128,.*type_K=q8_0,type_V=q8_0,'),
]

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--binary',required=True)
    p.add_argument('--out',type=Path,required=True)
    a = p.parse_args(); a.out.mkdir(parents=True,exist_ok=True)
    result = {'schema':'sparkpush.cuda_ops.v1','reference':'llama.cpp test-backend-ops CPU backend', 'cases':{}}
    for name, op, params in CASES:
        cmd = [a.binary,'test','-b','CUDA0','-o',op,'-p',params]
        start = time.perf_counter()
        proc = subprocess.run(cmd,capture_output=True,text=True,timeout=300)
        output = re.sub(r'\x1b\[[0-9;]*m','',proc.stdout+proc.stderr)
        (a.out/f'ops-{name}.txt').write_text(output)
        lines = [line for line in output.splitlines() if line.strip().startswith(op+'(')]
        passed = sum(line.endswith('OK') for line in lines)
        skipped = sum('not supported' in line.lower() for line in lines)
        ok = proc.returncode == 0 and passed > 0 and bool(re.search(r'Device description:.*NVIDIA',output))
        result['cases'][name] = {'command':cmd,'exit_code':proc.returncode,'duration_s':time.perf_counter()-start,
                                'selected':len(lines),'passed':passed,'skipped':skipped,'ok':ok}
        print(json.dumps({'operator':name,**result['cases'][name]}),flush=True)
    result['ok'] = all(row['ok'] for row in result['cases'].values())
    (a.out/'cuda-operators.json').write_text(json.dumps(result,indent=2)+'\n')
    return 0 if result['ok'] else 1

if __name__ == '__main__': raise SystemExit(main())
