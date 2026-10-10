#!/usr/bin/env python3
"""Record reproducible, non-secret CPU/CUDA/model provenance before a benchmark."""
import argparse
import datetime
import hashlib
import importlib.metadata
import json
import platform
import subprocess
from pathlib import Path

def command(args):
    proc = subprocess.run(args,capture_output=True,text=True,timeout=30)
    return {'command':args,'exit_code':proc.returncode,'output':(proc.stdout+proc.stderr).strip()}

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--model',type=Path,required=True)
    p.add_argument('--toolkit',type=Path,default=Path('/home/peco/tools/cuda-12.9'))
    p.add_argument('--engine',type=Path,default=Path('/home/peco/tools/llama.cpp'))
    p.add_argument('--out',type=Path,required=True)
    a = p.parse_args(); digest = hashlib.sha256()
    with a.model.open('rb') as f:
        for chunk in iter(lambda:f.read(2**20),b''): digest.update(chunk)
    data = {'schema':'sparkpush.cuda_environment.v1',
        'time_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'platform':platform.platform(),'python':platform.python_version(),
        'model':{'path':str(a.model.resolve()),'bytes':a.model.stat().st_size,'sha256':digest.hexdigest()},
        'cpu':command(['lscpu']),
        'gpu':command(['nvidia-smi','--query-gpu=name,driver_version,memory.total,memory.used,compute_cap',
                       '--format=csv,noheader']),
        'toolkit':command([str(a.toolkit/'bin/nvcc'),'--version']),
        'engine_commit':command(['git','-C',str(a.engine),'rev-parse','HEAD']),
        'cpu_runtime':command([str(a.engine/'build-cpu/bin/llama-server'),'--version']),
        'cuda_runtime':command([str(a.engine/'build-cuda/bin/llama-server'),'--version']),
        'packages':{name:importlib.metadata.version(name) for name in
                    ['grpcio','grpcio-tools','protobuf','websockets','torch','transformers','gguf']}}
    a.out.parent.mkdir(parents=True,exist_ok=True)
    a.out.write_text(json.dumps(data,indent=2)+'\n')
    print(json.dumps({'environment':str(a.out),'model_sha256':digest.hexdigest()}))

if __name__ == '__main__': main()
