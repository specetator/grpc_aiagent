#!/usr/bin/env python3
"""Read-only portability inventory; emits counts/field names, never values or secrets."""
import argparse
import json
from pathlib import Path
import re
import shutil
import yaml

WINDOWS_PATH=re.compile(r'(?<![A-Za-z0-9])[A-Za-z]:[\\/]')
LOOPBACK=re.compile(r'https?://(?:localhost|127\.0\.0\.1)(?=[:/]|$)',re.I)
WINDOWS_SHELL=re.compile(r'\bpowershell(?:\.exe)?\b|\bcmd\.exe\b|\bpythonw?\.exe\b',re.I)
SECRET=re.compile(r'key|token|secret|password|auth|cookie',re.I)


def audit(root):
    config=yaml.safe_load((root/'config.yaml').read_text(encoding='utf-8')) or {}
    references=[]
    def scan(value,field='config'):
        if isinstance(value,dict):
            for key,item in value.items():
                if not SECRET.search(str(key)):
                    scan(item,field+'.'+str(key))
        elif isinstance(value,list):
            for item in value:
                scan(item,field+'[]')
        elif isinstance(value,str):
            for category,pattern in [('windows_path',WINDOWS_PATH),('windows_shell',WINDOWS_SHELL),('loopback_service',LOOPBACK)]:
                if pattern.search(value):
                    references.append({'category':category,'field':field})
    scan(config)
    components={}
    for name in ('skills','hooks','memories','memory'):
        folder=root/name
        count=windows=loopback=shell=0
        if folder.is_dir():
            for path in folder.rglob('*'):
                if path.is_symlink() or not path.is_file():
                    continue
                count+=1
                if path.suffix.lower() not in {'.md','.py','.json','.yaml','.yml','.toml','.sh','.ps1','.cmd'} or path.stat().st_size>512*1024:
                    continue
                content=path.read_text(encoding='utf-8',errors='replace')
                windows+=bool(WINDOWS_PATH.search(content))
                loopback+=bool(LOOPBACK.search(content))
                shell+=bool(WINDOWS_SHELL.search(content))
        components[name]={'files':count,'files_with_windows_paths':windows,
                          'files_with_loopback_urls':loopback,'files_with_windows_shell_references':shell}
    return {'config_references':references,'components':components,
            'terminal_backend':config.get('terminal',{}).get('backend'),
            'tools_available':{name:bool(shutil.which(name)) for name in ('python3.11','uv','node','npm','git','ffmpeg','docker')},
            'notes':['Text references may be examples; counts alone are not executable dependency proof.',
                     'Secrets, live databases and conversation contents are not exported.']}


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile',type=Path,default=Path('/mnt/f/hermes/profiles/technical'))
    args=parser.parse_args()
    try:
        report=audit(args.profile)
    except (OSError,yaml.YAMLError):
        raise SystemExit('Profile audit failed: unable to read valid configuration') from None
    print(json.dumps(report,ensure_ascii=False,indent=2))
