#!/usr/bin/env python3
"""Provision only the technical API credential/port and IM registry.

No model secrets are printed, no Telegram configuration is copied, and no
second process is launched against the same Hermes profile. Restart the existing
technical gateway after applying. Default is a read-only plan.
"""
import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import secrets


def write_private(path, data):
    if path.is_symlink():
        raise ValueError('refusing symlink target')
    if path.exists():
        backup=path.with_name(path.name+'.before-im-agents-'+datetime.now().strftime('%Y%m%d%H%M%S%f'))
        with backup.open('xb') as out:
            os.fchmod(out.fileno(),0o600)
            out.write(path.read_bytes())
    temp=path.with_name(path.name+'.im-agents-tmp')
    with temp.open('xb') as out:
        os.fchmod(out.fileno(),0o600)
        out.write(data)
    os.replace(temp,path)


def provision(profile, pi_home, owner, apply=False):
    if profile.is_symlink() or pi_home.is_symlink() or not (profile/'config.yaml').is_file():
        raise ValueError('invalid existing profile/home')
    if owner<=0 or owner>=2**63 or owner==900000000001:
        raise ValueError('invalid IM owner')
    env_path=profile/'.env'
    lines=env_path.read_text(encoding='utf-8').splitlines() if env_path.exists() else []
    old={k.strip():v.strip().strip('\"').strip("'") for line in lines
         if '=' in line and not line.lstrip().startswith('#') for k,v in [line.split('=',1)]}
    changes={'API_SERVER_KEY':old.get('API_SERVER_KEY') or secrets.token_urlsafe(48),
             'API_SERVER_ENABLED':'true','API_SERVER_HOST':'127.0.0.1',
             'API_SERVER_PORT':'8644','API_SERVER_MODEL_NAME':'technical'}
    if len(changes['API_SERVER_KEY'])<16:
        raise ValueError('existing technical API credential is too short')
    result=[line for line in lines if line.split('=',1)[0].strip() not in changes]
    result+=['']+[key+'='+value for key,value in changes.items()]
    registry_path=pi_home/'agents.json'
    if registry_path.exists():
        registry=json.loads(registry_path.read_text())
        if registry.get('schema')!=1:
            raise ValueError('unknown existing registry schema')
    else:
        registry={'schema':1,'default_agent':'pi','bot_user_id':900000000001,
            'agents':[{'id':'pi','name':'Pi Agent','runtime':'pi_rpc','owner_user_ids':[],'bot_user_id':900000000001}]}
    entry={'id':'hermes-technical','name':'Hermes · technical','runtime':'hermes_http',
        'owner_user_ids':[owner],'bot_user_id':900000000101,'profile':'technical','base_url':'http://127.0.0.1:8644/v1',
        'transport':'windows_stdio','windows_python':'/mnt/f/hermes/hermes-agent/venv/Scripts/python.exe',
        'expected_model':'technical','credential_env_file':str(env_path),'credential_env_key':'API_SERVER_KEY'}
    existing=next((e for e in registry['agents'] if e.get('id')=='hermes-technical'),None)
    if existing is not None and {**existing,'bot_user_id':900000000101}!=entry:
        raise ValueError('existing technical registration differs; review it before changing ownership')
    for agent in registry['agents']:
        if agent['id']=='pi': agent.setdefault('bot_user_id',900000000001)
        if agent['id']=='hermes-technical': agent['bot_user_id']=900000000101
    if existing is None:
        registry['agents'].append(entry)
    if apply:
        write_private(env_path,('\n'.join(result)+'\n').encode())
        write_private(registry_path,(json.dumps(registry,ensure_ascii=False,indent=2)+'\n').encode())
    print(('Applied' if apply else 'Plan')+': technical loopback-only authenticated API :8644 via local stdio; owner-only IM Agent registration; existing model/workspace/settings preserved.')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--profile-root',type=Path,default=Path('/mnt/f/hermes/profiles/technical'))
    parser.add_argument('--pi-home',type=Path,default=Path.home()/'.pi-spark-agent')
    parser.add_argument('--owner-user-id',required=True,type=int)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    provision(args.profile_root,args.pi_home,args.owner_user_id,args.apply)
