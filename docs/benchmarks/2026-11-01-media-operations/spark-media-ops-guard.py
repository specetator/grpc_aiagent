import json,secrets,subprocess,sys
from pathlib import Path
root=Path(sys.argv[1]);unit='spark-media-worker-guard-'+secrets.token_hex(5)
subprocess.run(['systemd-run','--user','--collect','--unit='+unit,'--description=Unrelated owned test fixture','/usr/bin/sleep','60'],check=True,capture_output=True)
try:
    denied=subprocess.run(['bash',str(root/'scripts/sparkctl.sh'),'media','stop','--unit',unit],capture_output=True,text=True)
    active=subprocess.run(['systemctl','--user','is-active','--quiet',unit+'.service']).returncode==0
    assert denied.returncode!=0 and active
    print(json.dumps({'passed':True,'kind':'real systemd ownership guard','unit':unit,'foreign_description_stop_exit':denied.returncode,'foreign_fixture_still_active':active}))
finally:
    subprocess.run(['systemctl','--user','stop',unit+'.service'],check=True)
