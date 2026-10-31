"""Initialize only the owned Docker volume; never read host Agent credentials."""
import fcntl
import json
import os
from pathlib import Path
import secrets
import subprocess

ROOT = Path(os.environ.get('SPARK_SECRETS_DIR', '/run/spark-secrets'))
ROOT.mkdir(parents=True, exist_ok=True)
os.chmod(ROOT, 0o750)
os.chown(ROOT, 0, 10001)
ADMIN = Path('/run/spark-admin')
ADMIN.mkdir(parents=True, exist_ok=True)
os.chmod(ADMIN, 0o750)
os.chown(ADMIN, 0, 10001)

def save(path, data):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(data)
    temporary.chmod(0o640)
    os.chown(temporary, 0, 10001)
    os.replace(temporary, path)

with (ROOT / '.lock').open('w') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    if (ROOT/'ready').exists() and not all(p.is_file() for p in
            (ROOT/'security.json', ROOT/'rpc.key', ROOT/'rpc.crt', ADMIN/'mysql-root-password')):
        raise SystemExit('Initialized security volume is incomplete; restore it instead of rotating credentials.')
    supplied = os.environ.get('SPARK_PUSH_MYSQL_PASSWORD', '')
    if supplied == 'replace-with-a-local-password':
        supplied = ''
    root_password = ADMIN / 'mysql-root-password'
    if root_password.exists():
        if supplied and root_password.read_text() != supplied:
            raise SystemExit('Configured MySQL password differs from initialized volume; use the existing password.')
    else:
        save(root_password, supplied or secrets.token_hex(32))
    security = ROOT / 'security.json'
    if not security.exists():
        save(security, json.dumps({name: secrets.token_hex(32) for name in
             ('comet', 'job', 'admin', 'mysql', 'gateway')}))
    if not (ROOT / 'rpc.crt').exists():
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:3072', '-nodes',
                        '-keyout', str(ROOT/'rpc.key.tmp'), '-out', str(ROOT/'rpc.crt.tmp'),
                        '-days', '3650', '-subj', '/CN=Spark Push internal RPC',
                        '-addext', 'subjectAltName=DNS:logic,DNS:comet',
                        '-addext', 'basicConstraints=critical,CA:TRUE'],
                       check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for name in ('rpc.key', 'rpc.crt'):
            temporary = ROOT / (name + '.tmp')
            temporary.chmod(0o640)
            os.chown(temporary, 0, 10001)
            os.replace(temporary, ROOT/name)
    # Do not silently replace a missing private key behind an existing certificate.
    if not (ROOT / 'rpc.key').is_file():
        raise SystemExit('RPC certificate exists without private key; restore the secrets volume.')
    subprocess.run(['openssl', 'x509', '-in', str(ROOT/'rpc.crt'), '-checkend', '604800', '-noout'],
                   check=True, stdout=subprocess.DEVNULL)
    for name in ('/app/data/attachments', '/app/logs'):
        directory = Path(name)
        directory.mkdir(parents=True, exist_ok=True)
        os.chown(directory, 10001, 10001)
    save(ROOT/'ready', 'initialized\n')
print('Persistent application credentials and internal RPC TLS are ready.')
