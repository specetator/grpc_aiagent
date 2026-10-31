"""Container wiring only; protocol/permission rules remain in the C++ services."""
import json
import os
from pathlib import Path
import sys

role = sys.argv[1] if len(sys.argv) > 1 else 'logic'
if role == 'bootstrap':
    os.execv(sys.executable, [sys.executable, '/app/deploy/bootstrap.py'])
if role == 'migrate':
    os.execv(sys.executable, [sys.executable, '/app/deploy/migrate.py'])
if role == 'web':
    os.execv('/usr/local/bin/web_demo_server', ['web_demo_server', '--port', '9010', '--doc-root', '/app/static'])
if role not in {'logic', 'comet', 'job', 'hermes_bridge'}:
    raise SystemExit('Unknown Spark service: ' + role)
root = Path('/run/spark-secrets')
if not (root/'ready').is_file():
    raise SystemExit('Run the bootstrap service before starting Spark.')
security = json.loads((root/'security.json').read_text())
for name in ('comet', 'job', 'admin'):
    os.environ['SPARK_RPC_' + name.upper() + '_KEY'] = security[name]
os.environ['SPARK_RPC_TLS_CA'] = str(root/'rpc.crt')
os.environ['SPARK_RPC_TLS_CERT'] = str(root/'rpc.crt')
os.environ['SPARK_RPC_TLS_KEY'] = str(root/'rpc.key')
os.environ['SPARK_PUSH_MYSQL_PASSWORD'] = security['mysql']
os.environ.setdefault('SPARK_PUSH_HERMES_API_KEY', security['gateway'])

# Retain all shipped tuning/defaults, overriding only deployment endpoints.
config = Path('/app/conf/' + role + '.conf').read_text()
overrides = {'kafka_brokers':'kafka:29092', 'mysql_host':'mysql', 'mysql_port':'3306',
             'mysql_user':'spark', 'redis_host':'redis', 'internal_grpc_listen_addr':'0.0.0.0',
             'logic_grpc_target':'logic:9100', 'comet_targets':'comet-1=comet:9105',
             'attachment_dir':'/app/data/attachments'}
config += '\n' + '\n'.join(k+'='+v for k,v in overrides.items()) + '\n'
target = Path('/tmp/spark-' + role + '.conf')
target.write_text(config)
binary = role if role == 'hermes_bridge' else role + '_server'
os.execv('/usr/local/bin/' + binary, [binary, '--config', str(target)])
