import sys
import urllib.request

role = sys.argv[1]
port = {'logic':9101, 'comet':9203, 'job':9202, 'web':9010}[role]
path = '/metrics' if role == 'comet' else '/chatroom.html' if role == 'web' else '/ready'
try:
    with urllib.request.urlopen(f'http://127.0.0.1:{port}{path}', timeout=3) as response:
        body = response.read().decode()
    if role == 'comet' and 'spark_push_comet_logic_stream_ready 1' not in body:
        raise ValueError('Logic stream not ready')
except Exception:
    sys.exit(1)
