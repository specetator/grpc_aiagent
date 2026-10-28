"""Windows-side sequential loopback HTTP relay over inherited stdio.

No listening socket, no shell command execution, no upstream credential output.
The WSL service supplies a trusted request; EOF terminates the relay so a
disconnected IM stream cannot leave an abandoned HTTP request running.
"""
import base64
import json
import os
import re
import queue
import sys
import threading
from urllib.request import Request, build_opener, ProxyHandler, HTTPRedirectHandler
from urllib.parse import urlparse


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def emit(value):
    print(json.dumps(value),flush=True)


def allowed_technical_path(path):
    """Loopback allowlist for the existing technical API server.

    Legacy chat/session routes stay. Native mode adds only the Runs API
    routes confirmed in the installed Hermes 0.21.1 server, plus the
    read-only skills/toolsets catalog. Nothing else on port 8644 is opened.
    """
    if path in {'/v1/models','/v1/chat/completions','/api/model/options','/api/sessions',
                '/v1/skills','/v1/toolsets','/v1/health','/v1/capabilities'}:
        return True
    if re.fullmatch(r'/api/sessions/[A-Za-z0-9_-]{1,240}(?:/model)?', path):
        return True
    return re.fullmatch(
        r'/v1/runs(?:/[A-Za-z0-9_-]{1,80}(?:/(?:events|stop|approval))?)?', path) is not None


def streams_response(path):
    return path.endswith(('/chat/completions','/chat/stream','/events'))


def serve(request):
    parsed=urlparse(request['url'])
    if (parsed.scheme!='http' or parsed.hostname!='127.0.0.1' or parsed.port!=8644 or
        not allowed_technical_path(parsed.path) or
        parsed.query or parsed.fragment or parsed.username or parsed.password):
        raise ValueError('only the technical loopback API is allowed')
    body=request.get('payload')
    data=None if body is None else json.dumps(body,ensure_ascii=False).encode()
    opener=build_opener(ProxyHandler({}),NoRedirect())
    with opener.open(Request(request['url'],data=data,headers=request['headers']),timeout=request['timeout']) as response:
        emit({'headers':{'X-Hermes-Session-Id':response.headers.get('X-Hermes-Session-Id','')}})
        if not streams_response(parsed.path):
            data=response.read(1024*1024+1)
            if len(data)>1024*1024:
                raise ValueError('catalog too large')
            emit({'body':base64.b64encode(data).decode()})
        else:
            for data in iter(lambda:response.readline(1024*1024+1),b''):
                if len(data)>1024*1024:
                    raise ValueError('stream line too large')
                emit({'body':base64.b64encode(data).decode()})
        emit({'eof':True})


def main():
    requests=queue.Queue(maxsize=1)
    def receive():
        while True:
            raw=sys.stdin.buffer.readline(2*1024*1024+1)
            if not raw or len(raw)>2*1024*1024:
                os._exit(0)
            try:
                requests.put_nowait(json.loads(raw))
            except (ValueError,queue.Full):
                os._exit(1)
    threading.Thread(target=receive,daemon=True).start()
    while True:
        try:
            serve(requests.get())
        except Exception as exc:
            emit({'error':type(exc).__name__,'status':getattr(exc,'code',None)})
            return  # Failed transports are discarded, never implicitly retried.


if __name__=='__main__':
    main()
