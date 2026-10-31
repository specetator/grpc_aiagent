"""Bounded HTTP transport with DNS pinning; customer URLs are untrusted."""
import http.client
import ipaddress
import json
import os
import socket
import ssl
import threading
from urllib.parse import urlsplit

LIMIT = 8 * 1024 * 1024


class Endpoint:
    def __init__(self, url, token, *, trusted=False):
        if not isinstance(url, str) or len(url) > 2048 or any(ord(c) < 33 for c in url):
            raise ValueError('invalid connector URL')
        p = urlsplit(url)
        if p.scheme not in {'https', 'http'} or not p.hostname or p.username or p.password or p.query or p.fragment:
            raise ValueError('use an HTTP(S) endpoint without credentials, query or fragment')
        if not isinstance(token, str) or not token or len(token) > 4096 or any(ord(c) < 33 or ord(c) > 126 for c in token):
            raise ValueError('invalid connector key')
        self.url, self.token, self.parsed, self.trusted = url.rstrip('/'), token, p, trusted
        self.port = p.port or (443 if p.scheme == 'https' else 80)
        self.origin = p.scheme + '://' + p.netloc

    def open(self, path, payload=None, timeout=10):
        allow = {x.strip() for x in os.environ.get('SPARK_PUSH_AGENT_ALLOWED_ORIGINS', '').split(',') if x.strip()}
        exception = self.trusted or self.origin in allow
        if self.parsed.scheme != 'https' and not exception:
            raise ValueError('customer connectors require HTTPS; local deployments need an administrator origin allowlist')
        addresses = socket.getaddrinfo(self.parsed.hostname, self.port, type=socket.SOCK_STREAM)
        if not addresses:
            raise ValueError('connector address is unavailable')
        if not exception:
            for entry in addresses:
                addr = ipaddress.ip_address(entry[4][0])
                if not addr.is_global or (getattr(addr, 'ipv4_mapped', None) and not addr.ipv4_mapped.is_global):
                    raise ValueError('private connector address requires administrator allowlist')
        # Connect to the checked IP, preserving TLS hostname verification and
        # Host. No proxy, redirects, or second DNS resolution can bypass checks.
        conn = http.client.HTTPConnection(self.parsed.hostname, self.port, timeout=max(.1, timeout))
        try:
            conn.sock = socket.create_connection((addresses[0][4][0], self.port), timeout=max(.1, timeout))
            if self.parsed.scheme == 'https':
                conn.sock = ssl.create_default_context().wrap_socket(conn.sock, server_hostname=self.parsed.hostname)
            sock = conn.sock
            def expire():
                try:
                    sock.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
            timer = threading.Timer(max(.1, timeout), expire)
            timer.daemon = True
            timer.start()
            body = None if payload is None else json.dumps(payload, ensure_ascii=False).encode()
            conn.request('GET' if body is None else 'POST', self.parsed.path.rstrip('/') + path, body,
                         {'Authorization': 'Bearer ' + self.token, 'Content-Type': 'application/json', 'Connection': 'close'})
            response = conn.getresponse()
            original_close = response.close
            def close():
                timer.cancel()
                original_close()
            response.close = close
            if response.status != 200:
                response.close()
                raise RuntimeError('connector returned HTTP ' + str(response.status))
            return conn, response
        except Exception:
            if 'timer' in locals():
                timer.cancel()
            conn.close()
            raise

    def json(self, path, payload=None, timeout=10):
        conn, response = self.open(path, payload, timeout)
        try:
            raw = response.read(LIMIT+1)
            if len(raw) > LIMIT:
                raise ValueError('connector response exceeds limit')
            value = json.loads(raw)
            if not isinstance(value, dict):
                raise ValueError('invalid connector response')
            return value
        finally:
            response.close()
            conn.close()
