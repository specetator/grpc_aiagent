#!/usr/bin/env python3
"""Check the running Logic admission contract before enabling public Runtimes.

Uses normal login and an existing authorized space. It starts no Runtime and
changes no task, binding or configuration. Run against each restore phase.
"""
import argparse
import getpass
import json
import sys
import urllib.error
import urllib.parse
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


def verify(origin, credentials):
    parsed = urllib.parse.urlsplit(origin)
    if (parsed.scheme not in {'http', 'https'} or not parsed.hostname or parsed.username or
            parsed.password or parsed.path not in {'', '/'} or parsed.query or parsed.fragment):
        raise ValueError('provide the same Web origin used for login')
    origin = origin.rstrip('/')
    opener = urllib.request.build_opener(NoRedirect())

    def call(path, body, token=None):
        headers = {'Content-Type': 'application/json'}
        if token: headers['Authorization'] = 'Bearer ' + token
        request = urllib.request.Request(origin + path, json.dumps(body).encode(), headers)
        with opener.open(request, timeout=20) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
        if len(raw) > 4 * 1024 * 1024: raise ValueError('response exceeds verification limit')
        value = json.loads(raw)
        if (not isinstance(value, dict) or type(value.get('code')) is not int or
                value['code'] != 0 or not isinstance(value.get('data'), dict)):
            raise ValueError('authenticated request failed; keep public Agents stopped')
        return value['data']

    if (not isinstance(credentials, dict) or set(credentials) != {'account', 'password'} or
            any(not isinstance(v, str) or not v for v in credentials.values())):
        raise ValueError('account and password are required')
    token = call('/api/login', credentials).get('token')
    if not isinstance(token, str) or not token: raise ValueError('login did not return a session')
    spaces = call('/api/spaces/list', {}, token).get('spaces')
    if not isinstance(spaces, list) or not spaces:
        raise ValueError('first create or join a space through the Web client')
    if not isinstance(spaces[0], dict): raise ValueError('invalid authorized space')
    room = spaces[0].get('room_id')
    if type(room) is not int or room <= 0: raise ValueError('invalid authorized space')
    task = call('/api/chatroom/agent/task', {'room_id': room}, token)
    if task.get('cann_enabled') is not False:
        raise ValueError('CANN admission is enabled or unsupported by this image; keep public Agents stopped')
    return {'verified': True, 'cann_enabled': False,
            'scope': 'running Logic configuration only; no Runtime or Bridge was started'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--origin', default='http://127.0.0.1:9010')
    parser.add_argument('--credentials-stdin', action='store_true', help='automation: read account/password JSON from stdin')
    args = parser.parse_args()
    try:
        credentials = json.loads(sys.stdin.read(8193)) if args.credentials_stdin else {
            'account': input('Spark Push account: '), 'password': getpass.getpass('Password: ')}
        print(json.dumps(verify(args.origin, credentials)))
    except (OSError, ValueError, KeyError, TypeError, EOFError, urllib.error.URLError):
        # Never echo response bodies, passwords, tokens or URLs with credentials.
        print('Public Agent activation was not verified. Keep Runtimes and Bridge stopped; check the account, space, image support and CANN setting.', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
