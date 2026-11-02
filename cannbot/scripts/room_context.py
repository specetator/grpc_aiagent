"""Bounded, request-bound tools for existing public Agent runtimes.

Only the operator transport and captured admission scope select identity.
Source receipts attest server reads, not model understanding or final citations.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path
import re
import urllib.error
import urllib.request

from room_workspace import REQUEST, SLUG

TOOLS = json.loads((Path(__file__).resolve().parents[1] / 'room-context-tools.json').read_text())
TOOL_NAMES = frozenset(TOOLS)
MAX_RESPONSE = 2 * 1024 * 1024
HEX = re.compile(r'[a-f0-9]{64}')
ERRORS = {
    400: 'Invalid bounded room tool parameters.',
    401: 'Room tool authentication is unavailable.',
    403: 'Room access is revoked or authorization is unavailable.',
    404: 'The requested source is not available in this room.',
    409: 'Room context or source changed; start a new authorized request.',
    413: 'Source response is too large; read a smaller range.',
    416: 'Requested range is outside the source.',
    429: 'Request source budget exhausted; reuse an already-read range.',
    501: 'Image input is unavailable for the active model/runtime. Use a verified vision-capable configuration; do not infer image contents from metadata.',
    503: 'Room sources are temporarily unavailable; a read can be retried.',
}


class RoomContextError(Exception):
    def __init__(self, code=503):
        self.code = code if type(code) is int and code in ERRORS else 503
        super().__init__(ERRORS[self.code])


def validate_scope(scope):
    if not isinstance(scope, dict):
        raise RoomContextError(403)
    if (scope.get('kind') != 'room_agent_v1' or scope.get('policy') != 'read_only' or
        not isinstance(scope.get('request_id'), str) or not REQUEST.fullmatch(scope['request_id']) or
        not isinstance(scope.get('agent_id'), str) or not SLUG.fullmatch(scope['agent_id']) or
        not isinstance(scope.get('project_id'), str)):
        raise RoomContextError(403)
    for name in ('room_id', 'actor_user_id', 'generation', 'workspace_generation', 'context_generation'):
        value = scope.get(name)
        if type(value) is not int or not (1 if name in ('room_id', 'actor_user_id') else 0) <= value < 2**53:
            raise RoomContextError(403)
    if not isinstance(scope.get('source_msg_id'), str) or not re.fullmatch(
            r'r_' + str(scope['room_id']) + r'-[1-9][0-9]*', scope['source_msg_id']):
        raise RoomContextError(403)


def parameters(name, arguments):
    spec = TOOLS.get(name)
    if spec is None or not isinstance(arguments, dict):
        raise RoomContextError(400)
    schema = spec['parameters']
    if set(arguments) - schema['properties'].keys() or set(schema.get('required', [])) - arguments.keys():
        raise RoomContextError(400)
    for key, value in arguments.items():
        rule = schema['properties'][key]
        if rule['type'] == 'integer':
            if type(value) is not int or not rule['minimum'] <= value <= rule['maximum']:
                raise RoomContextError(400)
        elif not isinstance(value, str) or '\0' in value or not rule.get('minLength', 0) <= len(value) <= rule.get('maxLength', 4096):
            raise RoomContextError(400)
        if 'enum' in rule and value not in rule['enum']:
            raise RoomContextError(400)
        if 'pattern' in rule and not re.fullmatch(rule['pattern'], value):
            raise RoomContextError(400)
    if len(arguments.get('query', '').encode('utf-8')) > 240:
        raise RoomContextError(400)
    return spec


def validate_result(scope, name, arguments, value):
    if (not isinstance(value, dict) or value.get('request_id') != scope['request_id'] or
        type(value.get('context_generation')) is not int or value['context_generation'] != scope['context_generation'] or
        not isinstance(value.get('items'), list) or len(value['items']) > 60):
        raise RoomContextError()
    if name == 'room_read_media' and len(value['items']) != 1:
        raise RoomContextError()
    ceiling = int(scope['source_msg_id'].rsplit('-', 1)[1])
    kinds = {'message', 'media', 'fragment', 'image_metadata', 'image'}
    for item in value['items']:
        if not isinstance(item, dict) or not re.fullmatch(r'src_[a-f0-9]{64}', item.get('source_id', '')):
            raise RoomContextError()
        ref = item.get('reference')
        if (not isinstance(ref, dict) or type(ref.get('room_id')) is not int or ref['room_id'] != scope['room_id'] or
            ref.get('kind') not in kinds or not isinstance(ref.get('sha256'), str) or not HEX.fullmatch(ref['sha256'])):
            raise RoomContextError()
        if name == 'room_read' and ref['kind'] != arguments['kind']:
            raise RoomContextError()
        if name == 'room_search':
            allowed = {'all': {'message', 'media', 'fragment'}, 'messages': {'message'},
                       'media': {'media'}, 'fragments': {'fragment'}}[arguments.get('type', 'all')]
            if ref['kind'] not in allowed:
                raise RoomContextError()
        if ref['kind'] == 'message':
            if (type(ref.get('msg_seq')) is not int or not 0 < ref['msg_seq'] <= ceiling or
                ref.get('msg_id') != f"r_{scope['room_id']}-{ref['msg_seq']}" or
                not isinstance(item.get('text'), str) or type(ref.get('length')) is not int or
                ref['length'] != len(item['text']) or ref['length'] > (arguments.get('limit', 4096) if name == 'room_read' else 300) or
                type(ref.get('offset')) is not int or ref['offset'] < 0 or
                hashlib.sha256(item['text'].encode()).hexdigest() != ref['sha256']):
                raise RoomContextError()
            if name == 'room_read' and (ref['msg_seq'] != arguments.get('msg_seq') or ref['offset'] != arguments.get('offset', 0)):
                raise RoomContextError()
        elif name != 'room_search':
            if ref.get('asset_id') != arguments.get('asset_id') or ref.get('revision') != arguments.get('revision'):
                raise RoomContextError()
            if name == 'room_read' and (type(ref.get('index')) is not int or
                not arguments.get('offset', 0) <= ref['index'] < arguments.get('offset', 0) + arguments.get('limit', 5)):
                raise RoomContextError()
        if name == 'room_read_media':
            if (ref['kind'] != 'image' or type(ref.get('index')) is not int or ref['index'] != arguments['index'] or
                item.get('mime') != 'image/jpeg' or not isinstance(item.get('data'), str) or
                not isinstance(item.get('metadata'), dict) or item['metadata'].get('sha256') != ref['sha256'] or
                item['metadata'].get('anchor') != ref.get('anchor')):
                raise RoomContextError()
            data = base64.b64decode(item['data'], validate=True)
            if (base64.b64encode(data).decode() != item['data'] or not data.startswith(b'\xff\xd8\xff') or not data.endswith(b'\xff\xd9') or
                len(data) > MAX_RESPONSE or hashlib.sha256(data).hexdigest() != ref['sha256']):
                raise RoomContextError()
        elif 'data' in item:
            raise RoomContextError()
    return value


class RoomContextClient:
    def __init__(self, authority):
        self.authority = authority

    def call(self, scope, name, arguments):
        try:
            validate_scope(scope)
            spec = parameters(name, arguments)
            scope, arguments = copy.deepcopy(scope), copy.deepcopy(arguments)
            self.authority.authorize(scope['request_id'], scope)
            body = json.dumps({**arguments, 'request_id': scope['request_id']}, ensure_ascii=False).encode()
            if len(body) > 4096:
                raise RoomContextError(400)
            url = self.authority.url.removesuffix('/internal/room-agent/authorize') + '/internal/room-agent/context/' + spec['operation']
            request = urllib.request.Request(url, body,
                {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.authority.key})
            with self.authority.opener.open(request, timeout=10) as response:
                raw = response.read(MAX_RESPONSE + 1)
            if len(raw) > MAX_RESPONSE:
                raise RoomContextError(413)
            envelope = json.loads(raw)
            if type(envelope.get('code')) is not int or envelope['code'] != 0:
                raise RoomContextError(envelope.get('code'))
            value = validate_result(scope, name, arguments, envelope['data'])
            self.authority.authorize(scope['request_id'], scope)
            return value
        except RoomContextError:
            raise
        except PermissionError:
            raise RoomContextError(403) from None
        except urllib.error.HTTPError as error:
            raise RoomContextError(error.code) from None
        except Exception:
            # Never expose authority URLs/keys, private paths, or error bodies.
            raise RoomContextError() from None


def hermes_result(value):
    """Native envelope understood by Hermes' existing tool/vision pipeline."""
    text = json.dumps({**value, 'items': [{k: v for k, v in item.items() if k != 'data'}
                                        for item in value['items']]}, ensure_ascii=False)
    images = [{'type': 'image_url', 'image_url': {'url': 'data:' + item['mime'] + ';base64,' + item['data']}}
              for item in value['items'] if 'data' in item]
    if not images:
        return text
    return {'_multimodal': True, 'content': [{'type': 'text', 'text': text}, *images],
            'text_summary': json.dumps({'success': False, 'code': 501, 'error': ERRORS[501]}),
            'meta': {'source_ids': [item['source_id'] for item in value['items']]}}
