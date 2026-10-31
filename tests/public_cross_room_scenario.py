"""Real public-runtime isolation scenario, called by public_room_runtime_e2e.

No runtime doubles or authority bypasses. Nonces originate in two separately
approved project files, never in a model prompt. SQL/SQLite reads are evidence.
"""
from contextlib import closing
import hashlib
import json
import secrets
import sqlite3
import time


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def native_transcript(root, native, store, record):
    request, agent = record['request_id'], record['agent_id']
    route = store.get_turn(request)['metadata']['agent_route']
    if agent == 'public-pi':
        files = list((root / 'gateway/service/rooms' / route['backend_session_ref'] / 'sessions').glob('*.jsonl'))
        assert files
        return [json.loads(line) for path in sorted(files) for line in path.read_text().splitlines()]
    journal = store.get('hermes-native:public-hermes:run', request)
    with closing(sqlite3.connect(f'file:{native.root / "service/state.db"}?mode=ro', uri=True)) as db:
        return db.execute('SELECT role,content,tool_name FROM messages WHERE session_id=? ORDER BY id',
                          (journal['hermes_session_id'],)).fetchall()


def verify_transcripts(root, native, store, records):
    for record in records:
        assert fingerprint(store.get_turn(record['request_id'])) == record['turn_sha256']
        assert fingerprint(native_transcript(root, native, store, record)) == record['transcript_sha256']


async def exercise(*, root, call, sql, store, native, users, clients, room,
                   nonce, second_nonce, original_requests, until, rid, barrier, report, check):
    a, b, c, outsider = users
    denials = []
    second = call('/api/spaces/create', {'name': 'Independent private project', 'visibility': 'private'}, c)['room_id']
    call('/api/spaces/invite', {'room_id': second, 'user_id': a['user_id']}, c)
    call('/api/spaces/accept', {'room_id': second}, a)
    for agent in ('public-pi', 'public-hermes'):
        call('/api/chatroom/agents/install', {'room_id': second, 'agent_id': agent}, c)
    call('/api/chatroom/workspace/set', {'room_id': second, 'project_id': 'second-project', 'generation': 0}, c)

    async def join(client, expected):
        start = len(client.events)
        await client.ws.send(json.dumps({'type': 'chatroom_join', 'group_id': second}))
        event = await until(lambda: next((e for e in client.events[start:]
            if e.get('op') == 'chatroom_join' and e.get('room_id') == second), None), 15)
        assert event['type'] == ('ack' if expected == 0 else 'error'), event
        if expected:
            assert event['code'] == expected
            denials.append({k: event[k] for k in ('type', 'op', 'room_id', 'code')})

    await join(clients[0], 0)
    await join(clients[2], 0)
    await join(clients[1], 403)
    for denied in (b, outsider):
        call('/api/chatroom/join', {'room_id': second, 'user_id': denied['user_id']}, denied, 403)
        for path in ('agents/list', 'tasks/list'):
            call('/api/chatroom/' + path, {'room_id': second}, denied, 403)
        call('/api/session/history', {'session_id': f'r_{second}'}, denied, 403)
    _, denied = await clients[1].send(second, 'Unauthorized Agent request', 'public-pi')
    assert denied.get('code') == 403
    denials.append({k: denied[k] for k in ('type', 'code', 'client_msg_id')})
    check('second private room admits accepted invitees; nonmembers cannot join WS, call Agents or read history/tasks')

    marker = 'second-room-discussion-' + secrets.token_hex(12)
    _, ack = await clients[2].send(second, marker)
    assert ack['type'] == 'accepted_ack'
    await until(lambda: any(e.get('msg_id') == ack['msg_id'] and e.get('type') == 'chatroom'
                           for e in clients[0].events), 15)
    prompt = ('Read PROJECT_FILES.txt, probe.cpp and logic/message_reservation.cpp using the read tool. '
              'Return the exact nonce from probe.cpp, name MySqlMessageReservations::Reserve and give '
              'one short code review finding. Use only this room and project. No shell or test claims.')
    records = []

    async def submit(client, target, agent, text):
        payload, accepted = await client.send(target, text, agent)
        assert accepted['type'] == 'accepted_ack'
        record = {'request_id': rid(accepted['msg_id'], agent), 'room_id': target,
                  'agent_id': agent, 'actor_user_id': client.user['user_id'], 'payload': payload}
        records.append(record)
        return record

    # Require real execution to be in flight. Mere accepted ACKs or optimistic
    # UI messages do not demonstrate that model work and ordinary IM overlap.
    first = await submit(clients[0], room, 'public-pi', prompt)
    await until(lambda: store.get_turn(first['request_id']).get('status') == 'running', 30)
    started = time.monotonic()
    following = await submit(clients[2], room, 'public-pi', 'Read probe.cpp and return its exact nonce in one line.')
    second_accepted_s = time.monotonic() - started
    assert store.get_turn(first['request_id']).get('status') == 'running', 'no observed overlapping execution'
    assert not clients[0].final(following['request_id'])
    _, normal = await clients[1].send(room, 'Ordinary chat while two Agent requests are pending.')
    assert normal['type'] == 'accepted_ack'
    ordinary_accepted_s = time.monotonic() - started
    received_s = {}
    for client in clients:
        await until(lambda: any(e.get('msg_id') == normal['msg_id'] and e.get('type') == 'chatroom'
                               for e in client.events), 10)
        received_s[str(client.user['user_id'])] = time.monotonic() - started
    elapsed = time.monotonic() - started
    assert elapsed < 10 and store.get_turn(first['request_id']).get('status') == 'running', 'chat did not complete during actual Agent work'
    report['cross_room'] = {'ordinary_message_seconds': elapsed, 'chat_delivered_while_agent_running': True,
        'ws_denials': denials,
        'timing_seconds_from_first_running': {'second_request_accepted': second_accepted_s,
            'ordinary_message_accepted': ordinary_accepted_s, 'ordinary_message_received_by_user': received_s,
            'first_still_running_after_chat': elapsed, 'finals_observed': {}},
        'queue_evidence': 'second request accepted before the same room/Agent first execution completed; no Scheduler metric inferred'}
    check('two same-room Pi requests are accepted while the first runs; all three users receive ordinary chat before its final')

    for record in (first, following):
        for client in clients:
            answer = await until(lambda: client.final(record['request_id']), 240)
            assert answer.get('agent_status') == 'completed' and nonce in json.dumps(answer)
        report['cross_room']['timing_seconds_from_first_running']['finals_observed'][record['request_id']] = time.monotonic() - started
    first_route = store.get_turn(first['request_id'])['metadata']['agent_route']
    assert first_route['backend_session_ref'] == store.get_turn(following['request_id'])['metadata']['agent_route']['backend_session_ref']
    # Final message order is the externally visible same-session serialization
    # contract; Pi/Hermes in different sessions need not serialize with it.
    for client in clients:
        assert client.final(first['request_id'])['msg_seq'] < client.final(following['request_id'])['msg_seq']

    for client, agent in ((clients[0], 'public-pi'), (clients[2], 'public-hermes')):
        record = await submit(client, second, agent, prompt)
        for watcher in (clients[0], clients[2]):
            answer = await until(lambda: watcher.final(record['request_id']), 240)
            assert answer.get('agent_status') == 'completed' and second_nonce in json.dumps(answer)
            assert nonce not in json.dumps(answer)
            assert answer['actor_user_id'] == record['actor_user_id'] and answer['agent_id'] == agent
        assert not clients[1].final(record['request_id']), 'nonmember received private Agent final'
    check('both formal public Agents read the second project with correct actor attribution and no first-room nonce')

    await until(lambda: all(store.get('hermes-native:public-hermes:run', request).get('delivery_state') == 'broker_confirmed'
                           for request in (original_requests[1], records[-1]['request_id'])), 30)
    evidence = []
    all_records = [dict(request_id=request, room_id=room, agent_id=agent, actor_user_id=user['user_id'])
                   for request, agent, user in zip(original_requests, ('public-pi', 'public-hermes'), (a, b))] + records
    sessions = {}
    for record in all_records:
        request, target, agent = (record[k] for k in ('request_id', 'room_id', 'agent_id'))
        turn = store.get_turn(request)
        assert turn['status'] == 'completed'
        route = turn['metadata']['agent_route']
        assert route['conversation_id'] == f'r_{target}'
        sessions.setdefault((target, agent), set()).add(route['backend_session_ref'])
        expected, forbidden = (nonce, second_nonce) if target == room else (second_nonce, nonce)
        payload = json.loads(sql("SELECT payload FROM room_agent_request WHERE request_id='" + request + "'"))
        assert payload['collaboration']['room_id'] == target and payload['agent_id'] == agent
        assert payload['user_id'] == payload['collaboration']['actor_user_id'] == record['actor_user_id']
        assert turn['metadata']['actor_user_id'] == record['actor_user_id']
        discussion = 'Kafka accepted_ack confirms broker persistence, not SQL or reading.' if target == room else marker
        assert discussion in json.dumps(payload['messages']), 'own-room discussion missing from actual request context'
        assert forbidden not in json.dumps(payload['messages'])
        if target == room: assert marker not in json.dumps(payload['messages'])
        transcript = native_transcript(root, native, store, record)
        if agent == 'public-pi':
            tools = [row['message'] for row in transcript if row.get('message', {}).get('role') == 'toolResult']
        else:
            tools = [row for row in transcript if row[0] == 'tool']
        assert tools and expected in json.dumps(tools)
        assert discussion in json.dumps(transcript), 'own-room discussion missing from native transcript'
        assert forbidden not in json.dumps(transcript)
        if target == room: assert marker not in json.dumps(transcript)
        evidence.append({'request_id': request, 'room_id': target, 'agent_id': agent,
                         'actor_user_id': record['actor_user_id'], 'own_discussion_in_prompt_and_transcript': True,
                         'backend_session_ref': route['backend_session_ref'], 'tool_results': len(tools),
                         'turn_sha256': fingerprint(turn),
                         'transcript_sha256': fingerprint(transcript), 'prompt_sha256': fingerprint(payload['messages'])})
    assert len(sessions) == 4 and all(len(value) == 1 for value in sessions.values())
    assert len(set.union(*sessions.values())) == 4
    call('/api/chatroom/agents/control', {'room_id': second, 'request_id': original_requests[0], 'operation': 'status'}, a, 404)
    report['cross_room']['sessions'] = evidence
    report['cross_room']['room_id'] = second
    report['cross_room']['request_ids'] = [r['request_id'] for r in records if r['room_id'] == second]
    check('four distinct room/Agent native sessions and actual tool transcripts exclude the other project and private discussion')

    # Resubmit the exact second-room client messages. Do not invoke the model
    # directly or invent another request ID to make an idempotency test pass.
    before = [store.get_turn(r['request_id']) for r in records]
    for record in records[-2:]:
        client = next(client for client in clients if client.user['user_id'] == record['actor_user_id'])
        payload = record['payload']
        _, accepted = await client.send(second, payload['content']['text'], record['agent_id'], payload['client_msg_id'])
        assert accepted['type'] == 'accepted_ack'
        assert rid(accepted['msg_id'], record['agent_id']) == record['request_id']
    report['cross_room']['barriers'] = {'duplicates': await barrier()}
    assert before == [store.get_turn(r['request_id']) for r in records]
    verify_transcripts(root, native, store, evidence)
    verify_history(call, users, report['cross_room'])
    check('second-room duplicate client messages retain original turns and exactly one persisted result per authorized user')


def verify_history(call, users, evidence):
    a, b, c, outsider = users
    room = evidence['room_id']
    for user in (a, c):
        history = call('/api/session/history', {'session_id': f'r_{room}', 'limit': 100}, user)['messages']
        for request in evidence['request_ids']:
            assert sum(request in json.dumps(row) for row in history) == 1
    for user in (b, outsider):
        call('/api/session/history', {'session_id': f'r_{room}'}, user, 403)
