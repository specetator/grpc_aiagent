"""Real user uploads -> production media worker -> native room tools -> IM.

Invoked by public_room_runtime_e2e; no SQL writes or simulated runtimes. Inputs
are synthetic fixtures. Image hashes are observed in actual Native tool and
provider requests; HTTP/WS clients are not browser acceptance.
"""
import asyncio
import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys
import tempfile
import time
import wave

from connector_deployment_e2e import ROOT, http

sys.path.insert(0, str(ROOT / 'cannbot/scripts'))
from media_worker import Configuration


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str).encode()).hexdigest()


def answer_json(text):
    text = re.sub(r'^\s*【(?:Kernel Agent|Hermes|Pi)】\s*', '', text).strip()
    if text.startswith('```'):
        text = text.split('\n', 1)[1].rsplit('```', 1)[0]
    return json.loads(text)


def words(text):
    # The preserved real ASR diagnostic transcribes the synthetic US-spelled
    # "harbor" as British "Harbour". Accept this explicit orthographic variant,
    # not fuzzy name matching; retain the original transcript and answer.
    text = re.sub(r'\bharbour\b', 'harbor', text.casefold())
    return re.sub(r'[^a-z0-9]', '', text)


def validate_decisions(proposal, expected, receipts, fragments, asset):
    allowed = {r['source_id']: r['reference'] for r in receipts
               if r['reference'].get('asset_id') == asset['id'] and
               r['reference'].get('revision') == asset['revision'] and r['reference']['kind'] == 'fragment'}
    decisions = {words(d['code']): d for d in proposal['decisions']}
    assert len(decisions) == len(proposal['decisions']) == len(expected)
    for plan in expected:
        code = words(plan['code'])
        observed = decisions[code]
        assert all(isinstance(v.get('owner'), str) and v['owner'].strip() for v in observed['actions']), (plan['code'], 'missing named owner')
        assert {v['owner'].casefold() for v in observed['actions']} == {v['owner'].casefold() for v in plan['actions']}, (plan['code'], 'owner set differs from checkpoint')
        for action in plan['actions']:
            matching = [v['action'] for v in observed['actions'] if v['owner'].casefold() == action['owner'].casefold()]
            assert any(all(words(t) in words(v) for t in action['terms']) for v in matching), (plan['code'], action['owner'], 'action differs from checkpoint')
        indices = {f['index'] for f in fragments if code in words(f['content']['text'])}
        assert indices, ('ASR checkpoint absent', plan['code'])
        cites = observed['source_ids']
        assert cites and all(v in allowed for v in cites)
        assert any(allowed[v]['index'] in indices for v in cites), 'decision does not cite its audio checkpoint'
    assert proposal['tasks']
    for task in proposal['tasks']:
        validate_audio_task(task, expected, receipts, fragments, asset)
    return proposal['tasks'][0]


def validate_audio_task(task, expected, receipts, fragments, asset):
    """Independent downstream task gate; never replaces the whole-answer gate."""
    assert task.get('needs_confirmation') is not True, 'task still requires confirmation'
    code = words(task['decision_code'])
    plan = next((v for v in expected if words(v['code']) == code), None)
    assert plan is not None and isinstance(task.get('owner'), str), 'unknown task code or owner'
    action = next((v for v in plan['actions'] if v['owner'].casefold() == task['owner'].casefold()), None)
    assert action is not None, 'unexpected task owner'
    assert isinstance(task['steps'], list) and task['steps'] and all(isinstance(v, str) for v in task['steps'])
    description = words(' '.join([task['title'], task['description'], *task['steps']]))
    assert all(words(term) in description for term in action['terms']), 'unrelated proposed task'
    indices = {f['index'] + distance for f in fragments if code in words(f['content']['text']) for distance in range(4)}
    allowed = {r['source_id']: r['reference'] for r in receipts
               if r['reference'].get('asset_id') == asset['id'] and
               r['reference'].get('revision') == asset['revision'] and r['reference']['kind'] == 'fragment'}
    assert task['source_ids'] and all(v in allowed for v in task['source_ids'])
    assert any(allowed[v]['index'] in indices for v in task['source_ids']), 'task cites unrelated audio'
    return task


def validate_video(proposal, expected, receipts, asset):
    images = {r['source_id']: r['reference'] for r in receipts
              if r['reference'].get('asset_id') == asset['id'] and
              r['reference'].get('revision') == asset['revision'] and r['reference']['kind'] == 'image'}
    frames = sorted(proposal['frames'], key=lambda f: f['timestamp_ms'])
    assert len(frames) == 2
    transition = expected['transition_seconds'] * 1000
    assert frames[0]['timestamp_ms'] < transition <= frames[1]['timestamp_ms']
    for frame, marker in zip(frames, expected['markers']):
        assert frame['source_id'] in images
        assert frame['timestamp_ms'] == images[frame['source_id']]['anchor']['timestamp_ms']
        assert all(frame[k].casefold() == v for k, v in marker.items()), 'visual fact differs from fixture'
    assert proposal['transition_window_ms'] == [f['timestamp_ms'] for f in frames]
    assert frames[1]['timestamp_ms'] - frames[0]['timestamp_ms'] <= 5000
    summary = words(proposal['audio_summary'])
    assert 'connectionfailure' in summary and 'recovery' in summary


class MediaScenario:
    def __init__(self, image, model, fixtures, output, case="all", *, processing_timeout=900):
        if case not in ("all", "pdf", "audio", "video"):
            raise ValueError("unsupported media case")
        if type(processing_timeout) is not int or not 1 <= processing_timeout <= 3600:
            raise ValueError("processing timeout must be between 1 and 3600 seconds")
        self.processing_timeout = processing_timeout
        self.cases = ("pdf", "audio", "video") if case == "all" else (case,)
        self.image = subprocess.check_output(
            ['docker', 'image', 'inspect', image, '--format', '{{.Id}}'], text=True).strip()
        self.config = Configuration(self.image, model)
        self.fixtures = Path(fixtures).resolve(strict=True)
        self.expected = json.loads((self.fixtures / 'collaboration-expectations.json').read_text())
        self.temporary = tempfile.TemporaryDirectory(prefix='spark-public-media-')
        self.home = Path(self.temporary.name)
        self.key = secrets.token_hex(32)
        self.keyfile = self.home / 'worker.key'
        self.keyfile.write_text(self.key)
        self.keyfile.chmod(0o600)
        self.output = output
        self.log = None
        self.worker = None
        self.records = []
        self.assets = []
        self.evidence = {'scope': __doc__, 'configuration': self.config.description(),
                         'assets': self.assets, 'requests': self.records, 'checks': [],
                         'processing_timeout_seconds': processing_timeout,
                         'selected_cases': list(self.cases), 'cases_passed': [], 'case_failures': {},
                         'not_selected': [kind for kind in ('pdf', 'audio', 'video') if kind not in self.cases]}

    def environment(self):
        return {'SPARK_PUSH_MEDIA_WORKER_KEY': self.key,
                'SPARK_PUSH_MEDIA_PARSER_PIPELINE_SHA256': self.config.pipeline,
                'SPARK_PUSH_MEDIA_PARSER_SHA256': self.config.parser_sha}

    def start(self, origin):
        self.origin = origin
        self.log = self.output.with_suffix('.media-worker.log').open('w')
        os.chmod(self.log.name, 0o600)
        self.worker = subprocess.Popen([
            sys.executable, str(ROOT / 'cannbot/scripts/media_worker.py'),
            '--image-id', self.image, '--model-dir', str(self.config.model),
            '--origin', origin, '--key-file', str(self.keyfile), '--state-dir', str(self.home / 'state')],
            stdout=self.log, stderr=self.log)

    def close(self):
        if self.worker and self.worker.poll() is None:
            self.worker.terminate()
            try:
                self.worker.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.worker.kill()
                self.worker.wait(timeout=10)
                raise RuntimeError('media worker did not stop cooperatively; inspect its owned state')
        if self.log:
            self.log.close()
        self.temporary.cleanup()

    def check_routes(self, base, authority, workspace_key):
        observations = []
        # Routing and Service authentication are separate gates. A worker key
        # must never authorize a context read, nor the reverse.
        probes = [
            (base, '/internal/room-agent/context/search', {}, workspace_key, 'POST', 404, None),
            (base, '/internal/media/claim_media', {}, self.key, 'POST', 404, None),
            (authority, '/internal/room-agent/context/search', None, workspace_key, 'GET', 404, None),
            (authority, '/internal/media/unknown', {}, self.key, 'POST', 404, None),
            (authority, '/internal/room-agent/context/search', {'request_id': 'ra_' + '0' * 64}, self.key, 'POST', 401, 401),
            (authority, '/internal/media/claim_media', {}, workspace_key, 'POST', 401, 401),
        ]
        for origin, path, body, key, method, status, code in probes:
            actual, raw = http(origin, path, body, key, method=method)
            assert actual == status, (path, method, actual, status)
            if code is not None:
                assert json.loads(raw)['code'] == code, (path, raw[:300])
            observations.append({'listener': 'public' if origin == base else 'internal',
                                 'path': path, 'method': method, 'http_status': actual, 'code': code})
        self.evidence['routing'] = observations

    async def exercise(self, *, root, call, sql, store, native, users, clients, room,
                       nonce, until, rid, barrier, check):
        a, b, c, outsider = users
        self.room = room
        self.call = call
        def media(op, body, user=a, expected=0):
            return call('/api/room/media/' + op, body, user, expected)
        self.media = media
        if 'audio' in self.cases:
            with wave.open(str(self.fixtures / 'meeting-20min.wav')) as audio:
                seconds = audio.getnframes() / audio.getframerate()
            assert 1199 <= seconds <= 1201
            self.evidence['audio'] = {'seconds': seconds, 'kind': self.expected['audio_kind'],
                                     'expected_codes': self.expected['audio_codes']}
        before = len(call('/api/chatroom/agents/list', {'room_id': room}, a)['requests'])
        for kind, user, name, mime in [('pdf', a, 'design.pdf', 'application/pdf'),
                                       ('audio', b, 'meeting-20min.wav', 'audio/wav'),
                                       ('video', c, 'incident.mp4', 'video/mp4')]:
            if kind not in self.cases:
                continue
            raw = (self.fixtures / name).read_bytes()
            value = media('init', {'room_id': room, 'client_upload_id': secrets.token_hex(16),
                'name': name, 'mime': mime, 'bytes': len(raw), 'sha256': hashlib.sha256(raw).hexdigest()}, user)
            ident = value['id']
            for index, offset in enumerate(range(0, len(raw), 1024 * 1024)):
                await asyncio.to_thread(media, 'chunk', {'id': ident, 'index': index,
                    'data': base64.b64encode(raw[offset:offset + 1024 * 1024]).decode()}, user)
            media('finish', {'id': ident}, user)
            record = {'case': kind, 'id': ident, 'name': name, 'uploader': user['user_id'],
                      'sha256': hashlib.sha256(raw).hexdigest(), 'bytes': len(raw)}
            self.assets.append(record)
            media('processing', {'id': ident}, outsider, 403)
            # Uploaded material is mentioned in ordinary IM without a model call.
            client = next(v for v in clients if v.user['user_id'] == user['user_id'])
            _, ack = await client.send(room, 'Shared project material: ' + name)
            assert ack['type'] == 'accepted_ack'
        await barrier()
        assert len(call('/api/chatroom/agents/list', {'room_id': room}, a)['requests']) == before
        started = time.monotonic()
        for asset in self.assets:
            def ready():
                assert self.worker.poll() is None, 'production media worker exited'
                value = media('processing', {'id': asset['id']})
                assert value.get('state') not in ('failed', 'cancelled'), value
                return value if value.get('state') == 'ready' else None
            state = await until(ready, self.processing_timeout)
            asset['revision'] = state['revision']
            asset['processing'] = state
            first = media('fragments', {'id': asset['id'], 'revision': asset['revision'], 'offset': 0, 'limit': 1})
            assert first['items'], 'published fixture must contain text fragments'
            asset['first_fragment_sha256'] = digest(first['items'])
            pictures = media('artifacts', {'id': asset['id'], 'revision': asset['revision'], 'limit': 1})
            asset['expects_image'] = asset['name'] in ('design.pdf', 'incident.mp4')
            if asset['expects_image']:
                assert pictures['items'], 'published PDF/video must contain actual images'
                picture = media('artifact', {'id': asset['id'], 'revision': asset['revision'], 'index': 0})
                asset['first_image_sha256'] = hashlib.sha256(base64.b64decode(picture['data'], validate=True)).hexdigest()
        self.evidence['processing_seconds'] = time.monotonic() - started
        assert len(call('/api/chatroom/agents/list', {'room_id': room}, a)['requests']) == before
        check('selected media uploaded through real chunk APIs and published by production worker: ' + ', '.join(self.cases))
        assets = {asset['case']: asset for asset in self.assets}

        if 'audio' in self.cases:
            audio_asset = assets['audio']
            fragments = []
            offset = 0
            while True:
                page = media('fragments', {'id': audio_asset['id'], 'revision': audio_asset['revision'],
                                          'offset': offset, 'limit': 20})
                fragments.extend(page['items'])
                if not page.get('has_more'):
                    break
                assert page['next_offset'] > offset
                offset = page['next_offset']
            transcript = json.dumps(fragments, ensure_ascii=False).lower()
            transcript_file = self.output.with_suffix('.audio-transcript.json')
            transcript_file.write_text(json.dumps(fragments, ensure_ascii=False, indent=2) + '\n')
            transcript_file.chmod(0o600)
            self.evidence['audio']['transcript_artifact'] = transcript_file.name
            self.evidence['audio']['transcript_sha256'] = hashlib.sha256(transcript_file.read_bytes()).hexdigest()
            asr = {code: words(code) in words(transcript) for code in self.expected['audio_codes']}
            self.evidence['audio']['orthographic_equivalence'] = {'harbour': 'harbor'}
            self.evidence['audio']['asr_code_checks'] = asr
            self.evidence['audio']['fragment_count'] = len(fragments)
            assert all(asr.values()), ('real ASR missed a decision checkpoint', asr)

        async def request(client, agent, prompt, asset, kinds):
            payload, ack = await client.send(room, prompt, agent)
            assert ack['type'] == 'accepted_ack'
            record = {'request_id': rid(ack['msg_id'], agent), 'agent_id': agent,
                      'actor_user_id': client.user['user_id'], 'room_id': room, 'payload': payload}
            self.records.append(record)
            for watcher in clients:
                answer = await until(lambda: watcher.final(record['request_id']), 360)
                record.setdefault('final_statuses', []).append({'user_id': watcher.user['user_id'], 'status': answer.get('agent_status')})
                record.setdefault('answer', answer.get('content', {}).get('text', ''))
                assert answer.get('agent_status') == 'completed', (agent, answer)
                assert answer['agent_id'] == agent and answer['actor_user_id'] == client.user['user_id']
            answer = clients[0].final(record['request_id'])
            assert all(w.final(record['request_id'])['content'] == answer['content'] for w in clients)
            record['answer'] = answer['content']['text']
            record['msg_seq'] = answer['msg_seq']
            rows = sql("SELECT reference FROM room_context_source WHERE request_id='" + record['request_id'] + "'")
            refs = [json.loads(line) for line in rows.splitlines() if line]
            assert all(r['room_id'] == room for r in refs)
            actual = {r['kind'] for r in refs if r.get('asset_id') == asset['id']}
            assert set(kinds) <= actual, (agent, actual, kinds)
            record['source_receipts'] = refs
            return record

        if 'pdf' in self.cases:
            pdf = await request(clients[0], 'public-hermes',
                'Use room_search and room_read to read design.pdf, including its design nonce and page two. '
                'Also read the approved probe.cpp and logic/message_reservation.cpp. Return the exact nonce from '
                'the PDF and the exact code nonce, explain MySqlMessageReservations::Reserve in relation to the '
                'PDF rule, and cite source_id/page. No shell or invented test results.', assets['pdf'], ['fragment'])
            assert self.expected['pdf_nonce'] in pdf['answer'] and nonce in pdf['answer'] and 'Reserve' in pdf['answer']
            self.evidence['cases_passed'].append('pdf')
            check('Hermes reads uploaded PDF plus approved code; three users receive its attributed final')
        if 'audio' in self.cases:
            audio = await request(clients[1], 'public-pi',
                'Read meeting-20min.wav transcripts using room_search and room_read. Find all decision checkpoints '
                'at the beginning, middle and end; refine fragment searches if needed. Return ONLY a JSON object '
                'with decisions:[{code,actions:[{owner,action}],source_ids:[string]}] and '
                'tasks:[{decision_code,owner,title,description,steps:[string],verbatim_quote,interpretation,needs_confirmation,reason,source_ids:[string]}]. '
                'Include every named owner/action at each checkpoint, copy the code names, and cite the specific '
                'audio fragments. In actions, include explicit assignments to named people at that checkpoint; '
                'do not merge nearby general discussion, generic roles, or unassigned policies into assignments. '
                'Do not invent an owner or silently correct an uncertain term. '
                'Keep a verbatim quote separate from interpretation. For an unclear assignment or technical term, '
                'set needs_confirmation=true and explain the unresolved point in reason; do not invent a correction. '
                'Propose related follow-up tasks with concise titles; do not execute tools '
                'other than room reads, or claim tests have run.', assets['audio'], ['fragment'])
            assert all(words(code) in words(audio['answer']) for code in self.expected['audio_codes'])
            proposal = answer_json(audio['answer'])
            receipts = self._receipts(sql, audio['request_id'])
            try:
                validate_decisions(proposal, self.expected['audio_decisions'], receipts, fragments, audio_asset)
            except (AssertionError, KeyError, TypeError, ValueError) as error:
                self.evidence['case_failures']['audio'] = type(error).__name__ + ': ' + str(error)
                check('audio semantic gate FAILED; independent task and recovery observations may continue, overall acceptance remains failed')
            else:
                self.evidence['cases_passed'].append('audio')
                check('Pi extracts all three audio checkpoints with correct named assignments and citations')
            chosen = None
            evaluations = self.evidence['audio']['task_evaluations'] = []
            for index, candidate in enumerate(proposal.get('tasks', [])):
                try:
                    validate_audio_task(candidate, self.expected['audio_decisions'], receipts, fragments, audio_asset)
                except (AssertionError, KeyError, TypeError, ValueError) as error:
                    evaluations.append({'index': index, 'passed': False, 'error': type(error).__name__ + ': ' + str(error)})
                else:
                    evaluations.append({'index': index, 'passed': True})
                    if chosen is None:
                        chosen = candidate
                        self.evidence['audio']['selected_task_index'] = index
            if chosen is not None:
                # The Web save action is available only for a completed request.
                await until(lambda: next((row for row in call('/api/chatroom/agents/list', {'room_id': room}, a)['requests']
                                          if row['request_id'] == audio['request_id'] and row['status'] == 'completed'), None), 30)
                # Use the actual browser formatter, not a second implementation
                # of its review-note handling. Identity/API authorization is real.
                draft = json.loads(subprocess.check_output(['node', '-e',
                    "require(process.argv[1]);process.stdout.write(JSON.stringify(RoomAgents.taskDraft(JSON.parse(require('fs').readFileSync(0,'utf8')))))",
                    str(ROOT / 'web_demo/static/room_agents.js')], input=json.dumps(chosen), text=True))
                task = call('/api/chatroom/tasks/create', {'room_id': room, 'request_id': audio['request_id'],
                    **draft, 'source_ids': chosen['source_ids']}, a)
                assert task['source_ids'] == chosen['source_ids']
                for field in ('title', 'description', 'steps'):
                    assert task[field] == draft[field], ('task draft changed', field)
                task = call('/api/chatroom/tasks/update', {'room_id': room, 'task_id': task['task_id'],
                    'revision': task['revision'], 'claim': True, 'status': 'running',
                    'summary': 'B accepted the independently verified audio-derived follow-up; execution evidence is still pending.'}, b)
                self.evidence['task'] = task
                self.evidence['task_creation'] = 'first independently validated model proposal; actual Web formatter, authenticated task API, server-validated sources, member claim; no automatic task write; not a browser interaction'
                check('members persist an independently validated audio task, review notes and sources through real APIs')
            else:
                self.evidence['audio']['task_persistence'] = 'not executed: no independently validated task proposal'
                self.evidence['case_failures'].setdefault('audio', 'no independently validated task proposal')
        if 'video' in self.cases:
            video = await request(clients[2], 'public-hermes',
                'Analyze incident.mp4 using its transcript AND actual images from room_read_media. Read image_metadata '
                'and two frames from opposite ends. Return ONLY JSON: frames:[{timestamp_ms,panel_color,marker_color,'
                'marker_shape,source_id}] in time order, transition_window_ms:[before_frame_ms,after_frame_ms], '
                'audio_summary:string. Describe the geometric marker visible on each panel; its shape/color is '
                'not stated by the audio. Bound the transition by observed frames instead of guessing exact timing. '
                'Metadata alone is insufficient; do not claim vision if image content is unavailable.',
                assets['video'], ['fragment', 'image'])
            validate_video(answer_json(video['answer']), self.expected['video'],
                           self._receipts(sql, video['request_id']), assets['video'])
            images = [r for r in video['source_receipts'] if r['kind'] == 'image' and r.get('asset_id') == assets['video']['id']]
            assert len({r['index'] for r in images}) >= 2
            # Native intentionally persists a text-only projection (images become
            # [screenshot]). Verify real tool/provider bytes with the existing
            # observational probe instead of expecting JPEGs in SQLite history.
            audit = [json.loads(line) for line in native.audit.read_text().splitlines()]
            audit = [row for row in audit if row.get('request_id') == video['request_id']]
            video['native_image_audit'] = audit  # retain evidence even on failure
            expected_hashes = {r['sha256'] for r in images}
            tool_hashes = {h for row in audit if row['phase'] == 'tool' for h in row['hashes']}
            provider_hashes = {h for row in audit if row['phase'] == 'provider' and row.get('status') == 200
                               for h in row['hashes']}
            assert expected_hashes <= tool_hashes, 'Native tool result lacks the actual read JPEGs'
            assert expected_hashes <= provider_hashes, 'Native provider request lacks the actual read JPEGs'
            video['native_image_sha256'] = sorted(expected_hashes)
            self.evidence['cases_passed'].append('video')
            check('Hermes video answer matches nonspoken image facts and audio; actual JPEGs observed in Native tool and provider requests')
        await barrier()
        self.evidence['checks'].append('per-case quality and independent persistence results recorded; final quality assertion remains required')
        return self.records

    def assert_complete(self):
        assert not self.evidence['case_failures'], ('media quality failed', self.evidence['case_failures'])
        assert set(self.evidence['cases_passed']) == set(self.cases), 'selected media case not passed'

    @staticmethod
    def _receipts(sql, request):
        rows = sql("SELECT JSON_OBJECT('source_id',source_id,'reference',reference) FROM room_context_source WHERE request_id='" + request + "'")
        return [json.loads(line) for line in rows.splitlines() if line]

    def verify_restored(self, users, store):
        a, b, c, _ = users
        for user in (a, c):
            rows = self.call('/api/session/history', {'session_id': 'r_' + str(self.room), 'limit': 100}, user)['messages']
            for record in self.records:
                assert sum(record['request_id'] in json.dumps(row) for row in rows) == 1
                assert store.get_turn(record['request_id'])['status'] == 'completed'
            if 'task' in self.evidence:
                tasks = self.call('/api/chatroom/tasks/list', {'room_id': self.room}, user)['tasks']
                saved = next(t for t in tasks if t['task_id'] == self.evidence['task']['task_id'])
                assert saved['summary'] == self.evidence['task']['summary']
                assert saved['source_ids'] == self.evidence['task']['source_ids']
                for field in ('title', 'description', 'steps'):
                    assert saved[field] == self.evidence['task'][field], ('restored task draft changed', field)
            for asset in self.assets:
                state = self.media('processing', {'id': asset['id']}, user)
                assert state['state'] == 'ready' and state['revision'] == asset['revision']
                fragments = self.media('fragments', {'id': asset['id'], 'revision': asset['revision'],
                                       'offset': 0, 'limit': 1}, user)
                assert digest(fragments['items']) == asset['first_fragment_sha256']
                if asset['expects_image']:
                    picture = self.media('artifact', {'id': asset['id'], 'revision': asset['revision'], 'index': 0}, user)
                    assert hashlib.sha256(base64.b64decode(picture['data'], validate=True)).hexdigest() == asset['first_image_sha256']
        for asset in self.assets:
            for op, fields in [('processing', {}), ('read', {'offset': 0, 'length': 16}),
                               ('fragments', {'revision': asset['revision'], 'offset': 0, 'limit': 1})]:
                self.media(op, {'id': asset['id'], **fields}, b, 403)
            if asset['expects_image']:
                self.media('artifact', {'id': asset['id'], 'revision': asset['revision'], 'index': 0}, b, 403)
        self.evidence['checks'].append('selected media and attributed finals survive full restart; removed member denied source and derived reads')
        if 'task' in self.evidence:
            self.evidence['checks'].append('member-claimed audio task and source_ids survive full restart')
