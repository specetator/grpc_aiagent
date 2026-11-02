"""Opt-in real Pi tool-image probe. This is NOT room authorization or IM E2E.

Uses one explicitly selected operator model in a disposable Pi home, with one
fixture-only read tool and no shell, personal sessions, skills or project files.
No credentials or raw provider errors are written to the report.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import re
import struct
import subprocess
import sys
import tempfile
import zlib

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'cannbot/scripts'))
from public_runtime import clean_environment, model_environment


def png_fixture(rng):
    shapes = ['circle', 'square', 'triangle']
    colors = ['red', 'green', 'blue']
    rng.shuffle(shapes)
    rng.shuffle(colors)
    rgb = {'red': (225, 25, 25), 'green': (25, 180, 35), 'blue': (30, 55, 225)}
    centers = [(58, 64), (198, 64), (128, 198)]
    pixels = bytearray()
    for y in range(256):
        pixels.append(0)
        for x in range(256):
            color = (255, 255, 255)
            for (cx, cy), shape, name in zip(centers, shapes, colors):
                dx, dy = x-cx, y-cy
                inside = ((shape == 'circle' and dx*dx+dy*dy <= 30*30) or
                          (shape == 'square' and abs(dx) <= 28 and abs(dy) <= 28) or
                          (shape == 'triangle' and -30 <= dy <= 30 and abs(dx) <= (dy+30)/2))
                if inside:
                    color = rgb[name]
            pixels.extend(color)
    def chunk(kind, data):
        return struct.pack('!I', len(data)) + kind + data + struct.pack('!I', zlib.crc32(kind+data))
    png = b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('!2I5B', 256, 256, 8, 2, 0, 0, 0))
    png += chunk(b'IDAT', zlib.compress(bytes(pixels))) + chunk(b'IEND', b'')
    expected = {position: {'shape': shape, 'color': color}
                for position, shape, color in zip(('left', 'right', 'lower'), shapes, colors)}
    return png, expected


def run(args):
    original = args.models_file.read_bytes()
    source = json.loads(original)
    provider = source['providers'][args.provider]
    selected = [m for m in provider['models'] if m['id'] == args.model]
    if len(selected) != 1 or str(provider.get('apiKey', '')).startswith('!'):
        raise ValueError('one explicit model and a non-command credential are required')
    # A config capability flag is only admission to the probe; correctness must
    # be established by all randomized image answers and actual tool events.
    selected = [{**selected[0], 'input': ['text', 'image']}]
    report = {'kind': 'real-native-capability-probe', 'runtime': 'pi',
              'provider': args.provider, 'model': args.model, 'passed': False,
              'probe_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
              'scope': 'three synthetic no-text images through native tool results; no room/IM authorization'}
    artifacts = args.out.parent / (args.out.stem+'-images')
    artifacts.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='spark-pi-vision-') as directory:
        root = Path(directory)
        home, agent, work = root/'home', root/'agent', root/'work'
        for path in (home, agent, work):
            path.mkdir(mode=0o700)
        model_file = agent/'models.json'
        model_file.write_text(json.dumps({'providers': {args.provider: {**provider, 'models': selected}}}))
        model_file.chmod(0o600)
        expected, hashes = [], []
        rng = random.SystemRandom()
        for i in range(3):
            data, answer = png_fixture(rng)
            (work/f'fixture-{i}.png').write_bytes(data)
            expected.append(answer)
            hashes.append(hashlib.sha256(data).hexdigest())
            artifact = artifacts/(hashes[-1]+'.png')
            if artifact.exists():
                if artifact.read_bytes() != data:
                    raise ValueError('existing fixture artifact differs from its hash')
            else:
                artifact.write_bytes(data)
        extension = work/'probe.ts'
        extension.write_text('''import fs from "node:fs";
import path from "node:path";
export default function(pi: any) {
  pi.registerTool({name: "room_read_media", label: "Read probe images",
    description: "Read exactly three isolated synthetic test images, numbered 0, 1, 2.",
    parameters: {type: "object", properties: {}, additionalProperties: false},
    async execute() {
      return {content: [0,1,2].flatMap(i => [
        {type: "text", text: "Image index " + i},
        {type: "image", mimeType: "image/png", data: fs.readFileSync(path.join(process.cwd(),`fixture-${i}.png`)).toString("base64")}
      ]), details: {fixture_count: 3}};
    }
  });
}
''')
        env = clean_environment(home)
        env.update(PI_CODING_AGENT_DIR=str(agent), PI_OFFLINE='1',
                   PATH=str(args.pi.parent)+os.pathsep+env['PATH'])
        credentials = model_environment(args.model_env_file)
        # Provision only named model variables referenced by this provider.
        for brace, plain in re.findall(r'\$(?:\{([A-Z][A-Z0-9_]*)\}|([A-Z][A-Z0-9_]*))', json.dumps(provider)):
            name = brace or plain
            value = credentials.get(name) or os.environ.get(name)
            if not value:
                raise ValueError('selected credential environment variable is missing')
            env[name] = value
        command = [str(args.pi), '--mode', 'json', '--provider', args.provider, '--model', args.model,
                   '--no-builtin-tools', '--tools', 'room_read_media', '--no-extensions', '--extension', str(extension),
                   '--no-skills', '--no-prompt-templates', '--no-themes', '--no-context-files', '--no-session',
                   '--system-prompt', 'You inspect only the provided synthetic images using the one available read tool.',
                   '-p', 'Call room_read_media. For each image, identify the leftmost, rightmost and lowest shape and its color. '
                   'Return only a JSON array, in image index order, with each object having keys left, right, lower; '
                   'each value must be {"shape":"circle|square|triangle","color":"red|green|blue"}. Use the actual images.']
        completed = subprocess.run(command, cwd=work, env=env, capture_output=True, text=True, timeout=180)
        diagnostic = completed.stderr[-3000:]
        for protected in (provider.get('apiKey', ''), provider.get('baseUrl', ''), str(root), *credentials.values()):
            if protected:
                diagnostic = diagnostic.replace(protected, '<redacted>')
        diagnostic = re.sub(r'(?:sk-|Bearer )[^\s\"\']+', '<redacted>', diagnostic)
        report['diagnostic'] = diagnostic
        events = []
        for line in completed.stdout.splitlines():
            try:
                value = json.loads(line)
                if isinstance(value, dict):
                    events.append(value)
            except json.JSONDecodeError:
                pass
        tool_results = [e for e in events if e.get('type') == 'tool_execution_end' and e.get('toolName') == 'room_read_media']
        image_count = sum(part.get('type') == 'image' for e in tool_results for part in e.get('result', {}).get('content', []))
        text = ''
        terminal = {}
        for event in events:
            message = event.get('message', {})
            if event.get('type') == 'message_end' and message.get('role') == 'assistant':
                text = ''.join(p.get('text', '') for p in message.get('content', []) if p.get('type') == 'text')
                terminal = {k: message.get(k) for k in ('stopReason', 'errorMessage', 'model', 'provider')}
        try:
            answer = json.loads(re.sub(r'^```(?:json)?\s*|\s*```$', '', text.strip()))
        except json.JSONDecodeError:
            answer = None
        diagnostic = json.dumps(terminal, ensure_ascii=False)
        for protected in (provider.get('apiKey', ''), provider.get('baseUrl', ''), *credentials.values()):
            if protected:
                diagnostic = diagnostic.replace(protected, '<redacted>')
        report['terminal'] = json.loads(diagnostic)
        report['event_types'] = sorted(set(str(e.get('type')) for e in events))
        report.update(exit_code=completed.returncode, tool_results=len(tool_results), image_blocks=image_count,
                      image_sha256=hashes, expected=expected, answer=answer,
                      image_files=[str(Path(artifacts.name)/(value+'.png')) for value in hashes],
                      passed=completed.returncode == 0 and terminal.get('stopReason') == 'stop' and
                      not terminal.get('errorMessage') and image_count == 3 and answer == expected and
                      all(not e.get('isError') for e in tool_results))
    report['source_model_config_unchanged'] = args.models_file.read_bytes() == original
    report['passed'] = report['passed'] and report['source_model_config_unchanged']
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report['passed'] else 1


def run_with_hermes_model(args):
    # Explicit opt-in to the same previously authorized MODEL-only fixture;
    # never start, refresh, copy tools from, or write the personal profile.
    from hermes_room_runtime_fixture import HermesRoomRuntime
    fixture = HermesRoomRuntime(profile=args.hermes_model_profile)
    model, auth = fixture.model_credentials()
    with tempfile.TemporaryDirectory(prefix='spark-media-model-access-') as directory:
        path = Path(directory)/'models.json'
        path.write_text(json.dumps({'providers': {'openai-codex': {
            'baseUrl': 'https://chatgpt.com/backend-api', 'api': 'openai-codex-responses',
            'apiKey': auth['providers']['openai-codex']['tokens']['access_token'],
            'models': [{'id': model['default'], 'name': model['default'], 'reasoning': True,
                        'input': ['text', 'image'], 'contextWindow': 32768, 'maxTokens': 4096}]
        }}}))
        path.chmod(0o600)
        args.models_file, args.provider, args.model = path, 'openai-codex', model['default']
        result = run(args)
    unchanged = all(hashlib.sha256((fixture.profile/name).read_bytes()).hexdigest() == digest
                    for name, digest in fixture.hashes.items())
    report = json.loads(args.out.read_text())
    report.update(credential_source='explicit Hermes model-only fixture; no refresh state',
                  private_hermes_profile_unchanged=unchanged)
    report['passed'] = report['passed'] and unchanged
    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2)+'\n')
    print(json.dumps({'private_hermes_profile_unchanged': unchanged, 'passed': report['passed']}))
    return result if unchanged else 2


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--pi', type=Path, required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument('--models-file', type=Path)
    source.add_argument('--hermes-model-profile', type=Path)
    parser.add_argument('--model-env-file', type=Path)
    parser.add_argument('--provider')
    parser.add_argument('--model')
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    if args.hermes_model_profile:
        sys.exit(run_with_hermes_model(args))
    if not args.provider or not args.model:
        parser.error('--models-file requires --provider and --model')
    sys.exit(run(args))
