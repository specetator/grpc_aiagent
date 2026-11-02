"""Opt-in contract test against installed Native conversion/recovery methods.

No model, provider HTTP, IM or permission E2E. Model capability predicates are
controlled; actual Native projection, image stripping and learned state run.
Run with the Native Python environment and --source pointing to its source.
"""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    sys.dont_write_bytecode = True
    root = Path(__file__).resolve().parents[1]
    source_files = [args.source / 'agent/vision_message_prep.py', args.source / 'agent/tool_dispatch_helpers.py']
    hashes = {str(p.relative_to(args.source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    report = {'kind': 'installed-native-policy-contract', 'scope': __doc__, 'passed': False,
              'native_source_sha256': hashes, 'checks': []}
    with tempfile.TemporaryDirectory(prefix='spark-native-image-policy-') as temporary:
        os.environ['HERMES_HOME'] = temporary
        os.environ.pop('HERMES_PROFILE', None)
        sys.path[:0] = [str(root / 'cannbot/scripts'), str(args.source)]
        from agent.vision_message_prep import VisionMessagePrepMixin
        from hermes_public_workspace import install_room_media_policy
        from room_context import hermes_result
        fixture = json.loads((root / 'tests/fixtures/room-context.json').read_text())
        value = hermes_result(fixture['image'])

        class Agent(VisionMessagePrepMixin):
            provider = 'controlled-test-provider'
            model = 'controlled-test-model'
            valid_tool_names = {'workspace_read', 'todo_list', 'room_search', 'room_read', 'room_read_media'}
            vision = True
            tool_images = True
            def _model_supports_vision(self): return self.vision
            def _provider_supports_vision_tool_messages(self): return self.tool_images

        def unavailable(result):
            error = json.loads(result)
            assert error['success'] is False and error['code'] == 501
            assert 'source_id' not in result and fixture['image']['items'][0]['data'] not in result

        for mode in ('vision', 'text-model', 'provider-no-images', 'learned-rejection'):
            agent = Agent()
            if mode == 'text-model': agent.vision = False
            if mode == 'provider-no-images': agent.tool_images = False
            if mode == 'learned-rejection': agent._no_list_tool_content_models = {(agent.provider, agent.model)}
            install_room_media_policy(agent)
            before = copy.deepcopy(value)
            converted = agent._tool_result_content_for_active_model('room_read_media', value)
            assert value == before
            if mode == 'vision': assert converted == value['content']
            else: unavailable(converted)
            assert agent._tool_result_content_for_active_model('room_read', 'ordinary text') == 'ordinary text'
            report['checks'].append(mode)
        for remember in (True, False):
            agent = Agent(); install_room_media_policy(agent)
            messages = [{'role': 'user', 'content': copy.deepcopy(value['content'])},
                        {'role': 'tool', 'name': 'room_read_media', 'tool_call_id': 'call-original',
                         'content': copy.deepcopy(value['content'])},
                        {'role': 'tool', 'name': 'room_read', 'tool_call_id': 'call-text', 'content': 'ordinary text'}]
            before = copy.deepcopy(messages)
            assert agent._try_strip_image_parts_from_tool_messages(messages, remember_model=remember) is True
            assert messages[0] == before[0] and messages[2] == before[2]
            assert {k:v for k,v in messages[1].items() if k != 'content'} == {
                k:v for k,v in before[1].items() if k != 'content'}
            unavailable(messages[1]['content'])
            assert bool(getattr(agent, '_no_list_tool_content_models', set())) == remember
            assert agent._try_strip_image_parts_from_tool_messages(messages, remember_model=remember) is False
            report['checks'].append('native-strip-remember-' + str(remember).lower())
    assert hashes == {str(p.relative_to(args.source)): hashlib.sha256(p.read_bytes()).hexdigest() for p in source_files}
    report['passed'] = True
    args.out.write_text(json.dumps(report, indent=2) + '\n')
    print('Installed Native image policy: 6 contract groups passed; no model/provider E2E.')


if __name__ == '__main__':
    main()
