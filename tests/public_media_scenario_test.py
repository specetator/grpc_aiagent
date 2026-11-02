"""Acceptance assertion regression tests; synthetic answers, never model E2E."""
import copy
import unittest

from public_media_scenario import MediaScenario, validate_audio_task, validate_decisions, validate_video, words


class AcceptanceAssertions(unittest.TestCase):
    def setUp(self):
        self.asset = {'id': 'test-asset', 'revision': 'r1'}
        self.plan = [{'code': 'silver lantern', 'actions': [{'owner': 'Alice', 'terms': ['queue', 'backpressure']}]}]
        self.fragments = [{'index': 7, 'content': {'text': 'Silver lantern. Alice owns queue backpressure review.'}}]
        self.refs = [{'source_id': 'audio-source', 'reference': {'asset_id': 'test-asset', 'revision': 'r1',
                     'kind': 'fragment', 'index': 7}}]
        self.answer = {'decisions': [{'code': 'silver lantern', 'actions': [{'owner': 'Alice', 'action': 'Review queue backpressure'}],
                                     'source_ids': ['audio-source']}],
                       'tasks': [{'decision_code': 'silver lantern', 'owner': 'Alice', 'title': 'Review queue backpressure',
                                  'description': 'Inspect bounded queues', 'steps': ['Inspect admission'], 'source_ids': ['audio-source']}]}
        self.video = {'transition_seconds': 5, 'markers': [
            {'panel_color': 'red', 'marker_color': 'white', 'marker_shape': 'circle'},
            {'panel_color': 'green', 'marker_color': 'yellow', 'marker_shape': 'triangle'}]}
        self.images = [{'source_id': str(i), 'reference': {'asset_id': 'test-asset', 'revision': 'r1',
                       'kind': 'image', 'index': i, 'anchor': {'timestamp_ms': i * 5000}}} for i in range(2)]
        self.visual = {'frames': [dict(v, timestamp_ms=i * 5000, source_id=str(i)) for i, v in enumerate(self.video['markers'])],
                       'transition_window_ms': [0, 5000], 'audio_summary': 'Connection failure followed by recovery.'}

    def audio(self, answer=None, refs=None):
        return validate_decisions(answer or self.answer, self.plan, refs or self.refs, self.fragments, self.asset)

    def test_valid_structured_evidence(self):
        self.assertEqual(self.audio()['owner'], 'Alice')
        validate_video(self.visual, self.video, self.images, self.asset)

    def test_only_explicit_harbor_spelling_equivalence(self):
        self.assertEqual(words('Magenta Harbour'), words('magenta harbor'))
        self.assertNotEqual(words('magenta garden'), words('magenta harbor'))

    def test_code_names_do_not_cover_wrong_owners(self):
        value = copy.deepcopy(self.answer)
        value['decisions'][0]['actions'][0]['owner'] = 'Bob'
        with self.assertRaises(AssertionError): self.audio(value)

    def test_unnamed_owner_is_a_failed_gate_not_an_evaluator_crash(self):
        value = copy.deepcopy(self.answer)
        value['decisions'][0]['actions'].append({'owner': None, 'action': 'unassigned discussion'})
        with self.assertRaisesRegex(AssertionError, 'missing named owner'): self.audio(value)

    def test_nonempty_unrelated_task_is_rejected(self):
        value = copy.deepcopy(self.answer)
        value['tasks'][0].update(title='Buy coffee', description='Coffee purchase', steps=['Shop'])
        with self.assertRaises(AssertionError): self.audio(value)

    def test_independent_valid_task_does_not_make_whole_answer_pass(self):
        value = copy.deepcopy(self.answer)
        bad = copy.deepcopy(value['tasks'][0])
        bad.update(title='Buy coffee', description='Coffee purchase', steps=['Shop'])
        value['tasks'].insert(0, bad)
        with self.assertRaises(AssertionError): self.audio(value)
        self.assertEqual(validate_audio_task(value['tasks'][1], self.plan, self.refs,
                                            self.fragments, self.asset)['owner'], 'Alice')
        scenario = MediaScenario.__new__(MediaScenario)
        scenario.cases = ('audio',)
        scenario.evidence = {'case_failures': {'audio': 'unrelated proposed task'}, 'cases_passed': [],
                             'task': {'persisted': True}, 'checks': ['downstream recovery observed']}
        with self.assertRaisesRegex(AssertionError, 'media quality failed'): scenario.assert_complete()

    def test_uncertain_task_cannot_be_selected_as_independently_verified(self):
        task = copy.deepcopy(self.answer['tasks'][0]);task['needs_confirmation'] = True
        with self.assertRaisesRegex(AssertionError, 'still requires confirmation'):
            validate_audio_task(task, self.plan, self.refs, self.fragments, self.asset)

    def test_source_must_be_this_audio_checkpoint(self):
        for field, value in [('asset_id', 'other-asset'), ('revision', 'old'), ('index', 99)]:
            refs = copy.deepcopy(self.refs)
            refs[0]['reference'][field] = value
            with self.subTest(field=field), self.assertRaises(AssertionError): self.audio(refs=refs)

    def test_audio_colors_cannot_substitute_for_nonspoken_visual_fact(self):
        value = copy.deepcopy(self.visual)
        value['frames'][0]['marker_shape'] = 'triangle'
        with self.assertRaises(AssertionError): validate_video(value, self.video, self.images, self.asset)

    def test_two_frames_before_transition_do_not_pass(self):
        value, refs = copy.deepcopy(self.visual), copy.deepcopy(self.images)
        value['frames'][1]['timestamp_ms'] = 1000
        value['transition_window_ms'] = [0, 1000]
        refs[1]['reference']['anchor']['timestamp_ms'] = 1000
        with self.assertRaises(AssertionError): validate_video(value, self.video, refs, self.asset)

    def test_invented_timestamp_or_cross_asset_image_is_rejected(self):
        for field in ('timestamp', 'asset'):
            value, refs = copy.deepcopy(self.visual), copy.deepcopy(self.images)
            if field == 'timestamp': value['frames'][1]['timestamp_ms'] = 5500
            else: refs[1]['reference']['asset_id'] = 'other-asset'
            with self.subTest(field=field), self.assertRaises(AssertionError):
                validate_video(value, self.video, refs, self.asset)

    def test_video_requires_audio_meaning(self):
        value = copy.deepcopy(self.visual)
        value['audio_summary'] = 'No incident, everything remained healthy.'
        with self.assertRaises(AssertionError): validate_video(value, self.video, self.images, self.asset)


if __name__ == '__main__':
    unittest.main()
