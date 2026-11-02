"""Worker protocol contracts with fake time/transport; not a real parser E2E."""
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "cannbot/scripts"))
import media_worker as worker


class Contracts(unittest.TestCase):
    def test_operator_config_is_private_exact_and_absolute(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'worker.json'
            value=dict(schema=1,image_id='sha256:'+'a'*64,model_dir='/models/public',
                origin='http://127.0.0.1:9011',key_file='/private/key',state_dir='/private/state',
                pipeline_sha256='b'*64,parser_sha256='c'*64)
            path.write_text(json.dumps(value));path.chmod(0o600)
            self.assertEqual(worker.operator_config(path),value)
            for change in ({'state_dir':'relative'},{'schema':True},{'runtime_url':'http://elsewhere'},
                           {'origin':'http://127.0.0.1:9011/another/path'},{'pipeline_sha256':'bad'}):
                path.write_text(json.dumps(dict(value,**change)))
                with self.subTest(change=change),self.assertRaises((worker.WorkerError,ValueError)):
                    worker.operator_config(path)
            path.write_text(json.dumps(value));path.chmod(0o644)
            with self.assertRaises(ValueError):worker.operator_config(path)

    def test_changed_approved_pipeline_fails_before_state_or_network(self):
        approved=dict(schema=1,image_id='sha256:'+'a'*64,model_dir='/models/public',
            origin='http://127.0.0.1:9011',key_file='/private/key',state_dir='/private/state',
            pipeline_sha256='b'*64,parser_sha256='c'*64)
        with mock.patch.object(sys,'argv',['media_worker.py','--config','/private/config']), \
             mock.patch.object(worker,'operator_config',return_value=approved), \
             mock.patch.object(worker,'Configuration',return_value=SimpleNamespace(pipeline='d'*64,parser_sha='c'*64)), \
             mock.patch.object(worker,'State') as state,mock.patch.object(worker,'Client') as client:
            with self.assertRaisesRegex(worker.WorkerError,'worker_configuration_changed'):worker.main()
            state.assert_not_called();client.assert_not_called()

    def test_media_state_preserves_large_bounded_coverage(self):
        with tempfile.TemporaryDirectory() as directory:
            state=worker.State(Path(directory)/"state")
            try:
                value={"summary":{"coverage":{"sections":["header"+str(i) for i in range(2000)]}}}
                state.save(value)
                self.assertGreater((state.root/'claim.json').stat().st_size,16384)
                self.assertEqual(state.load(),value)
                with self.assertRaises(worker.WorkerError):state.save({"oversized":"a"*(128*1024)})
                self.assertEqual(state.load(),value) # Rejected save leaves original recoverable.
                with self.assertRaises(BlockingIOError):worker.State(state.root)
            finally:state.close()

    def test_ambiguous_runtime_errors_are_not_input_failure(self):
        self.assertTrue({'malformed_media','decoder_failed','parser_failed','parser_timeout','parser_cleanup_failed','asr_model_unavailable'}.isdisjoint(worker.INPUT_ERRORS))

    def test_lease_budget_starts_before_request_and_cannot_reset_hard_deadline(self):
        now = [100.0]
        class Client:
            def call(self, *_):now[0] += 25
        assignment = dict(id="att_" + "a" * 32, revision="b" * 64, attempt=1, lease_token="c" * 64, remaining_seconds=50)
        keep = worker.LeaseKeeper(Client(), assignment, 90.0, threading.Event(), clock=lambda: now[0])
        keep.renew()
        self.assertEqual(keep.hard, 140)
        self.assertEqual(keep.until, 140)
        self.assertFalse(keep.cancelled())
        keep.renew() # response arrives after the budget, even though transport says success
        self.assertTrue(keep.cancelled())
        self.assertEqual(keep.hard, 140)

    def test_short_lived_recovery_renews_before_any_work(self):
        assignment = dict(id="att_" + "a" * 32, revision="b" * 64, attempt=1, lease_token="c" * 64, remaining_seconds=300)
        client = mock.Mock();client.call.side_effect=worker.BackendError(409)
        keep=worker.LeaseKeeper(client,assignment,100,threading.Event(),clock=lambda:100)
        with self.assertRaises(worker.BackendError):keep.start()
        self.assertIsNone(keep.thread)

    def test_unknown_claim_response_preserves_preallocated_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            state=worker.State(Path(directory)/"state")
            config=SimpleNamespace(pipeline="a"*64)
            client=mock.Mock();client.call.side_effect=ValueError("bad response")
            try:
                w=worker.Worker(client,state,config)
                with self.assertRaises(ValueError):w.once()
                saved=state.load();self.assertIn("claim_id",saved);self.assertIn("execution_id",saved)
                with self.assertRaises(ValueError):w.once()
                self.assertEqual(saved,state.load())
                self.assertEqual(client.call.call_args_list[0],client.call.call_args_list[1])
                w.config=SimpleNamespace(pipeline="b"*64)
                with self.assertRaises(worker.WorkerError):w.load()
                self.assertEqual(saved,state.load())
            finally:state.close()

    def test_confirmed_complete_wins_over_concurrent_heartbeat_loss(self):
        config=SimpleNamespace(pipeline="a"*64)
        with tempfile.TemporaryDirectory() as directory:
            state=worker.State(Path(directory)/"state")
            try:
                lease=dict(id="att_"+"a"*32,revision="b"*64)
                keep=SimpleNamespace(lease=lease,count=3,guard=mock.Mock())
                client=mock.Mock()
                def complete(*_):
                    keep.guard.side_effect=worker.WorkerError("lease_unavailable")
                    return dict(lease,state="ready",manifest_sha256="c"*64)
                client.call.side_effect=complete
                result=worker.Worker(client,state,config).finish({"summary":{"manifest_sha256":"c"*64}},keep)
                self.assertEqual(result["state"],"ready")
                keep.guard.assert_called_once()
            finally:state.close()

    def test_cleanup_refuses_foreign_container_and_daemon_error(self):
        from cannbot.media import sandbox
        e="e"*32
        for result in (SimpleNamespace(returncode=1,stderr=b"Cannot connect to Docker daemon"),
                       SimpleNamespace(returncode=0,stdout=json.dumps([{"Config":{"Labels":{}},"HostConfig":{},
                           "Name":"/spark-media-"+e,"Image":"sha256:"+"b"*64,"Id":"c"*64}]).encode())):
            with mock.patch.object(sandbox.subprocess,"run",return_value=result) as run:
                with self.assertRaises(sandbox.SandboxError):sandbox.cleanup_execution(e,Path("/private"),"sha256:"+"a"*64,None)
                self.assertEqual(run.call_count,1) # Never rm based on name alone.


if __name__ == "__main__":unittest.main()
