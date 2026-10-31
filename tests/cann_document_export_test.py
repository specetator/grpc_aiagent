"""Public document export boundary tests; no model/runtime doubles are involved."""
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('cann_export', Path(__file__).resolve().parents[1] / 'scripts/export_cann_documents.py')
export = importlib.util.module_from_spec(spec)
spec.loader.exec_module(export)


class ExportTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.source = self.root / 'knowledge'
        self.output = self.root / 'public-data'
        self.generation = '20261101T000000Z-123456789abc'
        self.documents = self.source / 'data/generations' / self.generation / 'documents'
        self.documents.mkdir(parents=True)
        self.pointer = self.source / 'data/current.json'
        self.pointer.write_text(json.dumps({'generation': self.generation}))
        self.doc_id = 'doc_' + 'a' * 24
        self.path = self.documents / (self.doc_id + '.json')
        self.value = {'generation': self.generation, 'document': {'doc_id': self.doc_id, 'content': 'public'},
                      'chunks': [{'doc_id': self.doc_id, 'chunk_id': 'chk_' + 'b' * 24}]}
        self.path.write_text(json.dumps(self.value))
        self.path.chmod(0o600)
        self.pointer.chmod(0o600)

    def test_only_public_documents_and_source_permissions_preserved(self):
        (self.source / 'private.env').write_text('not exported')
        (self.source / 'data/index.sqlite').write_bytes(b'index not needed by Logic')
        original = {p: (p.read_bytes(), p.stat().st_mode) for p in (self.pointer, self.path)}
        receipt = export.export_documents(self.source, self.output)
        self.assertEqual(receipt['documents'], 1)
        files = [p for p in self.output.rglob('*') if p.is_file()]
        self.assertEqual(len(files), 2)
        self.assertTrue(all(p.stat().st_mode & 0o777 == 0o644 for p in files))
        self.assertEqual(self.output.stat().st_mode & 0o777, 0o755)
        for path, (content, mode) in original.items():
            self.assertEqual((path.read_bytes(), path.stat().st_mode), (content, mode))
            self.assertEqual((self.output / path.relative_to(self.source / 'data')).read_bytes(), content)

    def test_existing_output_is_never_modified(self):
        self.output.mkdir()
        sentinel = self.output / 'keep';sentinel.write_text('keep')
        with self.assertRaises(ValueError): export.export_documents(self.source, self.output)
        self.assertEqual(sentinel.read_text(), 'keep')

    def test_source_links_are_refused(self):
        for kind in ('symbolic', 'hard'):
            with self.subTest(kind=kind):
                original = self.path.read_bytes()
                self.path.unlink()
                target = self.root / ('target-' + kind);target.write_bytes(original)
                if kind == 'symbolic': self.path.symlink_to(target)
                else: os.link(target, self.path)
                with self.assertRaises((OSError, ValueError)): export.export_documents(self.source, self.output)
                self.assertFalse(self.output.exists())
                self.path.unlink();self.path.write_bytes(original)
        self.assertEqual(list(self.root.glob('.spark-cann-export-*')), [])

    def test_generation_and_chunk_mismatch_are_refused(self):
        for value in ({**self.value, 'generation': '../elsewhere'},
                      {**self.value, 'chunks': [{'doc_id': 'doc_' + 'c' * 24, 'chunk_id': 'chk_' + 'b' * 24}]}):
            self.path.write_text(json.dumps(value))
            with self.assertRaises(ValueError): export.export_documents(self.source, self.output)
            self.assertFalse(self.output.exists())

    def test_source_switch_during_export_is_refused(self):
        read = export.read_stable
        def change(path, limit):
            result = read(path, limit)
            if path == self.path: self.pointer.write_text(json.dumps({'generation': '20261101T000001Z-123456789abc'}))
            return result
        with patch.object(export, 'read_stable', side_effect=change):
            with self.assertRaisesRegex(ValueError, 'changed'): export.export_documents(self.source, self.output)
        self.assertFalse(self.output.exists())

    def test_output_inside_source_is_refused(self):
        with self.assertRaises(ValueError): export.export_documents(self.source, self.source / 'data/export')

    def test_parent_path_aliases_do_not_bypass_source_boundary(self):
        for source, output in ((self.source / '..' / self.source.name, self.source / 'data/export'),
                               (self.source, self.source / 'data/generations/../export')):
            with self.subTest(source=source, output=output):
                with self.assertRaises(ValueError): export.export_documents(source, output)
                self.assertFalse((self.source / 'data/export').exists())


if __name__ == '__main__': unittest.main()
