"""Atomic core/archive Git tests, all temporary bare repositories and fake data."""
from copy import deepcopy
from dataclasses import dataclass
import shutil
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

from zerotrader.cloud_checkpoint import (ARCHIVE_INDEX_PATH, CheckpointConflict,
    CheckpointCorrupt, CheckpointInvalid, CheckpointReceipt, CheckpointUncertain, GitCheckpointStore)


@unittest.skipUnless(shutil.which("git"), "Git is needed for synthetic integration tests")
class CheckpointFilesTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.remote = Path(self.temp.name) / "state.git"
        self.remote.mkdir()
        self.store = GitCheckpointStore(self.remote)
        self.store._checked(self.remote, "init", "--bare", "--quiet")
        self.state = {"version": 1, "account_id": "synthetic-account", "equity_peak": 100000.,
            "halted": None, "paused": False, "owners": {}, "intents": {}, "fills": {},
            "strategies": {}, "events": [], "last_cycle": None, "last_research": None}
        self.package = "archive/packages/" + "a" * 64 + "/manifest.json"
        self.files = {ARCHIVE_INDEX_PATH: b'{"synthetic":1}', self.package: b'synthetic-immutable-package'}

    def test_core_and_archive_commit_and_readback_are_at_one_head(self):
        first = self.store.bootstrap(self.state, files=self.files)
        fresh = GitCheckpointStore(self.remote)
        self.assertEqual(fresh.read_files(first, list(self.files)), self.files)
        proposed = deepcopy(self.state)
        proposed["equity_peak"] = 100100.
        second_path = "archive/packages/" + "b" * 64 + "/manifest.json"
        second_files = {ARCHIVE_INDEX_PATH: b'{"synthetic":2}', second_path: b'second-package'}
        second = fresh.commit(proposed, expected_head=first.commit, files=second_files)
        self.assertEqual(fresh.confirm(second).state, proposed)
        self.assertEqual(fresh.read_files(second, [self.package, second_path, ARCHIVE_INDEX_PATH]),
                         {**self.files, **second_files})
        self.assertTrue(fresh.prove_files_unchanged(second, first, [self.package]))
        with self.assertRaises(CheckpointCorrupt):
            fresh.prove_files_unchanged(second, first, [ARCHIVE_INDEX_PATH])

    def test_core_only_updates_preserve_prior_archive_and_exact_receipts(self):
        first = self.store.bootstrap(self.state, files=self.files)
        second = self.store.commit(self.state, expected_head=first.commit)
        self.assertTrue(self.store.prove_files_unchanged(second, first, list(self.files)))
        self.assertEqual(self.store.read_files(second, list(self.files)), self.files)
        with self.assertRaises(CheckpointConflict):
            self.store.read_files(first, list(self.files))

    def test_immutable_package_rewrite_cannot_advance_core_or_index(self):
        first = self.store.bootstrap(self.state, files=self.files)
        proposed = deepcopy(self.state)
        proposed["paused"] = True
        with self.assertRaises(CheckpointInvalid):
            self.store.commit(proposed, expected_head=first.commit,
                              files={self.package: b'changed', ARCHIVE_INDEX_PATH: b'changed-index'})
        self.assertEqual(self.store.load().receipt, first)
        self.assertEqual(self.store.read_files(first, list(self.files)), self.files)
        second = self.store.commit(proposed, expected_head=first.commit, files=self.files)
        self.assertTrue(self.store.prove_files_unchanged(second, first, list(self.files)))

    def test_invalid_paths_or_types_are_refused_before_remote_write(self):
        for path, data in (("../state", b'x'), (".github/workflows/x.yml", b'x'),
                           ("checkpoint/core-state.json", b'x'), (self.package, None),
                           ("archive/packages/" + "a" * 64 + "/../x", b'x')):
            with self.subTest(path=path), self.assertRaises(CheckpointInvalid):
                self.store.bootstrap(self.state, files={path: data})
        with self.store._repository() as directory:
            self.assertIsNone(self.store._head(directory))

    def test_missing_package_cannot_be_proven_or_used_as_cache(self):
        first = self.store.bootstrap(self.state)
        with self.assertRaises(CheckpointCorrupt):
            self.store.read_files(first, [self.package])
        with self.assertRaises(CheckpointCorrupt):
            self.store.prove_files_unchanged(first, first, [self.package])

    def test_competing_commit_preserves_newer_core_and_its_archive(self):
        first = self.store.bootstrap(self.state, files=self.files)
        winner = self.store.commit(self.state, expected_head=first.commit,
                                   files={ARCHIVE_INDEX_PATH: b'synthetic-new-index'})
        with self.assertRaises(CheckpointConflict):
            self.store.commit(self.state, expected_head=first.commit,
                              files={ARCHIVE_INDEX_PATH: b'synthetic-loser-index'})
        self.assertEqual(self.store.read_files(winner, [ARCHIVE_INDEX_PATH]),
                         {ARCHIVE_INDEX_PATH: b'synthetic-new-index'})

    def test_core_readback_accepts_verified_core_fields_of_bundle_subclass(self):
        @dataclass(frozen=True)
        class ExtraReceipt(CheckpointReceipt):
            extra: str = "synthetic-extra-evidence"
        first = self.store.bootstrap(self.state, files=self.files)
        decorated = ExtraReceipt(first.commit, first.previous_commit, first.state_sha256)
        self.assertEqual(self.store.confirm(decorated).receipt, first)
        self.assertEqual(self.store.read_files(decorated, list(self.files)), self.files)
        self.assertTrue(self.store.prove_files_unchanged(decorated, decorated, list(self.files)))

    def test_new_bootstrap_does_not_reuse_index_from_unpushed_attempt(self):
        with patch.object(self.store, "_push", return_value=None):
            with self.assertRaises(CheckpointUncertain):
                self.store.bootstrap(self.state, files=self.files)
        receipt = self.store.bootstrap(self.state)
        with self.assertRaises(CheckpointCorrupt):
            self.store.read_files(receipt, [self.package])

