"""Private bundles tested only with synthetic SQLite and temporary bare Git."""
from contextlib import closing
from copy import deepcopy
from dataclasses import replace
import gzip
import hashlib
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.archive import ResearchArchive
from zerotrader import cloud_archive
from zerotrader.cloud_bundle import (
    BundleCorrupt, BundleError, BundleReceipt, BundleUncertain, GitBundleStore, RecoveredBundle,
)
from zerotrader.cloud_checkpoint import (
    ARCHIVE_INDEX_PATH, CHECKPOINT_PATH, CheckpointConflict, CheckpointCorrupt,
    CheckpointError, CheckpointInvalid, CheckpointMissing, CheckpointReceipt, GitCheckpointStore,
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, allow_nan=False,
                      separators=(',', ':')).encode('utf-8')


def synthetic_state():
    client_id = 'synthetic-original-client-id'
    return {'version': 1, 'account_id': 'synthetic-account-never-a-broker',
            'equity_peak': 100123.45, 'paused': True, 'halted': 'synthetic latched halt',
            'owners': {'SYN': {'sleeve': 'swing', 'entry_cid': client_id}},
            'intents': {client_id: {'payload': {'client_order_id': client_id, 'symbol': 'SYN',
                                            'qty': '2', 'side': 'buy', 'type': 'limit', 'limit_price': '10'},
                                    'sleeve': 'swing', 'status': 'submitting',
                                    'created': '2026-01-02T15:00:00Z'}},
            'fills': {}, 'strategies': {}, 'events': [], 'last_research': None, 'last_cycle': None}


@unittest.skipUnless(shutil.which('git'), 'Git CLI is required for temporary bare-repository tests')
class CloudBundleTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='zoh-synthetic-bundle-tests-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.remote = self.root / 'synthetic.git'
        self.remote.mkdir()
        self.core = GitCheckpointStore(self.remote)
        self.addCleanup(self.core.close)
        self.core._checked(self.remote, 'init', '--bare', '--quiet')
        self.application = self.root / 'application'
        (self.application / 'zerotrader').mkdir(parents=True)
        (self.application / 'zerotrader' / 'engine.py').write_text('SYNTHETIC = 1\n', encoding='utf-8')
        source = patch('zerotrader.archive.ROOT', self.application)
        source.start()
        self.addCleanup(source.stop)
        self.archive = ResearchArchive(self.root / 'synthetic-runtime')
        self.run = self.archive.start_run('synthetic_test', {'budget': 100000}, {'period': 20},
                                          at='2026-01-02T15:00:00Z')
        self.state = synthetic_state()
        self.archive.dataset(self.run, 'synthetic_bars', {'SYN': [{'c': 10.0, 'v': 1000}]})
        self.archive.samples(self.run, [
            {'symbol': 'SYN', 'sleeve': 'swing', 'decision': 'selected', 'features': {'score': 1}},
            {'symbol': 'OTHER', 'sleeve': 'swing', 'decision': 'not_selected', 'features': {'score': 0}},
        ])
        self.archive.event(self.run, 'order_intent', self.state['intents']['synthetic-original-client-id'])
        self.bundle = self.new_bundle()

    def new_bundle(self, core=None):
        core = core or GitCheckpointStore(self.remote)
        self.addCleanup(core.close)
        result = GitBundleStore(core, chunk_limit=4096)
        self.addCleanup(result.close)
        return result

    def bootstrap(self):
        return self.bundle.bootstrap(self.state, self.archive.path)

    def index(self, receipt):
        core = receipt.checkpoint if isinstance(receipt, BundleReceipt) else receipt
        return json.loads(self.core.read_files(core, [ARCHIVE_INDEX_PATH])[ARCHIVE_INDEX_PATH])

    def rewrite(self, path, content):
        """Synthetic external writer bypassing the safe store to test corruption."""
        with self.core._repository() as directory:
            head = self.core._head(directory)
            self.core._fetch(directory, head)
            self.core._checked(directory, 'read-tree', head)
            envelope = json.loads(self.core._checked(directory, 'show', head + ':' + CHECKPOINT_PATH))
            envelope['previous_commit'] = head
            blob = self.core._checked(directory, 'hash-object', '-w', '--stdin', data=canonical(envelope)).decode().strip()
            self.core._checked(directory, 'update-index', '--add', '--cacheinfo', '100644', blob, CHECKPOINT_PATH)
            if content is None:
                self.core._checked(directory, 'update-index', '--force-remove', path)
            else:
                blob = self.core._checked(directory, 'hash-object', '-w', '--stdin', data=content).decode().strip()
                self.core._checked(directory, 'update-index', '--add', '--cacheinfo', '100644', blob, path)
            tree = self.core._checked(directory, 'write-tree').decode().strip()
            commit = self.core._checked(directory, 'commit-tree', tree, '-p', head,
                                        '-m', 'Synthetic corruption fixture').decode().strip()
            self.assertEqual(self.core._push(directory, commit).returncode, 0)
            return commit

    def test_fresh_runner_restores_atomic_core_archive_original_ids_and_risk(self):
        self.bundle.chunk_limit = 1024  # Force several chunks without a large fixture.
        receipt = self.bootstrap()
        self.assertIsInstance(receipt, BundleReceipt)
        self.assertEqual(receipt.state_sha256, hashlib.sha256(canonical(self.state)).hexdigest())
        target = self.root / 'fresh.sqlite3'
        fresh = self.new_bundle()
        with patch('zerotrader.cloud_bundle._READ_BATCH_FILES', 1):
            with patch.object(fresh.core, '_read_files_at', wraps=fresh.core._read_files_at) as reads:
                loaded = fresh.restore(target)
        self.assertTrue(all(len(call.args[2]) <= 1 for call in reads.call_args_list))
        self.assertGreater(sum(path.endswith('.gz') for call in reads.call_args_list for path in call.args[2]), 1)
        self.assertTrue(fresh.restore_timings['pinned_read_session'])
        self.assertEqual(fresh.restore_timings['replayed_generations'], 1)
        self.assertEqual(loaded.receipt, receipt)
        self.assertEqual(loaded.state, self.state)
        self.assertEqual(cloud_archive._source_snapshot(self.archive.path), cloud_archive._source_snapshot(target))
        self.assertNotIn(self.state['account_id'], repr(loaded))
        with closing(sqlite3.connect(target)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM samples').fetchone()[0], 2)
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM events')

    def test_incremental_finalizes_old_runs_preserves_fill_corrections_and_package_history(self):
        first = self.bootstrap()
        before = self.index(first)
        for price in ('10.01', '10.02'):
            fill = {'id': 'synthetic-fill', 'price': price, 'qty': '2'}
            digest = hashlib.sha256(canonical(fill)).hexdigest()
            self.archive.event_once(self.run, 'broker_fill_activity',
                {'activity_id': fill['id'], 'activity_hash': digest, 'activity': fill},
                'synthetic-fill-version:' + digest)
        self.archive.finish_run(self.run, 'completed', {'synthetic_equity': 100001})
        state = deepcopy(self.state)
        state['last_cycle'] = '2026-01-02T16:00:00Z'
        second = self.bundle.commit(state, self.archive.path, expected_head=first.commit)
        after = self.index(second)
        self.assertEqual(after['packages'][:-1], before['packages'])
        self.assertEqual(second.previous_commit, first.commit)
        self.assertEqual(second.generation, 1)
        old_path = 'archive/packages/' + before['packages'][0] + '/manifest.json'
        self.assertTrue(self.core.prove_files_unchanged(second.checkpoint, first.checkpoint, [old_path]))
        target = self.root / 'incremental.sqlite3'
        loaded = self.new_bundle().restore(target)
        self.assertEqual(loaded.state, state)
        self.assertEqual(cloud_archive._source_snapshot(self.archive.path), cloud_archive._source_snapshot(target))
        with closing(sqlite3.connect(target)) as db:
            self.assertEqual(db.execute('SELECT status FROM runs WHERE run_id=?', (self.run,)).fetchone()[0], 'completed')
            versions = [json.loads(gzip.decompress(row[0]))['activity']['price'] for row in db.execute(
                "SELECT b.payload FROM events e JOIN blobs b ON b.sha256=e.payload_hash WHERE e.kind='broker_fill_activity' ORDER BY e.sequence")]
            self.assertEqual(versions, ['10.01', '10.02'])

    def test_missing_remote_and_core_only_remote_never_create_default_or_target(self):
        target = self.root / 'missing.sqlite3'
        with self.assertRaises(CheckpointMissing):
            self.bundle.restore(target)
        self.assertFalse(target.exists())
        self.core.bootstrap(self.state)
        with self.assertRaises(CheckpointCorrupt):
            self.bundle.restore(target)
        self.assertFalse(target.exists())
        with self.assertRaises(BundleError):
            self.bundle.commit(self.state, self.archive.path, expected_head=None)

    def test_core_only_protective_save_has_no_entry_receipt_until_new_bundle(self):
        initial = self.bootstrap()
        state = deepcopy(self.state)
        state['last_cycle'] = '2026-01-02T17:00:00Z'
        core_only = self.core.commit(state, expected_head=initial.commit)
        self.assertNotIsInstance(core_only, BundleReceipt)
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().load()
        self.archive.event(self.run, 'synthetic_protective_exit_observation', {'only': 'fixture'})
        confirmed = self.bundle.commit(state, self.archive.path, expected_head=core_only.commit)
        self.assertEqual(confirmed.previous_commit, core_only.commit)
        self.assertEqual(self.new_bundle().load().state, state)

    def test_recovery_restores_latest_exit_and_risk_then_appends_without_core_rollback(self):
        initial = self.bootstrap()
        original_index = self.index(initial)
        current = deepcopy(self.state)
        current['equity_peak'] += 500
        current['last_cycle'] = '2026-01-02T18:00:00Z'
        current['owners']['SYN']['exit_reason'] = 'synthetic protective exit'
        current['intents']['synthetic-original-client-id']['order_id'] = 'synthetic-entry-order'
        exit_id = 'synthetic-original-exit-client-id'
        current['intents'][exit_id] = {
            'payload': {'client_order_id': exit_id, 'symbol': 'SYN', 'qty': '2',
                        'side': 'sell', 'type': 'market', 'time_in_force': 'day'},
            'sleeve': 'swing', 'status': 'exit_prepared', 'created': '2026-01-02T18:00:00Z',
            'exit_for': 'synthetic-original-client-id', 'cancel_order_ids': ['synthetic-stop-order'],
        }
        prepared = self.core.commit(current, expected_head=initial.commit)
        current['intents'][exit_id].update(status='filled', order_id='synthetic-exit-order')
        current['known_order_ids'] = ['synthetic-entry-order', 'synthetic-stop-order', 'synthetic-exit-order']
        current['fills'] = {'synthetic-exit-order': {'qty': '2', 'price': '9.99'}}
        latest = self.core.commit(current, expected_head=prepared.commit)

        recovered_runtime = self.root / 'recovered-runtime'
        target = recovered_runtime / 'archive' / 'research.sqlite3'
        target.parent.mkdir(parents=True)
        cold = self.new_bundle()
        with patch.object(cold.core, '_push', wraps=cold.core._push) as pushes:
            recovered = cold.restore_recovery(target)
        self.assertFalse(pushes.called)
        self.assertIsInstance(recovered, RecoveredBundle)
        self.assertIs(type(recovered.receipt), CheckpointReceipt)
        self.assertNotIsInstance(recovered.receipt, BundleReceipt)
        self.assertTrue(recovered.evidence_stale)
        self.assertFalse(recovered.entries_allowed)
        self.assertEqual(recovered.receipt, latest)
        self.assertEqual(recovered.state, current)
        self.assertEqual(recovered.archive_state_sha256, initial.state_sha256)
        self.assertEqual(recovered.archive_manifest_sha256, initial.archive_manifest_sha256)
        self.assertEqual(recovered.archive_snapshot_sha256, initial.archive_snapshot_sha256)
        self.assertEqual(cloud_archive._source_snapshot(target), cloud_archive._source_snapshot(self.archive.path))
        self.assertNotIn(current['account_id'], repr(recovered))
        with self.assertRaises(BundleError):
            cold.confirm(recovered.receipt)
        with self.assertRaises(BundleCorrupt):
            cold.load()
        with self.assertRaises(BundleCorrupt):
            cold.restore(self.root / 'strict-stale.sqlite3')
        self.assertFalse((self.root / 'strict-stale.sqlite3').exists())
        unchanged = target.read_bytes()
        with self.assertRaises(BundleCorrupt):
            cold.restore_recovery(target)
        self.assertEqual(target.read_bytes(), unchanged)

        # Recovered state, not the old archive's bound state, must continue the
        # head. Even an explicit caller mistake cannot remove the durable exit.
        with self.assertRaises(CheckpointInvalid):
            cold.commit(self.state, target, expected_head=recovered.receipt.commit)
        self.assertEqual(self.core.load().state, current)
        continued = ResearchArchive(recovered_runtime)
        continued.finish_run(self.run, 'interrupted', {'synthetic_recovery_gap': True})
        run = continued.start_run('synthetic_reconciliation', {'budget': 100000}, {'period': 20})
        continued.event(run, 'synthetic_recovery_observation', {'exit': current['intents'][exit_id]})
        receipt = cold.commit(recovered.state, continued.path, expected_head=recovered.receipt.commit)
        self.assertIsInstance(receipt, BundleReceipt)
        self.assertEqual(receipt.previous_commit, latest.commit)
        self.assertEqual(self.index(receipt)['packages'][:-1], original_index['packages'])
        loaded = self.new_bundle().restore(self.root / 'resynchronized.sqlite3')
        self.assertEqual(loaded.state, current)
        self.assertEqual(loaded.receipt, receipt)
        self.assertEqual(cloud_archive._source_snapshot(continued.path),
                         cloud_archive._source_snapshot(self.root / 'resynchronized.sqlite3'))

    def test_recovery_of_synchronized_bundle_still_does_not_issue_entry_receipt(self):
        initial = self.bootstrap()
        recovered = self.new_bundle().load_recovery()
        self.assertFalse(recovered.evidence_stale)
        self.assertFalse(recovered.entries_allowed)
        self.assertIs(type(recovered.receipt), CheckpointReceipt)
        self.assertEqual(recovered.receipt, initial.checkpoint)
        self.assertEqual(recovered.state, self.state)
        self.assertNotIsInstance(recovered, BundleReceipt)
        self.assertFalse(hasattr(recovered.receipt, 'archive_manifest_sha256'))
        self.assertEqual(self.new_bundle().load().receipt, initial)

    def test_recovery_refuses_missing_corrupt_or_moving_remote_before_target_creation(self):
        target = self.root / 'refused-recovery.sqlite3'
        with self.assertRaises(CheckpointMissing):
            self.new_bundle().restore_recovery(target)
        self.assertFalse(target.exists())
        initial = self.bootstrap()
        current = deepcopy(self.state)
        current['last_cycle'] = '2026-01-02T18:00:00Z'
        latest = self.core.commit(current, expected_head=initial.commit)
        cold = self.new_bundle()
        original = cold.core._read_files_at
        advanced = []

        def move_head_after_download(directory, receipt, paths):
            result = original(directory, receipt, paths)
            if any(path.endswith('.gz') for path in paths) and not advanced:
                current['last_cycle'] = '2026-01-02T18:01:00Z'
                advanced.append(self.core.commit(current, expected_head=latest.commit))
            return result

        with patch.object(cold.core, '_read_files_at', side_effect=move_head_after_download):
            with self.assertRaises(CheckpointConflict):
                cold.restore_recovery(target)
        self.assertFalse(target.exists())
        manifest_path = 'archive/packages/' + initial.archive_manifest_sha256 + '/manifest.json'
        self.rewrite(manifest_path, b'corrupt synthetic recovery manifest')
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().restore_recovery(target)
        self.assertFalse(target.exists())
        self.rewrite(manifest_path, None)
        with self.assertRaises(CheckpointCorrupt):
            self.new_bundle().restore_recovery(target)
        self.assertFalse(target.exists())

    def test_remote_missing_chunk_or_corrupt_chunk_fails_before_target_creation(self):
        receipt = self.bootstrap()
        index = self.index(receipt)
        prefix = 'archive/packages/' + index['packages'][0] + '/'
        manifest = json.loads(self.core.read_files(receipt.checkpoint, [prefix + 'manifest.json'])[prefix + 'manifest.json'])
        chunk_path = prefix + manifest['chunks'][0]['name']
        self.rewrite(chunk_path, b'corrupt synthetic chunk')
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().restore(self.root / 'corrupt.sqlite3')
        self.assertFalse((self.root / 'corrupt.sqlite3').exists())
        self.rewrite(chunk_path, None)
        with self.assertRaises(CheckpointCorrupt):
            self.new_bundle().restore(self.root / 'missing-chunk.sqlite3')
        self.assertFalse((self.root / 'missing-chunk.sqlite3').exists())

    def test_wrong_index_state_binding_or_chain_is_rejected(self):
        receipt = self.bootstrap()
        index = self.index(receipt)
        index['state_sha256'] = '0' * 64
        self.rewrite(ARCHIVE_INDEX_PATH, canonical(index))
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().load()
        index['state_sha256'] = receipt.state_sha256
        index['packages'] = [index['packages'][0], index['packages'][0]]
        index['generation'] = 1
        self.rewrite(ARCHIVE_INDEX_PATH, canonical(index))
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().load()

    def test_readback_failure_after_push_is_uncertain_and_never_repushes(self):
        with patch.object(self.bundle.core, '_push', wraps=self.bundle.core._push) as pushes:
            with patch.object(self.bundle.core, 'read_files', side_effect=CheckpointError('synthetic readback failure')):
                with self.assertRaises(BundleUncertain) as caught:
                    self.bootstrap()
            self.assertEqual(pushes.call_count, 1)
        self.assertNotIsInstance(caught.exception.checkpoint, BundleReceipt)
        recovered = self.new_bundle().load()
        self.assertEqual(recovered.receipt.checkpoint, caught.exception.checkpoint)
        self.assertEqual(recovered.state, self.state)

    def test_cached_commit_reads_new_packages_and_proves_old_remote_objects(self):
        initial = self.bootstrap()
        # Establish a full restore point first; ordinary tail commits still
        # reuse verified old content without inflating old market inputs.
        self.archive.event(self.run, 'synthetic-establish-point', {'n': 1})
        initial = self.bundle.commit(self.state, self.archive.path, expected_head=initial.commit)
        old_prefix = 'archive/packages/' + initial.archive_manifest_sha256 + '/'
        with closing(sqlite3.connect(self.archive.path)) as db:
            old_market_blob = db.execute(
                'SELECT payload FROM blobs WHERE sha256=(SELECT blob_hash FROM datasets LIMIT 1)').fetchone()[0]
        self.archive.event(self.run, 'synthetic_more_evidence', {'n': 2})
        with patch.object(cloud_archive, '_gunzip', wraps=cloud_archive._gunzip) as inflates:
            with patch.object(self.bundle.core, 'read_files', wraps=self.bundle.core.read_files) as reads, \
                    patch.object(self.bundle.core, 'prove_files_unchanged', wraps=self.bundle.core.prove_files_unchanged) as proofs:
                second = self.bundle.commit(self.state, self.archive.path, expected_head=initial.commit)
        self.assertFalse(any(call.args[0] == old_market_blob for call in inflates.call_args_list))
        paths = [path for call in reads.call_args_list for path in call.args[1]]
        self.assertTrue(proofs.called)
        self.assertFalse(any(path.startswith(old_prefix) for path in paths))
        self.assertTrue(any(second.archive_manifest_sha256 in path for path in paths))
        self.assertEqual(self.bundle.confirm(second).receipt, second)
        advanced = self.bundle.core.commit(self.state, expected_head=second.commit)
        with patch.object(self.bundle.core, 'prove_files_unchanged', return_value=False):
            with self.assertRaises(BundleCorrupt):
                self.bundle.commit(self.state, self.archive.path, expected_head=advanced.commit)

    def test_cache_cannot_hide_remote_old_package_rewrite_on_new_core_head(self):
        initial = self.bootstrap()
        prefix = 'archive/packages/' + initial.archive_manifest_sha256 + '/'
        head = self.rewrite(prefix + 'manifest.json', b'corrupt synthetic manifest')
        with patch.object(self.bundle.core, '_push', wraps=self.bundle.core._push) as pushes:
            with self.assertRaises(CheckpointError):
                self.bundle.commit(self.state, self.archive.path, expected_head=head)
            self.assertFalse(pushes.called)

    def test_corrupted_local_cache_is_not_trusted_even_with_unchanged_remote_head(self):
        initial = self.bootstrap()
        directory = self.bundle._root / initial.archive_manifest_sha256
        manifest = json.loads((directory / 'manifest.json').read_bytes())
        (directory / manifest['chunks'][0]['name']).write_bytes(b'corrupt local cache')
        with self.assertRaises(BundleCorrupt):
            self.bundle.confirm(initial)

    def test_stale_head_or_forged_receipt_never_gets_confirmation(self):
        first = self.bootstrap()
        second = self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)
        with self.assertRaises(CheckpointConflict):
            self.bundle.confirm(first)
        with self.assertRaises(BundleCorrupt):
            self.bundle.confirm(replace(second, archive_snapshot_sha256='0' * 64))
        with self.assertRaises(CheckpointConflict):
            self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)

    def test_restore_refuses_existing_target_and_repeated_fresh_restore_is_identical(self):
        receipt = self.bootstrap()
        one, two = self.root / 'one.sqlite3', self.root / 'two.sqlite3'
        self.assertEqual(self.new_bundle().restore(one).receipt, receipt)
        original = one.read_bytes()
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().restore(one)
        self.assertEqual(one.read_bytes(), original)
        self.assertEqual(self.new_bundle().restore(two).receipt, receipt)
        self.assertEqual(cloud_archive._source_snapshot(one), cloud_archive._source_snapshot(two))

    def test_restore_point_keeps_original_history_and_restores_exact_state(self):
        first = self.bootstrap()
        original = self.index(first)['packages']
        self.archive.finish_run(self.run, 'completed', {'equity': 100123.45})
        second = self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)
        index = self.index(second)
        self.assertEqual(index['version'], 2)
        self.assertEqual(index['packages'][:len(original)], original)
        self.assertEqual(index['restore_point']['generation'], second.generation)
        fresh = self.new_bundle()
        with patch('zerotrader.cloud_archive.seed_restore_point', wraps=cloud_archive.seed_restore_point) as seeded:
            restored = fresh.restore_recovery(self.root / 'point-restored.sqlite3')
        self.assertEqual(seeded.call_count, 1)
        self.assertEqual(restored.state, self.state)
        self.assertFalse(restored.entries_allowed)
        self.assertEqual(cloud_archive._source_snapshot(self.root / 'point-restored.sqlite3'),
                         cloud_archive._source_snapshot(self.archive.path))
        self.archive.event(self.run, 'synthetic-tail', {'n': 2})
        third = self.bundle.commit(self.state, self.archive.path, expected_head=second.commit)
        self.assertEqual(self.index(third)['restore_point'], index['restore_point'])
        self.assertEqual(self.new_bundle().restore(self.root / 'tail-restored.sqlite3').receipt, third)

    def test_restore_point_cannot_hide_corrupted_original_chunk(self):
        first = self.bootstrap()
        self.archive.finish_run(self.run, 'completed', {'equity': 100123.45})
        second = self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)
        index = self.index(second)
        old = index['packages'][0]
        prefix = 'archive/packages/' + old + '/'
        manifest = json.loads(self.core.read_files(second, [prefix + 'manifest.json'])[prefix + 'manifest.json'])
        self.rewrite(prefix + manifest['chunks'][0]['name'], b'corrupt-history')
        with self.assertRaises((BundleCorrupt, CheckpointCorrupt)):
            self.new_bundle().restore(self.root / 'never-created.sqlite3')
        self.assertFalse((self.root / 'never-created.sqlite3').exists())

    def test_forged_restore_point_binding_fails_closed(self):
        first = self.bootstrap()
        self.archive.finish_run(self.run, 'completed', {'equity': 100123.45})
        second = self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)
        index = self.index(second)
        index['restore_point']['snapshot_sha256'] = '0' * 64
        self.rewrite(ARCHIVE_INDEX_PATH, canonical(index))
        with self.assertRaises(BundleCorrupt):
            self.new_bundle().restore(self.root / 'forged-never-created.sqlite3')

    def test_periodic_restore_point_is_bound_to_latest_generation(self):
        first = self.bootstrap()
        with patch('zerotrader.cloud_bundle._RESTORE_POINT_INTERVAL', 1):
            self.archive.event(self.run, 'synthetic-point-one', {'n': 1})
            second = self.bundle.commit(self.state, self.archive.path, expected_head=first.commit)
            old = self.index(second)['restore_point']
            self.archive.event(self.run, 'synthetic-point-two', {'n': 2})
            third = self.bundle.commit(self.state, self.archive.path, expected_head=second.commit)
        latest = self.index(third)
        self.assertNotEqual(latest['restore_point']['package'], old['package'])
        self.assertEqual(latest['restore_point']['generation'], third.generation)
        self.assertTrue(self.core.prove_files_unchanged(third, second,
            ['archive/packages/' + old['package'] + '/manifest.json']))
        self.assertEqual(self.new_bundle().load().receipt, third)


if __name__ == '__main__':
    unittest.main()
