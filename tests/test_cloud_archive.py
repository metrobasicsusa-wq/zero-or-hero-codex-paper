"""Offline archive restoration: synthetic SQLite and temporary local files only."""
import base64
from contextlib import closing
from copy import deepcopy
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
from zerotrader import cloud_archive as cloud


class CloudArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='zoh-synthetic-archive-')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.application = self.root / 'application'
        (self.application / 'zerotrader').mkdir(parents=True)
        (self.application / 'zerotrader' / 'engine.py').write_bytes(b'SYNTHETIC_VERSION = 1\n')
        (self.application / 'settings.json').write_text('{"synthetic":true}', encoding='utf-8')
        source_patch = patch('zerotrader.archive.ROOT', self.application)
        source_patch.start()
        self.addCleanup(source_patch.stop)
        self.archive = ResearchArchive(self.root / 'synthetic-runtime')
        self.at = '2026-10-01T14:00:00+00:00'
        self.run = self.archive.start_run('synthetic_test', {'budget': 100000}, {'period': 20}, at=self.at)
        self.dataset = {'SYN': [{'t': self.at, 'c': 10.25, 'v': 1000}],
                        'notes': 'Unicode 仅为合成数据\n"quoted",=' + 'sample ' * 200}
        self.archive.dataset(self.run, 'synthetic_bars', self.dataset,
                             {'feed': 'synthetic', 'adjustment': 'split', 'observed_at': self.at})
        self.archive.samples(self.run, [
            {'symbol': 'SYN', 'sleeve': 'swing', 'at': self.at, 'decision': 'selected', 'features': {'score': .5}},
            {'symbol': 'OTHER', 'sleeve': 'swing', 'at': self.at, 'decision': 'not_selected', 'features': {'score': None}},
            {'symbol': '=SYNTHETIC()', 'sleeve': 'intraday', 'at': self.at, 'decision': 'missing_data', 'features': {}},
        ])

    def export(self, name, parents=(), limit=4096):
        destination = self.root / name
        receipt = cloud.export_archive(self.archive.path, destination, parents=parents, chunk_limit=limit)
        return destination, receipt

    def restored(self, packages, name='restored.sqlite3'):
        path = self.root / name
        receipt = cloud.restore_archive(packages, path)
        return path, receipt

    def manifest(self, path):
        return json.loads((path / 'manifest.json').read_bytes())

    def write_manifest(self, path, value):
        (path / 'manifest.json').write_bytes(cloud._json(value))

    def event(self, price, activity='synthetic-fill-one', sleeve=None):
        fill = {'id': activity, 'symbol': 'SYN', 'qty': '2', 'price': str(price), 'side': 'buy',
                'transaction_time': self.at, 'order_id': 'synthetic-order-never-broker'}
        version = hashlib.sha256(cloud._json(fill)).hexdigest()
        self.archive.event_once(self.run, 'broker_fill_activity',
            {'source': 'synthetic_fixture', 'activity_id': activity, 'activity_hash': version, 'activity': fill},
            'synthetic-fill:' + activity + ':' + version, at=self.at)
        attribution = {'activity_id': activity, 'activity_hash': version, 'sleeve': sleeve}
        self.archive.event_once(self.run, 'broker_fill_attribution', attribution,
            'synthetic-attribution:' + hashlib.sha256(cloud._json(attribution)).hexdigest(), at=self.at)

    def decoded_events(self, path, kind):
        with closing(sqlite3.connect(path)) as db:
            return [json.loads(gzip.decompress(row[0])) for row in db.execute(
                'SELECT b.payload FROM events e JOIN blobs b ON b.sha256=e.payload_hash WHERE e.kind=? ORDER BY e.sequence', (kind,))]

    def test_baseline_roundtrip_preserves_schema_rows_hashes_and_nested_sources(self):
        self.archive.finish_run(self.run, 'completed', {'synthetic_equity': 100000})
        baseline, receipt = self.export('baseline')
        target, restored = self.restored([baseline])
        self.assertEqual(receipt, restored)
        self.assertEqual(cloud._source_snapshot(self.archive.path), cloud._source_snapshot(target))
        manifest = self.manifest(baseline)
        self.assertEqual(manifest['kind'], 'baseline')
        self.assertGreater(len(manifest['chunks']), 1)
        self.assertTrue(all((baseline / chunk['name']).stat().st_size <= 4096 for chunk in manifest['chunks']))
        with closing(sqlite3.connect(target)) as db:
            self.assertEqual(db.execute('PRAGMA integrity_check').fetchall(), [('ok',)])
            self.assertEqual(db.execute('PRAGMA foreign_key_check').fetchall(), [])
            for table in ('blobs', 'events', 'samples', 'datasets'):
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute('DELETE FROM ' + table)
            sample = db.execute("SELECT symbol FROM samples WHERE decision='missing_data'").fetchone()[0]
            self.assertEqual(sample, '=SYNTHETIC()')

    def test_incrementals_preserve_old_run_completion_original_ids_and_sequence(self):
        baseline, _ = self.export('baseline')
        self.archive.finish_run(self.run, 'completed', {'synthetic_equity': 99999})
        (self.application / 'zerotrader' / 'engine.py').write_bytes(b'SYNTHETIC_VERSION = 2\n')
        second = self.archive.start_run('synthetic_test', {'budget': 100000}, {'period': 30}, at=self.at)
        self.archive.dataset(second, 'same_synthetic_bars', self.dataset, {'feed': 'synthetic'})
        # A backdated observation and equal start time must not be lost by a time watermark.
        self.archive.event(second, 'backdated_synthetic', {'only': 'fixture'}, at='2026-09-30T13:00:00Z')
        first_delta, _ = self.export('increment-one', [baseline])
        self.archive.finish_run(second, 'failed', {'synthetic_equity': 99998}, 'synthetic failure')
        second_delta, _ = self.export('increment-two', [baseline, first_delta])
        target, _ = self.restored([baseline, first_delta, second_delta])
        self.assertEqual(cloud._source_snapshot(self.archive.path), cloud._source_snapshot(target))
        prefix, _ = self.restored([baseline, first_delta], 'prefix.sqlite3')
        with closing(sqlite3.connect(prefix)) as db:
            self.assertEqual(db.execute('SELECT status FROM runs WHERE run_id=?', (second,)).fetchone()[0], 'running')
            self.assertEqual(db.execute('SELECT status FROM runs WHERE run_id=?', (self.run,)).fetchone()[0], 'completed')
        with closing(sqlite3.connect(target)) as db, db:
            previous = db.execute('SELECT MAX(sequence) FROM events').fetchone()[0]
            existing = db.execute('SELECT run_id,payload_hash FROM events LIMIT 1').fetchone()
            db.execute('INSERT INTO events(event_id,run_id,at,kind,payload_hash) VALUES (?,?,?,?,?)',
                       ('synthetic-next-record', existing[0], self.at, 'synthetic_after_restore', existing[1]))
            self.assertGreater(db.execute('SELECT MAX(sequence) FROM events').fetchone()[0], previous)

    def test_same_package_chain_rebuilds_identically_without_duplicate_records(self):
        baseline, initial = self.export('baseline')
        empty_delta, receipt = self.export('empty-increment', [baseline])
        self.assertEqual(initial['counts'], receipt['counts'])
        first, one = self.restored([baseline, empty_delta], 'first.sqlite3')
        second, two = self.restored([baseline, empty_delta], 'second.sqlite3')
        self.assertEqual(one, two)
        self.assertEqual(cloud._source_snapshot(first), cloud._source_snapshot(second))
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.verify_packages([baseline, empty_delta, empty_delta])

    def test_wal_snapshot_excludes_concurrent_later_commit_and_includes_existing_wal(self):
        with closing(sqlite3.connect(self.archive.path)) as held:
            held.execute('PRAGMA journal_mode=WAL')
            held.execute('PRAGMA wal_autocheckpoint=0')
            self.archive.event(self.run, 'synthetic_before_snapshot', {'before': True})
            self.assertGreater(Path(str(self.archive.path) + '-wal').stat().st_size, 0)
            original_schema = cloud._schema
            injected = False

            def concurrent_write(db):
                nonlocal injected
                result = original_schema(db)
                if not injected:
                    injected = True
                    self.archive.event(self.run, 'synthetic_after_snapshot', {'after': True})
                return result

            with patch.object(cloud, '_schema', side_effect=concurrent_write):
                baseline, _ = self.export('wal-baseline')
            target, _ = self.restored([baseline])
            self.assertEqual(len(self.decoded_events(target, 'synthetic_before_snapshot')), 1)
            self.assertEqual(self.decoded_events(target, 'synthetic_after_snapshot'), [])
            self.assertEqual(len(self.decoded_events(self.archive.path, 'synthetic_after_snapshot')), 1)

    def test_fill_versions_attribution_and_cumulative_observations_keep_separate_identities(self):
        self.event('10.00')
        baseline, _ = self.export('baseline')
        self.event('10.00', sleeve='swing')
        self.event('10.00', sleeve='swing')  # repeated poll
        self.event('10.05', sleeve='swing')  # corrected version, same economic execution
        self.event('11.00', activity='synthetic-fill-two', sleeve='swing')
        self.archive.event(self.run, 'fill_observed', {'source': 'cumulative_order_poll', 'delta_qty': 4})
        delta, _ = self.export('increment', [baseline])
        target, _ = self.restored([baseline, delta])
        raw = self.decoded_events(target, 'broker_fill_activity')
        self.assertEqual(len(raw), 3)
        latest = {row['activity_id']: row['activity'] for row in raw}
        self.assertEqual(len(latest), 2)
        self.assertEqual(latest['synthetic-fill-one']['price'], '10.05')
        self.assertEqual(sum(float(row['qty']) for row in latest.values()), 4)
        self.assertEqual(len(self.decoded_events(target, 'broker_fill_attribution')), 4)
        prefix, _ = self.restored([baseline], 'fill-prefix.sqlite3')
        self.assertEqual(self.decoded_events(prefix, 'broker_fill_activity')[0]['activity']['price'], '10.00')
        self.assertEqual(len(self.decoded_events(target, 'fill_observed')), 1)

    def test_missing_corrupt_or_wrong_parent_package_never_creates_target(self):
        baseline, _ = self.export('baseline')
        self.archive.finish_run(self.run, 'completed')
        delta, _ = self.export('delta', [baseline])
        for case in ('missing_chunk', 'damaged_chunk', 'bad_parent', 'wrong_count', 'wrong_version'):
            with self.subTest(case=case):
                copy = self.root / case
                shutil.copytree(delta, copy)
                manifest = self.manifest(copy)
                chunk = copy / manifest['chunks'][0]['name']
                if case == 'missing_chunk':
                    chunk.unlink()
                elif case == 'damaged_chunk':
                    chunk.write_bytes(chunk.read_bytes()[:-1])
                elif case == 'bad_parent':
                    manifest['parent'] = '0' * 64
                elif case == 'wrong_count':
                    manifest['counts']['events'] += 1
                else:
                    manifest['format'] = 'unsupported-version'
                self.write_manifest(copy, manifest)
                target = self.root / (case + '.sqlite3')
                with self.assertRaises(cloud.ArchivePackageError):
                    cloud.restore_archive([baseline, copy], target)
                self.assertFalse(target.exists())
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.restore_archive([delta], self.root / 'missing-baseline.sqlite3')

    def test_existing_destination_package_or_database_is_never_overwritten(self):
        baseline, _ = self.export('baseline')
        manifest_before = (baseline / 'manifest.json').read_bytes()
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.export_archive(self.archive.path, baseline)
        self.assertEqual(manifest_before, (baseline / 'manifest.json').read_bytes())
        target = self.root / 'occupied.sqlite3'
        target.write_bytes(b'synthetic existing evidence')
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.restore_archive([baseline], target)
        self.assertEqual(target.read_bytes(), b'synthetic existing evidence')

    def test_bad_nested_source_reference_is_rejected_before_destination_creation(self):
        # Create a valid blob but deliberately invalid nested source closure in a synthetic DB.
        bad_manifest = self.archive.put_blob({'files': [{'path': 'synthetic.py', 'blob_hash': '0' * 64, 'bytes': 1}]}, 'source_manifest')
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('UPDATE runs SET source_hash=? WHERE run_id=?', (bad_manifest, self.run))
        output = self.root / 'invalid-nested-source'
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.export_archive(self.archive.path, output)
        self.assertFalse(output.exists())

    def test_source_immutable_rewrite_and_finalized_run_rewrite_are_rejected(self):
        self.archive.finish_run(self.run, 'completed')
        baseline, _ = self.export('baseline')
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('UPDATE runs SET status=? WHERE run_id=?', ('failed', self.run))
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('invalid-final-update', [baseline])
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('UPDATE runs SET status=? WHERE run_id=?', ('completed', self.run))
            trigger = db.execute("SELECT sql FROM sqlite_schema WHERE name='immutable_events_update'").fetchone()[0]
            db.execute('DROP TRIGGER immutable_events_update')
            db.execute("UPDATE events SET kind='synthetic_illegal_rewrite' WHERE sequence=1")
            db.execute(trigger)
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('invalid-event-update', [baseline])

    def test_invalid_sample_batch_is_not_present_in_export(self):
        before = self.archive.stats()['samples']
        with self.assertRaises(ValueError):
            self.archive.samples(self.run, [{'symbol': 'SYN'}, {'symbol': 'BAD', 'score': float('nan')}])
        baseline, receipt = self.export('baseline')
        self.assertEqual(receipt['counts']['samples'], before)
        _, restored = self.restored([baseline])
        self.assertEqual(restored['counts']['samples'], before)

    def test_missing_or_weakened_immutable_trigger_is_not_exported(self):
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            original = db.execute("SELECT sql FROM sqlite_schema WHERE name='immutable_events_update'").fetchone()[0]
            db.execute('DROP TRIGGER immutable_events_update')
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('missing-trigger')
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('CREATE TRIGGER immutable_events_update BEFORE UPDATE ON events BEGIN SELECT 1; END')
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('weakened-trigger')
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('DROP TRIGGER immutable_events_update')
            db.execute(original)
        self.export('reviewed-trigger')

    def test_unknown_sqlite_schema_version_is_rejected(self):
        with closing(sqlite3.connect(self.archive.path)) as db:
            db.execute('PRAGMA user_version=2')
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('unknown-schema')
        self.assertFalse((self.root / 'unknown-schema').exists())

    def test_chunk_failure_leaves_no_committed_manifest_and_no_success(self):
        original = cloud._write_new
        calls = 0

        def disk_failure(path, raw):
            nonlocal calls
            calls += 1
            if calls == 2:
                raise OSError('synthetic disk full')
            return original(path, raw)

        destination = self.root / 'incomplete-package'
        with patch.object(cloud, '_write_new', side_effect=disk_failure):
            with self.assertRaises(cloud.ArchivePackageError):
                cloud.export_archive(self.archive.path, destination, chunk_limit=4096)
        self.assertFalse((destination / 'manifest.json').exists())
        with self.assertRaises(cloud.ArchivePackageError):
            cloud.verify_packages([destination])

    def test_restore_install_failure_does_not_publish_partial_database(self):
        baseline, _ = self.export('baseline')
        target = self.root / 'failed-install.sqlite3'
        with patch.object(cloud.os, 'link', side_effect=OSError('synthetic installation failure')):
            with self.assertRaises(cloud.ArchivePackageError):
                cloud.restore_archive([baseline], target)
        self.assertFalse(target.exists())
        self.assertEqual(list(self.root.glob('.archive-restore-*')), [])
        self.restored([baseline], target.name)

    def test_chunk_path_traversal_and_oversized_decompression_are_refused(self):
        baseline, _ = self.export('baseline')
        for case in ('traversal', 'raw_length'):
            copy = self.root / case
            shutil.copytree(baseline, copy)
            manifest = self.manifest(copy)
            if case == 'traversal':
                manifest['chunks'][0]['name'] = '../outside.gz'
            else:
                manifest['chunks'][0]['raw_bytes'] = cloud.MAX_CHUNK_BYTES + 1
            self.write_manifest(copy, manifest)
            with self.assertRaises(cloud.ArchivePackageError):
                cloud.verify_packages([copy])

    def test_blob_hash_is_verified_even_when_outer_package_hashes_are_recomputed(self):
        baseline, _ = self.export('baseline')
        forged = self.root / 'forged-inner-blob'
        forged.mkdir()
        manifest = self.manifest(baseline)
        raw = b''.join(gzip.decompress((baseline / chunk['name']).read_bytes()) for chunk in manifest['chunks'])
        payload = json.loads(raw)
        row = payload['rows']['blobs'][0]
        altered = gzip.compress(b'{"synthetic_corruption":true}', mtime=0)
        row[4], row[5], row[6] = len(b'{"synthetic_corruption":true}'), len(altered), {'$blob': base64.b64encode(altered).decode('ascii')}
        altered_payload = cloud._json(payload)
        chunks = []
        for index, offset in enumerate(range(0, len(altered_payload), 2048)):
            part = altered_payload[offset:offset + 2048]
            packed = gzip.compress(part, mtime=0)
            digest = hashlib.sha256(packed).hexdigest()
            name = f'chunk-{index:06d}-{digest}.gz'
            (forged / name).write_bytes(packed)
            chunks.append({'name': name, 'sha256': digest, 'bytes': len(packed), 'raw_bytes': len(part)})
        manifest['chunks'] = chunks
        manifest['payload_sha256'] = hashlib.sha256(altered_payload).hexdigest()
        self.write_manifest(forged, manifest)
        with self.assertRaises(cloud.ArchivePackageError) as caught:
            cloud.restore_archive([forged], self.root / 'forged.sqlite3')
        self.assertIn('blob SHA-256', str(caught.exception))
        self.assertFalse((self.root / 'forged.sqlite3').exists())

    def test_chunk_bounds_hold_for_small_limit_and_incompressible_synthetic_data(self):
        import random
        synthetic = base64.b64encode(random.Random(42).randbytes(12000)).decode('ascii')
        self.archive.dataset(self.run, 'synthetic_incompressible', {'data': synthetic})
        baseline, _ = self.export('tiny-chunks', limit=256)
        self.assertTrue(all((baseline / chunk['name']).stat().st_size <= 256 for chunk in self.manifest(baseline)['chunks']))
        self.restored([baseline], 'tiny-chunks.sqlite3')
        with self.assertRaises(cloud.ArchivePackageError):
            self.export('overlarge-limit', limit=cloud.MAX_CHUNK_BYTES + 1)

    def test_package_errors_do_not_echo_private_fixture_data(self):
        marker = 'synthetic-sensitive-marker'
        self.archive.event(self.run, 'synthetic_private_event', {'message': marker})
        baseline, _ = self.export('baseline')
        manifest = self.manifest(baseline)
        path = baseline / manifest['chunks'][0]['name']
        path.write_bytes(marker.encode())
        with self.assertRaises(cloud.ArchivePackageError) as caught:
            cloud.verify_packages([baseline])
        self.assertNotIn(marker, str(caught.exception))
        self.assertNotIn(str(self.root), str(caught.exception))


if __name__ == '__main__':
    unittest.main()
