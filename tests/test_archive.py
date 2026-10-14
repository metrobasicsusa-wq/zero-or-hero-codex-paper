"""Research archive persistence, replay, atomicity, and credential hygiene."""
import csv
from contextlib import closing
import gzip
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from zerotrader.archive import REDACTED, ResearchArchive


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'application'
        (self.root / 'zerotrader').mkdir(parents=True)
        (self.root / 'zerotrader' / 'engine.py').write_bytes(b'VERSION = 1\n')
        (self.root / 'settings.json').write_text('{"budget":100000}', encoding='utf-8')
        self.root_patch = patch('zerotrader.archive.ROOT', self.root)
        self.root_patch.start()
        self.addCleanup(self.root_patch.stop)
        self.runtime = Path(self.temp.name) / 'runtime'
        self.archive = ResearchArchive(self.runtime)

    def start(self, **kwargs):
        return self.archive.start_run('cycle', {'budget': 100000}, {'swing': {'period': 20}}, **kwargs)

    def rows(self, query, params=()):
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.row_factory = sqlite3.Row
            return [dict(row) for row in db.execute(query, params)]

    def test_survives_reopen_with_complete_replay_evidence(self):
        run = self.start()
        bars = {'AAPL': [{'t': '2026-10-01T14:00:00Z', 'o': 1.0, 'c': 2.0}]}
        digest = self.archive.dataset(run, 'daily_bars', bars, {'feed': 'iex', 'adjustment': 'all'})
        sample = {'symbol': 'AAPL', 'sleeve': 'swing', 'features': {'momentum': .12},
                  'decision': 'selected', 'asof': '2026-10-01T10:00:00-04:00'}
        self.archive.samples(run, [sample])
        self.archive.event(run, 'order_submitted', {'order_id': 'o1', 'qty': 5})
        self.archive.finish_run(run, 'completed', {'equity': 100000})
        reopened = ResearchArchive(self.runtime)
        self.assertEqual(bars, reopened.get_blob(digest))
        record = self.rows('SELECT * FROM runs')[0]
        self.assertEqual('completed', record['status'])
        self.assertEqual({'equity': 100000}, reopened.get_blob(record['snapshot_hash']))
        self.assertEqual({'budget': 100000}, reopened.get_blob(record['config_hash']))
        manifest = reopened.get_blob(record['source_hash'])
        engine = next(file for file in manifest['files'] if file['path'] == 'zerotrader/engine.py')
        self.assertEqual('VERSION = 1\n', reopened.get_blob(engine['blob_hash']))
        row = self.rows('SELECT * FROM samples')[0]
        self.assertEqual(sample, reopened.get_blob(row['sample_hash']))
        self.assertEqual('2026-10-01T14:00:00+00:00', row['at'])
        self.assertEqual(1, reopened.stats()['datasets'])

    def test_identical_data_and_sources_are_deduplicated_not_overwritten(self):
        run1 = self.start()
        bars = {'AAPL': [{'c': 25, 'v': 123}]}
        first = self.archive.dataset(run1, 'daily', bars)
        count = self.archive.stats()['blobs']
        run2 = self.start()
        second = self.archive.dataset(run2, 'daily', bars)
        self.assertEqual(first, second)
        self.assertEqual(count, self.archive.stats()['blobs'])
        self.assertEqual(2, self.archive.stats()['datasets'])
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('UPDATE blobs SET kind=? WHERE sha256=?', ('altered', first))
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM events')
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute('DELETE FROM runs')
        self.assertEqual(bars, self.archive.get_blob(first))

    def test_partial_fill_events_are_distinct_and_stable_broker_ids_deduplicate(self):
        run = self.start()
        first = {'order_id': 'o1', 'filled_qty': '2', 'filled_avg_price': '50.1'}
        second = {'order_id': 'o1', 'filled_qty': '4', 'filled_avg_price': '50.2'}
        first_id = self.archive.event_once(run, 'broker_fill', first, 'fill:f1')
        second_id = self.archive.event_once(run, 'broker_fill', second, 'fill:f2')
        next_run = self.start()
        self.assertEqual(first_id, self.archive.event_once(next_run, 'broker_fill', first, 'fill:f1'))
        self.assertNotEqual(first_id, second_id)
        rows = self.rows("SELECT * FROM events WHERE kind='broker_fill' ORDER BY sequence")
        self.assertEqual(2, len(rows))
        self.assertEqual([first, second], [self.archive.get_blob(row['payload_hash']) for row in rows])
        with self.assertRaises(ValueError):
            self.archive.event_once(next_run, 'broker_fill', second, 'fill:f1')

    def test_nested_secrets_and_auth_errors_are_redacted(self):
        secret = 'SUPER_PRIVATE_TEST_SECRET'
        original = {'APCA-API-KEY-ID': secret, 'nested': [{'APCA_API_SECRET_KEY': secret,
                    'Authorization': 'Bearer ' + secret, 'password': secret, 'access_token': secret}],
                    'error': 'Authorization: Bearer ' + secret, 'symbol': 'AAPL'}
        run = self.archive.start_run('cycle', original, {'API_KEY': secret})
        self.archive.samples(run, [{'symbol': 'AAPL', 'features': original}])
        self.archive.event(run, 'error', original)
        self.archive.dataset(run, 'bars', original, original)
        self.archive.finish_run(run, 'failed', original, original)
        digest = self.archive.put_blob(original, 'hygiene')
        replay = self.archive.get_blob(digest)
        self.assertEqual(REDACTED, replay['APCA-API-KEY-ID'])
        self.assertEqual(secret, original['APCA-API-KEY-ID'])
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            text = ''.join(gzip.decompress(row[0]).decode('utf-8') for row in db.execute('SELECT payload FROM blobs'))
            text += ''.join(str(row) for row in db.execute('SELECT metadata_json FROM datasets'))
            text += ''.join(str(row) for row in db.execute('SELECT error_json FROM runs'))
        self.assertNotIn(secret, text)
        self.assertIn('AAPL', text)

    def test_does_not_read_credentials_or_unrelated_files(self):
        (self.root / 'credentials.json').write_text('{"secret":"hidden"}', encoding='utf-8')
        (self.root / 'AlpacaPaper.xml').write_text('private-encrypted-material', encoding='utf-8')
        (self.runtime / 'credentials.json').write_text('{"secret":"runtime-hidden"}', encoding='utf-8')
        run = self.start()
        source_hash = self.rows('SELECT source_hash FROM runs WHERE run_id=?', (run,))[0]['source_hash']
        manifest = self.archive.get_blob(source_hash)
        self.assertEqual({'settings.json', 'zerotrader/engine.py'}, {row['path'] for row in manifest['files']})

    def test_csv_preserves_unicode_quotes_and_nested_json(self):
        run = self.start()
        payload = {'reason': '未入选, "成交量不足"\n下一行', 'score': 1.23}
        self.archive.event(run, 'selection', payload)
        self.archive.samples(run, [{'symbol': '=FORMULA()', 'decision': payload['reason'], 'features': payload}])
        self.archive.finish_run(run, 'completed')
        exported = self.archive.export_csv(Path(self.temp.name) / 'export')
        path = Path(exported['files']['events'])
        self.assertTrue(path.read_bytes().startswith(b'\xef\xbb\xbf'))
        with path.open(encoding='utf-8-sig', newline='') as stream:
            rows = list(csv.DictReader(stream))
        selection = next(row for row in rows if row['kind'] == 'selection')
        self.assertEqual(payload, json.loads(selection['payload_json']))
        with Path(exported['files']['samples']).open(encoding='utf-8-sig', newline='') as stream:
            sample = next(csv.DictReader(stream))
        self.assertEqual("'=FORMULA()", sample['symbol'])
        self.assertEqual('=FORMULA()', json.loads(sample['sample_json'])['symbol'])
        self.assertEqual(1, exported['counts']['runs'])

    def test_bad_sample_rolls_back_entire_batch_including_blobs(self):
        run = self.start()
        before = self.archive.stats()
        with self.assertRaises(ValueError):
            self.archive.samples(run, [{'symbol': 'AAPL'}, {'symbol': 'MSFT', 'score': float('nan')}])
        after = self.archive.stats()
        self.assertEqual(before['samples'], after['samples'])
        self.assertEqual(before['blobs'], after['blobs'])
        with self.assertRaises(sqlite3.IntegrityError):
            self.archive.samples('nonexistent-run', [{'symbol': 'AAPL'}])
        self.assertEqual(before['blobs'], self.archive.stats()['blobs'])

    def test_same_timestamp_runs_have_unique_ids_and_keep_source_versions(self):
        at = '2026-10-01T14:00:00Z'
        first = self.start(at=at)
        (self.root / 'zerotrader' / 'engine.py').write_bytes(b'VERSION = 2\n')
        second = self.start(at=at)
        self.assertNotEqual(first, second)
        rows = self.rows('SELECT source_hash FROM runs ORDER BY rowid')
        self.assertNotEqual(rows[0]['source_hash'], rows[1]['source_hash'])
        for row, expected in zip(rows, (1, 2)):
            manifest = self.archive.get_blob(row['source_hash'])
            file = next(file for file in manifest['files'] if file['path'].endswith('engine.py'))
            self.assertEqual(f'VERSION = {expected}\n', self.archive.get_blob(file['blob_hash']))

    def test_corrupted_content_hash_fails_replay(self):
        digest = self.archive.put_blob({'test': True}, 'fixture')
        with closing(sqlite3.connect(self.archive.path)) as db, db:
            db.execute('DROP TRIGGER immutable_blobs_update')
            db.execute('UPDATE blobs SET payload=? WHERE sha256=?', (gzip.compress(b'{"test":false}'), digest))
        with self.assertRaises(ValueError):
            self.archive.get_blob(digest)

    def test_finalized_runs_cannot_silently_replace_result(self):
        run = self.start()
        self.archive.finish_run(run, 'completed', {'equity': 100000})
        before = self.archive.stats()
        with self.assertRaises(ValueError):
            self.archive.finish_run(run, 'failed', {'equity': 1})
        self.assertEqual(before['events'], self.archive.stats()['events'])
        self.assertEqual('completed', self.rows('SELECT status FROM runs')[0]['status'])

    def test_blob_stats_report_actual_compressed_storage(self):
        self.archive.put_blob({'rows': ['same sample ' * 100] * 100}, 'fixture')
        stats = self.archive.stats()
        self.assertGreater(stats['raw_blob_bytes'], stats['compressed_blob_bytes'])
        row = self.rows('SELECT SUM(length(payload)) AS bytes FROM blobs')[0]
        self.assertEqual(row['bytes'], stats['compressed_blob_bytes'])
        self.assertGreater(stats['database_bytes'], 0)


if __name__ == '__main__':
    unittest.main()
