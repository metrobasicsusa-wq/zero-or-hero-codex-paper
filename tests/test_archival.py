"""Recovery tests for legacy import and incremental brokerage fill archival."""
from contextlib import closing
import copy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from zerotrader.archival import bootstrap, capture_fills
from zerotrader.archive import ResearchArchive
from zerotrader.storage import read_json, write_json


class ArchivalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name) / 'runtime'
        self.directory.mkdir()
        application = Path(self.temp.name) / 'application'
        application.mkdir()
        root_patch = patch('zerotrader.archive.ROOT', application)
        root_patch.start()
        self.addCleanup(root_patch.stop)
        self.archive = ResearchArchive(self.directory)
        self.broker = Mock()
        self.broker.fill_activities.return_value = []
        self.state = {'fills': {}, 'intents': {}}
        self.now = datetime(2026, 10, 1, 15, 0, tzinfo=timezone.utc)
        self.cursor_path = self.directory / 'fill-archive-cursor.json'
        self.marker_path = self.directory / 'archive-backfill.json'

    def run_id(self):
        return self.archive.start_run('test', {'mode': 'paper'}, self.state)

    def fill(self, activity_id='fill-1', qty='2', price='100.00'):
        return {'id': activity_id, 'order_id': 'order-1', 'activity_type': 'FILL',
                'transaction_time': self.now.isoformat(), 'symbol': 'AAPL',
                'side': 'buy', 'qty': qty, 'price': price}

    def events(self, kind):
        with closing(sqlite3.connect(self.archive.path)) as db:
            hashes = [row[0] for row in db.execute(
                'SELECT payload_hash FROM events WHERE kind=? ORDER BY sequence', (kind,))]
        return [self.archive.get_blob(digest) for digest in hashes]

    def test_cursor_advances_only_after_every_fill_and_attribution_are_durable(self):
        previous = self.now - timedelta(hours=1)
        write_json(self.cursor_path, {'last_successful_poll': previous.isoformat(), 'overlap_days': 1})
        self.broker.fill_activities.return_value = [self.fill(), self.fill('fill-2', qty='3')]
        real_event = self.archive.event_once

        def failing_event(run_id, kind, payload, key, at=None):
            if kind == 'broker_fill_attribution' and payload['activity_id'] == 'fill-2':
                raise OSError('simulated unavailable archive disk')
            return real_event(run_id, kind, payload, key, at=at)

        with patch.object(self.archive, 'event_once', side_effect=failing_event):
            with self.assertRaises(OSError):
                capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.assertEqual(previous.isoformat(), read_json(self.cursor_path)['last_successful_poll'])
        self.assertEqual(2, len(self.events('broker_fill_activity')))
        self.assertEqual(1, len(self.events('broker_fill_attribution')))
        self.assertEqual(2, capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now))
        self.assertEqual(self.now.isoformat(), read_json(self.cursor_path)['last_successful_poll'])
        self.assertEqual(2, len(self.events('broker_fill_activity')))
        self.assertEqual(2, len(self.events('broker_fill_attribution')))
        self.assertEqual((previous - timedelta(days=1)).isoformat(), self.broker.fill_activities.call_args.kwargs['after'])

    def test_first_poll_failure_does_not_create_cursor(self):
        self.broker.fill_activities.side_effect = RuntimeError('broker history unavailable')
        with self.assertRaises(RuntimeError):
            capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.assertFalse(self.cursor_path.exists())
        self.assertEqual([], self.events('broker_fill_activity'))

    def test_overlap_deduplicates_raw_fills_and_keeps_attribution_changes_separate(self):
        fill = self.fill()
        original = copy.deepcopy(fill)
        self.broker.fill_activities.return_value = [fill]
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.state['intents']['cid'] = {'order_id': 'order-1', 'sleeve': 'swing'}
        later = self.now + timedelta(minutes=1)
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, later)
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, later)
        raw = self.events('broker_fill_activity')
        attribution = self.events('broker_fill_attribution')
        self.assertEqual(1, len(raw))
        self.assertEqual(original, raw[0]['activity'])
        self.assertNotIn('sleeve', raw[0])
        self.assertEqual([None, 'swing'], [row['sleeve'] for row in attribution])
        self.assertTrue(all(row['activity_hash'] == raw[0]['activity_hash'] for row in attribution))
        self.assertEqual(original, fill)

    def test_corrected_fill_keeps_both_raw_versions_under_one_activity_identity(self):
        first, corrected = self.fill(), self.fill(price='100.05')
        self.broker.fill_activities.return_value = [first]
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.broker.fill_activities.return_value = [corrected]
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now + timedelta(minutes=1))
        raw = self.events('broker_fill_activity')
        self.assertEqual([first, corrected], [row['activity'] for row in raw])
        self.assertEqual({'fill-1'}, {row['activity_id'] for row in raw})
        self.assertNotEqual(raw[0]['activity_hash'], raw[1]['activity_hash'])

    def test_malformed_later_fill_cannot_advance_cursor(self):
        self.broker.fill_activities.return_value = [self.fill(), {'order_id': 'bad'}]
        with self.assertRaises(ValueError):
            capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.assertFalse(self.cursor_path.exists())
        self.broker.fill_activities.return_value = [self.fill()]
        capture_fills(self.archive, self.run_id(), self.broker, self.directory, self.state, self.now)
        self.assertEqual(1, len(self.events('broker_fill_activity')))

    def test_bootstrap_retry_preserves_changed_files_and_probe_receipts(self):
        write_json(self.directory / 'state.json', {'version': 1})
        history_path = self.directory / 'research-history' / '20261001.json'
        write_json(history_path, {'score': .1})
        write_json(self.directory / 'paper-probe.json', {'order_id': 'probe-1'})
        self.broker.get_order.return_value = {'id': 'probe-1', 'status': 'new'}
        real_write = write_json

        def fail_marker(path, value):
            if path == self.marker_path:
                raise OSError('simulated crash before bootstrap marker')
            return real_write(path, value)

        with patch('zerotrader.archival.write_json', side_effect=fail_marker):
            with self.assertRaises(OSError):
                bootstrap(self.archive, self.run_id(), self.directory, self.broker)
        self.assertFalse(self.marker_path.exists())
        write_json(self.directory / 'state.json', {'version': 2})
        write_json(history_path, {'score': .2})
        self.broker.get_order.return_value = {'id': 'probe-1', 'status': 'canceled'}
        bootstrap(self.archive, self.run_id(), self.directory, self.broker)
        self.assertTrue(self.marker_path.exists())
        states = [row['data'] for row in self.events('legacy_import') if row['source_file'] == 'state.json']
        self.assertEqual([{'version': 1}, {'version': 2}], states)
        self.assertEqual([.1, .2], [row['data']['score'] for row in self.events('legacy_research')])
        self.assertEqual(['new', 'canceled'], [row['order']['status'] for row in self.events('integration_probe_order')])
        probes = [row for row in self.events('legacy_import') if row['source_file'] == 'paper-probe.json']
        self.assertEqual(1, len(probes))
        self.assertTrue(all(not row['is_strategy_trade'] for row in self.events('integration_probe_order')))
        event_count = self.archive.stats()['events']
        bootstrap(self.archive, 'unused-after-marker', self.directory, self.broker)
        self.assertEqual(event_count, self.archive.stats()['events'])


if __name__ == '__main__':
    unittest.main()
