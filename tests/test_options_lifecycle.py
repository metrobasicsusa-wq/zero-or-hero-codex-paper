"""Synthetic option NTA evidence tests; no broker network or order mutations."""
from contextlib import closing
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch

from zerotrader.archive import ResearchArchive
from zerotrader.archival import capture_fills
from zerotrader.storage import read_json, write_json


SYMBOL = 'SPY261106C00650000'


class OptionsLifecycleTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name) / 'runtime'
        self.directory.mkdir()
        application = Path(temporary.name) / 'source'
        application.mkdir()
        source = patch('zerotrader.archive.ROOT', application)
        source.start()
        self.addCleanup(source.stop)
        self.archive = ResearchArchive(self.directory)
        self.broker = Mock()
        self.broker.fill_activities.return_value = []
        self.broker.options_lifecycle_activities.return_value = []
        self.now = datetime(2026, 11, 6, 21, 15, tzinfo=timezone.utc)
        self.state = {'paused': False, 'halted': None, 'equity_peak': 103000,
            'owners': {SYMBOL: {'sleeve': 'options', 'entry_cid': 'zoh-synthetic'}},
            'intents': {'zoh-synthetic': {'sleeve': 'options', 'order_id': 'synthetic-order',
                'payload': {'symbol': SYMBOL, 'side': 'buy'}, 'contract': {'symbol': SYMBOL}}},
            'fills': {}, 'strategies': {'options': {'risk_ledger': {'version': 1, 'orders': {}}}}, 'events': []}
        self.cursor = self.directory / 'fill-archive-cursor.json'

    def activity(self, kind='OPEXP', identity='synthetic-lifecycle', **extra):
        return dict({'id': identity, 'activity_type': kind, 'date': '2026-11-06',
                     'symbol': SYMBOL, 'qty': '-1', 'description': 'synthetic lifecycle evidence'}, **extra)

    def collect(self, now=None):
        run_id = self.archive.start_run('synthetic', {'mode': 'paper'}, self.state['strategies'])
        return capture_fills(self.archive, run_id, self.broker, self.directory, self.state, now or self.now)

    def events(self, kind):
        with closing(sqlite3.connect(self.archive.path)) as db:
            hashes = [row[0] for row in db.execute('SELECT payload_hash FROM events WHERE kind=? ORDER BY sequence', (kind,))]
        return [self.archive.get_blob(value) for value in hashes]

    def test_stock_only_state_does_not_query_option_history(self):
        self.state['strategies'] = {}
        self.collect()
        self.broker.options_lifecycle_activities.assert_not_called()
        self.assertNotIn('option_lifecycle_last_successful_poll', read_json(self.cursor))
        self.assertFalse(self.state['paused'])

    def test_normal_option_fill_remains_a_fill_and_does_not_pause(self):
        self.broker.fill_activities.return_value = [{'id': 'synthetic-fill', 'order_id': 'synthetic-order',
            'activity_type': 'FILL', 'symbol': SYMBOL, 'qty': '1', 'price': '2.00', 'side': 'buy',
            'transaction_time': self.now.isoformat()}]
        self.assertEqual(self.collect(), 1)
        self.assertEqual(len(self.events('broker_fill_activity')), 1)
        self.assertEqual(self.events('broker_option_lifecycle_activity'), [])
        self.assertFalse(self.state['paused'])

    def test_all_lifecycle_types_are_separate_from_fills_and_latch_manual_review(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity(kind, 'synthetic-' + kind)
            for kind in ('OPEXC', 'OPTRD', 'OPASN', 'OPEXP')]
        before = deepcopy(self.state)
        self.assertEqual(self.collect(), 0)
        self.assertTrue(self.state['paused'])
        raw = self.events('broker_option_lifecycle_activity')
        self.assertEqual(len(raw), 4)
        self.assertTrue(all(value['is_fill'] is False for value in raw))
        self.assertEqual(self.events('broker_fill_activity'), [])
        self.assertEqual(len(self.events('options_lifecycle_manual_review')), 4)
        for field in ('owners', 'intents', 'fills', 'halted', 'equity_peak'):
            self.assertEqual(self.state[field], before[field])
        self.assertFalse((self.directory / 'state.json').exists())
        self.broker.submit_order.assert_not_called()
        self.broker.submit_option_order.assert_not_called()
        self.broker.cancel_order.assert_not_called()
        self.broker.do_not_exercise.assert_not_called()

    def test_unknown_underlying_stock_activity_never_creates_an_owner_or_fill(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity('OPTRD', symbol='SPY', qty='100')]
        self.collect()
        self.assertTrue(self.state['paused'])
        self.assertNotIn('SPY', self.state['owners'])
        attribution = self.events('broker_option_lifecycle_attribution')[0]
        self.assertFalse(attribution['attributed_to_options_pilot'])
        self.assertFalse(attribution['underlying_stock_adopted'])
        self.assertFalse(attribution['inventory_change_independently_reconciled'])
        self.assertEqual(self.state['fills'], {})

    def test_known_contract_attribution_does_not_suppress_required_review(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity()]
        self.collect()
        attribution = self.events('broker_option_lifecycle_attribution')[0]
        self.assertEqual(attribution['entry_client_order_ids'], ['zoh-synthetic'])
        self.assertTrue(attribution['attributed_to_options_pilot'])
        self.assertTrue(self.state['paused'])

    def test_overlap_deduplicates_and_new_content_preserves_correction_versions(self):
        first = self.activity()
        self.broker.options_lifecycle_activities.return_value = [first, deepcopy(first)]
        self.collect()
        self.collect(self.now + timedelta(minutes=1))
        self.assertEqual(len(self.events('broker_option_lifecycle_activity')), 1)
        self.assertEqual(len(self.state['events']), 1)
        corrected = self.activity(qty='-2', description='synthetic correction requiring new review')
        self.broker.options_lifecycle_activities.return_value = [corrected]
        self.collect(self.now + timedelta(minutes=2))
        raw = self.events('broker_option_lifecycle_activity')
        self.assertEqual([value['activity'] for value in raw], [first, corrected])
        self.assertEqual({value['activity_id'] for value in raw}, {first['id']})
        self.assertNotEqual(raw[0]['activity_hash'], raw[1]['activity_hash'])
        self.assertEqual(len(self.state['events']), 2)
        self.assertEqual(len(self.state['strategies']['options']['lifecycle_reviews']), 2)

    def test_repeated_old_version_does_not_repause_a_separately_reviewed_account(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity()]
        self.collect()
        self.state['paused'] = False  # Simulate an explicit separate human review/resume.
        self.collect(self.now + timedelta(minutes=1))
        self.assertFalse(self.state['paused'])
        self.assertEqual(len(self.state['events']), 1)

    def test_poll_uses_two_day_overlap_from_prior_success_not_current_wall_time(self):
        previous = self.now - timedelta(days=3)
        write_json(self.cursor, {'last_successful_poll': previous.isoformat(), 'overlap_days': 1,
            'option_lifecycle_last_successful_poll': previous.isoformat(), 'retained_marker': 'synthetic'})
        self.collect()
        self.assertEqual(self.broker.options_lifecycle_activities.call_args.kwargs,
            {'after': (previous - timedelta(days=2)).isoformat(), 'until': self.now.isoformat()})
        cursor = read_json(self.cursor)
        self.assertEqual(cursor['option_lifecycle_overlap_days'], 2)
        self.assertEqual(cursor['option_lifecycle_last_successful_poll'], self.now.isoformat())
        self.assertEqual(cursor['retained_marker'], 'synthetic')

    def test_first_poll_uses_two_day_window(self):
        self.collect()
        self.assertEqual(self.broker.options_lifecycle_activities.call_args.kwargs['after'],
                         (self.now - timedelta(days=2)).isoformat())

    def test_network_failure_does_not_advance_either_cursor(self):
        previous = {'last_successful_poll': (self.now - timedelta(hours=1)).isoformat(),
                    'option_lifecycle_last_successful_poll': (self.now - timedelta(hours=1)).isoformat()}
        write_json(self.cursor, previous)
        self.broker.options_lifecycle_activities.side_effect = RuntimeError('synthetic read unavailable')
        with self.assertRaises(RuntimeError):
            self.collect()
        self.assertEqual(read_json(self.cursor), previous)

    def test_diagnostic_failure_leaves_pause_for_callers_finally_save_and_retries(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity()]
        original = self.archive.event_once
        def fail_review(run_id, kind, payload, key, at=None):
            if kind == 'options_lifecycle_manual_review':
                raise OSError('synthetic disk unavailable')
            return original(run_id, kind, payload, key, at=at)
        with patch.object(self.archive, 'event_once', side_effect=fail_review):
            with self.assertRaises(OSError):
                self.collect()
        self.assertTrue(self.state['paused'])
        self.assertFalse(self.cursor.exists())
        self.assertEqual(len(self.events('broker_option_lifecycle_activity')), 1)
        self.assertEqual(self.state['strategies']['options']['lifecycle_reviews'], {})
        self.collect(self.now + timedelta(minutes=1))
        self.assertEqual(len(self.events('broker_option_lifecycle_activity')), 1)
        self.assertEqual(len(self.state['events']), 1)

    def test_later_invalid_row_does_not_lose_first_review_or_advance_cursor(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity(), {'id': 'invalid'}]
        with self.assertRaises(ValueError):
            self.collect()
        self.assertTrue(self.state['paused'])
        self.assertFalse(self.cursor.exists())
        self.assertEqual(len(self.events('broker_option_lifecycle_activity')), 1)

    def test_cursor_write_failure_keeps_archived_review_and_pause_retryable(self):
        self.broker.options_lifecycle_activities.return_value = [self.activity()]
        with patch('zerotrader.archival.write_json', side_effect=OSError('synthetic cursor write failure')):
            with self.assertRaises(OSError):
                self.collect()
        self.assertTrue(self.state['paused'])
        self.assertFalse(self.cursor.exists())
        self.collect(self.now + timedelta(minutes=1))
        self.assertEqual(len(self.state['events']), 1)
        self.assertEqual(len(self.events('options_lifecycle_manual_review')), 1)

    def test_future_or_naive_lifecycle_cursor_is_rejected_without_advancement(self):
        for value in ((self.now + timedelta(minutes=1)).isoformat(), '2026-11-06T20:00:00'):
            with self.subTest(value=value):
                write_json(self.cursor, {'option_lifecycle_last_successful_poll': value})
                with self.assertRaises(ValueError):
                    self.collect()
                self.assertEqual(read_json(self.cursor)['option_lifecycle_last_successful_poll'], value)

    def test_invalid_lifecycle_type_or_naive_datetime_cannot_be_called_actual_fill(self):
        for activity in (self.activity('FILL'), self.activity(date='2026-11-06T15:00:00')):
            with self.subTest(activity=activity):
                self.broker.options_lifecycle_activities.return_value = [activity]
                with self.assertRaises(ValueError):
                    self.collect()
                self.assertEqual(self.events('broker_fill_activity'), [])
                self.assertFalse(self.cursor.exists())


if __name__ == '__main__':
    unittest.main()
