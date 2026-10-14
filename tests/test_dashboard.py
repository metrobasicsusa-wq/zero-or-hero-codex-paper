import gzip
import json
import sqlite3
import unittest
from datetime import datetime
from scripts.build_dashboard import evidence


class DashboardEvidenceTests(unittest.TestCase):
    def test_fill_corrections_and_unattributed_activity_are_not_double_counted(self):
        db = sqlite3.connect(':memory:')
        db.executescript('CREATE TABLE events(sequence INTEGER,at TEXT,kind TEXT,payload_hash TEXT); CREATE TABLE blobs(sha256 TEXT,encoding TEXT,payload BLOB);')
        seq = 0
        def add(kind, item):
            nonlocal seq
            seq += 1
            db.execute('INSERT INTO events VALUES(?,?,?,?)', (seq, '2026-10-01T14:00:00+00:00', kind, str(seq)))
            db.execute('INSERT INTO blobs VALUES(?,?,?)', (str(seq), 'gzip', gzip.compress(json.dumps(item).encode())))
        fill = {'transaction_time': '2026-10-01T13:50:00+00:00', 'symbol': 'ABC', 'side': 'buy', 'qty': '2', 'price': '10', 'order_id': 'private-order'}
        add('broker_fill_activity', {'activity_id': 'private-fill', 'activity': fill})
        add('broker_fill_attribution', {'activity_id': 'private-fill', 'sleeve': 'swing'})
        add('broker_fill_activity', {'activity_id': 'private-fill', 'activity': {**fill, 'price': '11'}})
        add('broker_fill_activity', {'activity_id': 'test-fill', 'activity': fill})
        stop = {'id': 'private-stop', 'symbol': 'ABC', 'side': 'sell', 'type': 'stop', 'status': 'new', 'qty': '2', 'filled_qty': '0', 'stop_price': '9', 'time_in_force': 'gtc'}
        add('broker_snapshot', {'orders': [stop, stop], 'clock': {'timestamp': '2026-10-01T14:00:00+00:00'}})
        result = evidence(db, datetime.fromisoformat('2026-09-30T21:13:00+00:00'), datetime.fromisoformat('2026-10-01T15:00:00+00:00'))
        self.assertEqual(len(result['trades']), 1)
        self.assertEqual(result['trades'][0]['price'], '11')
        self.assertEqual(len(result['stops']), 1)
        self.assertNotIn('private-', json.dumps(result))
        self.assertNotIn('order_id', json.dumps(result))
        db.close()
