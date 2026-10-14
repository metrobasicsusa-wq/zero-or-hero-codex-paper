"""Adversarial execution tests. Fake broker only; no network or credentials."""
import copy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from zerotrader.broker import OrderAmbiguous, OrderRejected
from zerotrader.data import NY
from zerotrader.engine import Engine, new_state
from zerotrader.risk import validated_quote
from zerotrader.storage import read_json


TERMINAL = {'filled', 'canceled', 'expired', 'rejected', 'replaced'}


class FakeBroker:
    def __init__(self):
        self.now = datetime(2026, 10, 1, 11, 0, tzinfo=NY)
        self.account_value = {'id': 'paper-account', 'status': 'ACTIVE', 'equity': '100000',
                              'cash': '100000', 'trading_blocked': False, 'account_blocked': False}
        self.position_rows = []
        self.records = {}
        self.submitted = []
        self.canceled = []
        self.next_error = None
        self.open = True
        self.quote_age = 0
        self.cancels_complete = True

    def account(self):
        return copy.deepcopy(self.account_value)

    def clock(self):
        return {'timestamp': self.now.isoformat(), 'is_open': self.open,
                'next_close': self.now.replace(hour=16, minute=0).isoformat(),
                'next_open': (self.now + timedelta(days=1)).replace(hour=9, minute=30).isoformat()}

    def positions(self):
        return copy.deepcopy(self.position_rows)

    def orders(self, status='open'):
        return [copy.deepcopy(r) for r in self.records.values() if r['status'] not in TERMINAL]

    def get_order(self, order_id):
        row = copy.deepcopy(self.records[order_id])
        if row['side'] == 'buy':
            row['legs'] = [copy.deepcopy(r) for r in self.records.values() if r.get('parent_id') == order_id]
        return row

    def order_by_client_id(self, cid):
        row = next((r for r in self.records.values() if r.get('client_order_id') == cid), None)
        return self.get_order(row['id']) if row else None

    def asset(self, symbol):
        return {'tradable': True, 'status': 'active', 'class': 'us_equity'}

    def latest_quote(self, symbol):
        return {'bp': 100.0, 'ap': 100.01, 't': (self.now - timedelta(seconds=self.quote_age)).isoformat()}

    def submit_order(self, payload):
        self.submitted.append(copy.deepcopy(payload))
        if self.next_error is not None:
            error, self.next_error = self.next_error, None
            raise error
        row = dict(payload, id='order-' + str(len(self.submitted)), status='new',
                   filled_qty='0', filled_avg_price=None, legs=[])
        self.records[row['id']] = row
        return copy.deepcopy(row)

    def cancel_order(self, order_id):
        self.canceled.append(order_id)
        if self.cancels_complete:
            self.records[order_id]['status'] = 'canceled'
            for row in self.records.values():
                if row.get('parent_id') == order_id:
                    row['status'] = 'canceled'
        else:
            self.records[order_id]['status'] = 'pending_cancel'


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.config = json.loads((Path(__file__).resolve().parents[1] / 'settings.json').read_text())
        self.config['enabled'] = True
        self.state = new_state()
        self.broker = FakeBroker()

    def tearDown(self):
        self.tmp.cleanup()

    def engine(self):
        return Engine(self.broker, self.config, self.state, self.directory)

    def signals(self, swing=('AAPL',), intraday=()):
        return {sleeve: [{'symbol': symbol, 'signal_time': self.broker.now.isoformat()}
                        for symbol in symbols] for sleeve, symbols in [('swing', swing), ('intraday', intraday)]}

    def owned(self, *, sleeve='swing', status='filled', filled=10, protective='new'):
        cid = 'zoh-seed-entry'
        payload = {'symbol': 'AAPL', 'qty': '10', 'side': 'buy', 'type': 'limit',
                   'time_in_force': 'gtc', 'order_class': 'oto', 'limit_price': '100',
                   'stop_loss': {'stop_price': '95'}, 'client_order_id': cid}
        self.state['owners']['AAPL'] = {'sleeve': sleeve, 'entry_cid': cid,
                                        'entry_day': self.broker.now.date().isoformat()}
        self.state['intents'][cid] = {'payload': payload, 'sleeve': sleeve, 'status': status,
                                     'order_id': 'seed-entry', 'created': (self.broker.now - timedelta(hours=1)).isoformat()}
        self.broker.records['seed-entry'] = dict(payload, id='seed-entry', status=status,
                                                filled_qty=str(filled), filled_avg_price='100')
        if protective is not None:
            self.broker.records['seed-stop'] = {'id': 'seed-stop', 'parent_id': 'seed-entry',
                 'symbol': 'AAPL', 'side': 'sell', 'type': 'stop', 'status': protective,
                 'qty': str(filled), 'filled_qty': '0', 'filled_avg_price': None, 'stop_price': '95'}
        self.broker.position_rows = [{'symbol': 'AAPL', 'qty': str(filled), 'side': 'long',
                                     'market_value': str(100 * filled), 'cost_basis': str(100 * filled)}]
        self.broker.account_value['cash'] = str(100000 - 100 * filled)

    def test_ambiguous_submit_persists_and_never_blindly_retries(self):
        self.broker.next_error = OrderAmbiguous('unknown')
        with self.assertRaises(RuntimeError):
            self.engine().cycle(self.signals())
        saved = read_json(self.directory / 'state.json')
        self.assertEqual(next(iter(saved['intents'].values()))['status'], 'uncertain')
        self.state = saved
        result = self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertTrue(any('Unresolved submission' in x for x in result['alerts']))

    def test_crash_after_broker_acceptance_reconciles_instead_of_duplicating(self):
        self.engine().cycle(self.signals())
        intent = next(iter(self.state['intents'].values()))
        intent.pop('order_id')
        intent['status'] = 'submitting'
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(intent['status'], 'new')

    def test_both_sleeves_reserve_cash_and_do_not_overlap(self):
        self.engine().cycle(self.signals(('AAPL', 'MSFT', 'NVDA', 'AMZN', 'GOOGL'),
                                         ('AAPL', 'META', 'TSLA', 'AMD')))
        costs = {'swing': 0, 'intraday': 0}
        symbols = set()
        for row in self.broker.submitted:
            self.assertNotIn(row['symbol'], symbols)
            symbols.add(row['symbol'])
            sleeve = self.state['owners'][row['symbol']]['sleeve']
            costs[sleeve] += int(row['qty']) * float(row['limit_price'])
        self.assertLessEqual(costs['swing'], 50000)
        self.assertLessEqual(costs['intraday'], 50000)
        self.assertLessEqual(sum(costs.values()), 98000)
        self.assertGreater(costs['swing'], 0)
        self.assertGreater(costs['intraday'], 0)

    def test_pending_cancel_prevents_market_exit(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.cancels_complete = False
        result = self.engine().cycle(self.signals())
        self.assertTrue(result['halted'])
        self.assertEqual(self.broker.submitted, [])
        self.assertIn('seed-stop', self.broker.canceled)

    def test_drawdown_cancels_protection_then_sends_one_long_exit(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        result = self.engine().cycle(self.signals())
        self.assertTrue(result['halted'])
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]['side'], 'sell')
        self.assertEqual(self.broker.submitted[0]['qty'], '10')
        self.assertEqual(self.broker.canceled, ['seed-stop'])
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)

    def test_rejected_risk_exit_must_be_retried_after_reconciliation(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.next_error = OrderRejected('temporarily cannot sell', status=422)
        self.engine().cycle(self.signals())
        self.broker.now += timedelta(minutes=1)
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 2,
                         'Definitively rejected exits must not become stranded behind their deterministic CID.')
        self.assertNotEqual(self.broker.submitted[0]['client_order_id'], self.broker.submitted[1]['client_order_id'])

    def test_timeout_partial_entry_must_not_leave_unprotected_holding(self):
        self.owned(status='partially_filled', filled=4, protective='held')
        self.broker.next_error = OrderRejected('temporarily cannot sell', status=422)
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.records['seed-entry']['status'], 'canceled')
        self.broker.now += timedelta(minutes=1)
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 2,
                         'After parent cancellation, remaining shares need an exit even though entry is terminal.')

    def test_missing_stop_requires_owned_holding_recovery(self):
        self.owned(protective='canceled')
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1,
                         'A filled owned position whose protective stop disappeared must not remain unprotected.')
        self.assertEqual(self.broker.submitted[0]['side'], 'sell')

    def test_partial_entry_exits_immediately_without_waiting_for_timeout(self):
        self.owned(status='partially_filled', filled=4, protective='held')
        self.state['intents']['zoh-seed-entry']['created'] = self.broker.now.isoformat()
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]['qty'], '4')
        self.assertEqual(self.broker.submitted[0]['side'], 'sell')

    def test_uncertain_exit_never_retries_until_reconciled(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.next_error = OrderAmbiguous('unknown outcome')
        with self.assertRaises(RuntimeError):
            self.engine().cycle(self.signals())
        self.broker.now += timedelta(minutes=1)
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)

    def test_canceled_partially_filled_exit_retries_only_remaining_shares(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.engine().cycle(self.signals())
        self.broker.records['order-1'].update(status='canceled', filled_qty='4', filled_avg_price='99')
        self.broker.position_rows[0].update(qty='6', market_value='600', cost_basis='600')
        self.broker.now += timedelta(minutes=1)
        self.engine().cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 2)
        self.assertEqual(self.broker.submitted[-1]['qty'], '6')
        self.assertEqual(self.state['fills']['order-1']['qty'], 4)

    def test_done_for_day_gtc_order_must_still_be_canceled_on_risk_halt(self):
        self.owned(status='done_for_day', filled=0, protective=None)
        self.broker.position_rows = []
        self.broker.account_value['equity'] = '90000'
        self.engine().cycle(self.signals())
        self.assertIn('seed-entry', self.broker.canceled,
                      'done_for_day may resume next session and is not final cancellation.')

    def test_replaced_lineage_pauses_entries_for_manual_reconciliation(self):
        self.owned(status='replaced')
        result = self.engine().cycle(self.signals(('AAPL', 'MSFT')))
        self.assertTrue(self.state['paused'])
        self.assertTrue(any('lineage' in alert for alert in result['alerts']))
        self.assertEqual(self.broker.submitted, [])

    def test_untracked_order_on_owned_symbol_is_not_canceled_or_sold_around(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.records['manual-order'] = {'id': 'manual-order', 'symbol': 'AAPL',
             'side': 'buy', 'qty': '2', 'filled_qty': '0', 'status': 'new', 'type': 'limit', 'limit_price': '80'}
        result = self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.canceled, [])
        self.assertTrue(any('manual reconciliation' in alert for alert in result['alerts']))

    def test_stale_quote_prevents_entry(self):
        self.broker.quote_age = self.config['max_quote_age_seconds'] + 1
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_stale_future_or_missing_intraday_signal_never_enters(self):
        for signal_time in ((self.broker.now - timedelta(minutes=15)).isoformat(),
                            (self.broker.now + timedelta(seconds=1)).isoformat(), ''):
            with self.subTest(signal_time=signal_time):
                self.engine().cycle({'intraday': [{'symbol': 'AAPL', 'signal_time': signal_time}]})
                self.assertEqual(self.broker.submitted, [])

    def test_signal_expiring_during_asset_lookup_never_enters(self):
        signals = self.signals((), ('AAPL',))
        original = self.broker.asset
        def slow_asset(symbol):
            self.broker.now += timedelta(minutes=16)
            return original(symbol)
        self.broker.asset = slow_asset
        self.engine().cycle(signals)
        self.assertEqual(self.broker.submitted, [])

    def test_market_closing_during_asset_lookup_never_enters(self):
        original = self.broker.asset
        def delayed_asset(symbol):
            self.broker.open = False
            return original(symbol)
        self.broker.asset = delayed_asset
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_closed_market_never_submits(self):
        self.broker.open = False
        self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_dry_run_never_mutates_broker(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.engine().cycle(self.signals(), execute=False)
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.canceled, [])

    def test_unattributed_position_blocks_entries(self):
        self.broker.position_rows = [{'symbol': 'XOM', 'qty': '3', 'market_value': '300', 'cost_basis': '300'}]
        result = self.engine().cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertTrue(result['alerts'])

    def test_quote_nan_and_future_time_rejected(self):
        for quote in ({'bp': float('nan'), 'ap': 100, 't': self.broker.now.isoformat()},
                      {'bp': 100, 'ap': 100, 't': (self.broker.now + timedelta(seconds=10)).isoformat()}):
            with self.assertRaises(ValueError):
                validated_quote(quote, self.broker.now, self.config)


if __name__ == '__main__':
    unittest.main()
