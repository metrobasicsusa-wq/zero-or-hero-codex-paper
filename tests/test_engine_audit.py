"""Archive faults must stop new exposure without disabling owned risk exits."""
import copy
from datetime import timedelta
import json
from pathlib import Path
import tempfile
import unittest

from test_engine import FakeBroker
from zerotrader.broker import OrderAmbiguous
from zerotrader.engine import Engine, new_state
from zerotrader.storage import read_json


class FakeAudit:
    def __init__(self, fail=(), observe=None):
        self.fail = set(fail)
        self.attempts = []
        self.events = []
        self.observe = observe

    def event(self, run_id, kind, payload):
        self.attempts.append(kind)
        if self.observe:
            self.observe(run_id, kind, payload)
        if '*' in self.fail or kind in self.fail:
            raise OSError('Simulated archive disk failure')
        self.events.append((run_id, kind, copy.deepcopy(payload)))


class EngineAuditTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.config = json.loads((Path(__file__).resolve().parents[1] / 'settings.json').read_text())
        self.config['enabled'] = True
        self.state = new_state()
        self.broker = FakeBroker()

    def tearDown(self):
        self.tmp.cleanup()

    def engine(self, audit):
        return Engine(self.broker, self.config, self.state, self.directory, audit=audit, run_id='research-run')

    def signals(self, symbols=('AAPL', 'MSFT')):
        return {'swing': [{'symbol': symbol, 'score': 1.2} for symbol in symbols]}

    def owned(self, sleeve='swing', protective='new'):
        cid = 'zoh-seed-entry'
        payload = {'symbol': 'AAPL', 'qty': '10', 'side': 'buy', 'type': 'limit',
                   'time_in_force': 'gtc', 'order_class': 'oto', 'limit_price': '100',
                   'stop_loss': {'stop_price': '95'}, 'client_order_id': cid}
        self.state['owners']['AAPL'] = {'sleeve': sleeve, 'entry_cid': cid,
                                        'entry_day': self.broker.now.date().isoformat()}
        self.state['intents'][cid] = {'payload': payload, 'sleeve': sleeve, 'status': 'filled',
                                     'order_id': 'seed-entry', 'created': (self.broker.now - timedelta(hours=1)).isoformat()}
        self.broker.records['seed-entry'] = dict(payload, id='seed-entry', status='filled',
                                                filled_qty='10', filled_avg_price='100')
        if protective is not None:
            self.broker.records['seed-stop'] = {'id': 'seed-stop', 'parent_id': 'seed-entry',
                 'symbol': 'AAPL', 'side': 'sell', 'type': 'stop', 'status': protective,
                 'qty': '10', 'filled_qty': '0', 'filled_avg_price': None, 'stop_price': '95'}
        self.broker.position_rows = [{'symbol': 'AAPL', 'qty': '10', 'side': 'long',
                                     'market_value': '1000', 'cost_basis': '1000'}]
        self.broker.account_value['cash'] = '99000'

    def test_successful_audit_precedes_network_buy_and_contains_decision_evidence(self):
        def observe(run_id, kind, payload):
            if kind != 'order_intent':
                return
            self.assertEqual(run_id, 'research-run')
            self.assertEqual(self.broker.submitted, [])
            persisted = read_json(self.directory / 'state.json')
            self.assertEqual(persisted['intents'][payload['client_order_id']]['status'], 'submitting')
            self.assertEqual(payload['context']['signal']['symbol'], 'AAPL')
            self.assertEqual(payload['context']['quote']['ap'], 100.01)
            self.assertGreater(payload['context']['quantity'], 0)

        audit = FakeAudit(observe=observe)
        snapshot = self.engine(audit).cycle(self.signals(('AAPL',)))
        self.assertEqual(len(self.broker.submitted), 1)
        kinds = [kind for _, kind, _ in audit.events]
        self.assertLess(kinds.index('quote_observed'), kinds.index('order_intent'))
        self.assertLess(kinds.index('order_intent'), kinds.index('order_response'))
        self.assertEqual(snapshot['alerts'], [])

    def test_snapshot_and_quote_archive_failures_block_buys_without_intents(self):
        for event in ('broker_snapshot', 'quote_observed'):
            with self.subTest(event=event):
                self.state = new_state()
                audit = FakeAudit(fail=[event])
                engine = self.engine(audit)
                snapshot = engine.cycle(self.signals())
                self.assertEqual(self.broker.submitted, [])
                self.assertEqual(self.state['intents'], {})
                self.assertFalse(engine.audit_available)
                self.assertTrue(any('archive write failed' in alert for alert in snapshot['alerts']))

    def test_intent_archive_failure_is_terminal_without_broker_write_or_blind_retry(self):
        audit = FakeAudit(fail=['order_intent'])
        engine = self.engine(audit)
        engine.cycle(self.signals())
        saved = read_json(self.directory / 'state.json')
        self.assertEqual(len(saved['intents']), 1)
        intent = next(iter(saved['intents'].values()))
        self.assertEqual(intent['status'], 'not_submitted')
        self.assertEqual(intent['reason'], 'archive_failure_before_submit')
        self.assertEqual(self.broker.submitted, [])
        self.state = saved
        self.engine(FakeAudit()).cycle(self.signals(('AAPL',)))
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.state['owners'], {})

    def test_response_archive_failure_preserves_broker_acceptance_and_blocks_next_buy(self):
        audit = FakeAudit(fail=['order_response'])
        engine = self.engine(audit)
        engine.cycle(self.signals())
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]['symbol'], 'AAPL')
        intent = next(iter(self.state['intents'].values()))
        self.assertEqual(intent['status'], 'new')
        self.assertEqual(intent['order_id'], 'order-1')
        self.assertIn('order-1', self.state['known_order_ids'])
        self.assertFalse(engine.audit_available)
        self.engine(FakeAudit()).cycle(self.signals(('AAPL',)))
        self.assertEqual(len(self.broker.submitted), 1)

    def test_audit_failure_stays_latched_when_subsequent_event_writes_recover(self):
        audit = FakeAudit(fail=['quote_observed'])
        engine = self.engine(audit)
        engine.cycle(self.signals())
        audit.fail.clear()
        self.assertTrue(engine.audit_event('recovered_event', {}))
        self.assertFalse(engine.audit_available)
        engine.cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])

    def test_total_archive_outage_does_not_disable_drawdown_exit(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        snapshot = self.engine(FakeAudit(fail=['*'])).cycle(self.signals())
        self.assertTrue(snapshot['halted'])
        self.assertEqual(self.broker.canceled, ['seed-stop'])
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual(self.broker.submitted[0]['side'], 'sell')
        self.assertEqual(self.broker.submitted[0]['position_intent'], 'sell_to_close')
        self.assertEqual(self.broker.submitted[0]['qty'], '10')
        saved = read_json(self.directory / 'state.json')
        exits = [intent for intent in saved['intents'].values() if intent['payload']['side'] == 'sell']
        self.assertEqual(exits[0]['exit_for'], 'zoh-seed-entry')
        self.assertEqual(exits[0]['status'], 'new')

    def test_archive_failure_does_not_bypass_cancel_confirmation(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.cancels_complete = False
        snapshot = self.engine(FakeAudit(fail=['*'])).cycle(self.signals())
        self.assertEqual(self.broker.canceled, ['seed-stop'])
        self.assertEqual(self.broker.submitted, [])
        self.assertTrue(any('Waiting for cancel confirmation' in alert for alert in snapshot['alerts']))

    def test_archive_failure_preserves_unprotected_and_intraday_close_exits(self):
        for reason in ('unprotected', 'intraday_close'):
            with self.subTest(reason=reason):
                self.state = new_state()
                self.broker = FakeBroker()
                self.owned(sleeve='intraday' if reason == 'intraday_close' else 'swing',
                           protective='new' if reason == 'intraday_close' else None)
                if reason == 'intraday_close':
                    self.broker.now = self.broker.now.replace(hour=15, minute=45)
                self.engine(FakeAudit(fail=['*'])).cycle(self.signals())
                self.assertEqual(len(self.broker.submitted), 1)
                self.assertEqual(self.broker.submitted[0]['side'], 'sell')

    def test_archive_failure_never_allows_touching_unowned_orders(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        self.broker.records['manual-order'] = {'id': 'manual-order', 'symbol': 'AAPL',
             'side': 'buy', 'qty': '2', 'filled_qty': '0', 'status': 'new', 'type': 'limit', 'limit_price': '80'}
        self.engine(FakeAudit(fail=['*'])).cycle(self.signals())
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.canceled, [])

    def test_failed_uncertainty_audit_keeps_persisted_intent_and_no_blind_retry(self):
        self.broker.next_error = OrderAmbiguous('unknown outcome')
        with self.assertRaises(RuntimeError):
            self.engine(FakeAudit(fail=['order_uncertain'])).cycle(self.signals(('AAPL',)))
        saved = read_json(self.directory / 'state.json')
        self.assertEqual(next(iter(saved['intents'].values()))['status'], 'uncertain')
        self.state = saved
        self.engine(FakeAudit()).cycle(self.signals(('AAPL',)))
        self.assertEqual(len(self.broker.submitted), 1)

    def test_successful_fill_audit_uses_quantity_and_notional_deltas_without_poll_duplicates(self):
        self.owned()
        self.broker.records['seed-entry'].update(status='partially_filled', filled_qty='4', filled_avg_price='100')
        audit = FakeAudit()
        engine = self.engine(audit)
        engine.reconcile()
        engine.reconcile()
        self.broker.records['seed-entry'].update(status='partially_filled', filled_qty='6', filled_avg_price='101')
        engine.reconcile()
        fills = [payload for _, kind, payload in audit.events if kind == 'fill_observed']
        self.assertEqual(len(fills), 2)
        self.assertEqual(fills[0]['delta_qty'], 4)
        self.assertEqual(fills[0]['delta_notional'], 400)
        self.assertEqual(fills[1]['delta_qty'], 2)
        self.assertEqual(fills[1]['delta_notional'], 206)
        self.assertEqual(fills[1]['source'], 'cumulative_order_poll')
        observations = [payload for _, kind, payload in audit.events if kind == 'order_observed']
        self.assertEqual(len(observations), 2)

    def test_dry_run_with_archive_failure_never_cancels_or_submits(self):
        self.owned()
        self.broker.account_value['equity'] = '90000'
        snapshot = self.engine(FakeAudit(fail=['*'])).cycle(self.signals(), execute=False)
        self.assertTrue(snapshot['halted'])
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.canceled, [])


if __name__ == '__main__':
    unittest.main()
