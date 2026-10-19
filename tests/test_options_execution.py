"""Adversarial integration tests for the bounded PAPER options pilot.

All account, quote, broker and persistence responses are synthetic. These tests
make no API requests and do not prove executable prices or broker connectivity.
"""
from copy import deepcopy
from datetime import datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

from test_engine import FakeBroker
import test_engine
from test_engine_audit import FakeAudit
from test_cloud_engine import SyntheticDurableStore
from zerotrader.broker import BrokerError, OrderAmbiguous, OrderRejected
from zerotrader.cloud_engine import CloudDurabilityError, CloudEngine
from zerotrader.data import NY
from zerotrader.engine import Engine, new_state
from zerotrader.options_pilot import DEFAULT_POLICY, reservation_summary
from zerotrader.storage import read_json


class FakeOptionBroker(FakeBroker):
    def __init__(self):
        super().__init__()
        self.now = datetime(2026, 10, 15, 11, 0, tzinfo=NY)
        self.account_value.update(options_trading_level=2, options_buying_power='1000')
        self.option_submitted = []
        self.option_quote_calls = []
        self.dne_calls = []
        self.trace = []
        self.option_error = None
        self.dne_error = None
        self.option_quote_error = None
        self.option_quote_age = 0
        self.contract_expiry_days = 14
        self.before_option_submit = None
        self.before_dne = None

    def contract(self, underlying='AAPL'):
        expiry = self.now.date() + timedelta(days=self.contract_expiry_days)
        symbol = underlying + expiry.strftime('%y%m%d') + 'C00100000'
        return {'symbol': symbol, 'underlying_symbol': underlying,
                'expiration_date': expiry.isoformat(), 'type': 'call',
                'strike_price': '100', 'size': '100', 'status': 'active',
                'tradable': True, 'asset_class': 'us_option'}

    def option_contracts(self, underlying, expiration_from, expiration_to):
        contract = self.contract(underlying)
        if expiration_from <= contract['expiration_date'] <= expiration_to:
            return [deepcopy(contract)]
        return []

    def option_contract(self, symbol):
        underlying = symbol[:-15]
        result = self.contract(underlying)
        if result['symbol'] != symbol:
            raise BrokerError('Synthetic contract not found', status=404)
        return result

    def option_latest_quote(self, symbol, feed='indicative'):
        if feed != 'indicative':
            raise AssertionError('A test attempted a paid feed.')
        self.option_quote_calls.append(symbol)
        if self.option_quote_error:
            raise self.option_quote_error
        return {'bp': 2.0, 'ap': 2.1, 't': (self.now - timedelta(seconds=self.option_quote_age)).isoformat(),
                'feed': 'indicative'}

    def submit_option_order(self, payload):
        if self.before_option_submit:
            self.before_option_submit(payload)
        self.trace.append(('option_order', payload['side'], payload['symbol']))
        self.option_submitted.append(deepcopy(payload))
        if self.option_error:
            error, self.option_error = self.option_error, None
            self.submitted.append(deepcopy(payload))
            raise error
        row = super().submit_order(payload)
        self.records[row['id']]['asset_class'] = 'us_option'
        row['asset_class'] = 'us_option'
        return row

    def do_not_exercise(self, symbol):
        if self.before_dne:
            self.before_dne(symbol)
        self.dne_calls.append(symbol)
        self.trace.append(('dne', symbol))
        if self.dne_error:
            raise self.dne_error
        return None

    def options_lifecycle_activities(self, after=None, until=None):
        return []


class OptionFixture:
    cloud = False

    def enter_only(self, signal=None):
        from zerotrader import options_execution
        engine = self.engine()
        options_execution.initialize(engine)
        options_execution.enter(engine, signal or self.signals('AAPL'), self.broker.now)
        return engine

    def events(self, kind):
        return [data for _, event_kind, data in self.audit.events if event_kind == kind]

    def test_delayed_stock_submission_does_not_make_new_option_quote_future(self):
        original = self.broker.submit_order
        def delayed(payload):
            result = original(payload)
            if payload['symbol'] == 'AAPL':
                self.broker.now += timedelta(seconds=20)
            return result
        self.broker.submit_order = delayed
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertEqual(self.broker.option_submitted[0]['qty'], '1')
        self.assertEqual(self.broker.option_submitted[0]['limit_price'], '2.10')
        self.assertTrue(self.events('options_entry_clock'))
        self.assertFalse(self.events('options_entry_skipped'))

    def test_delayed_chain_and_option_quote_requests_refresh_candidate_clock(self):
        original = self.broker.option_contracts
        def delayed(*args):
            self.broker.now += timedelta(seconds=10)
            return original(*args)
        self.broker.option_contracts = delayed
        self.enter_only()
        self.assertEqual(len(self.broker.option_submitted), 1)
        clocks = self.events('options_entry_clock')
        self.assertNotEqual(clocks[0]['clock']['timestamp'], clocks[1]['clock']['timestamp'])

    def test_invalid_underlying_quote_is_archived_before_detailed_skip(self):
        for age in (91, -6):
            with self.subTest(age=age):
                self.broker.quote_age = age
                self.enter_only()
                error = self.events('options_entry_skipped')[-1]
                self.assertEqual(error['phase'], 'underlying_quote_validation')
                self.assertEqual(error['reason_type'], 'ValueError')
                self.assertIn('stale or from the future', error['reason'])
                self.assertFalse(error['new_order_sent'])
                self.assertEqual(self.events('options_quote_observed')[-1]['quote'],
                                 self.broker.latest_quote('AAPL'))
        self.assertEqual(self.broker.option_submitted, [])
        self.assertEqual(self.state['intents'], {})

    def test_missing_quote_timestamp_retains_raw_quote_and_key_error(self):
        self.broker.latest_quote = lambda symbol: {'bp': 100, 'ap': 100.01}
        self.enter_only()
        error = self.events('options_entry_skipped')[-1]
        self.assertEqual(error['reason_type'], 'KeyError')
        self.assertIn('t', error['reason'])
        self.assertNotIn('t', self.events('options_quote_observed')[-1]['quote'])
        self.assertEqual(self.broker.option_submitted, [])

    def test_invalid_option_quote_has_explicit_candidate_rejection(self):
        self.broker.option_quote_age = 91
        self.enter_only()
        decision = self.events('options_candidate_decision')[-1]
        self.assertEqual(decision['selected_count'], 0)
        self.assertIn('stale or from the future', decision['diagnostics'][0]['reason'])
        self.assertTrue(any(e['phase'] == 'candidate_option' for e in self.events('options_quote_observed')))
        self.assertEqual(self.broker.option_submitted, [])

    def test_expensive_candidate_is_logged_without_relaxing_240_cap(self):
        original = self.broker.option_latest_quote
        def expensive(*args, **kwargs):
            return dict(original(*args, **kwargs), bp=4.0, ap=4.1)
        self.broker.option_latest_quote = expensive
        self.enter_only()
        rejection = self.events('options_candidate_decision')[-1]['diagnostics'][0]
        self.assertEqual(rejection['reason'], 'premium_above_cap')
        self.assertEqual(rejection['cap'], 240)
        self.assertEqual(self.broker.option_submitted, [])

    def test_signal_expiring_during_quote_fetch_blocks_entry(self):
        signals = self.signals('AAPL')
        original = self.broker.latest_quote
        def delayed(symbol):
            self.broker.now += timedelta(seconds=900)
            return original(symbol)
        self.broker.latest_quote = delayed
        self.enter_only(signals)
        self.assertIn('signal expired', self.events('options_entry_skipped')[-1]['reason'])
        self.assertEqual(self.broker.option_submitted, [])

    def test_entry_window_ending_during_chain_fetch_blocks_entry(self):
        self.broker.now = self.broker.now.replace(hour=14, minute=29, second=55)
        original = self.broker.option_contracts
        def delayed(*args):
            self.broker.now += timedelta(seconds=6)
            return original(*args)
        self.broker.option_contracts = delayed
        self.enter_only()
        self.assertIn('window is closed', self.events('options_entry_skipped')[-1]['reason'])
        self.assertEqual(self.broker.option_submitted, [])

    def test_quote_evidence_failure_blocks_new_option_intent(self):
        self.audit.fail.add('options_quote_observed')
        engine = self.enter_only()
        self.assertFalse(engine.audit_available)
        self.assertEqual(self.state['intents'], {})
        self.assertEqual(self.broker.option_submitted, [])

    def test_candidate_quote_archival_delay_still_expires_old_underlying(self):
        def delay(run_id, kind, payload):
            if kind == 'options_quote_observed' and payload['phase'] == 'candidate_option':
                self.broker.now += timedelta(seconds=91)
        self.audit.observe = delay
        self.enter_only()
        self.assertEqual(self.broker.option_submitted, [])
        self.assertIn('stale or from the future', self.events('options_candidate_decision')[-1]['diagnostics'][0]['reason'])

    def test_pre_submit_quote_fetch_delay_does_not_use_old_clock(self):
        original = self.broker.option_latest_quote
        calls = 0
        def delayed(*args, **kwargs):
            nonlocal calls
            calls += 1
            if calls > 1:
                self.broker.now += timedelta(seconds=6)
            return original(*args, **kwargs)
        self.broker.option_latest_quote = delayed
        self.enter_only()
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertTrue(any(e['phase'] == 'pre_submit_option' for e in self.events('options_quote_observed')))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.directory = Path(self.tmp.name)
        self.config = json.loads((Path(__file__).resolve().parents[1] / 'settings.json').read_text())
        self.config['enabled'] = True
        self.config['intraday']['allocation'] = 49000
        self.config['options'] = dict(DEFAULT_POLICY, enabled=True)
        self.state = new_state()
        self.broker = FakeOptionBroker()
        self.state['account_id'] = self.broker.account_value['id']
        self.audit = FakeAudit()
        self.remote = SyntheticDurableStore()

    def tearDown(self):
        self.tmp.cleanup()

    def engine(self):
        if self.cloud:
            return CloudEngine(self.broker, self.config, self.state, self.directory,
                               checkpoint=self.remote.checkpoint, evidence_barrier=self.remote.evidence,
                               audit=self.audit, run_id='synthetic-options')
        return Engine(self.broker, self.config, self.state, self.directory,
                      audit=self.audit, run_id='synthetic-options')

    def signals(self, *symbols):
        return {'intraday': [{'symbol': symbol, 'signal_time': self.broker.now.isoformat(),
                              'reason': 'Synthetic current bullish breakout above VWAP', 'score': 1.2}
                             for symbol in symbols]}

    def seed_option(self, underlying='MSFT', *, status='filled', position=True,
                    dne_status=None, number=1):
        contract = self.broker.contract(underlying)
        symbol = contract['symbol']
        cid, order_id = f'zoh-options-seed-{number}', f'options-seed-{number}'
        payload = {'symbol': symbol, 'qty': '1', 'side': 'buy', 'type': 'limit',
                   'limit_price': '2.10', 'time_in_force': 'day',
                   'position_intent': 'buy_to_open', 'client_order_id': cid}
        metadata = {'asset_class': 'us_option', 'multiplier': 100, 'contract': contract}
        owner = {'sleeve': 'options', 'entry_cid': cid,
                 'entry_day': self.broker.now.date().isoformat(), **deepcopy(metadata)}
        if dne_status is not None:
            owner['dne_status'] = dne_status
        self.state['owners'][symbol] = owner
        self.state['intents'][cid] = {'payload': payload, 'sleeve': 'options', 'status': status,
                                     'order_id': order_id, 'created': self.broker.now.isoformat(), **deepcopy(metadata)}
        self.broker.records[order_id] = dict(payload, id=order_id, status=status, asset_class='us_option',
            filled_qty='1' if status == 'filled' else '0',
            filled_avg_price='2.10' if status == 'filled' else None,
            filled_at=self.broker.now.isoformat() if status == 'filled' else None)
        if position:
            self.broker.position_rows.append({'symbol': symbol, 'asset_class': 'us_option',
                'side': 'long', 'qty': '1', 'market_value': '210', 'cost_basis': '210', 'avg_entry_price': '2.10'})
        return symbol, cid

    def closed_option_trade(self, underlying, number):
        symbol, cid = self.seed_option(underlying, position=False, number=number)
        del self.state['owners'][symbol]
        order_id = self.state['intents'][cid]['order_id']
        exit_cid, exit_order = f'zoh-options-close-{number}', f'options-close-{number}'
        payload = {'symbol': symbol, 'qty': '1', 'side': 'sell', 'type': 'market',
                   'time_in_force': 'day', 'position_intent': 'sell_to_close', 'client_order_id': exit_cid}
        self.state['intents'][exit_cid] = {'payload': payload, 'sleeve': 'options', 'status': 'filled',
            'order_id': exit_order, 'created': self.broker.now.isoformat(), 'exit_for': cid}
        self.state['fills'][order_id] = {'symbol': symbol, 'side': 'buy', 'qty': 1, 'price': 2.1,
                                         'sleeve': 'options', 'multiplier': 100}
        self.state['fills'][exit_order] = {'symbol': symbol, 'side': 'sell', 'qty': 1, 'price': 4,
                                         'sleeve': 'options', 'multiplier': 100}

    def assert_bad_response_not_adopted(self, changes):
        original = self.broker.submit_option_order

        def corrupt_response(payload):
            row = original(payload)
            row.update(changes)
            self.broker.records[row['id']].update(changes)
            return row

        self.broker.submit_option_order = corrupt_response
        snapshot = self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        intent = next(row for row in self.state['intents'].values() if row['sleeve'] == 'options')
        self.assertEqual(intent['status'], 'uncertain')
        self.assertNotIn('order_id', intent)
        self.assertEqual(intent['payload']['qty'], '1')
        self.assertTrue(snapshot['paused'])
        self.assertEqual(self.state['fills'], {})
        self.assertFalse(any(event['event'] == 'order_submitted' and event.get('sleeve') == 'options'
                             for event in self.state['events']))
        self.state = deepcopy(self.remote.state) if self.cloud else read_json(self.directory / 'state.json')
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertTrue(self.state['paused'])


class OptionExecutionTests(OptionFixture, unittest.TestCase):
    def test_wrong_client_id_option_response_stays_uncertain_and_is_not_retried(self):
        self.assert_bad_response_not_adopted({'client_order_id': 'zoh-another-owner'})

    def test_two_contract_option_response_stays_uncertain_and_is_not_retried(self):
        self.assert_bad_response_not_adopted({'qty': '2'})

    def test_reconcile_does_not_accept_wrong_identity_or_quantity(self):
        symbol, cid = self.seed_option(status='new', position=False)
        original = deepcopy(self.state['intents'][cid])
        broker_order = self.broker.records[original['order_id']]
        broker_order.update(qty='2', filled_qty='2', filled_avg_price='2.10', status='filled',
                            client_order_id='zoh-unrelated-order')
        snapshot = self.engine().cycle({})
        self.assertEqual(self.state['intents'][cid], original)
        self.assertEqual(self.state['owners'][symbol]['entry_cid'], cid)
        self.assertEqual(self.state['fills'], {})
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.dne_calls, [])
        self.assertTrue(snapshot['paused'])
        self.assertTrue(any(kind == 'option_order_identity_mismatch' for _, kind, _ in self.audit.events))

    def test_current_stock_signal_can_create_both_stock_and_affordable_call_intents(self):
        signal = self.signals('AAPL')
        before = deepcopy(signal)
        self.engine().cycle(signal)
        self.assertEqual(signal, before)
        self.assertEqual(len(self.broker.option_submitted), 1)
        option = self.broker.option_submitted[0]
        self.assertEqual(option['symbol'], self.broker.contract('AAPL')['symbol'])
        self.assertEqual(option['qty'], '1')
        self.assertEqual(option['position_intent'], 'buy_to_open')
        self.assertEqual(option['limit_price'], '2.10')
        stocks = [p for p in self.broker.submitted if p['symbol'] == 'AAPL']
        self.assertEqual(len(stocks), 1)
        self.assertEqual(stocks[0]['order_class'], 'oto')
        intents = [row for _, kind, row in self.audit.events if kind == 'order_intent' and row['sleeve'] == 'options']
        self.assertEqual(intents[0]['context']['signal'], signal['intraday'][0])
        self.assertEqual(intents[0]['context']['feed'], 'indicative')
        self.assertEqual(intents[0]['context']['multiplier'], 100)

    def test_pending_option_premium_reserves_100_multiplier_before_stock_allocation(self):
        self.seed_option(status='new', position=False)
        self.broker.account_value['cash'] = '2240'
        self.engine().cycle({'swing': [{'symbol': 'AAPL', 'score': 1.2}]})
        self.assertEqual(self.broker.submitted, [])
        ledger = self.state['strategies']['options']['risk_ledger']['orders']
        self.assertEqual(next(iter(ledger.values()))['premium_reserved'], '210.00')

    def test_four_lifetime_contracts_remain_exhausted_after_profitable_sales(self):
        for number, symbol in enumerate(('MSFT', 'NVDA', 'AMZN', 'META'), 1):
            self.closed_option_trade(symbol, number)
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(self.broker.option_submitted, [])
        result = reservation_summary(self.state, [], self.config)
        self.assertEqual(result['lifetime_contracts_reserved'], 4)
        self.assertEqual(result['lifetime_premium_reserved'], '840.00')
        self.assertEqual(result['active_contracts'], 0)
        self.assertFalse(result['sale_proceeds_reusable'])
        self.assertIn('lifetime_contract_allowance_exhausted', result['entry_block_reasons'])

    def test_two_pending_contracts_block_a_third_option_but_not_stock_strategy(self):
        self.seed_option('MSFT', status='new', position=False, number=1)
        self.seed_option('NVDA', status='new', position=False, number=2)
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(self.broker.option_submitted, [])
        self.assertEqual(len([row for row in self.broker.submitted if row['symbol'] == 'AAPL']), 1)
        self.assertEqual(reservation_summary(self.state, [], self.config)['active_contracts'], 2)

    def test_filled_order_without_visible_position_keeps_owner_and_occupied_slot(self):
        symbol, cid = self.seed_option('MSFT', status='filled', position=False, number=1)
        self.seed_option('NVDA', status='new', position=False, number=2)
        self.engine().cycle(self.signals('AAPL'))
        self.assertIn(symbol, self.state['owners'])
        self.assertEqual(self.state['owners'][symbol]['entry_cid'], cid)
        self.assertEqual(self.state['intents'][cid]['status'], 'filled')
        self.assertEqual(reservation_summary(self.state, [], self.config)['active_contracts'], 2)
        self.assertEqual(self.broker.option_submitted, [])
        self.assertEqual(self.broker.dne_calls, [])

    def test_upward_fill_price_correction_is_not_released_by_later_lower_price(self):
        symbol, cid = self.seed_option('MSFT', dne_status='acknowledged')
        order_id = self.state['intents'][cid]['order_id']
        self.broker.records[order_id]['filled_avg_price'] = '2.50'
        self.engine().cycle(self.signals('AAPL'))
        ledger = self.state['strategies']['options']['risk_ledger']['orders']
        self.assertEqual(ledger[cid]['premium_reserved'], '250.00')
        self.assertEqual(self.broker.option_submitted, [])
        self.broker.records[order_id]['filled_avg_price'] = '2.00'
        self.engine().cycle(self.signals('NVDA'))
        ledger = self.state['strategies']['options']['risk_ledger']['orders']
        self.assertEqual(ledger[cid]['premium_reserved'], '250.00')
        self.assertEqual(self.state['fills'][order_id]['price'], 2.0)
        self.assertEqual(self.broker.option_submitted, [])
        result = reservation_summary(self.state, [dict(row, multiplier=100) for row in self.broker.positions()], self.config)
        self.assertFalse(result['can_enter'])
        self.assertIn('per_order_contract_or_premium_limit_exceeded', result['entry_block_reasons'])

    def test_normal_fourteen_dte_option_needs_no_dne_or_equity_stop_to_remain_open(self):
        symbol, _ = self.seed_option()
        snapshot = self.engine().cycle({})
        self.assertEqual(self.broker.dne_calls, [])
        self.assertNotIn('dne_status', self.state['owners'][symbol])
        self.assertEqual(self.broker.option_submitted, [])
        self.assertEqual(self.broker.canceled, [])
        self.assertFalse(snapshot['paused'])
        self.assertFalse(any('exercise protection unverified' in alert for alert in snapshot['alerts']))

    def test_expiry_day_residual_persists_dne_before_ack_and_immediate_close(self):
        self.broker.contract_expiry_days = 0
        symbol, cid = self.seed_option()

        def check_dne(value):
            saved = read_json(self.directory / 'state.json')
            self.assertEqual(value, symbol)
            self.assertEqual(saved['owners'][symbol]['dne_status'], 'submitting')

        def check_close(payload):
            saved = read_json(self.directory / 'state.json')
            self.assertEqual(saved['owners'][symbol]['dne_status'], 'acknowledged')
            self.assertEqual(saved['intents'][payload['client_order_id']]['exit_for'], cid)

        self.broker.before_dne, self.broker.before_option_submit = check_dne, check_close
        self.engine().cycle({})
        self.assertEqual(self.broker.trace[0], ('dne', symbol))
        self.assertEqual(self.broker.trace[1], ('option_order', 'sell', symbol))
        self.assertEqual(self.broker.option_submitted[0]['type'], 'market')
        self.assertEqual(self.broker.option_quote_calls, [])

    def test_ambiguous_dne_closes_once_without_repeating_dne_after_restart(self):
        self.broker.contract_expiry_days = 0
        symbol, _ = self.seed_option()
        self.broker.dne_error = OrderAmbiguous('Synthetic DNE transport outage')
        snapshot = self.engine().cycle({})
        self.assertEqual(self.broker.dne_calls, [symbol])
        self.assertEqual(self.state['owners'][symbol]['dne_status'], 'uncertain')
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertEqual(self.broker.option_submitted[0]['side'], 'sell')
        self.assertTrue(any('exercise protection unverified' in alert for alert in snapshot['alerts']))
        self.state = read_json(self.directory / 'state.json')
        self.engine().cycle({})
        self.assertEqual(self.broker.dne_calls, [symbol])
        self.assertEqual(len(self.broker.option_submitted), 1)

    def test_time_exit_survives_paused_entries_disabled_and_unavailable_quote(self):
        symbol, _ = self.seed_option()
        self.state['paused'] = True
        self.config['options']['enabled'] = False
        self.broker.option_quote_error = BrokerError('Synthetic indicative outage')
        self.broker.now = self.broker.now.replace(hour=15, minute=40)
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        payload = self.broker.option_submitted[0]
        self.assertEqual((payload['symbol'], payload['side'], payload['type']), (symbol, 'sell', 'market'))
        self.assertEqual(self.broker.dne_calls, [])
        self.assertTrue(self.state['paused'])

    def test_scheduled_exit_ignores_bad_cost_basis_quote_trigger_on_first_of_two_options(self):
        first, _ = self.seed_option('MSFT', number=1)
        second, _ = self.seed_option('NVDA', number=2)
        self.broker.position_rows[0]['avg_entry_price'] = 'NaN'
        self.broker.now = self.broker.now.replace(hour=15, minute=40)
        self.engine().cycle({})
        exits = self.broker.option_submitted
        self.assertEqual({row['symbol'] for row in exits}, {first, second})
        self.assertTrue(all(row['side'] == 'sell' and row['type'] == 'market' and row['qty'] == '1'
                            for row in exits))
        self.assertEqual(self.broker.option_quote_calls, [])

    def test_expired_quote_during_local_archive_marks_entry_not_submitted(self):
        def delay(run_id, kind, payload):
            if kind == 'order_intent' and payload['sleeve'] == 'options':
                self.broker.now += timedelta(seconds=91)

        self.audit.observe = delay
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(self.broker.option_submitted, [])
        intents = [row for row in self.state['intents'].values() if row['sleeve'] == 'options']
        self.assertEqual(len(intents), 1)
        self.assertEqual(intents[0]['status'], 'not_submitted')

    def test_broker_rejection_is_not_recorded_as_an_actual_fill(self):
        self.broker.option_error = OrderRejected('Synthetic order rejection', status=422)
        self.engine().cycle(self.signals('AAPL'))
        intents = [row for row in self.state['intents'].values() if row['sleeve'] == 'options']
        self.assertEqual(intents[0]['status'], 'rejected')
        self.assertEqual(self.state['fills'], {})
        self.assertFalse(any(kind in {'fill_observed', 'broker_fill_activity'} for _, kind, _ in self.audit.events))
        self.assertTrue(any(kind == 'order_rejected' for _, kind, _ in self.audit.events))

    def test_unknown_option_is_not_adopted_liquidated_or_given_dne(self):
        symbol = self.broker.contract('MSFT')['symbol']
        self.broker.position_rows = [{'symbol': symbol, 'asset_class': 'us_option', 'qty': '1',
            'side': 'long', 'cost_basis': '210', 'market_value': '210', 'avg_entry_price': '2.10'}]
        snapshot = self.engine().cycle(self.signals('AAPL'))
        self.assertTrue(any('manual reconciliation' in message for message in snapshot['alerts']))
        self.assertNotIn(symbol, self.state['owners'])
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.dne_calls, [])

    def test_unknown_option_cannot_prevent_owned_stock_drawdown_exit(self):
        test_engine.EngineTests.owned(self)
        symbol = self.broker.contract('MSFT')['symbol']
        self.broker.position_rows.append({'symbol': symbol, 'asset_class': 'us_option', 'qty': '1',
            'side': 'long', 'cost_basis': '210', 'market_value': '210', 'avg_entry_price': '2.10'})
        self.broker.account_value['equity'] = '90000'
        snapshot = self.engine().cycle({})
        self.assertTrue(snapshot['halted'])
        self.assertNotIn(symbol, self.state['owners'])
        self.assertEqual(self.broker.option_submitted, [])
        self.assertEqual(self.broker.dne_calls, [])
        self.assertEqual(len(self.broker.submitted), 1)
        self.assertEqual((self.broker.submitted[0]['symbol'], self.broker.submitted[0]['side']), ('AAPL', 'sell'))
        self.assertIn('seed-stop', self.broker.canceled)

    def test_preview_never_initializes_pilot_or_dispatches_options(self):
        self.engine().cycle(self.signals('AAPL'), execute=False)
        self.assertNotIn('options', self.state['strategies'])
        self.assertEqual(self.state['intents'], {})
        self.assertEqual(self.broker.submitted, [])
        self.assertEqual(self.broker.dne_calls, [])


class CloudOptionExecutionTests(OptionFixture, unittest.TestCase):
    cloud = True

    def test_wrong_client_id_option_response_stays_uncertain_after_remote_restore(self):
        self.assert_bad_response_not_adopted({'client_order_id': 'zoh-another-owner'})

    def test_two_contract_option_response_stays_uncertain_after_remote_restore(self):
        self.assert_bad_response_not_adopted({'qty': '2'})

    def test_confirmed_context_and_two_option_evidence_barriers_precede_entry(self):
        option_barriers = []

        def evidence(state):
            if any(row['sleeve'] == 'options' for row in state['intents'].values()):
                option_barriers.append(deepcopy(state))

        def before_post(payload):
            intent = self.remote.state['intents'][payload['client_order_id']]
            self.assertEqual(intent['status'], 'submitting')
            self.assertEqual(intent['payload'], payload)
            self.assertEqual(intent['multiplier'], 100)
            self.assertEqual(len(option_barriers), 2)
            events = [row for _, kind, row in self.audit.events if kind == 'order_intent' and row['sleeve'] == 'options']
            self.assertEqual(events[-1]['context']['quote']['feed'], 'indicative')
            self.assertEqual(events[-1]['context']['contract']['symbol'], payload['symbol'])

        self.remote.archive_before = evidence
        self.broker.before_option_submit = before_post
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)

    def test_second_option_archive_barrier_expiring_quote_prevents_post(self):
        option_barriers = 0

        def delay(state):
            nonlocal option_barriers
            if any(row['sleeve'] == 'options' for row in state['intents'].values()):
                option_barriers += 1
                if option_barriers == 2:
                    self.broker.now += timedelta(seconds=91)

        self.remote.archive_before = delay
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(option_barriers, 2)
        self.assertEqual(self.broker.option_submitted, [])
        intent = next(row for row in self.remote.state['intents'].values() if row['sleeve'] == 'options')
        self.assertEqual(intent['status'], 'not_submitted')
        self.assertEqual(intent['reason'], 'fresh_pre_submit_check_failed')

    def test_remote_archive_failure_blocks_option_entry_without_broker_write(self):
        def fail(state):
            if any(row['sleeve'] == 'options' for row in state['intents'].values()):
                self.remote.archive_failed = True

        self.remote.archive_before = fail
        self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(self.broker.option_submitted, [])
        intent = next(row for row in self.remote.state['intents'].values() if row['sleeve'] == 'options')
        self.assertEqual(intent['status'], 'not_submitted')

    def test_global_drawdown_exit_has_remote_lineage_before_market_close_post(self):
        symbol, cid = self.seed_option()
        self.broker.account_value['equity'] = '90000'

        def inspect(payload):
            intent = self.remote.state['intents'][payload['client_order_id']]
            self.assertEqual(intent['status'], 'submitting')
            self.assertEqual(intent['exit_for'], cid)
            self.assertEqual(intent['sleeve'], 'options')
            self.assertEqual(intent['payload']['qty'], '1')
            self.assertTrue(any(state['intents'].get(payload['client_order_id'], {}).get('status') == 'exit_prepared'
                                for state in self.remote.commits))

        self.broker.before_option_submit = inspect
        snapshot = self.engine().cycle({})
        self.assertTrue(snapshot['halted'])
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertEqual(self.broker.option_submitted[0]['symbol'], symbol)
        self.assertEqual(self.broker.option_submitted[0]['position_intent'], 'sell_to_close')
        self.assertEqual(self.broker.option_submitted[0]['type'], 'market')

    def test_core_failure_before_dne_intent_confirmation_prevents_dne(self):
        self.broker.contract_expiry_days = 0
        self.seed_option()

        def fail(state):
            if any(owner.get('dne_status') == 'submitting' for owner in state['owners'].values()):
                raise OSError('Synthetic remote checkpoint unavailable')

        self.remote.before = fail
        with self.assertRaises(CloudDurabilityError):
            self.engine().cycle({})
        self.assertEqual(self.broker.dne_calls, [])
        self.assertEqual(self.broker.option_submitted, [])

    def test_ambiguous_option_entry_is_not_resubmitted_after_remote_restore(self):
        self.broker.option_error = OrderAmbiguous('Synthetic transport loss')
        with self.assertRaises(CloudDurabilityError):
            self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.state = deepcopy(self.remote.state)
        snapshot = self.engine().cycle(self.signals('AAPL'))
        self.assertEqual(len(self.broker.option_submitted), 1)
        self.assertTrue(any('Unresolved submission' in alert for alert in snapshot['alerts']))


if __name__ == '__main__':
    unittest.main()
