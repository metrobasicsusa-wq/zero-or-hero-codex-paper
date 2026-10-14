"""Deterministic execution; learning may change signals, never account/risk rules."""
from datetime import datetime, timedelta
import hashlib
import math
from .broker import BrokerError, OrderAmbiguous, OrderRejected
from .data import NY, timestamp
from .risk import drawdown, entry_size, validated_quote, validate_config
from .storage import write_json

TERMINAL = {'filled', 'canceled', 'expired', 'rejected', 'replaced', 'not_submitted'}


def flatten(orders):
    for order in orders:
        yield order
        yield from flatten(order.get('legs') or [])


def new_state():
    return {'version': 1, 'account_id': None, 'equity_peak': 100000., 'halted': None,
            'paused': False, 'owners': {}, 'intents': {}, 'fills': {}, 'strategies': {},
            'last_research': None, 'last_cycle': None, 'events': []}


class Engine:
    def __init__(self, broker, config, state, directory, audit=None, run_id=None):
        validate_config(config)
        self.broker, self.config, self.state, self.directory = broker, config, state, directory
        self.actions, self.alerts = [], []
        self.audit, self.run_id, self.audit_available = audit, run_id, True

    def audit_event(self, kind, payload):
        if self.audit is None:
            return self.audit_available
        try:
            self.audit.event(self.run_id, kind, payload)
            return True
        except Exception:
            self.audit_available = False
            message = 'Research archive write failed; new entries blocked, risk exits remain enabled.'
            if message not in self.alerts:
                self.alerts.append(message)
            return False

    def save(self):
        write_json(self.directory / 'state.json', self.state)

    def record(self, event):
        self.audit_event(event['event'], event)
        self.actions.append(event)
        self.state['events'] = (self.state['events'] + [event])[-500:]
        self.save()

    def reconcile(self):
        # Include stop children after the parent entry fills, until its position is gone.
        for cid, intent in self.state['intents'].items():
            needs_children = intent['payload']['side'] == 'buy' and self.state['owners'].get(intent['payload']['symbol'], {}).get('entry_cid') == cid
            if intent.get('status') in TERMINAL and not needs_children:
                continue
            order = self.broker.get_order(intent['order_id']) if intent.get('order_id') else self.broker.order_by_client_id(cid)
            if order is None:
                if intent['status'] in ('submitting', 'uncertain'):
                    self.alerts.append('Unresolved submission ' + cid + '; new entries blocked. No automatic resubmission.')
                continue
            intent.update(order_id=order['id'], status=order['status'])
            digest = hashlib.sha256(repr(sorted(order.items())).encode()).hexdigest()
            if intent.get('last_order_observation') != digest:
                if self.audit_event('order_observed', {'client_order_id': cid, 'sleeve': intent['sleeve'], 'broker_order': order}):
                    intent['last_order_observation'] = digest
            if order['status'] == 'replaced':
                self.alerts.append('Replaced order needs lineage reconciliation: ' + cid)
                self.state['paused'] = True
            for leg in flatten([order]):
                known = self.state.setdefault('known_order_ids', [])
                if leg['id'] not in known:
                    known.append(leg['id'])
                quantity = float(leg.get('filled_qty') or 0)
                price = float(leg.get('filled_avg_price') or 0)
                if quantity > 0 and price > 0:
                    previous_fill = self.state['fills'].get(leg['id'], {})
                    old_quantity = previous_fill.get('qty', 0)
                    old_price = previous_fill.get('price', 0)
                    self.state['fills'][leg['id']] = {'symbol': leg['symbol'], 'side': leg['side'], 'qty': quantity, 'price': price, 'sleeve': intent['sleeve'], 'filled_at': leg.get('filled_at')}
                    if quantity != old_quantity or price != old_price:
                        self.record({'at': leg.get('filled_at'), 'event': 'fill_observed', 'source': 'cumulative_order_poll', 'symbol': leg['symbol'], 'sleeve': intent['sleeve'], 'side': leg['side'], 'filled_qty': quantity, 'filled_avg_price': price, 'delta_qty': quantity - old_quantity, 'delta_notional': quantity * price - old_quantity * old_price, 'order_id': leg['id']})
        self.save()

    def submit(self, payload, sleeve, now):
        cid = payload['client_order_id']
        if cid in self.state['intents']:
            return False
        if payload['side'] == 'buy' and not self.audit_available:
            return False
        # WAL before network write prevents a process crash from duplicating this order.
        self.state['intents'][cid] = {'payload': payload, 'sleeve': sleeve, 'status': 'submitting', 'created': now.isoformat()}
        if payload['side'] == 'sell':
            self.state['intents'][cid]['exit_for'] = getattr(self, '_exit_for', None)
        if payload['side'] == 'buy':
            self.state['owners'][payload['symbol']] = {'sleeve': sleeve, 'entry_cid': cid, 'entry_day': now.date().isoformat()}
        self.save()
        audited = self.audit_event('order_intent', {'client_order_id': cid, 'sleeve': sleeve, 'payload': payload, 'strategy': self.state['strategies'].get(sleeve), 'context': getattr(self, '_entry_context', None) if payload['side'] == 'buy' else {'exit_for': getattr(self, '_exit_for', None)}})
        if not audited and payload['side'] == 'buy':
            self.state['intents'][cid].update(status='not_submitted', reason='archive_failure_before_submit')
            self.save()
            return False
        try:
            order = self.broker.submit_order(payload)
            self.audit_event('order_response', {'client_order_id': cid, 'sleeve': sleeve, 'broker_order': order})
            self.state['intents'][cid].update(order_id=order['id'], status=order['status'])
            self.state.setdefault('known_order_ids', []).extend(o['id'] for o in flatten([order]) if o['id'] not in self.state.get('known_order_ids', []))
            self.record({'at': now.isoformat(), 'event': 'order_submitted', 'sleeve': sleeve, 'symbol': payload['symbol'], 'side': payload['side'], 'qty': payload['qty'], 'order_id': order['id']})
            return True
        except OrderRejected as exc:
            self.audit_event('order_rejected', {'client_order_id': cid, 'status': exc.status, 'message': str(exc)})
            self.state['intents'][cid]['status'] = 'rejected'
            self.alerts.append(str(exc))
            self.save()
            return False
        except (OrderAmbiguous, BrokerError):
            self.audit_event('order_uncertain', {'client_order_id': cid, 'next_action': 'reconcile_without_resubmitting'})
            self.state['intents'][cid]['status'] = 'uncertain'
            self.save()
            raise RuntimeError('Paper order outcome is uncertain. Persisted intent will be reconciled; no blind retry.') from None

    def exit_position(self, symbol, owner, now, reason):
        owner['exit_reason'] = owner.get('exit_reason') or reason
        self.save()
        previous = [v for v in self.state['intents'].values() if v.get('exit_for') == owner['entry_cid']]
        if any(v['status'] not in TERMINAL for v in previous):
            return False
        # Confirm removal of protective/open orders before sending sell-to-close.
        orders = [o for o in flatten(self.broker.orders()) if o['symbol'] == symbol and o['status'] not in TERMINAL]
        known = set(self.state.get('known_order_ids', [])) | {v.get('order_id') for v in self.state['intents'].values()}
        if any(o['id'] not in known for o in orders):
            self.alerts.append('Untracked order on owned symbol ' + symbol + '; manual reconciliation required before exit.')
            return False
        existing_exits = {v.get('order_id') for v in self.state['intents'].values() if v['payload']['side'] == 'sell'}
        if any(o['id'] in existing_exits for o in orders):
            return False
        for order in orders:
            try:
                self.audit_event('cancel_requested', {'order_id': order['id'], 'symbol': symbol, 'reason': reason})
                self.broker.cancel_order(order['id'])
                self.audit_event('cancel_acknowledged', {'order_id': order['id'], 'symbol': symbol, 'terminal_status_confirmed': False})
            except BrokerError as exc:
                self.audit_event('cancel_error', {'order_id': order['id'], 'symbol': symbol, 'http_status': exc.status})
                if exc.status not in (404, 422):
                    self.alerts.append('Cancellation not confirmed for ' + symbol + '; exit deferred.')
                    return False
        remaining = [o for o in flatten(self.broker.orders()) if o['symbol'] == symbol and o['status'] not in TERMINAL]
        if remaining:
            self.alerts.append('Waiting for cancel confirmation: ' + symbol)
            return False
        self.audit_event('cancel_confirmed', {'symbol': symbol, 'open_orders_remaining': 0, 'reason': reason})
        positions = {p['symbol']: p for p in self.broker.positions()}
        if symbol not in positions:
            return True
        quantity = float(positions[symbol]['qty'])
        if quantity <= 0 or not quantity.is_integer():
            self.alerts.append('Unexpected non-integer/short holding: ' + symbol)
            return False
        token = hashlib.sha256(owner['entry_cid'].encode()).hexdigest()[:10]
        cid = 'zoh-exit-' + token + '-' + str(len(previous) + 1)
        # Attach the exit lineage before network submission, including uncertain outcomes.
        self._exit_for = owner['entry_cid']
        try:
            sent = self.submit({'symbol': symbol, 'qty': str(int(quantity)), 'side': 'sell', 'type': 'market', 'time_in_force': 'day', 'position_intent': 'sell_to_close', 'client_order_id': cid}, owner['sleeve'], now)
        finally:
            self._exit_for = None
        if sent:
            self.record({'at': now.isoformat(), 'event': 'exit_reason', 'symbol': symbol, 'reason': reason})
        return sent

    def snapshot(self, account, positions, clock, signals=None):
        sleeves = {}
        for sleeve in ('swing', 'intraday'):
            cashflow = sum(v['qty'] * v['price'] * (1 if v['side'] == 'sell' else -1) for v in self.state['fills'].values() if v['sleeve'] == sleeve)
            value = sum(float(p['market_value']) for p in positions if self.state['owners'].get(p['symbol'], {}).get('sleeve') == sleeve)
            sleeves[sleeve] = {'budget': self.config[sleeve]['allocation'], 'tracked_equity': self.config[sleeve]['allocation'] + cashflow + value, 'market_value': value, 'params': self.state['strategies'].get(sleeve, {}).get('params'), 'stage': self.state['strategies'].get(sleeve, {}).get('stage', 'baseline_paper_experiment')}
        return {'at': clock['timestamp'], 'mode': 'PAPER', 'enabled': self.config['enabled'], 'paused': self.state['paused'], 'halted': self.state['halted'], 'account_status': account['status'], 'equity': float(account['equity']), 'cash': float(account['cash']), 'high_water': self.state['equity_peak'], 'drawdown': drawdown(account['equity'], self.state['equity_peak']), 'drawdown_limit': self.config['max_drawdown'], 'market_open': clock['is_open'], 'next_open': clock['next_open'], 'positions': positions, 'sleeves': sleeves, 'signals': signals or {}, 'actions': self.actions, 'alerts': self.alerts, 'last_research': self.state['last_research'], 'events': self.state['events'][-30:]}

    def cycle(self, signals, execute=True):
        account, clock = self.broker.account(), self.broker.clock()
        now = timestamp(clock['timestamp']).astimezone(NY)
        if self.state['account_id'] and self.state['account_id'] != account['id']:
            raise RuntimeError('Account differs from the bound paper account; execution blocked.')
        self.state['account_id'] = account['id']
        self.reconcile()
        positions, orders = self.broker.positions(), list(flatten(self.broker.orders()))
        self.audit_event('broker_snapshot', {'account': {key: account.get(key) for key in ('status', 'currency', 'cash', 'equity', 'buying_power', 'trading_blocked', 'account_blocked')}, 'clock': clock, 'positions': positions, 'orders': orders, 'phase': 'selection' if signals else 'risk_check'})
        active_symbols = {o['symbol'] for o in orders if o['status'] not in TERMINAL}
        for symbol in list(self.state['owners']):
            owner = self.state['owners'][symbol]
            entry = self.state['intents'][owner['entry_cid']]
            if symbol not in {p['symbol'] for p in positions} and symbol not in active_symbols and entry['status'] in TERMINAL:
                del self.state['owners'][symbol]
        unknown = [p['symbol'] for p in positions if p['symbol'] not in self.state['owners']]
        known = set(self.state.get('known_order_ids', [])) | {v.get('order_id') for v in self.state['intents'].values()}
        unknown_orders = [o['id'] for o in orders if o['id'] not in known]
        if unknown or unknown_orders:
            self.alerts.append('Unattributed account holdings/orders detected. New entries paused; manual reconciliation required.')
        if any(float(p['qty']) < 0 for p in positions):
            self.state['halted'] = 'Unexpected short position; manual reconciliation required.'
        equity = float(account['equity'])
        self.state['equity_peak'] = max(self.state['equity_peak'], equity)
        if drawdown(equity, self.state['equity_peak']) >= self.config['max_drawdown'] - 1e-9:
            self.state['halted'] = '10% account drawdown reached; cancel entries and flatten owned positions.'
        if account['status'] != 'ACTIVE' or account.get('trading_blocked') or account.get('account_blocked'):
            self.alerts.append('Account is not trade-ready.')
        self.state['last_cycle'] = now.isoformat()
        self.save()
        if not execute or not self.config['enabled'] or not clock['is_open']:
            self.audit_event('execution_gate', {'at': now.isoformat(), 'execute_requested': execute, 'enabled': self.config['enabled'], 'market_open': clock['is_open']})
            return self.snapshot(account, positions, clock, signals)
        close_time = timestamp(clock['next_close']).astimezone(NY)
        exit_intraday = now >= close_time - timedelta(minutes=20)
        rebalance_swing = (now.hour, now.minute) >= (9, 45)
        for symbol, owner in list(self.state['owners'].items()):
            entry = self.state['intents'][owner['entry_cid']]
            stale_entry = entry['status'] not in TERMINAL and now - timestamp(entry['created']) >= timedelta(minutes=self.config['entry_timeout_minutes'])
            stale_overnight = owner['sleeve'] == 'intraday' and owner['entry_day'] != now.date().isoformat()
            swing_exit = owner['sleeve'] == 'swing' and rebalance_swing and 'swing' in signals and symbol not in {r['symbol'] for r in signals['swing']}
            position_qty = next((float(p['qty']) for p in positions if p['symbol'] == symbol), 0.)
            protected_qty = sum(max(0., float(o['qty']) - float(o.get('filled_qty') or 0)) for o in orders if o['symbol'] == symbol and o['side'] == 'sell' and o.get('type') == 'stop' and o['status'] in ('new', 'accepted', 'pending_new', 'accepted_for_bidding'))
            unprotected = position_qty > 0 and (entry['status'] == 'partially_filled' or protected_qty < position_qty)
            reasons = [name for name, value in [('pending_exit', owner.get('exit_reason')), ('risk_halt', self.state['halted']), ('entry_timeout', stale_entry), ('unprotected_position', unprotected), ('intraday_close', owner['sleeve'] == 'intraday' and exit_intraday), ('overnight_recovery', stale_overnight), ('swing_rebalance', swing_exit)] if value]
            if reasons:
                self.exit_position(symbol, owner, now, ','.join(reasons))
        # Exits may be in flight. Re-read before allocating; no reliance on submitted sells as cash.
        positions, orders = self.broker.positions(), list(flatten(self.broker.orders()))
        if self.state['halted'] or self.state['paused'] or self.alerts or now >= close_time - timedelta(minutes=45):
            self.audit_event('entry_gate', {'at': now.isoformat(), 'halted': self.state['halted'], 'paused': self.state['paused'], 'alerts': list(self.alerts), 'near_close': now >= close_time - timedelta(minutes=45)})
            return self.snapshot(account, positions, clock, signals)
        if (now.hour, now.minute) < (9, 45):
            self.audit_event('entry_gate', {'at': now.isoformat(), 'reason': 'before_0945'})
            return self.snapshot(account, positions, clock, signals)
        held = {p['symbol'] for p in positions}
        active_symbols = {o['symbol'] for o in orders if o['status'] not in TERMINAL}
        reservation = sum(max(0., float(o['qty']) - float(o.get('filled_qty') or 0)) * float(o.get('limit_price') or 0) for o in orders if o['side'] == 'buy' and o['status'] not in TERMINAL)
        gross = sum(max(float(p['market_value']), float(p.get('cost_basis') or 0)) for p in positions)
        current_account = self.broker.account()
        available = min(float(current_account['cash']) - reservation, self.config['capital'] - gross - reservation) - self.config['capital'] * self.config['cash_reserve_fraction']
        for sleeve in ('swing', 'intraday'):
            if sleeve not in signals or (sleeve == 'intraday' and (now.hour, now.minute) >= (14, 30)):
                continue
            budget = self.config[sleeve]['allocation']
            slot_count = min(self.config[sleeve]['max_positions'], int(self.state['strategies'].get(sleeve, {}).get('params', {}).get('top_n', self.config[sleeve]['max_positions'])))
            exposure = sum(max(float(p['market_value']), float(p.get('cost_basis') or 0)) for p in positions if self.state['owners'].get(p['symbol'], {}).get('sleeve') == sleeve)
            pending = sum(max(0., float(o['qty']) - float(o.get('filled_qty') or 0)) * float(o.get('limit_price') or 0) for o in orders if o['side'] == 'buy' and o['status'] not in TERMINAL and self.state['owners'].get(o['symbol'], {}).get('sleeve') == sleeve)
            used = {symbol for symbol in held | active_symbols if self.state['owners'].get(symbol, {}).get('sleeve') == sleeve}
            for row in signals[sleeve]:
                symbol = row['symbol']
                if len(used) >= slot_count or available <= 0:
                    self.audit_event('entry_decision', {'sleeve': sleeve, 'symbol': symbol, 'decision': 'skipped', 'reason': 'position_or_cash_limit'})
                    continue
                if symbol in held | active_symbols or symbol not in self.config[sleeve + '_universe']:
                    self.audit_event('entry_decision', {'sleeve': sleeve, 'symbol': symbol, 'decision': 'skipped', 'reason': 'already_held_or_open_order' if symbol in held | active_symbols else 'outside_universe'})
                    continue
                cid = 'zoh-' + sleeve[0] + '-' + now.strftime('%Y%m%d') + '-' + symbol.replace('.', '-')
                if cid in self.state['intents']:
                    self.audit_event('entry_decision', {'sleeve': sleeve, 'symbol': symbol, 'decision': 'skipped', 'reason': 'already_attempted_today'})
                    continue
                try:
                    if sleeve == 'intraday':
                        signal_at = timestamp(row.get('signal_time', ''))
                        if not 0 <= (now - signal_at).total_seconds() < 15 * 60:
                            raise ValueError('Intraday signal is stale or from the future.')
                    asset = self.broker.asset(symbol)
                    if not asset.get('tradable') or asset.get('status') != 'active' or asset.get('class') != 'us_equity':
                        self.audit_event('entry_decision', {'sleeve': sleeve, 'symbol': symbol, 'decision': 'skipped', 'reason': 'asset_not_tradable', 'asset': asset})
                        continue
                    entry_clock = self.broker.clock()
                    entry_now = timestamp(entry_clock['timestamp']).astimezone(NY)
                    if not entry_clock['is_open'] or entry_now >= timestamp(entry_clock['next_close']) - timedelta(minutes=45):
                        break
                    if sleeve == 'intraday' and not 0 <= (entry_now - signal_at).total_seconds() < 15 * 60:
                        raise ValueError('Intraday signal expired while checking the account.')
                    raw_quote = self.broker.latest_quote(symbol)
                    self.audit_event('quote_observed', {'symbol': symbol, 'sleeve': sleeve, 'feed': 'iex', 'quote': raw_quote, 'decision_clock': entry_now.isoformat()})
                    bid, ask = validated_quote(raw_quote, entry_now, self.config)
                    quantity, limit = entry_size(ask, budget - exposure - pending, available, budget / slot_count)
                    if quantity < 1:
                        self.audit_event('entry_decision', {'sleeve': sleeve, 'symbol': symbol, 'decision': 'skipped', 'reason': 'insufficient_cash_for_one_share'})
                        continue
                    stop = round(min(bid * (1 - self.config[sleeve]['stop_pct']), limit - .02), 2)
                    self._entry_context = {'signal': row, 'quote': raw_quote, 'decision_clock': entry_now.isoformat(), 'budget': budget, 'sleeve_exposure': exposure, 'reserved': pending, 'account_available': available, 'quantity': quantity, 'limit': limit, 'stop': stop}
                    payload = {'symbol': symbol, 'qty': str(quantity), 'side': 'buy', 'position_intent': 'buy_to_open', 'type': 'limit', 'time_in_force': 'gtc', 'order_class': 'oto', 'limit_price': str(limit), 'stop_loss': {'stop_price': str(stop)}, 'extended_hours': False, 'client_order_id': cid}
                    if self.submit(payload, sleeve, now):
                        cost = quantity * limit
                        available -= cost
                        pending += cost
                        active_symbols.add(symbol)
                        used.add(symbol)
                except ValueError as exc:
                    self.record({'event': 'entry_skipped', 'at': now.isoformat(), 'sleeve': sleeve, 'symbol': symbol, 'reason': str(exc)})
        self.save()
        return self.snapshot(current_account, positions, clock, signals)
