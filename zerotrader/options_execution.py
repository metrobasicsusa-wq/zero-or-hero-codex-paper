"""User-authorized small indicative-data PAPER pilot inside the single engine.

This is an unvalidated experiment, not a promoted strategy. The first phase buys
one standard call on an existing, fresh bullish intraday stock signal. Option
quotes are indicative derivatives, not executable OPRA quotes. Full premium is
at risk. Profit/stop triggers are observations, never guaranteed exit prices.
"""
from copy import deepcopy
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
import hashlib

from .broker import BrokerError, OrderAmbiguous
from .data import NY, timestamp
from .options_pilot import (policy_config, reservation_summary, select_candidates,
                            validate_contract, validated_option_quote)
from .risk import validated_quote, drawdown


STAGE = 'user_authorized_indicative_paper_pilot_unvalidated'
LIMITATIONS = ['Free indicative option quotes are modified derivatives, not executable OPRA quotes.',
               'Broker PAPER fills are simulated executions, not live market liquidity evidence.',
               'Fees use a contingency reserve until independently reconciled.',
               'Exit triggers and DNE acknowledgements do not guarantee continuous availability.']


def initialize(engine):
    """Idempotent natural-cycle migration; Engine.save supplies cloud barriers."""
    policy = policy_config(engine.config)
    if not policy['enabled']:
        return
    strategies = engine.state['strategies']
    if 'options' not in strategies:
        strategies['options'] = {'params': {'signal': 'existing_intraday_bullish_call',
                                          'version': '2026-10-15.1'},
                                 'stage': STAGE, 'risk_ledger': {},
                                 'authorized_capital': 1000, 'feed': 'indicative'}
        engine.save()
        engine.audit_event('options_pilot_initialized', {'allocation': policy['allocation'],
                           'feed': 'indicative', 'stage': STAGE, 'limitations': LIMITATIONS})


def owner_metadata(context):
    contract = deepcopy(context['contract'])
    return {'asset_class': 'us_option', 'multiplier': 100, 'contract': contract}


def is_option(engine, symbol):
    return engine.state.get('owners', {}).get(symbol, {}).get('sleeve') == 'options'


def order_multiplier(engine, order):
    # Asset class alone is not ownership; unknown activity is blocked elsewhere.
    return 100 if order.get('asset_class') == 'us_option' or is_option(engine, order['symbol']) else 1


def inventory_problem(engine):
    message = 'Option inventory or order identity is unverified; new entries paused for manual reconciliation.'
    if message not in engine.alerts:
        engine.alerts.append(message)
    engine.state['paused'] = True
    engine.save()


def validate_order(intent, order):
    """Validate before changing lineage or accepting cumulative observations."""
    payload = intent['payload']
    contract = intent['contract']
    validate_contract(contract)
    if (intent.get('multiplier') != 100 or intent.get('asset_class') != 'us_option'
            or payload['symbol'] != contract['symbol'] or order.get('symbol') != payload['symbol']
            or order.get('client_order_id') != payload['client_order_id']
            or order.get('side') != payload['side'] or order.get('asset_class') != 'us_option'
            or Decimal(str(order.get('qty'))) != 1 or Decimal(str(payload['qty'])) != 1
            or order.get('legs') or not order.get('id')
            or (intent.get('order_id') and order['id'] != intent['order_id'])):
        raise ValueError('Option order identity mismatch.')
    filled = Decimal(str(order.get('filled_qty') or 0))
    if not filled.is_finite() or filled not in (0, 1):
        raise ValueError('Invalid option cumulative quantity.')
    if filled:
        price = Decimal(str(order.get('filled_avg_price')))
        if not price.is_finite() or price <= 0:
            raise ValueError('Invalid option cumulative price.')


def validate_owner(engine, symbol, owner):
    intent = engine.state['intents'][owner['entry_cid']]
    validate_contract(owner['contract'])
    if (owner.get('multiplier') != 100 or owner.get('asset_class') != 'us_option'
            or intent.get('sleeve') != 'options' or intent.get('asset_class') != 'us_option'
            or intent.get('multiplier') != 100 or intent.get('contract') != owner['contract']
            or intent['payload']['symbol'] != symbol or owner['contract']['symbol'] != symbol
            or intent['payload']['side'] != 'buy' or Decimal(str(intent['payload']['qty'])) != 1):
        raise ValueError('Option owner lineage mismatch.')
    return intent


def can_release_owner(engine, owner):
    """An absent eventual-consistency position is not proof that a fill closed."""
    entry = engine.state['intents'][owner['entry_cid']]
    ledger = engine.state['strategies'].get('options', {}).get('risk_ledger', {}).get('orders', {})
    bought = max(Decimal(str(engine.state['fills'].get(entry.get('order_id'), {}).get('qty', 0))),
                 Decimal(str(ledger.get(owner['entry_cid'], {}).get('filled_contracts_observed', 0))))
    if entry['status'] == 'filled':
        bought = max(bought, Decimal(str(entry['payload']['qty'])))
    sold = sum((Decimal(str(engine.state['fills'].get(i.get('order_id'), {}).get('qty', 0)))
                for i in engine.state['intents'].values()
                if i.get('sleeve') == 'options' and i.get('exit_for') == owner['entry_cid']
                and i['payload']['side'] == 'sell'), Decimal(0))
    return sold >= bought


def safe_synchronize_ledger(engine, positions):
    try:
        return synchronize_ledger(engine, positions)
    except (ValueError, KeyError, TypeError, InvalidOperation):
        inventory_problem(engine)
        return None


def synchronize_ledger(engine, positions):
    strategy = engine.state['strategies'].get('options')
    if strategy is None:
        return None
    result = reservation_summary(engine.state, annotated_positions(engine, positions), engine.config,
                                 previous=strategy.get('risk_ledger', {}))
    if strategy.get('risk_ledger') != result['risk_ledger']:
        strategy['risk_ledger'] = deepcopy(result['risk_ledger'])
        engine.save()
    return result


def annotated_positions(engine, positions):
    result = deepcopy(positions)
    for row in result:
        owner = engine.state['owners'].get(row['symbol'], {})
        if owner.get('sleeve') == 'options':
            validate_contract(owner['contract'])
            if owner.get('multiplier') != 100 or row.get('asset_class') != 'us_option':
                raise ValueError('Option ownership/asset metadata mismatch.')
            # Alpaca position objects omit contract size; use persisted verified metadata.
            if 'multiplier' not in row and 'size' not in row:
                row['multiplier'] = 100
    return result


ENTRY_CLOCK_VERSION = '2026-10-19.1'


def entry_clock(engine, signal, phase):
    """Read broker time AFTER inputs arrive; never use the cycle-start clock."""
    policy = policy_config(engine.config)
    clock = engine.broker.clock()
    if not engine.audit_event('options_entry_clock', {
            'version': ENTRY_CLOCK_VERSION, 'phase': phase,
            'underlying': signal.get('symbol'), 'signal_time': signal.get('signal_time'),
            'clock': clock}):
        raise ValueError('Options decision-clock archive failed; entry blocked.')
    now = timestamp(clock['timestamp']).astimezone(NY)
    if (not policy['enabled'] or not clock['is_open'] or (now.hour, now.minute) < (9, 45)
            or (now.hour, now.minute) >= (14, 30)
            or now >= timestamp(clock['next_close']) - timedelta(minutes=45)):
        raise ValueError('Options entry window is closed.')
    if not 0 <= (now - timestamp(signal['signal_time'])).total_seconds() < 900:
        raise ValueError('Options underlying signal expired or is from the future.')
    return clock, now


def entry_quote(engine, symbol, quote, phase, feed):
    if not engine.audit_event('options_quote_observed', {
            'version': ENTRY_CLOCK_VERSION, 'symbol': symbol, 'phase': phase,
            'quote': quote, 'feed': feed, 'executable_quotes': False}):
        raise ValueError('Options quote archive failed; entry blocked.')


def entry_error(engine, exc, phase, underlying, **extra):
    # Broker exception text may contain transport details. Keep status/type only;
    # validation exceptions are local and their explicit reason stays private.
    reason = ('Broker request failed.' if isinstance(exc, BrokerError) else str(exc)[:500])
    engine.audit_event('options_entry_skipped', {
        'version': ENTRY_CLOCK_VERSION, 'phase': phase, 'underlying': underlying,
        'reason_type': type(exc).__name__, 'reason': reason,
        'broker_status': getattr(exc, 'status', None), 'feed': 'indicative', **extra})


def clock_guard(engine, context):
    try:
        return _clock_guard(engine, context)
    except (ValueError, KeyError, TypeError, InvalidOperation, BrokerError) as exc:
        entry_error(engine, exc, 'pre_submit_context', context.get('signal', {}).get('symbol'),
                    new_order_sent=False)
        raise


def _clock_guard(engine, context):
    policy = policy_config(engine.config)
    clock, now = entry_clock(engine, context['signal'], 'pre_submit_context')
    validated_option_quote(context['quote'], now, policy)
    validated_quote(context['underlying_quote'], now, engine.config)
    validate_contract(context['contract'], now, policy)
    if not 0 <= (now - timestamp(context['signal']['signal_time'])).total_seconds() < 900:
        raise ValueError('Options underlying signal expired.')
    return clock, now


def fresh_guard(engine, payload, context, *, record=True):
    try:
        return _fresh_guard(engine, payload, context, record=record)
    except (ValueError, KeyError, TypeError, InvalidOperation, BrokerError) as exc:
        entry_error(engine, exc, 'pre_submit', context.get('signal', {}).get('symbol'),
                    symbol=payload.get('symbol'), new_order_sent=False)
        raise


def _fresh_guard(engine, payload, context, *, record=True):
    """Recheck the whole account after durable intent, including its own reservation."""
    from .engine import TERMINAL, flatten
    policy = policy_config(engine.config)
    clock, now = clock_guard(engine, context)
    account = engine.broker.account()
    if account.get('id') != engine.state['account_id']:
        raise ValueError('Bound paper account changed.')
    equity = float(account['equity'])
    peak = engine.state['equity_peak']
    engine.state['equity_peak'] = max(peak, equity)
    if drawdown(equity, engine.state['equity_peak']) >= engine.config['max_drawdown'] - 1e-9:
        engine.state['halted'] = engine.state['halted'] or '10% account drawdown reached; cancel entries and flatten owned positions.'
    if peak != engine.state['equity_peak'] or engine.state['halted']:
        engine.save()
    if (engine.state['paused'] or engine.state['halted'] or engine.alerts
            or account.get('status') != 'ACTIVE' or account.get('trading_blocked') or account.get('account_blocked')
            or int(account.get('options_trading_level') or 0) < 2):
        raise ValueError('Account is not option-entry ready.')
    positions = engine.broker.positions()
    orders = [o for o in flatten(engine.broker.orders()) if o['status'] not in TERMINAL]
    known = set(engine.state.get('known_order_ids', [])) | {i.get('order_id') for i in engine.state['intents'].values()}
    if (any(p['symbol'] not in engine.state['owners'] for p in positions)
            or any(o['id'] not in known for o in orders)):
        engine.alerts.append('Unattributed activity blocks options entry; manual reconciliation required.')
        raise ValueError('Unknown account activity.')
    if payload['symbol'] in {p['symbol'] for p in positions} | {o['symbol'] for o in orders}:
        raise ValueError('Option is already held or has an active order.')
    def number(value):
        result = Decimal(str(value))
        if not result.is_finite() or result < 0:
            raise ValueError('Invalid nonnegative account value.')
        return result
    for position in positions:
        number(position['qty'])
        number(position['market_value'])
        number(position['cost_basis'])
    reserve = Decimal(0)
    for order in orders:
        qty, filled = number(order['qty']), number(order.get('filled_qty') or 0)
        if filled > qty:
            raise ValueError('Invalid order quantity.')
        if order['side'] == 'buy':
            limit = number(order.get('limit_price') or 0)
            if limit <= 0:
                raise ValueError('Unbounded buy reservation.')
            reserve += (qty - filled) * limit * order_multiplier(engine, order)
    gross = sum((max(number(p['market_value']), number(p['cost_basis'])) for p in positions), Decimal(0))
    available = min(number(account['cash']) - reserve, number(engine.config['capital']) - gross - reserve)
    available -= number(engine.config['capital']) * number(engine.config['cash_reserve_fraction'])
    cost = number(payload['limit_price']) * 100
    summary = synchronize_ledger(engine, positions)
    if (payload['qty'] != '1' or payload['symbol'] != context['contract']['symbol']
            or context['contract']['type'] != 'call' or cost <= 0 or cost > number(policy['max_premium_per_contract'])
            or cost + number(policy['fee_contingency']) > available
            or cost + number(policy['fee_allowance_per_contract']) > number(account.get('options_buying_power', 0))
            or number(summary['lifetime_premium_reserved']) > number(policy['lifetime_premium_cap'])
            or number(summary['fee_reserved']) > number(policy['fee_contingency'])
            or number(summary['total_reserved']) > number(policy['allocation'])
            or summary['lifetime_contracts_reserved'] > policy['max_lifetime_contracts']
            or summary['active_contracts'] > policy['max_positions']
            or set(summary['entry_block_reasons']) & {'per_order_contract_or_premium_limit_exceeded',
                                                       'per_contract_premium_cap_exceeded'}):
        raise ValueError('Options entry exceeds reserved capital or contract allowance.')
    fresh_quote = engine.broker.option_latest_quote(payload['symbol'], feed='indicative')
    entry_quote(engine, payload['symbol'], fresh_quote, 'pre_submit_option', 'indicative')
    # Account/persistence work and the quote request may take several seconds.
    # Recheck original inputs AND the new quote against a new broker clock.
    clock, now = clock_guard(engine, context)
    checked = validated_option_quote(fresh_quote, now, policy)
    if number(checked['ask']) > number(payload['limit_price']):
        raise ValueError('Indicative quote moved above persisted entry limit.')
    if record:
        engine.audit_event('cloud_pre_submit_guard', {'client_order_id': payload['client_order_id'],
            'sleeve': 'options', 'clock': clock, 'positions': positions, 'orders': orders,
            'available': str(available), 'option_reservations': summary, 'quote': fresh_quote,
            'feed': 'indicative', 'executable_quotes': False})
    return now


def protect_and_manage(engine, positions, orders, now, close_time):
    """Protect/close only owned contracts, even if entries or the pilot are off."""
    by_symbol = {p['symbol']: p for p in positions}
    for symbol, owner in list(engine.state['owners'].items()):
        if owner['sleeve'] != 'options':
            continue
        try:
            entry = validate_owner(engine, symbol, owner)
        except (ValueError, KeyError, TypeError, InvalidOperation):
            inventory_problem(engine)
            continue
        position = by_symbol.get(symbol)
        reason = None
        if engine.state['halted']:
            reason = 'risk_halt'
        if owner.get('exit_reason'):
            reason = owner['exit_reason']
        if now >= close_time - timedelta(minutes=20):
            reason = 'option_session_close'
        if (entry['status'] not in {'filled', 'canceled', 'expired', 'rejected', 'replaced', 'not_submitted'}
                and now - timestamp(entry['created']) >= timedelta(minutes=engine.config['entry_timeout_minutes'])):
            reason = 'option_entry_timeout'
        if position:
            try:
                quantity = Decimal(str(position['qty']))
                valid_inventory = (position.get('asset_class') == 'us_option' and position.get('side') == 'long'
                    and quantity.is_finite() and quantity == 1 and owner.get('multiplier') == 100)
            except (KeyError, ValueError, TypeError, InvalidOperation):
                valid_inventory = False
            if not valid_inventory:
                engine.alerts.append('Unexpected option inventory; manual reconciliation required.')
                engine.state['paused'] = True
                engine.save()
                continue
            expiry = date.fromisoformat(owner['contract']['expiration_date'])
            # Alpaca accepts DNE only on expiry day. Normal positions must exit
            # each session, well before expiry. This is a residual-inventory fallback.
            if now.date() == expiry and not owner.get('dne_status'):
                owner['dne_status'] = 'submitting'
                owner['dne_requested_at'] = now.isoformat()
                engine.save()
                engine.audit_event('option_dne_requested', {'symbol': symbol, 'entry_cid': owner['entry_cid']})
                try:
                    engine.broker.do_not_exercise(symbol)
                    owner['dne_status'] = 'acknowledged'
                except OrderAmbiguous:
                    owner['dne_status'] = 'uncertain'
                except BrokerError:
                    owner['dne_status'] = 'rejected_or_unverified'
                engine.save()
                engine.audit_event('option_dne_observed', {'symbol': symbol, 'status': owner['dne_status']})
            if now.date() == expiry and owner.get('dne_status') != 'acknowledged':
                engine.alerts.append('Option exercise protection unverified; closing owned contract without repeating DNE.')
                reason = 'option_dne_unverified'
            if now.date() > expiry:
                inventory_problem(engine)
            if now.date() >= expiry - timedelta(days=2):
                reason = 'option_expiry_exit'
            if owner['entry_day'] != now.date().isoformat():
                reason = 'option_overnight_recovery'
            if not reason:
                try:
                    quote = engine.broker.option_latest_quote(symbol, feed='indicative')
                    checked = validated_option_quote(quote, now, policy_config(engine.config))
                    engine.audit_event('option_quote_observed', {'symbol': symbol, 'feed': 'indicative',
                                       'executable': False, 'quote': quote, 'purpose': 'exit_trigger'})
                    entry_price = Decimal(str(position['avg_entry_price']))
                    bid = Decimal(str(checked['bid']))
                    if entry_price > 0 and bid <= entry_price * Decimal('0.70'):
                        reason = 'option_indicative_loss_trigger'
                    elif entry_price > 0 and bid >= entry_price * Decimal('1.50'):
                        reason = 'option_indicative_profit_trigger'
                except (BrokerError, ValueError, KeyError, TypeError, InvalidOperation):
                    engine.audit_event('option_exit_quote_unavailable', {'symbol': symbol,
                                       'full_premium_at_risk': True, 'scheduled_exit_still_required': True})
        if reason:
            engine.exit_position(symbol, owner, now, reason)
    safe_synchronize_ledger(engine, engine.broker.positions())


def enter(engine, signals, now):
    policy = policy_config(engine.config)
    if (not policy['enabled'] or not signals.get('intraday') or engine.alerts
            or engine.state['paused'] or engine.state['halted'] or not engine.audit_available
            or (now.hour, now.minute) >= (14, 30)):
        return
    if int(engine.broker.account().get('options_trading_level') or 0) < 2:
        engine.audit_event('options_entry_unavailable', {'reason': 'options_permission_not_confirmed'})
        return
    positions = engine.broker.positions()
    allowance = synchronize_ledger(engine, positions)
    if not allowance or not allowance['can_enter']:
        return
    attempted_today = {v.get('contract', {}).get('underlying_symbol') for v in engine.state['intents'].values()
                       if v.get('sleeve') == 'options' and v['payload']['side'] == 'buy'
                       and timestamp(v['created']).astimezone(NY).date() == now.date()}
    for signal in signals['intraday'][:3]:
        allowance = synchronize_ledger(engine, engine.broker.positions())
        if not allowance['can_enter']:
            break
        underlying = signal['symbol']
        if underlying in attempted_today or underlying not in engine.config['intraday_universe']:
            continue
        phase = 'underlying_quote_fetch'
        try:
            underlying_quote = engine.broker.latest_quote(underlying)
            entry_quote(engine, underlying, underlying_quote, 'underlying', 'iex')
            phase = 'underlying_quote_validation'
            _, quote_now = entry_clock(engine, signal, phase)
            bid, ask = validated_quote(underlying_quote, quote_now, engine.config)
            phase = 'contract_fetch'
            contracts = engine.broker.option_contracts(underlying,
                (quote_now.date() + timedelta(days=policy['min_dte'])).isoformat(),
                (quote_now.date() + timedelta(days=policy['max_dte'])).isoformat())
            eligible = []
            for contract in contracts:
                try:
                    validate_contract(contract, quote_now, policy)
                    strike = float(contract['strike_price'])
                    if contract['type'] == 'call' and abs(strike / ((bid + ask) / 2) - 1) <= policy['max_moneyness_fraction']:
                        eligible.append(contract)
                except (ValueError, KeyError, TypeError):
                    continue
            eligible.sort(key=lambda c: (c['expiration_date'], abs(float(c['strike_price']) - (bid + ask) / 2)))
            eligible = eligible[:4]
            phase = 'option_quote_fetch'
            quotes = {}
            for contract in eligible:
                symbol = contract['symbol']
                quotes[symbol] = engine.broker.option_latest_quote(symbol, feed='indicative')
                entry_quote(engine, symbol, quotes[symbol], 'candidate_option', 'indicative')
            phase = 'candidate_validation'
            _, candidate_now = entry_clock(engine, signal, phase)
            if not engine.audit_event('options_candidate_inputs', {'underlying': underlying, 'signal': signal,
                               'contracts': contracts, 'queried_shortlist': eligible,
                               'filter': 'standard active call, DTE7-21, strike within1pct; expiry then moneyness; first4',
                               'unqueried_contracts_not_evaluated': True,
                               'quotes': quotes, 'underlying_quote': underlying_quote,
                               'feed': 'indicative', 'executable_quotes': False, 'stage': STAGE,
                               'decision_clock': candidate_now.isoformat(), 'version': ENTRY_CLOCK_VERSION}):
                return
            diagnostics = []
            candidates = select_candidates([signal], eligible, quotes, {underlying: underlying_quote},
                                           candidate_now, engine.config, diagnostics=diagnostics)
            if not engine.audit_event('options_candidate_decision', {
                    'version': ENTRY_CLOCK_VERSION, 'underlying': underlying,
                    'decision_clock': candidate_now.isoformat(), 'selected_count': len(candidates),
                    'queried_count': len(eligible), 'diagnostics': diagnostics}):
                return
            for candidate in candidates[:1]:
                phase = 'intent_preparation'
                _, decision_now = entry_clock(engine, signal, phase)
                symbol = candidate['symbol']
                contract = next(c for c in eligible if c['symbol'] == symbol)
                cid = 'zoh-opt-' + now.strftime('%Y%m%d') + '-' + hashlib.sha256(symbol.encode()).hexdigest()[:12]
                if cid in engine.state['intents']:
                    continue
                engine._entry_context = {'signal': signal, 'quote': quotes[symbol],
                    'underlying_quote': underlying_quote, 'contract': contract, 'feed': 'indicative',
                    'limitations': LIMITATIONS, 'decision_clock': decision_now.isoformat(),
                    'quantity': 1, 'limit': candidate['limit_price'], 'multiplier': 100,
                    'premium': candidate['premium'], 'fee_allowance': candidate['fee_allowance']}
                payload = {'symbol': symbol, 'qty': '1', 'side': 'buy', 'position_intent': 'buy_to_open',
                    'type': 'limit', 'time_in_force': 'day', 'limit_price': candidate['limit_price'],
                    'client_order_id': cid}
                phase = 'submit'
                engine.submit(payload, 'options', decision_now)
                phase = 'post_submit_reconciliation'
                synchronize_ledger(engine, engine.broker.positions())
                attempted_today.add(underlying)
        except (ValueError, KeyError, TypeError, InvalidOperation, BrokerError) as exc:
            # Errors remain evidence. No market order fallback or subscription switch.
            entry_error(engine, exc, phase, underlying,
                        new_order_sent=None if phase in ('submit', 'post_submit_reconciliation') else False)

