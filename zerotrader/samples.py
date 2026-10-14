"""Point-in-time research rows for the entire configured stock universe.

These rows describe selection, not order acceptance or execution. ``signals`` is
the authoritative record of selection; omitting a sleeve means that the runner
did not evaluate/use that sleeve. Features never contain future-return labels.
Daily candles dated today are excluded even after the close. Intraday candles
must have completed by ``asof``. Missing/invalid observations stay missing.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
import math
from statistics import mean, stdev

from . import intraday, swing


def _finite(value):
    """Keep archived rows strict-JSON-compatible, including extreme inputs."""
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {key: _finite(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_finite(item) for item in value]
    return value


def _completed_daily(bars, now):
    result = {}
    for symbol, rows in (bars or {}).items():
        completed = []
        for raw in rows:
            try:
                stamp = str(raw['t'])
                day = (date.fromisoformat(stamp) if len(stamp) == 10
                       else intraday._dt(stamp).date())
                if day < now.date():
                    completed.append(dict(raw, t=day.isoformat()))
            except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
                continue
        result[str(symbol).upper()] = completed
    return result


def _completed_intraday(bars, now):
    # _prepare is the strategy's canonical validator. Normalize first so a bad
    # Python numeric object (for example an overflowing integer) cannot abort
    # archiving all other observations.
    cleaned = {}
    for symbol, rows in (bars or {}).items():
        cleaned_rows = []
        for raw in rows:
            try:
                stamp = intraday._dt(raw['t'])
                if stamp + intraday.BAR > now:
                    continue
                values = {key: float(raw[key]) for key in ('o', 'h', 'l', 'c', 'v')}
                if all(math.isfinite(value) for value in values.values()):
                    cleaned_rows.append(dict(values, t=stamp.isoformat()))
            except (KeyError, TypeError, ValueError, AttributeError, OverflowError):
                continue
        cleaned[str(symbol).upper()] = cleaned_rows
    return intraday._prepare(cleaned, now)


def _row(sleeve, symbol, now, params):
    return {'sleeve': sleeve, 'symbol': symbol, 'decision': 'not_selected',
            'reason_codes': [], 'rank': None, 'score': None, 'features': {},
            'asof': now.isoformat(), 'bar_time': None, 'params': dict(params)}


def _swing_rows(daily, symbols, params, now):
    market = swing._Market(_completed_daily(daily, now))
    spy = market.spy
    benchmark_day = market.days[-1] if market.days else None
    spy_price = spy.closes[-1] if spy and spy.closes else None
    spy_sma = spy.average(len(spy.days) - 1, 120) if spy and len(spy.days) >= 120 else None
    market_ok = spy_price > spy_sma if spy_sma is not None else None
    rows, eligible = [], []
    for symbol in symbols:
        row = _row('swing', symbol, now, params)
        series = market.series.get(symbol)
        index = series.index.get(benchmark_day) if series else None
        # With no benchmark row, retain the last observable stock features, but
        # do not claim they constitute a usable strategy signal.
        feature_index = index if index is not None else (len(series.days) - 1 if series and series.days else None)
        enough = feature_index is not None and feature_index >= max(params['momentum'], params['trend'] - 1, 20)
        price = series.closes[feature_index] if feature_index is not None else None
        momentum = (price / series.closes[feature_index - params['momentum']] - 1
                    if feature_index is not None and feature_index >= params['momentum'] else None)
        average = (series.average(feature_index, params['trend'])
                   if feature_index is not None and feature_index >= params['trend'] - 1 else None)
        volatility = (stdev(series.returns[feature_index - 19:feature_index + 1]) * math.sqrt(252)
                      if feature_index is not None and feature_index >= 20 else None)
        aligned = (series.days[index - 20:index + 1] == market.days[-21:]
                   if index is not None and index >= 20 and len(market.days) >= 21 else None)
        row['bar_time'] = series.days[feature_index] if feature_index is not None else None
        row['features'] = {
            'price': price, 'momentum': momentum, 'trend_sma': average,
            'volatility_20_annualized': volatility,
            'history_bars': feature_index + 1 if feature_index is not None else 0,
            'required_history_bars': max(params['momentum'], params['trend'] - 1, 20) + 1,
            'sessions_aligned_21': aligned, 'benchmark_bar_time': benchmark_day,
            'spy_close': spy_price, 'spy_sma_120': spy_sma,
            'spy_above_sma_120': market_ok,
        }
        unavailable, reasons = [], []
        if not benchmark_day:
            unavailable.append('benchmark_data_missing')
        elif spy_sma is None:
            unavailable.append('benchmark_history_insufficient')
        if feature_index is None:
            unavailable.append('daily_data_missing')
        elif index is None and benchmark_day:
            unavailable.append('benchmark_session_missing')
        if feature_index is not None and not enough:
            unavailable.append('daily_history_insufficient')
        if aligned is False:
            unavailable.append('daily_session_gap')
        if market_ok is False:
            reasons.append('benchmark_below_or_equal_sma')
        if price is not None and price < 10:
            reasons.append('price_below_minimum')
        if average is not None and price <= average:
            reasons.append('price_below_or_equal_trend_sma')
        if momentum is not None and momentum <= 0:
            reasons.append('momentum_not_positive')
        if momentum is not None and volatility is not None:
            row['score'] = round(momentum / max(volatility, 0.05), 8)
        if unavailable:
            row.update(decision='data_unavailable', reason_codes=unavailable + reasons)
        else:
            row['reason_codes'] = reasons
            if not reasons and symbol != 'SPY':
                eligible.append(row)
        rows.append(row)
    _rank_eligible(eligible)
    return rows


def _intraday_rows(bars, symbols, params, now):
    prepared = _completed_intraday(bars, now)
    window_open = time(10) <= now.time() <= time(14, 30)
    rows, eligible = [], []
    for symbol in symbols:
        row = _row('intraday', symbol, now, params)
        history = prepared.get(symbol, [])
        today = [bar for bar in history if bar['t'].date() == now.date()]
        previous = [bar for bar in history if bar['t'].date() < now.date()]
        features = {'entry_window_open': window_open, 'completed_session_bars': len(today),
                    'price': None, 'opening_range_high': None, 'opening_range_low': None,
                    'opening_range_complete': False, 'session_vwap': None,
                    'last_volume': None, 'same_slot_prior_observations': 0,
                    'same_slot_prior_mean_volume': None, 'volume_baseline_source': None,
                    'volume_baseline_observations': 0, 'volume_baseline_mean': None,
                    'volume_ratio': None, 'signal_time': None, 'signal_age_seconds': None,
                    'signal_fresh': False}
        unavailable, reasons = [], []
        if not window_open:
            reasons.append('outside_entry_window')
        if not today:
            unavailable.append('current_session_data_missing')
        else:
            last = today[-1]
            row['bar_time'] = last['t'].isoformat()
            signal_time = last['t'] + intraday.BAR
            age = (now - signal_time).total_seconds()
            opening = last['t'].replace(hour=9, minute=30, second=0, microsecond=0)
            range_end = opening + timedelta(minutes=params['opening_range_minutes'])
            range_bars = [bar for bar in today if opening <= bar['t'] < range_end]
            expected = {opening + i * intraday.BAR for i in range(params['opening_range_minutes'] // 15)}
            range_complete = {bar['t'] for bar in range_bars} == expected
            range_high = max((bar['h'] for bar in range_bars), default=None)
            total_volume = sum(bar['v'] for bar in today)
            vwap = (sum(((bar['h'] + bar['l'] + bar['c']) / 3) * bar['v'] for bar in today) / total_volume
                    if total_volume > 0 else None)
            same_slot = [bar['v'] for bar in previous if bar['t'].time() == last['t'].time()][-20:]
            prior_slots = len(same_slot) >= 5
            baseline = same_slot if prior_slots else [bar['v'] for bar in today[:-1]]
            average_volume = mean(baseline) if baseline else 0
            ratio = last['v'] / average_volume if average_volume > 0 else 0
            features.update({
                'price': last['c'], 'opening_range_high': range_high,
                'opening_range_low': min((bar['l'] for bar in range_bars), default=None),
                'opening_range_complete': range_complete, 'session_vwap': vwap,
                'last_volume': last['v'], 'same_slot_prior_observations': len(same_slot),
                'same_slot_prior_mean_volume': mean(same_slot) if same_slot else None,
                'volume_baseline_source': 'prior_same_slot' if prior_slots else 'current_session_prior_bars',
                'volume_baseline_observations': len(baseline), 'volume_baseline_mean': average_volume,
                'volume_ratio': ratio, 'signal_time': signal_time.isoformat(),
                'signal_age_seconds': age, 'signal_fresh': 0 <= age < intraday.BAR.total_seconds(),
            })
            if age >= intraday.BAR.total_seconds():
                unavailable.append('intraday_signal_stale')
            if not time(10) <= signal_time.time() <= time(14, 30):
                reasons.append('bar_signal_outside_entry_window')
            if last['t'] < range_end:
                reasons.append('opening_range_not_ready')
            elif not range_complete:
                unavailable.append('opening_range_incomplete')
            if total_volume <= 0:
                reasons.append('zero_session_volume')
            if range_high is not None and last['c'] <= range_high:
                reasons.append('close_below_or_equal_opening_range')
            if vwap is not None and last['c'] <= vwap:
                reasons.append('close_below_or_equal_vwap')
            if ratio < params['volume_ratio']:
                reasons.append('relative_volume_below_threshold')
            candidate = intraday._candidate(symbol, today, previous, params)
            if candidate:
                row['score'] = candidate['score']
                if window_open and not unavailable and not reasons:
                    eligible.append(row)
        row['features'] = features
        # The clock alone explains a non-selection outside trading hours. Do
        # not misclassify every overnight observation as a data outage.
        if not window_open:
            row['reason_codes'] = reasons
        elif unavailable:
            row.update(decision='data_unavailable', reason_codes=unavailable + reasons)
        else:
            row['reason_codes'] = reasons
        rows.append(row)
    _rank_eligible(eligible)
    return rows


def _rank_eligible(rows):
    for rank, row in enumerate(sorted(rows, key=lambda item: (-item['score'], item['symbol'])), 1):
        row['rank'] = rank


def build_samples(daily, intraday_bars, config, strategy_state, signals, asof):
    """Return one row per configured symbol per sleeve, including non-winners.

    ``strategy_state`` may be the full runtime state or its ``strategies`` map.
    ``selected`` means present in the supplied strategy signal list; holdings,
    risk checks, and actual broker fills belong in separate linked records.
    """
    now = intraday._dt(asof)
    strategies = strategy_state.get('strategies', strategy_state)
    rows = []
    for sleeve, module, builder, bars in (
        ('swing', swing, _swing_rows, daily),
        ('intraday', intraday, _intraday_rows, intraday_bars),
    ):
        active = strategies.get(sleeve, {})
        params = module._params(active.get('params', active) or None)
        symbols = list(dict.fromkeys(str(symbol).upper() for symbol in config[sleeve + '_universe']))
        sleeve_rows = builder(bars, symbols, params, now)
        evaluated = isinstance(signals, dict) and sleeve in signals
        selected = {str(item['symbol']).upper(): index for index, item in enumerate(
            signals.get(sleeve, []) if evaluated else [], 1)}
        for row in sleeve_rows:
            if not evaluated:
                row.update(decision='not_evaluated', reason_codes=['sleeve_not_evaluated'], rank=None)
            elif row['symbol'] in selected:
                row.update(decision='selected', reason_codes=['selected_by_strategy'], rank=selected[row['symbol']])
            elif not row['reason_codes']:
                row['reason_codes'] = ['below_top_n' if row['rank'] is not None and row['rank'] > params['top_n']
                                       else 'did_not_meet_signal']
        rows.extend(sleeve_rows)
    return _finite(rows)
