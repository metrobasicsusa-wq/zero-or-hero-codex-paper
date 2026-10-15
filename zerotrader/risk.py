from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import math


def validate_config(config):
    if config.get('mode') != 'paper':
        raise ValueError('Only paper mode exists.')
    if not 0 < float(config['capital']) <= 100000:
        raise ValueError('Capital must be within the authorized $100,000.')
    if not 0 < float(config['max_drawdown']) <= .10:
        raise ValueError('Drawdown threshold must not exceed authorized 10%.')
    total = sum(float(config[s]['allocation']) for s in ('swing', 'intraday'))
    if total > config['capital'] or any(config[s]['allocation'] <= 0 for s in ('swing', 'intraday')):
        raise ValueError('Sleeve allocations must fit the total capital.')
    for sleeve, cap, stop in [('swing', 5, .05), ('intraday', 3, .015)]:
        item = config[sleeve]
        if not 1 <= item['max_positions'] <= cap or item['stop_pct'] != stop:
            raise ValueError('Position caps and stop rules cannot be evolved.')
    if not .01 <= config['cash_reserve_fraction'] <= .5:
        raise ValueError('A cash reserve is required.')
    if not 1 <= config['max_quote_age_seconds'] <= 120:
        raise ValueError('Quote freshness must be within 120 seconds.')
    if not 0 < config['max_spread_fraction'] <= .005:
        raise ValueError('Spread threshold may not exceed 0.5%.')
    if 'options' in config:
        from .options_pilot import policy_config
        policy_config(config)


def drawdown(equity, high_water):
    equity, high_water = float(equity), float(high_water)
    if not math.isfinite(equity) or not math.isfinite(high_water) or high_water <= 0:
        raise ValueError('Invalid account equity.')
    return max(0., 1 - equity / high_water)


def validated_quote(quote, now, config):
    bid, ask = float(quote.get('bp', 0)), float(quote.get('ap', 0))
    if not all(math.isfinite(x) for x in (bid, ask)) or bid <= 0 or ask < bid:
        raise ValueError('Missing or crossed bid/ask; skip entry.')
    timestamp = datetime.fromisoformat(quote['t'].replace('Z', '+00:00'))
    age = (now.astimezone(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds()
    if age < -5 or age > config['max_quote_age_seconds']:
        raise ValueError('Quote is stale or from the future; skip entry.')
    if (ask - bid) / ((ask + bid) / 2) > config['max_spread_fraction']:
        raise ValueError('Spread exceeds the configured entry threshold.')
    if ask < 10:
        raise ValueError('Stocks below $10 are outside this system universe.')
    return bid, ask


def entry_size(ask, sleeve_available, account_available, per_position):
    limit = float((Decimal(str(ask)) * Decimal('1.0005')).quantize(Decimal('.01'), rounding=ROUND_DOWN))
    funds = max(0., min(sleeve_available, account_available, per_position))
    quantity = math.floor(funds / limit)
    return quantity, limit
