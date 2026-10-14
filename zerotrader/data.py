from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from .storage import read_json, write_json

NY = ZoneInfo('America/New_York')


def timestamp(value):
    return datetime.fromisoformat(value.replace('Z', '+00:00'))


def refresh(broker, config, directory, now, full=False):
    daily_path, intra_path = directory / 'daily.json', directory / 'intraday.json'
    meta = read_json(directory / 'data_meta.json', {})
    day = now.astimezone(NY).date()
    daily = read_json(daily_path, {})
    if full or not daily or meta.get('daily_cutoff') != day.isoformat():
        symbols = list(dict.fromkeys(config['swing_universe'] + ['SPY']))
        daily = broker.daily_bars(symbols, (day - timedelta(days=1100)).isoformat(), day.isoformat())
        daily = {s: [b for b in rows if timestamp(b['t']).astimezone(NY).date() < day] for s, rows in daily.items()}
        write_json(daily_path, daily)
        meta['daily_cutoff'] = day.isoformat()
    intraday = read_json(intra_path, {})
    start = day - timedelta(days=200 if full or not intraday else 3)
    update = broker.intraday_bars(config['intraday_universe'], start.isoformat(), now.isoformat())
    for symbol in config['intraday_universe']:
        merged = {b['t']: b for b in intraday.get(symbol, [])}
        merged.update({b['t']: b for b in update.get(symbol, [])})
        intraday[symbol] = [b for _, b in sorted(merged.items()) if timestamp(b['t']).astimezone(NY).date() >= day - timedelta(days=220) and timestamp(b['t']) + timedelta(minutes=15) <= now]
    write_json(intra_path, intraday)
    meta['updated_at'] = now.isoformat()
    write_json(directory / 'data_meta.json', meta)
    return daily, intraday
